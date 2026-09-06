//! Bounded shard operation service. One configured logical partition per process.
//! Ownership directory and the remaining Temporal stores are separate delivery gates.
use prost::Message;
use sha2::{Digest, Sha256};
use slatedb::{Db, IsolationLevel};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use tonic::{Request, Response, Status};
pub mod wire {
    tonic::include_proto!("xenon.v1");
}
use wire::{shard_command::Kind, shard_result::Error as LogicalError, *};

// Ingress compatibility only: preserve legacy journal keys and accept the Go
// canonical 128-bit base62 operation IDs without changing stored semantics.
fn valid_operation_reference(id: &str) -> bool {
    if !id.is_empty()
        && id.len() <= 128
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return true;
    }
    let Some(suffix) = id.strip_prefix("op_") else {
        return false;
    };
    if suffix.len() != 22 {
        return false;
    }
    suffix
        .bytes()
        .try_fold(0u128, |value, b| {
            let digit = match b {
                b'0'..=b'9' => b - b'0',
                b'A'..=b'Z' => b - b'A' + 10,
                b'a'..=b'z' => b - b'a' + 36,
                _ => return None,
            };
            value.checked_mul(62)?.checked_add(u128::from(digit))
        })
        .is_some()
}

struct Partition {
    db: Db,
    quarantined: bool,
}
#[derive(Clone)]
pub struct ShardService {
    partition: Arc<Mutex<Partition>>,
    name: String,
    max_outcomes: u64,
    operation_timeout: Duration,
    admission: Arc<Semaphore>,
}
impl ShardService {
    pub fn new(db: Db, name: String, max_outcomes: u64) -> Self {
        Self {
            partition: Arc::new(Mutex::new(Partition {
                db,
                quarantined: false,
            })),
            name,
            max_outcomes,
            operation_timeout: Duration::from_secs(20),
            admission: Arc::new(Semaphore::new(64)),
        }
    }
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }
    async fn execute_owned(self, req: ShardRequest) -> Result<ShardResult, Status> {
        if req.protocol_version != 1
            || req.partition != self.name
            || !valid_operation_reference(&req.operation_id)
        {
            return Err(Status::invalid_argument(
                "invalid protocol, partition or operation identity",
            ));
        }
        let command = req
            .command
            .ok_or_else(|| Status::invalid_argument("missing command"))?;
        let kind = Kind::try_from(command.kind)
            .map_err(|_| Status::invalid_argument("unknown shard operation"))?;
        if kind == Kind::Unspecified || command.shard_id < 0 || command.data.len() > 1024 * 1024 {
            return Err(Status::invalid_argument(
                "invalid shard operation or payload size",
            ));
        }
        let digest = Sha256::digest(command.encode_to_vec()).to_vec();
        if digest != req.command_sha256 {
            return Err(Status::invalid_argument("command digest mismatch"));
        }
        let mut partition = tokio::time::timeout(Duration::from_secs(5), self.partition.lock())
            .await
            .map_err(|_| Status::resource_exhausted("partition admission timeout"))?;
        if partition.quarantined {
            return Err(Status::unavailable("partition requires recovery"));
        }
        let result = match tokio::time::timeout(
            self.operation_timeout,
            Self::apply(
                &partition.db,
                &req.operation_id,
                &digest,
                command,
                kind,
                self.max_outcomes,
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(Status::unavailable(
                "storage deadline expired; outcome unknown, partition quarantined",
            )),
        };
        // A failed backend operation has an unknown durability outcome. Never release
        // subsequent readers onto this handle; a new process must recover from storage.
        if result
            .as_ref()
            .err()
            .is_some_and(|e| e.code() == tonic::Code::Unavailable)
        {
            partition.quarantined = true;
        }
        result
    }
    async fn apply(
        db: &Db,
        id: &str,
        digest: &[u8],
        command: ShardCommand,
        kind: Kind,
        limit: u64,
    ) -> Result<ShardResult, Status> {
        let tx = db
            .begin(IsolationLevel::SerializableSnapshot)
            .await
            .map_err(backend)?;
        let outcome_key = format!("v1/outcome/{id}");
        if let Some(bytes) = tx.get(outcome_key.as_bytes()).await.map_err(backend)? {
            let outcome = StoredOutcome::decode(bytes).map_err(backend)?;
            if outcome.command_sha256 != digest {
                return Err(Status::invalid_argument(
                    "operation ID reused with different command",
                ));
            }
            let result = match outcome.result {
                Some(stored_outcome::Result::ShardResult(result)) => result,
                _ => {
                    return Err(Status::invalid_argument(
                        "operation ID belongs to another family",
                    ))
                }
            };
            // Nonempty durable write fences replay responses too. Toggle an existing
            // marker rather than retaining one new marker per read.
            let old = tx.get(b"v1/barrier").await.map_err(backend)?;
            let marker = if old.as_deref() == Some(&[1][..]) {
                [0]
            } else {
                [1]
            };
            tx.put(b"v1/barrier", marker).map_err(backend)?;
            tx.commit()
                .await
                .map_err(backend)?
                .ok_or_else(|| Status::unavailable("missing durability handle"))?
                .await_durable()
                .await
                .map_err(backend)?;
            return Ok(result);
        }
        let count = match tx.get(b"v1/outcome_count").await.map_err(backend)? {
            None => 0,
            Some(bytes) => u64::from_be_bytes(
                bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| Status::unavailable("corrupt outcome count"))?,
            ),
        };
        if count >= limit {
            return Err(Status::resource_exhausted(
                "durable outcome capacity reached; no unsafe expiry",
            ));
        }
        let shard_key = format!("v1/shard/{:010}", command.shard_id);
        let existing = tx
            .get(shard_key.as_bytes())
            .await
            .map_err(backend)?
            .map(StoredShard::decode)
            .transpose()
            .map_err(backend)?;
        let mut result = ShardResult {
            shard_id: command.shard_id,
            ..Default::default()
        };
        match (kind, existing) {
            (Kind::Get, None) => {
                result.error = LogicalError::NotFound as i32;
                result.message = format!("shard {} not found", command.shard_id);
            }
            (Kind::CreateOrGet, None) => {
                let shard = StoredShard {
                    range_id: command.range_id,
                    data: command.data,
                    encoding: command.encoding,
                };
                tx.put(shard_key.as_bytes(), shard.encode_to_vec())
                    .map_err(backend)?;
                copy_shard(&mut result, shard);
            }
            (Kind::Get | Kind::CreateOrGet, Some(shard)) => copy_shard(&mut result, shard),
            (Kind::Update, None) => {
                result.error = LogicalError::Unavailable as i32;
                result.message = format!(
                    "Failed to lock shard {}: shard does not exist",
                    command.shard_id
                );
            }
            (Kind::Update | Kind::Assert, current) => {
                let expected = if kind == Kind::Update {
                    command.previous_range_id
                } else {
                    command.range_id
                };
                if current.as_ref().is_none_or(|s| s.range_id != expected) {
                    result.error = LogicalError::OwnershipLost as i32;
                    result.message = format!(
                        "shard {} range mismatch: expected {}, observed {:?}",
                        command.shard_id,
                        expected,
                        current.map(|s| s.range_id)
                    );
                } else if kind == Kind::Update {
                    let shard = StoredShard {
                        range_id: command.range_id,
                        data: command.data,
                        encoding: command.encoding,
                    };
                    tx.put(shard_key.as_bytes(), shard.encode_to_vec())
                        .map_err(backend)?;
                    copy_shard(&mut result, shard);
                }
            }
            _ => return Err(Status::invalid_argument("invalid operation")),
        }
        let outcome = StoredOutcome {
            command_sha256: digest.to_vec(),
            result: Some(stored_outcome::Result::ShardResult(result.clone())),
        };
        tx.put(outcome_key.as_bytes(), outcome.encode_to_vec())
            .map_err(backend)?;
        tx.put(b"v1/outcome_count", (count + 1).to_be_bytes())
            .map_err(backend)?;
        tx.commit()
            .await
            .map_err(backend)?
            .ok_or_else(|| Status::unavailable("missing durability handle"))?
            .await_durable()
            .await
            .map_err(backend)?;
        Ok(result)
    }
}
fn copy_shard(out: &mut ShardResult, shard: StoredShard) {
    out.range_id = shard.range_id;
    out.data = shard.data;
    out.encoding = shard.encoding;
}
fn backend(error: impl std::fmt::Display) -> Status {
    Status::unavailable(format!("storage outcome unknown: {error}"))
}
#[tonic::async_trait]
impl wire::shard_persistence_server::ShardPersistence for ShardService {
    async fn execute(
        &self,
        request: Request<ShardRequest>,
    ) -> Result<Response<ShardResult>, Status> {
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("partition admission full"))?;
        let service = self.clone();
        // Cancellation of the transport drops only the join handle. The operation
        // retains its gate until durability settles; reconnect using the same ID.
        match tokio::spawn(async move {
            let _permit = permit;
            service.execute_owned(request.into_inner()).await
        })
        .await
        {
            Ok(result) => result.map(Response::new),
            Err(_) => {
                self.partition.lock().await.quarantined = true;
                Err(Status::unavailable(
                    "partition task failed; recovery required",
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;
    fn request(id: &str, command: ShardCommand) -> ShardRequest {
        ShardRequest {
            protocol_version: 1,
            partition: "p".into(),
            operation_id: id.into(),
            command_sha256: Sha256::digest(command.encode_to_vec()).to_vec(),
            command: Some(command),
        }
    }
    #[test]
    fn operation_reference_compatibility() {
        for id in [
            "legacy-replay",
            "723ef4ba-ea7b-4c25-8b7a-bf19074691f4",
            "op_0000000000000000000000",
            "op_7n42DGM5Tflk9n8mt7Fhc7",
            &"a".repeat(128),
        ] {
            assert!(valid_operation_reference(id), "{id:?}");
        }
        for id in [
            "",
            "user_name",
            "../path",
            "a\nb",
            "op_000000000000000000001",
            "op_7n42DGM5Tflk9n8mt7Fhc8",
            "op_zzzzzzzzzzzzzzzzzzzzzz",
            "inc_0000000000000000000001",
            "op_000000000000000000000-",
            "é",
            &"a".repeat(129),
        ] {
            assert!(!valid_operation_reference(id), "{id:?}");
        }
    }

    #[tokio::test]
    async fn outcome_and_shard_recover_together() {
        let objects = Arc::new(InMemory::new());
        let service = ShardService::new(
            Db::builder("recovery", objects.clone())
                .build()
                .await
                .unwrap(),
            "p".into(),
            100,
        );
        let create = request(
            "op_0000000000000000000001",
            ShardCommand {
                kind: Kind::CreateOrGet as i32,
                shard_id: 1,
                range_id: 3,
                data: vec![0, 255, 1],
                encoding: 2,
                ..Default::default()
            },
        );
        let result = service.clone().execute_owned(create.clone()).await.unwrap();
        service.partition.lock().await.db.close().await.unwrap();
        let recovered = ShardService::new(
            Db::builder("recovery", objects).build().await.unwrap(),
            "p".into(),
            100,
        );
        assert_eq!(
            result,
            recovered.clone().execute_owned(create).await.unwrap()
        );
        let guard = request(
            "guard",
            ShardCommand {
                kind: Kind::Assert as i32,
                shard_id: 1,
                range_id: 3,
                ..Default::default()
            },
        );
        assert_eq!(recovered.execute_owned(guard).await.unwrap().error, 0);
    }
    #[tokio::test]
    async fn replay_is_fenced_and_capacity_fails_closed() {
        let objects = Arc::new(InMemory::new());
        let old = ShardService::new(
            Db::builder("fence", objects.clone()).build().await.unwrap(),
            "p".into(),
            1,
        );
        let create = request(
            "create",
            ShardCommand {
                kind: Kind::CreateOrGet as i32,
                shard_id: 1,
                range_id: 3,
                ..Default::default()
            },
        );
        old.clone().execute_owned(create.clone()).await.unwrap();
        let new_request = request("another", create.command.clone().unwrap());
        assert_eq!(
            old.clone()
                .execute_owned(new_request)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::ResourceExhausted
        );
        let _replacement = Db::builder("fence", objects).build().await.unwrap();
        assert_eq!(
            old.clone().execute_owned(create).await.unwrap_err().code(),
            tonic::Code::Unavailable
        );
        assert!(old.partition.lock().await.quarantined);
    }
    #[tokio::test]
    async fn stalled_durability_times_out_and_quarantines() {
        let settings = slatedb::Settings {
            flush_interval: None,
            ..Default::default()
        };
        let db = Db::builder("timeout", Arc::new(InMemory::new()))
            .with_settings(settings)
            .build()
            .await
            .unwrap();
        let service = ShardService::new(db, "p".into(), 100)
            .with_operation_timeout(Duration::from_millis(40));
        let create = request(
            "create",
            ShardCommand {
                kind: Kind::CreateOrGet as i32,
                shard_id: 1,
                range_id: 3,
                ..Default::default()
            },
        );
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            service.clone().execute_owned(create),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(service.partition.lock().await.quarantined);
        let read = request(
            "read",
            ShardCommand {
                kind: Kind::Get as i32,
                shard_id: 1,
                ..Default::default()
            },
        );
        assert_eq!(
            service.execute_owned(read).await.unwrap_err().message(),
            "partition requires recovery"
        );
    }
}
