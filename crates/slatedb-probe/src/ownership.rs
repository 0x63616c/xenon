//! Bounded ownership experiment, not a controller or election service.
//! A one-shot reservation can still disrupt a newer writer when resumed late.
//! Readiness CAS prevents serving under that stale reservation; finite stale
//! contenders must retire before a fresh current reservation restores progress.
use crate::{open, Result};
use serde::{Deserialize, Serialize};
use slatedb::{
    object_store::{
        path::Path, Error as ObjectError, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
        UpdateVersion,
    },
    Db,
};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Entry {
    pub generation: u64,
    pub transition: String,
    pub desired_incarnation: String,
    pub state: State,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum State {
    Opening,
    Ready,
}

pub struct Directory {
    objects: Arc<dyn ObjectStore>,
    path: Path,
}

// Deliberately not Clone/Serialize: opening consumes this local attempt once.
// This is not persistent replay prevention across process restarts. A process
// must create a fresh incarnation/reservation after restart.
pub struct Reservation {
    entry: Entry,
    version: UpdateVersion,
}

impl Directory {
    pub fn new(objects: Arc<dyn ObjectStore>, path: impl Into<Path>) -> Self {
        Self {
            objects,
            path: path.into(),
        }
    }

    pub async fn read(&self) -> Result<(Entry, UpdateVersion)> {
        let result = self.objects.get(&self.path).await?;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        if version.e_tag.is_none() && version.version.is_none() {
            return Err("directory object lacks conditional update identity".into());
        }
        let entry: Entry = serde_json::from_slice(&result.bytes().await?)?;
        if entry.generation == 0
            || entry.desired_incarnation.is_empty()
            || Uuid::parse_str(&entry.transition).is_err()
        {
            return Err("invalid directory entry".into());
        }
        Ok((entry, version))
    }

    /// Candidate read authority gate. The captured bytes are not returned until a
    /// nonempty, unique, durable write demonstrates current engine authority.
    /// Caller supplies the READY entry that admitted this writer. Internal engine
    /// mutation is mandatory even for absent application keys.
    pub async fn read_with_barrier(
        &self,
        db: &Db,
        admitted: &Entry,
        key: &[u8],
    ) -> Result<Option<slatedb::bytes::Bytes>> {
        let (current, _) = self.read().await?;
        if current.state != State::Ready || current != *admitted {
            return Err("read authority no longer matches READY incarnation".into());
        }
        if key == b"\x00xenon/read-barrier" {
            return Err("reserved barrier key".into());
        }
        let captured = db.get(key).await?;
        read_barrier(db).await?;
        Ok(captured)
    }

    /// One CAS attempt. Concurrent reservations conflict rather than silently retry.
    pub async fn reserve(&self, incarnation: &str) -> Result<Reservation> {
        if incarnation.is_empty() {
            return Err("empty incarnation".into());
        }
        let (generation, mode) = match self.read().await {
            Ok((current, version)) => (
                current
                    .generation
                    .checked_add(1)
                    .ok_or("generation exhausted")?,
                PutMode::Update(version),
            ),
            Err(err)
                if matches!(
                    err.downcast_ref::<ObjectError>(),
                    Some(ObjectError::NotFound { .. })
                ) =>
            {
                (1, PutMode::Create)
            }
            Err(err) => return Err(err),
        };
        let entry = Entry {
            generation,
            transition: Uuid::new_v4().to_string(),
            desired_incarnation: incarnation.into(),
            state: State::Opening,
        };
        let result = self
            .objects
            .put_opts(
                &self.path,
                serde_json::to_vec(&entry)?.into(),
                PutOptions {
                    mode,
                    ..Default::default()
                },
            )
            .await?;
        Ok(Reservation {
            entry,
            version: result.into(),
        })
    }

    /// No pre-open check can eliminate pause-before-open. Always validate after
    /// opening and retire on any failed readiness CAS, including unknown responses.
    pub async fn open_once(&self, attempt: Reservation, db_path: &str) -> Result<Db> {
        let db = open(db_path, self.objects.clone(), false).await?;
        let ready = Entry {
            state: State::Ready,
            ..attempt.entry
        };
        let publication = self
            .objects
            .put_opts(
                &self.path,
                serde_json::to_vec(&ready)?.into(),
                PutOptions {
                    mode: PutMode::Update(attempt.version),
                    ..Default::default()
                },
            )
            .await;
        match publication {
            Ok(_) => Ok(db),
            Err(err) => {
                // Close, never reopen under this consumed attempt. A failed close
                // remains an explicit failure rather than pretending retirement.
                db.close().await?;
                Err(err.into())
            }
        }
    }
}

// A single overwritten reserved key; never an empty transaction or mere flush.
async fn read_barrier(db: &Db) -> Result<()> {
    db.put(b"\x00xenon/read-barrier", Uuid::new_v4().as_bytes())
        .await?
        .await_durable()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::{object_store::memory::InMemory, CloseReason, ErrorKind};
    use tokio::time::{timeout, Duration};

    async fn fenced(db: &Db) -> Result<()> {
        let failure = match db.put(b"stale", b"must-not-ack").await {
            Err(err) => err,
            Ok(handle) => handle
                .await_durable()
                .await
                .expect_err("stale writer must not acknowledge"),
        };
        assert!(
            matches!(failure.kind(), ErrorKind::Closed(CloseReason::Fenced)),
            "{failure:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn stale_captured_read_cannot_cross_durable_barrier() -> Result<()> {
        timeout(Duration::from_secs(30), async {
            let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let directory = Directory::new(objects, "directory/read");
            let old = directory
                .open_once(directory.reserve("A").await?, "data/read")
                .await?;
            old.put(b"value", b"ack-A").await?.await_durable().await?;
            let admitted_a = directory.read().await?.0;
            // Pause after a fresh READY check and capture, before authority barrier.
            assert_eq!(directory.read().await?.0, admitted_a);
            let captured = old.get(b"value").await?;
            assert_eq!(captured.unwrap().as_ref(), b"ack-A");
            let new = directory
                .open_once(directory.reserve("B").await?, "data/read")
                .await?;
            new.put(b"value", b"ack-B").await?.await_durable().await?;
            let err = read_barrier(&old)
                .await
                .expect_err("captured stale result cannot be published");
            let engine = err
                .downcast_ref::<slatedb::Error>()
                .expect("engine fencing error");
            assert!(matches!(
                engine.kind(),
                ErrorKind::Closed(CloseReason::Fenced)
            ));
            assert!(directory
                .read_with_barrier(&old, &admitted_a, b"value")
                .await
                .is_err());
            let admitted_b = directory.read().await?.0;
            assert_eq!(
                directory
                    .read_with_barrier(&new, &admitted_b, b"value")
                    .await?
                    .unwrap()
                    .as_ref(),
                b"ack-B"
            );
            assert!(directory
                .read_with_barrier(&new, &admitted_b, b"absent")
                .await?
                .is_none());
            let _ = old.close().await;
            new.close().await?;
            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn delayed_stale_opener_retires_and_fresh_owner_recovers() -> Result<()> {
        timeout(Duration::from_secs(30), async {
            let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let directory = Directory::new(objects, "directory/p");
            let first = directory.reserve("A").await?;
            let old = directory.open_once(first, "data/p").await?;
            old.put(b"ack-A", b"A").await?.await_durable().await?;

            // Two reserved attempts pause before opening; later C supersedes both.
            let delayed_b = directory.reserve("B").await?;
            let delayed_d = directory.reserve("D").await?;
            let current = directory.reserve("C").await?;
            let mut serving = directory.open_once(current, "data/p").await?;
            serving.put(b"ack-C", b"C").await?.await_durable().await?;
            fenced(&old).await?;
            let _ = old.close().await;

            for (i, delayed) in [delayed_b, delayed_d].into_iter().enumerate() {
                let (before, _) = directory.read().await?;
                assert_eq!(before.state, State::Ready);
                let rejected = directory.open_once(delayed, "data/p").await;
                let err = match rejected {
                    Err(err) => err,
                    Ok(db) => {
                        db.close().await?;
                        panic!("stale opener published readiness")
                    }
                };
                assert!(
                    matches!(
                        err.downcast_ref::<ObjectError>(),
                        Some(ObjectError::Precondition { .. })
                    ),
                    "{err:?}"
                );
                // Delayed opening DID disrupt C: acknowledge this liveness limit.
                fenced(&serving).await?;
                let _ = serving.close().await;
                assert_eq!(
                    directory.read().await?.0,
                    before,
                    "stale opener changed directory"
                );
                let fresh = directory.reserve(&format!("C-recovery-{i}")).await?;
                assert!(fresh.entry.generation > before.generation);
                assert_ne!(fresh.entry.transition, before.transition);
                serving = directory.open_once(fresh, "data/p").await?;
                assert_eq!(serving.get(b"ack-A").await?.unwrap().as_ref(), b"A");
                assert_eq!(serving.get(b"ack-C").await?.unwrap().as_ref(), b"C");
                assert!(serving.get(b"stale").await?.is_none());
                let key = format!("recovered-{i}");
                serving
                    .put(key.as_bytes(), b"progress")
                    .await?
                    .await_durable()
                    .await?;
            }
            assert_eq!(
                serving.get(b"recovered-0").await?.unwrap().as_ref(),
                b"progress"
            );
            serving.close().await?;
            Ok(())
        })
        .await?
    }
}
