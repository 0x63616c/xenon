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
    pub data_prefix: String,
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
    data_prefix: String,
}

/// Bound engine ownership handle. Its engine and gate cannot be substituted by callers.
pub struct Owner {
    db: Db,
    admitted: Entry,
    directory_path: Path,
    objects: Arc<dyn ObjectStore>,
    admission: tokio::sync::Mutex<()>,
}

impl Owner {
    pub async fn close(self) -> Result<()> {
        Ok(self.db.close().await?)
    }
}

// Deliberately not Clone/Serialize: opening consumes this local attempt once.
// This is not persistent replay prevention across process restarts. A process
// must create a fresh incarnation/reservation after restart.
pub struct Reservation {
    directory_path: Path,
    objects: Arc<dyn ObjectStore>,
    entry: Entry,
    version: UpdateVersion,
}

impl Directory {
    pub fn new(objects: Arc<dyn ObjectStore>, path: impl Into<Path>, data_prefix: &str) -> Self {
        Self {
            objects,
            path: path.into(),
            data_prefix: data_prefix.into(),
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
        if self.data_prefix.is_empty()
            || entry.data_prefix != self.data_prefix
            || entry.generation == 0
            || entry.desired_incarnation.is_empty()
            || Uuid::parse_str(&entry.transition).is_err()
        {
            return Err("invalid directory entry".into());
        }
        Ok((entry, version))
    }

    /// Candidate read authority gate. The captured bytes are not returned until a
    /// nonempty, unique, durable write demonstrates current engine authority.
    /// This helper holds this Owner handle's admission gate through capture
    /// and durable barrier. A production adapter must route all partition operations
    /// through one shared gate; raw Db calls here are experiment setup only.
    /// The Owner captures the READY entry that admitted its writer. Internal engine
    /// mutation is mandatory even for absent application keys.
    pub async fn read_with_barrier(
        &self,
        owner: &Owner,
        key: &[u8],
    ) -> Result<Option<slatedb::bytes::Bytes>> {
        if owner.directory_path != self.path
            || owner.admitted.data_prefix != self.data_prefix
            || !Arc::ptr_eq(&owner.objects, &self.objects)
        {
            return Err("owner belongs to a different partition or object store".into());
        }
        let _admission = owner.admission.lock().await;
        let (current, _) = self.read().await?;
        if current.state != State::Ready || current != owner.admitted {
            return Err("read authority no longer matches READY incarnation".into());
        }
        if key == b"\x00xenon/read-barrier" {
            return Err("reserved barrier key".into());
        }
        let captured = owner.db.get(key).await?;
        read_barrier(&owner.db).await?;
        Ok(captured)
    }

    /// One CAS attempt. Concurrent reservations conflict rather than silently retry.
    pub async fn reserve(&self, incarnation: &str) -> Result<Reservation> {
        if incarnation.is_empty() || self.data_prefix.is_empty() {
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
            data_prefix: self.data_prefix.clone(),
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
            directory_path: self.path.clone(),
            objects: self.objects.clone(),
            entry,
            version: result.into(),
        })
    }

    /// No pre-open check can eliminate pause-before-open. Always validate after
    /// opening and retire on any failed readiness CAS, including unknown responses.
    pub async fn open_once(&self, attempt: Reservation) -> Result<Owner> {
        if attempt.directory_path != self.path
            || attempt.entry.data_prefix != self.data_prefix
            || !Arc::ptr_eq(&attempt.objects, &self.objects)
        {
            return Err("reservation belongs to a different partition or object store".into());
        }
        let db = open(&self.data_prefix, self.objects.clone(), false).await?;
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
            Ok(_) => Ok(Owner {
                db,
                admitted: ready,
                directory_path: self.path.clone(),
                objects: self.objects.clone(),
                admission: tokio::sync::Mutex::new(()),
            }),
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
    async fn reservation_is_bound_and_read_admission_is_held() -> Result<()> {
        timeout(Duration::from_secs(30), async {
            let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let directory = Directory::new(objects.clone(), "directory/binding", "data/binding");
            let wrong_prefix = Directory::new(objects.clone(), "directory/binding", "data/wrong");
            let wrong_directory = Directory::new(objects, "directory/wrong", "data/binding");
            assert!(wrong_prefix
                .open_once(directory.reserve("A").await?)
                .await
                .is_err());
            assert!(wrong_directory
                .open_once(directory.reserve("B").await?)
                .await
                .is_err());
            let db = directory.open_once(directory.reserve("C").await?).await?;
            assert!(wrong_directory
                .read_with_barrier(&db, b"absent")
                .await
                .is_err());
            let held = db.admission.lock().await;
            assert!(timeout(
                Duration::from_millis(25),
                directory.read_with_barrier(&db, b"absent")
            )
            .await
            .is_err());
            drop(held);
            assert!(directory.read_with_barrier(&db, b"absent").await?.is_none());
            db.close().await?;
            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn stale_captured_read_cannot_cross_durable_barrier() -> Result<()> {
        timeout(Duration::from_secs(30), async {
            let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let directory = Directory::new(objects, "directory/read", "data/read");
            let old = directory.open_once(directory.reserve("A").await?).await?;
            old.db
                .put(b"value", b"ack-A")
                .await?
                .await_durable()
                .await?;
            let admitted_a = directory.read().await?.0;
            // Pause after a fresh READY check and capture, before authority barrier.
            assert_eq!(directory.read().await?.0, admitted_a);
            let captured = old.db.get(b"value").await?;
            assert_eq!(captured.unwrap().as_ref(), b"ack-A");
            let new = directory.open_once(directory.reserve("B").await?).await?;
            new.db
                .put(b"value", b"ack-B")
                .await?
                .await_durable()
                .await?;
            let err = read_barrier(&old.db)
                .await
                .expect_err("captured stale result cannot be published");
            let engine = err
                .downcast_ref::<slatedb::Error>()
                .expect("engine fencing error");
            assert!(matches!(
                engine.kind(),
                ErrorKind::Closed(CloseReason::Fenced)
            ));
            assert!(directory.read_with_barrier(&old, b"value").await.is_err());
            assert_eq!(
                directory
                    .read_with_barrier(&new, b"value")
                    .await?
                    .unwrap()
                    .as_ref(),
                b"ack-B"
            );
            assert!(directory
                .read_with_barrier(&new, b"absent")
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
            let directory = Directory::new(objects, "directory/p", "data/p");
            let first = directory.reserve("A").await?;
            let old = directory.open_once(first).await?;
            old.db.put(b"ack-A", b"A").await?.await_durable().await?;

            // Two reserved attempts pause before opening; later C supersedes both.
            let delayed_b = directory.reserve("B").await?;
            let delayed_d = directory.reserve("D").await?;
            let current = directory.reserve("C").await?;
            let mut serving = directory.open_once(current).await?;
            serving
                .db
                .put(b"ack-C", b"C")
                .await?
                .await_durable()
                .await?;
            fenced(&old.db).await?;
            let _ = old.close().await;

            for (i, delayed) in [delayed_b, delayed_d].into_iter().enumerate() {
                let (before, _) = directory.read().await?;
                assert_eq!(before.state, State::Ready);
                let rejected = directory.open_once(delayed).await;
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
                fenced(&serving.db).await?;
                let _ = serving.close().await;
                assert_eq!(
                    directory.read().await?.0,
                    before,
                    "stale opener changed directory"
                );
                let fresh = directory.reserve(&format!("C-recovery-{i}")).await?;
                assert!(fresh.entry.generation > before.generation);
                assert_ne!(fresh.entry.transition, before.transition);
                serving = directory.open_once(fresh).await?;
                assert_eq!(serving.db.get(b"ack-A").await?.unwrap().as_ref(), b"A");
                assert_eq!(serving.db.get(b"ack-C").await?.unwrap().as_ref(), b"C");
                assert!(serving.db.get(b"stale").await?.is_none());
                let key = format!("recovered-{i}");
                serving
                    .db
                    .put(key.as_bytes(), b"progress")
                    .await?
                    .await_durable()
                    .await?;
            }
            assert_eq!(
                serving.db.get(b"recovered-0").await?.unwrap().as_ref(),
                b"progress"
            );
            serving.close().await?;
            Ok(())
        })
        .await?
    }
}
