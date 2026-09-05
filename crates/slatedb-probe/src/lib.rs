//! Executable primitive experiments against SlateDB 0.16.0, not a production adapter.
use slatedb::{
    object_store::{aws::AmazonS3Builder, memory::InMemory, ObjectStore},
    Db, Settings,
};
use std::{error::Error, sync::Arc};

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// Memory is the default. S3 mode requires an explicit bucket and unique test prefix.
/// AWS credentials/region/endpoint use object_store's standard AWS_* environment.
/// Use a dedicated disposable prefix: SlateDB maintenance may delete objects within it.
/// No bucket-wide cleanup is performed.
pub fn object_store_from_env() -> Result<(Arc<dyn ObjectStore>, String)> {
    match std::env::var("XENON_PROBE_BACKEND").as_deref() {
        Ok("s3") => {
            let bucket = std::env::var("XENON_PROBE_BUCKET")?;
            let prefix = std::env::var("XENON_PROBE_PREFIX")?;
            if prefix.is_empty() {
                return Err("XENON_PROBE_PREFIX must be nonempty".into());
            }
            Ok((
                Arc::new(
                    AmazonS3Builder::from_env()
                        .with_bucket_name(bucket)
                        .build()?,
                ),
                prefix,
            ))
        }
        Err(std::env::VarError::NotPresent) | Ok("memory") => {
            Ok((Arc::new(InMemory::new()), "primitive-probe".into()))
        }
        _ => Err("XENON_PROBE_BACKEND must be memory or s3".into()),
    }
}

pub async fn open(path: &str, objects: Arc<dyn ObjectStore>, manual_flush: bool) -> Result<Db> {
    // Keep default GC and compactor settings. Only automatic WAL flushing is disabled
    // in deterministic durability tests; those tests explicitly flush and await durability.
    let mut settings = Settings::default();
    if manual_flush {
        settings.flush_interval = None;
    }
    Ok(Db::builder(path, objects)
        .with_settings(settings)
        .build()
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::{
        config::{DurabilityLevel, ReadOptions},
        CloseReason, ErrorKind, IsolationLevel, WriteBatch,
    };
    use tokio::{
        sync::Mutex,
        time::{timeout, Duration},
    };

    #[tokio::test]
    async fn atomic_batch_conflict_and_recovery() -> Result<()> {
        timeout(
            Duration::from_secs(30),
            atomic_batch_conflict_and_recovery_scenario(),
        )
        .await?
    }

    async fn atomic_batch_conflict_and_recovery_scenario() -> Result<()> {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open("atomic", objects.clone(), false).await?;
        let mut batch = WriteBatch::new();
        batch.put(b"current", b"run1");
        batch.put(b"run1", b"state1");
        db.write(batch).await?.await_durable().await?;
        let a = db.begin(IsolationLevel::SerializableSnapshot).await?;
        let b = db.begin(IsolationLevel::SerializableSnapshot).await?;
        assert_eq!(a.get(b"current").await?.unwrap().as_ref(), b"run1");
        assert_eq!(b.get(b"current").await?.unwrap().as_ref(), b"run1");
        a.put(b"current", b"run2")?;
        a.put(b"run2", b"state2")?;
        a.commit().await?.unwrap().await_durable().await?;
        b.put(b"current", b"bad")?;
        b.put(b"partial", b"must-not-exist")?;
        let failure = b
            .commit()
            .await
            .expect_err("conflicting transaction must fail");
        assert!(matches!(failure.kind(), ErrorKind::Transaction));
        assert!(db.get(b"partial").await?.is_none());
        // A new writer replays remote WAL while the original process is still open.
        // This is recovery without a graceful flush/close of the original writer.
        let recovered = open("atomic", objects, false).await?;
        assert_eq!(recovered.get(b"current").await?.unwrap().as_ref(), b"run2");
        assert_eq!(recovered.get(b"run2").await?.unwrap().as_ref(), b"state2");
        assert!(recovered.get(b"partial").await?.is_none());
        let _ = db.close().await;
        recovered.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn await_durable_and_admission_gate() -> Result<()> {
        timeout(
            Duration::from_secs(30),
            await_durable_and_admission_gate_scenario(),
        )
        .await?
    }

    async fn await_durable_and_admission_gate_scenario() -> Result<()> {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let db = open("gate", objects, true).await?;
        let gate = Mutex::new(());
        let writer = gate.lock().await;
        let handle = db.put(b"task", b"dispatch").await?;
        // Demonstrate the hazard: default reads expose not-yet-durable commits.
        assert!(db.get(b"task").await?.is_some());
        let remote = ReadOptions::new().with_durability_filter(DurabilityLevel::Remote);
        assert!(db.get_with_options(b"task", &remote).await?.is_none());
        assert!(timeout(Duration::from_millis(25), handle.await_durable())
            .await
            .is_err());
        assert!(
            gate.try_lock().is_err(),
            "reader cannot enter publication gate"
        );
        db.flush().await?;
        timeout(Duration::from_secs(5), handle.await_durable()).await??;
        drop(writer);
        let _reader = gate.lock().await;
        assert_eq!(
            db.get_with_options(b"task", &remote)
                .await?
                .unwrap()
                .as_ref(),
            b"dispatch"
        );
        db.close().await?;
        Ok(())
    }

    #[tokio::test]
    async fn competing_writer_fences_old_writer() -> Result<()> {
        timeout(
            Duration::from_secs(30),
            competing_writer_fences_old_writer_scenario(),
        )
        .await?
    }

    async fn competing_writer_fences_old_writer_scenario() -> Result<()> {
        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let old = open("fence", objects.clone(), true).await?;
        let acknowledged = old.put(b"ack", b"preserved").await?;
        old.flush().await?;
        acknowledged.await_durable().await?;
        let new = open("fence", objects, false).await?;
        assert_eq!(new.get(b"ack").await?.unwrap().as_ref(), b"preserved");
        // A stale writer may commit to memory; it must never get a durable ack.
        let failed = timeout(Duration::from_secs(10), async {
            match old.put(b"stale", b"forbidden").await {
                Err(e) => Err(e),
                Ok(handle) => match old.flush().await {
                    Err(e) => Err(e),
                    Ok(()) => handle.await_durable().await,
                },
            }
        })
        .await?
        .expect_err("old writer must be fenced before acknowledgement");
        assert!(
            matches!(failed.kind(), ErrorKind::Closed(CloseReason::Fenced)),
            "{failed:?}"
        );
        new.put(b"new", b"progress").await?.await_durable().await?;
        assert!(new.get(b"stale").await?.is_none());
        let _ = old.close().await;
        new.close().await?;
        Ok(())
    }
    #[tokio::test]
    async fn serializable_read_dependency_rejects_write_skew() -> Result<()> {
        timeout(Duration::from_secs(30), async {
            let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let db = open("skew", objects, false).await?;
            let mut initial = WriteBatch::new();
            initial.put(b"doctor-a", b"on");
            initial.put(b"doctor-b", b"on");
            db.write(initial).await?.await_durable().await?;
            let a = db.begin(IsolationLevel::SerializableSnapshot).await?;
            let b = db.begin(IsolationLevel::SerializableSnapshot).await?;
            assert_eq!(a.get(b"doctor-b").await?.unwrap().as_ref(), b"on");
            assert_eq!(b.get(b"doctor-a").await?.unwrap().as_ref(), b"on");
            // Disjoint write sets: only read-dependency tracking prevents both off.
            a.put(b"doctor-a", b"off")?;
            b.put(b"doctor-b", b"off")?;
            a.commit().await?.unwrap().await_durable().await?;
            let failure = b.commit().await.expect_err("write skew must conflict");
            assert!(matches!(failure.kind(), ErrorKind::Transaction));
            assert_eq!(db.get(b"doctor-b").await?.unwrap().as_ref(), b"on");
            db.close().await?;
            Ok(())
        })
        .await?
    }
}
