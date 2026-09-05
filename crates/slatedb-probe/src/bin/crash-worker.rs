//! A process boundary probe; all successful acknowledgements follow remote durability.
use slatedb::WriteBatch;
use slatedb_probe::{object_store_from_env, open, Result};
use std::io::{self, Write};

fn emit(event: &str, sequence: u32) {
    println!(
        "{}",
        serde_json::json!({"event": event, "sequence": sequence})
    );
    io::stdout().flush().expect("flush fault barrier");
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("XENON_PROBE_BACKEND").as_deref() != Ok("s3") {
        return Err("process recovery requires shared S3 backend, not process memory".into());
    }
    let args: Vec<String> = std::env::args().collect();
    let role = args.get(1).ok_or("missing writer/verify role")?;
    let stage = args.get(2).ok_or("missing fault stage")?;
    if !matches!(
        stage.as_str(),
        "before-durable" | "after-durable" | "after-ack"
    ) {
        return Err("unknown fault stage".into());
    }
    let (objects, prefix) = object_store_from_env()?;
    let db = open(&prefix, objects, role == "writer").await?;
    if role == "writer" {
        for sequence in 1..=3 {
            let mut batch = WriteBatch::new();
            batch.put(format!("state/{sequence}"), format!("value/{sequence}"));
            batch.put(format!("task/{sequence}"), format!("value/{sequence}"));
            let handle = db.write(batch).await?;
            if sequence == 3 && stage == "before-durable" {
                emit("fault-ready", sequence);
                std::future::pending::<()>().await;
            }
            db.flush().await?;
            handle.await_durable().await?;
            if sequence == 3 && stage == "after-durable" {
                emit("fault-ready", sequence);
                std::future::pending::<()>().await;
            }
            emit("ack", sequence);
            if sequence == 3 {
                emit("fault-ready", sequence);
                std::future::pending::<()>().await;
            }
        }
        unreachable!();
    } else if role == "verify" {
        let acknowledged: u32 = args.get(3).ok_or("missing observed ack count")?.parse()?;
        if acknowledged != if stage == "after-ack" { 3 } else { 2 } {
            return Err("observer acknowledgement count differs from fault contract".into());
        }
        for sequence in 1..=3 {
            let state = db.get(format!("state/{sequence}")).await?;
            let task = db.get(format!("task/{sequence}")).await?;
            if state != task {
                return Err("partial recovered state/task batch".into());
            }
            if sequence <= acknowledged || stage != "before-durable" {
                if state.as_deref() != Some(format!("value/{sequence}").as_bytes()) {
                    return Err("acknowledged/durable state absent after process kill".into());
                }
            } else if state.is_some() {
                return Err("manual-flush pre-durable mutation unexpectedly published".into());
            }
        }
        db.put(b"recovery-progress", b"resumed")
            .await?
            .await_durable()
            .await?;
        db.close().await?;
        emit("verified", acknowledged);
        Ok(())
    } else {
        Err("unknown worker role".into())
    }
}
