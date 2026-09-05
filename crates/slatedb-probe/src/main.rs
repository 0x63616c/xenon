use slatedb_probe::{object_store_from_env, open, Result};

#[tokio::main]
async fn main() -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(120), run()).await?
}

async fn run() -> Result<()> {
    let (objects, prefix) = object_store_from_env()?;
    let db = open(&prefix, objects.clone(), false).await?;
    db.put(b"probe", b"acknowledged")
        .await?
        .await_durable()
        .await?;
    db.close().await?;
    let recovered = open(&prefix, objects, false).await?;
    assert_eq!(
        recovered.get(b"probe").await?.unwrap().as_ref(),
        b"acknowledged"
    );
    recovered.close().await?;
    println!(
        "PASS durable write and reopen; backend={}",
        std::env::var("XENON_PROBE_BACKEND").unwrap_or("memory".into())
    );
    Ok(())
}
