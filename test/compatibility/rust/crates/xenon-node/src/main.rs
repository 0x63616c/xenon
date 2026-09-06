use slatedb::{
    object_store::{aws::AmazonS3Builder, memory::InMemory, ObjectStore},
    Db,
};
use std::{env, sync::Arc};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use xenon_node::{wire::shard_persistence_server::ShardPersistenceServer, ShardService};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = env::var("XENON_BACKEND").unwrap_or_else(|_| "s3".into());
    let store: Arc<dyn ObjectStore> = match backend.as_str() {
        "memory" => Arc::new(InMemory::new()),
        "s3" => Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(env::var("XENON_BUCKET")?)
                .build()?,
        ),
        _ => return Err("XENON_BACKEND must be s3 or memory".into()),
    };
    let prefix = env::var("XENON_PREFIX")?;
    if prefix.is_empty() {
        return Err("XENON_PREFIX must not be empty".into());
    }
    let name = env::var("XENON_PARTITION")?;
    let db = Db::builder(prefix, store).build().await?;
    let limit = env::var("XENON_MAX_OUTCOMES")
        .unwrap_or_else(|_| "10000".into())
        .parse()?;
    let listener =
        TcpListener::bind(env::var("XENON_LISTEN").unwrap_or_else(|_| "127.0.0.1:7235".into()))
            .await?;
    println!("READY {}", listener.local_addr()?);
    tonic::transport::Server::builder()
        .add_service(
            ShardPersistenceServer::new(ShardService::new(db, name, limit))
                .max_decoding_message_size(2 * 1024 * 1024),
        )
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}
