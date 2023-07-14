use broadcaster::start_broadcaster;
use std::sync::Arc;

mod broadcaster;
mod grpc_server;
mod message;
mod pgpool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let password: String = std::env::var("EVENTSDB_PASSWORD")?;
    let config = tokio_postgres::Config::default()
        .host("eventsdb")
        .user("postgres")
        .password(password)
        .clone();
    let pool = Arc::new(pgpool::PgPool::new(config.clone()));
    let eventrx = start_broadcaster(config.clone(), pool.clone());
    let queuerx = start_broadcaster(config, pool.clone());
    grpc_server::start(pool, eventrx, queuerx).await?;
    Ok(())
}
