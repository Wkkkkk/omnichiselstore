use omnichiselstore::boost::{setup_replicas, log, shutdown_replicas};

use tokio::main;
use tokio::signal;

#[tokio::main]
async fn main() {
    let mut replicas = setup_replicas(3).await;

    match signal::ctrl_c().await {
        Ok(()) => {
            log(format!("Shut down the server"));
        },
        Err(err) => {
            log(format!("Unable to listen for shutdown signal: {}", err));
        },
    }

    shutdown_replicas(replicas).await;
}