use omnichiselstore::boost::{setup_replicas, log, shutdown_replicas};

use tokio::main;
use tokio::signal;

#[tokio::main]
async fn main() {
    let mut replicas = setup_replicas(3).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await; // wait for leader election

    // print leader
    let mut leader_idx = 0;
    for r in replicas.iter() {
        if r.is_leader() {
            leader_idx = r.get_id();
            break
        }
    }    
    log(format!("Leader is node {}", leader_idx).to_string());

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