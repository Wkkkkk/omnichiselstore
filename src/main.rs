use omnichiselstore::boost::{start_replica, setup_replicas, log, shutdown_replicas};

use tokio::main;
use tokio::signal;use 

std::{
    env,
};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("not enough arguments.");
    }
    let id: u64 = args[1].parse().unwrap();
    let num_replicas: u64 = args[2].parse().unwrap();

    let mut peers: Vec<u64> = (1..num_replicas+1).collect();
    peers.remove((id - 1) as usize);
    let r = start_replica(id, peers).await;

    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await; // wait for leader election
    // print leader
    if r.is_leader() {
        let leader_idx = r.get_id();
        log(format!("Leader is node {}", leader_idx).to_string());
    }

    match signal::ctrl_c().await {
        Ok(()) => {
            log(format!("Shut down the server"));
        },
        Err(err) => {
            log(format!("Unable to listen for shutdown signal: {}", err));
        },
    }

    r.shutdown().await;
}