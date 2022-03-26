use chiselstore::rpc::proto::rpc_server::RpcServer;
use chiselstore::{
    rpc::{RpcService, RpcTransport},
    StoreServer,
};
use std::sync::Arc;
use tonic::transport::Server;

extern crate futures;
use crate::futures::FutureExt;

pub mod proto {
  tonic::include_proto!("proto");
}
use proto::rpc_client::RpcClient;
use proto::{Query};
use tokio::sync::oneshot;

use slog::info;
use sloggers::Build;
use sloggers::terminal::{TerminalLoggerBuilder, Destination};
use sloggers::types::Severity;
fn log(s: String) {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stderr);

    let logger = builder.build().unwrap();
    info!(logger, "{}", s);
}

/// Node authority (host and port) in the cluster.
fn node_authority(id: u64) -> (&'static str, u16) {
    let host = "127.0.0.1";
    let port = 50000 + (id as u16);
    (host, port)
}

/// Node RPC address in cluster.
fn node_rpc_addr(id: u64) -> String {
    let (host, port) = node_authority(id);
    format!("http://{}:{}", host, port)
}

struct Replica {
    store_server: std::sync::Arc<StoreServer<RpcTransport>>,
    store_message_handle: tokio::task::JoinHandle<()>,
    store_ble_handle: tokio::task::JoinHandle<()>,
    rpc_handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    halt_sender: tokio::sync::oneshot::Sender<()>,
    shutdown_sender: tokio::sync::oneshot::Sender<()>,
}

impl Replica {
    pub async fn shutdown(self) {
        self.shutdown_sender.send(());
        self.rpc_handle.await.unwrap();

        self.halt_sender.send(());
        self.store_message_handle.await.unwrap();
        self.store_ble_handle.await.unwrap();
    }

    pub fn is_leader(&self) -> bool {
        self.store_server.get_current_leader() == self.store_server.get_id()
    }

    pub fn get_id(&self) -> u64 {
        self.store_server.get_id()
    }

    pub fn get_current_leader(&self) -> u64 {
        self.store_server.get_current_leader()
    }

    pub fn reconfigure(&self, new_cluster: Vec<u64>) {
        self.store_server.reconfigure(new_cluster);
    }
}

async fn start_replica(id: u64, peers: Vec<u64>) -> Replica {
    let (host, port) = node_authority(id);
    let rpc_listen_addr = format!("{}:{}", host, port).parse().unwrap();
    let transport = RpcTransport::new(Box::new(node_rpc_addr));
    let server = StoreServer::start(id, peers, transport).unwrap();
    let server = Arc::new(server);
    let (halt_sender, halt_receiver) = oneshot::channel::<()>();
    let store_handles = {
        let server_receiver = server.clone();
        let server_message_loop = server.clone();
        let server_ble_loop = server.clone();
        tokio::task::spawn(async move {
            match halt_receiver.await {
                Ok(_) => server_receiver.set_halt(true),
                Err(_) => println!("Received error in halt_receiver"),
            };
        });
        let ble_loop = tokio::task::spawn(async move {
            log(format!("BLE loop starting for pid: {}", id).to_string());
            server_ble_loop.run_ble_loop().await;
            log("BLE loop shutting down".to_string());
        });
        let message_loop = tokio::task::spawn(async move {
            log(format!("Message loop starting for pid: {}", id).to_string());
            server_message_loop.run_message_loop().await;
            log("Message loop shutting down".to_string());
        });
        (message_loop, ble_loop)
    };
    let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
    let rpc_handle = {
        let server = server.clone();
        let rpc = RpcService::new(server);
        tokio::task::spawn(async move {
            log(format!("RPC listening to {} ...", rpc_listen_addr).to_string());
            let ret = Server::builder()
                .add_service(RpcServer::new(rpc))
                .serve_with_shutdown(rpc_listen_addr, shutdown_receiver.map(drop))
                .await;
            log("RPC Server shutting down...".to_string());
            ret
        })
    };

    return Replica {
        store_server: server.clone(),
        store_message_handle: store_handles.0,
        store_ble_handle: store_handles.1,
        rpc_handle,
        halt_sender,
        shutdown_sender,
    }
}

async fn setup_replicas(num_replicas: u64) -> Vec<Replica> {
    let mut replicas: Vec<Replica> = Vec::new();
    for id in 1..(num_replicas+1) {
        let mut peers: Vec<u64> = (1..num_replicas+1).collect();
        peers.remove((id - 1) as usize);

        log(format!("setup pid: {} peers: {:?}", id, peers).to_string());

        replicas.push(start_replica(id, peers).await);
    }

    return replicas
}

async fn shutdown_replicas(mut replicas: Vec<Replica>) {
    while let Some(r) = replicas.pop() {
        r.shutdown().await;
    }
}

use std::error::Error;
async fn query(replica_id: u64, sql: String) -> Result<String, Box<dyn Error>> {
    // create RPC client
    let addr = node_rpc_addr(replica_id);
    let mut client = RpcClient::connect(addr).await.unwrap();
    
    // create request
    let query = tonic::Request::new(Query {
        sql: sql,
    });

    // execute request
    let response = client.execute(query).await.unwrap();
    let response = response.into_inner();
    if response.rows.len() == 0 || response.rows[0].values.len() == 0 {
        return Ok(String::from(""));
    }

    let res = response.rows[0].values[0].clone();

    Ok(res)
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_to_cluster() {
    let mut replicas = setup_replicas(2).await;

    // run test
    tokio::task::spawn(async {
        let res = query(1, String::from("SELECT 1+1;")).await.unwrap();

        assert!(res == "2");
    }).await.unwrap();

    shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn write_read() {
    // create 2 replicas
    let mut replicas = setup_replicas(2).await;

    // run test
    tokio::task::spawn(async {
        // create table
        query(1, String::from("CREATE TABLE IF NOT EXISTS test (id integer PRIMARY KEY)")).await.unwrap();

        
        // create new entry
        query(1, String::from("INSERT INTO test VALUES(1)")).await.unwrap();
        
        // read new entry from replica 1
        let res = query(1, String::from("SELECT id FROM test WHERE id = 1")).await.unwrap();
        assert!(res == "1");
        log(format!("query res: {}", res).to_string());
        
        // read new entry from replica 2
        let res = query(2, String::from("SELECT id FROM test WHERE id = 1")).await.unwrap();
        assert!(res == "1");
        log(format!("query res: {}", res).to_string());

        // drop table
        query(1, String::from("DROP TABLE test")).await.unwrap();
    }).await.unwrap();

    shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn synchronous_writes() {
    // ChiselStore uses SQLite which only allows for synchronous writes
    // 1. write (1,1) -> replica A
    // 2. (over-)write (1,2) -> replica B
    // 3. read x from replica A
    // 4. read y from replica B
    // 5. x == y
  
    // create 2 replicas
    let mut replicas = setup_replicas(2).await;

    // START: test

    // create table
    tokio::task::spawn(async {
        query(1, String::from("CREATE TABLE IF NOT EXISTS test_synchronous (id integer PRIMARY KEY, value integer NOT NULL)")).await.unwrap();
    }).await.unwrap();
    
    // write replica A
    let write_a = tokio::task::spawn(async {
        // create new entry
        println!("write_a");
        query(1, String::from("INSERT OR REPLACE INTO test_synchronous VALUES(1,1)")).await.unwrap();
    });
    
    // write replica B
    let write_b = tokio::task::spawn(async {
        // create new entry
        println!("write_b");
        query(2, String::from("INSERT OR REPLACE INTO test_synchronous VALUES(1,2)")).await.unwrap();
    });
    
    write_a.await.unwrap();
    write_b.await.unwrap();

    // read new entry from replica 1
    let x = query(1, String::from("SELECT value FROM test_synchronous WHERE id = 1")).await.unwrap();
    
    // read new entry from replica 2
    let y = query(2, String::from("SELECT value FROM test_synchronous WHERE id = 1")).await.unwrap();
    
    assert!(x == y);

    // END: TEST
    
    // drop table
    query(1, String::from("DROP TABLE test_synchronous")).await.unwrap();

    shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_dies() {
    /// Note: LittleRaft does not support changes to the cluster and will get stuck
    // Write to cluster, kill leader, read written value from another node
    
    let mut replicas = setup_replicas(3).await;

    // START: test

    // create table
    tokio::task::spawn(async {
        query(1, String::from("CREATE TABLE IF NOT EXISTS test_leader_drop (id integer PRIMARY KEY)")).await.unwrap();
    }).await.unwrap();

    
    // kill leader
    let mut leader_idx = 0;
    for (i, r) in replicas.iter().enumerate() {
        if r.is_leader() {
            leader_idx = i;
            break
        }
    }
    
    let leader = replicas.remove(leader_idx);
    leader.shutdown().await;

    log(format!("Leader with idx {} dead", leader_idx).to_string());
    
    // reconfigure
    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
    
    let mut new_leader_idx = 0;
    let mut new_cluster: Vec<u64> = Vec::new();
    for (i, r) in replicas.iter().enumerate() {
        if r.is_leader() {
            new_leader_idx = i;
        }
        new_cluster.push(r.get_id());
    }

    replicas[new_leader_idx].reconfigure(new_cluster);

    log(format!("Leader with idx {} reconfigure", new_leader_idx).to_string());
    
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;
    
    // continue querying
    let living_replica_id = replicas[new_leader_idx].get_id();
    
    // write to table
    tokio::task::spawn(async move {
        query(living_replica_id, String::from("INSERT INTO test_leader_drop VALUES(1)")).await.unwrap();
    }).await.unwrap();
    
    // END: Test
    
    // drop table
    let living_replica_id = replicas[new_leader_idx].get_id();
    tokio::task::spawn(async move {
        query(living_replica_id, String::from("DROP TABLE test_leader_drop")).await.unwrap();
    }).await.unwrap();
    
    shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn follower_dies() {
    // Write to cluster, kill follower, read written value from another node
    /// Note: LittleRaft does not support changes to the cluster and will get stuck
    
    let mut replicas = setup_replicas(3).await;

    // START: test

    // create table
    tokio::task::spawn(async {
        query(1, String::from("CREATE TABLE IF NOT EXISTS test_follower_drop (id integer PRIMARY KEY)")).await.unwrap();
    }).await.unwrap();
    
    // kill a follower
    let mut follower_idx = 0;
    for (i, r) in replicas.iter().enumerate() {
        if !r.is_leader() {
            follower_idx = i;
            break
        }
    }

    let follower = replicas.remove(follower_idx);
    follower.shutdown().await;

    let living_replica_id = replicas[0].get_id();
    
    // write to table
    tokio::task::spawn(async move {
        query(living_replica_id, String::from("INSERT INTO test_follower_drop VALUES(1)")).await.unwrap();
    }).await.unwrap();

    // END: Test

    // drop table
    tokio::task::spawn(async move {
        query(living_replica_id, String::from("DROP TABLE test_follower_drop")).await.unwrap();
    }).await.unwrap();
    
    shutdown_replicas(replicas).await;
}