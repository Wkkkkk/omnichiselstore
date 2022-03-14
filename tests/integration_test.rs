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
use proto::{Consistency, Query};
use tokio::sync::oneshot;

/// Node authority (host and port) in the cluster.
fn node_authority(id: usize) -> (&'static str, u16) {
    let host = "127.0.0.1";
    let port = 50000 + (id as u16);
    (host, port)
}

/// Node RPC address in cluster.
fn node_rpc_addr(id: usize) -> String {
    let (host, port) = node_authority(id);
    format!("http://{}:{}", host, port)
}

struct Replica {
  pub store_handle: tokio::task::JoinHandle<()>,
  pub rpc_handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
  pub halt_sender: tokio::sync::oneshot::Sender<()>,
  pub shutdown_sender: tokio::sync::oneshot::Sender<()>,
}

impl Replica {
    pub async fn shutdown(self) {
        self.shutdown_sender.send(());
        self.rpc_handle.await.unwrap();

        self.halt_sender.send(());
        self.store_handle.await.unwrap();
    }
}

async fn start_replica(id: usize, peers: Vec<usize>) -> Replica {
  let (host, port) = node_authority(id);
  let rpc_listen_addr = format!("{}:{}", host, port).parse().unwrap();
  let transport = RpcTransport::new(Box::new(node_rpc_addr));
  let server = StoreServer::start(id, peers, transport).unwrap();
  let server = Arc::new(server);
  let (halt_sender, halt_receiver) = oneshot::channel::<()>();
  let store_handle = {
    let server = server.clone();
    let s = server.clone();
    tokio::task::spawn(async move {
      match halt_receiver.await {
        Ok(_) => s.set_halt(true),
        Err(_) => println!("Received error in halt_receiver"),
      };
    });
    tokio::task::spawn(async move {
      println!("ChiselStore node starting..");
      server.run();
      println!("ChiselStore node shutting down..");
    })
  };
  let rpc = RpcService::new(server);
  let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
  let rpc_handle = tokio::task::spawn(async move {
      println!("RPC listening to {} ...", rpc_listen_addr);
      let ret = Server::builder()
          .add_service(RpcServer::new(rpc))
          .serve_with_shutdown(rpc_listen_addr, shutdown_receiver.map(drop))
          .await;
      println!("RPC Server shutting down...");
      ret
  });

  return Replica {
    store_handle,
    rpc_handle,
    halt_sender,
    shutdown_sender,
  }
}

async fn setup_replicas(num_replicas: usize) -> Vec<Replica> {
    let mut replicas: Vec<Replica> = Vec::new();
    for id in 1..(num_replicas+1) {
        let mut p1: Vec<usize> = (1..id).collect();
        let mut p2: Vec<usize> = (id..num_replicas+1).collect();
        
        p1.append(&mut p2);
        let peers = p1;

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
async fn query(replica_id: usize, sql: String) -> Result<String, Box<dyn Error>> {
    // create RPC client
    let addr = node_rpc_addr(replica_id);
    let mut client = RpcClient::connect(addr).await.unwrap();
    
    // create request
    let query = tonic::Request::new(Query {
        sql: sql,
        consistency: Consistency::Strong as i32,
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
async fn single_write_single_read() {
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
        
        // read new entry from replica 2
        let res = query(2, String::from("SELECT id FROM test WHERE id = 1")).await.unwrap();
        assert!(res == "1");

        // drop table
        query(1, String::from("DROP TABLE test")).await.unwrap();
    }).await.unwrap();

    shutdown_replicas(replicas).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn single_write_multi_read() {
  
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_write_single_read() {
  // ChiselStore uses SQLite which only allows for single sequential writes

}