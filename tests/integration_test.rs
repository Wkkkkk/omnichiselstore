use chiselstore::rpc::proto::rpc_server::RpcServer;
use chiselstore::{
    rpc::{RpcService, RpcTransport},
    StoreServer,
};
use std::sync::Arc;
use tonic::transport::Server;

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

extern crate futures;

use crate::futures::FutureExt;

async fn start_server(id: usize, peers: Vec<usize>) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<Result<(), tonic::transport::Error>>, tokio::sync::oneshot::Sender<()>) {
  let (host, port) = node_authority(id);
  let rpc_listen_addr = format!("{}:{}", host, port).parse().unwrap();
  let transport = RpcTransport::new(Box::new(node_rpc_addr));
  let server = StoreServer::start(id, peers, transport).unwrap();
  let server = Arc::new(server);
  let f = {
      let server = server.clone();
      tokio::task::spawn_blocking(move || {
          println!("ChiselStore node starting..");
          server.run();
          println!("ChiselStore node shutting down..");
      })
  };
  let rpc = RpcService::new(server);
  let (shtdwn_sender, shtdwn_receiver) = oneshot::channel::<()>();
  let g = tokio::task::spawn(async move {
      println!("RPC listening to {} ...", rpc_listen_addr);
      let ret = Server::builder()
          .add_service(RpcServer::new(rpc))
          .serve_with_shutdown(rpc_listen_addr, shtdwn_receiver.map(drop))
          .await;
      println!("RPC Server shutting down...");
      ret
  });

  return (f, g, shtdwn_sender)
}

pub mod proto {
  tonic::include_proto!("proto");
}
use proto::rpc_client::RpcClient;
use proto::{Consistency, Query};
use tokio::sync::oneshot;

#[tokio::test(flavor = "multi_thread")]
async fn single_write_single_read() {
  let (f1, g1, s1) = start_server(1, vec![2]).await;
  let (f2, g2, s2) = start_server(2, vec![1]).await;

  let write_thread = tokio::task::spawn(async {
    
    let addr = "http://127.0.0.1:50001";
    let mut client = RpcClient::connect(addr).await.unwrap();
    
    let query = tonic::Request::new(Query {
      sql: String::from("SELECT 1+1;"),
      consistency: Consistency::Strong as i32,
    });
    let response = client.execute(query).await.unwrap();
    let response = response.into_inner();

    return response.rows[0].values[0].clone();
  });

  let res = write_thread.await.unwrap();

  assert!(res == "2");

  // shut down servers
  f1.abort();
  g1.abort();
  s1.send(());

  f2.abort();
  g2.abort();
  s2.send(());

  return;
}

#[test]
fn single_write_multi_read() {
  
}

#[test]
fn multi_write_single_read() {
  // ChiselStore uses SQLite which only allows for single sequential writes

}