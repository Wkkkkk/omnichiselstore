use crate::rpc::proto::rpc_server::RpcServer;
use crate::{
    rpc::{RpcService, RpcTransport},
    StoreServer,
};
use std::sync::Arc;
use std::error::Error;
use tonic::transport::Server;

extern crate futures;
use futures::FutureExt;

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
pub fn log(s: String) {
    let mut builder = TerminalLoggerBuilder::new();
    builder.level(Severity::Debug);
    builder.destination(Destination::Stderr);

    let logger = builder.build().unwrap();
    info!(logger, "{}", s);
}

/// Node authority (host and port) in the cluster.
pub fn node_authority(id: u64) -> (&'static str, u16) {
    let hosts = vec!["127.0.0.1", // local host 
                     "34.127.15.38", 
                     "34.118.45.186",
                     "104.198.64.213"];
    let host = hosts[id as usize];

    let port = 50000 + (id as u16);
    (host, port)
}

/// Node RPC address in cluster.
pub fn node_rpc_addr(id: u64) -> String {
    let (host, port) = node_authority(id);
    format!("http://{}:{}", host, port)
}

pub struct Replica {
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

pub async fn start_replica(id: u64, peers: Vec<u64>) -> Replica {
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

pub async fn setup_replicas(num_replicas: u64) -> Vec<Replica> {
    let mut replicas: Vec<Replica> = Vec::new();
    for id in 1..(num_replicas+1) {
        let mut peers: Vec<u64> = (1..num_replicas+1).collect();
        peers.remove((id - 1) as usize);

        log(format!("setup pid: {} peers: {:?}", id, peers).to_string());

        replicas.push(start_replica(id, peers).await);
    }

    return replicas
}

pub async fn shutdown_replicas(mut replicas: Vec<Replica>) {
    while let Some(r) = replicas.pop() {
        r.shutdown().await;
    }
}

pub async fn query(replica_id: u64, sql: String) -> Result<String, Box<dyn Error>> {
    // create RPC client
    let addr = node_rpc_addr(replica_id);
    let mut client = RpcClient::connect(addr).await.unwrap();
    
    // create request
    // log(format!("execute query: {}", sql));
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
