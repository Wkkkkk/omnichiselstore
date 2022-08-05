//! ChiselStore RPC module.

use crate::rpc::proto::rpc_server::Rpc;
use crate::{StoreServer, StoreTransport};
use async_mutex::Mutex;
use async_trait::async_trait;
use crossbeam::queue::ArrayQueue;
use derivative::Derivative;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use tonic::{Request, Response, Status};
use omnipaxos_core::{
    ballot_leader_election::messages::{BLEMessage, HeartbeatMsg, HeartbeatRequest, HeartbeatReply},
    messages::{
        Message, PaxosMsg, Prepare, Promise, AcceptSync, 
        FirstAccept, AcceptDecide, Accepted, Decide, 
        Compaction, AcceptStopSign, AcceptedStopSign, DecideStopSign,
        StoreCommand
    },
    util::{SyncItem}
};
use crate::boost::log;
use crate::preprocessing::*;
use lecar::controller::Controller;
use std::time::Instant;
use serde::{Serialize, Deserialize};

#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::{
    Query, QueryResults, QueryRow, Void,
    Ballot, StopSign, PrepareReq, PromiseReq, 
    AcceptSyncReq, FirstAcceptReq, AcceptDecideReq, AcceptedReq, 
    DecideReq, ProposalForwardReq, CompactionReq, ForwardCompactionReq,
    AcceptStopSignReq, AcceptedStopSignReq, DecideStopSignReq,
    HeartbeatRequestReq, HeartbeatReplyReq,
};

type NodeAddrFn = dyn Fn(u64) -> String + Send + Sync;

#[derive(Debug)]
struct ConnectionPool {
    connections: ArrayQueue<RpcClient<tonic::transport::Channel>>,
}

struct Connection {
    conn: RpcClient<tonic::transport::Channel>,
    pool: Arc<ConnectionPool>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.pool.replenish(self.conn.clone())
    }
}

impl ConnectionPool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            connections: ArrayQueue::new(16),
        })
    }

    async fn connection<S: ToString>(&self, addr: S) -> RpcClient<tonic::transport::Channel> {
        let addr = addr.to_string();
        // log(format!("Connecting to {}", addr).to_string());
        match self.connections.pop() {
            Some(x) => x,
            None => RpcClient::connect(addr).await.unwrap(),
        }
    }

    fn replenish(&self, conn: RpcClient<tonic::transport::Channel>) {
        let _ = self.connections.push(conn);
    }
}

#[derive(Debug, Clone)]
struct Connections(Arc<Mutex<HashMap<String, Arc<ConnectionPool>>>>);

impl Connections {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }

    async fn connection<S: ToString>(&self, addr: S) -> Connection {
        let mut conns = self.0.lock().await;
        let addr = addr.to_string();
        let pool = conns
            .entry(addr.clone())
            .or_insert_with(ConnectionPool::new);
        Connection {
            conn: pool.connection(addr).await,
            pool: pool.clone(),
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcTransport {
    /// Node address mapping function.
    #[derivative(Debug = "ignore")]
    node_addr: Box<NodeAddrFn>,
    connections: Connections,
}

impl RpcTransport {
    /// Creates a new RPC transport.
    pub fn new(node_addr: Box<NodeAddrFn>) -> Self {
        RpcTransport {
            node_addr,
            connections: Connections::new(),
        }
    }
}

fn ballot_from_proto(b: Ballot) -> omnipaxos_core::ballot_leader_election::Ballot {
    omnipaxos_core::ballot_leader_election::Ballot {
        n: b.n,
        priority: b.priority,
        pid: b.pid,
    }
}

fn store_command_from_proto(sc: proto::StoreCommand) -> StoreCommand {
    StoreCommand {
        id: sc.id,
        sql: sc.sql,
    }
}

fn stopsign_from_proto(ss: StopSign) -> omnipaxos_core::storage::StopSign {
    let config_id = ss.config_id;
    let nodes = ss.nodes;
    let metadata = Some(ss.metadata.into_iter().map(|md| md as u8).collect());
    
    omnipaxos_core::storage::StopSign {
        config_id,
        nodes,
        metadata,
    }
}

fn sync_item_from_proto(si: proto::SyncItem) -> SyncItem<StoreCommand,()> {
    match si.item.unwrap() {
        proto::sync_item::Item::Entries(entries) => {
            let entries = entries.store_commands.into_iter().map(|sc| store_command_from_proto(sc)).collect();
            return SyncItem::Entries(entries);
        },
        proto::sync_item::Item::Snapshot(_) => {
            return SyncItem::Snapshot(omnipaxos_core::storage::SnapshotType::Delta(())) // TODO: Support SnapshotType::Complete
        },
        proto::sync_item::Item::None(_) => {
            return SyncItem::None
        },
    }
}

fn proto_from_ballot(b: omnipaxos_core::ballot_leader_election::Ballot) -> Ballot {
    Ballot {
        n: b.n,
        priority: b.priority,
        pid: b.pid,
    }
}

fn proto_from_store_command(sc: StoreCommand) -> proto::StoreCommand {
    proto::StoreCommand {
        id: sc.id,
        sql: sc.sql,
    }
}

fn proto_from_sync_item(si: SyncItem<StoreCommand, ()>) -> proto::SyncItem {
    match si {
        SyncItem::Entries(entries) => {
            proto::SyncItem {
                item: Some(proto::sync_item::Item::Entries(proto::sync_item::Entries {
                    store_commands: entries.into_iter().map(|e| proto_from_store_command(e)).collect(),
                }))
            }
        },
        SyncItem::Snapshot(_) => {
            proto::SyncItem {
                item: Some(proto::sync_item::Item::Snapshot(true)),
            }
        },
        SyncItem::None => {
            proto::SyncItem {
                item: Some(proto::sync_item::Item::None(true)),
            }
        },
    }
}

fn proto_from_stopsign(ss: omnipaxos_core::storage::StopSign) -> StopSign {
    let metadata: Vec<u32> = match ss.metadata {
        Some(md) => {
            md.into_iter().map(|md| md as u32).collect()
        },
        None => [].to_vec(),
    };
    StopSign {
        config_id: ss.config_id,
        nodes: ss.nodes,
        metadata,
    }
}

#[async_trait]
impl StoreTransport for RpcTransport {
    fn send_sp(&self, to_id: u64, msg: Message<StoreCommand, ()>) {
        match msg.msg {
            PaxosMsg::Prepare(prepare) => {
                let from = msg.from;
                let to = msg.to;

                let n = Some(proto_from_ballot(prepare.n));
                let ld = prepare.ld;
                let n_accepted = Some(proto_from_ballot(prepare.n_accepted));
                let la = prepare.la;

                let req = PrepareReq {
                    from,
                    to,
                    n,
                    ld,
                    n_accepted,
                    la,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.prepare(req).await.unwrap();
                });
            },
            PaxosMsg::Promise(promise) => {
                let from = msg.from;
                let to = msg.to;

                let n = Some(proto_from_ballot(promise.n));
                let n_accepted = Some(proto_from_ballot(promise.n_accepted));
                let sync_item: Option<proto::SyncItem> = match promise.sync_item {
                    Some(si) => {
                        Some(proto_from_sync_item(si))
                    },
                    None => None,
                };
                let ld = promise.ld;
                let la = promise.la;
                let stop_sign: Option<StopSign> = match promise.stopsign {
                    Some(si) => {
                        Some(proto_from_stopsign(si))
                    },
                    None => None,
                };

                let req = PromiseReq {
                    from,
                    to,
                    n,
                    n_accepted,
                    sync_item,
                    ld,
                    la,
                    stop_sign,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.promise(req).await.unwrap();
                });
            },
            PaxosMsg::AcceptSync(accept_sync) => {
                let from = msg.from;
                let to = msg.to;

                let n = Some(proto_from_ballot(accept_sync.n));
                let sync_item = Some(proto_from_sync_item(accept_sync.sync_item));
                let sync_idx = accept_sync.sync_idx;
                let decide_idx = accept_sync.decide_idx;
                let stop_sign: Option<StopSign> = match accept_sync.stopsign {
                    Some(si) => {
                        Some(proto_from_stopsign(si))
                    },
                    None => None,
                };
                let cache = accept_sync.cache;

                let req = AcceptSyncReq {
                    from,
                    to,
                    n,
                    sync_item,
                    sync_idx,
                    decide_idx,
                    stop_sign,
                    cache
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();

                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.accept_sync(req).await.unwrap();
                });
            },
            PaxosMsg::FirstAccept(first_accept) => {
                let from = msg.from;
                let to = msg.to;

                let n = Some(proto_from_ballot(first_accept.n));
                // let entries = first_accept.entries.into_iter().map(|e| proto_from_store_command(e)).collect();

                let req = FirstAcceptReq {
                    from,
                    to,
                    n,
                    // entries,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.first_accept(req).await.unwrap();
                });
            },
            PaxosMsg::AcceptDecide(accept_decide) => {
                let from = msg.from;
                let to = msg.to;

                let n = Some(proto_from_ballot(accept_decide.n));
                let ld = accept_decide.ld;
                let entries = accept_decide.entries.into_iter().map(|e| proto_from_store_command(e)).collect();

                let req = AcceptDecideReq {
                    from,
                    to,
                    n,
                    ld,
                    entries,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    // TODO: dead lock
                    client.conn.accept_decide(req).await.unwrap();
                });
            },
            PaxosMsg::Accepted(accepted) => {
                let from = msg.from;
                let to = msg.to;

                let n = Some(proto_from_ballot(accepted.n));
                let la = accepted.la;

                let req = AcceptedReq {
                    from,
                    to,
                    n,
                    la,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.accepted(req).await.unwrap();
                });
            },
            PaxosMsg::Decide(decide) => {
                let from = msg.from;
                let to = msg.to;

                let n = Some(proto_from_ballot(decide.n));
                let ld = decide.ld;

                let req = DecideReq {
                    from,
                    to,
                    n,
                    ld,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    // TODO: dead lock
                    client.conn.decide(req).await.unwrap();
                });
            },
            PaxosMsg::ProposalForward(entries) => {
                let from = msg.from;
                let to = msg.to;

                let entries = entries.into_iter().map(|e| proto_from_store_command(e)).collect();

                let req = ProposalForwardReq {
                    from,
                    to,
                    entries,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.proposal_forward(req).await.unwrap();
                });
            },
            PaxosMsg::Compaction(compaction) => {
                let from = msg.from;
                let to = msg.to;

                let compaction = match compaction {
                    Compaction::Trim(vec) => {
                        proto::compaction_req::Compaction::Trim(proto::compaction_req::Trim {
                            trim: vec,
                        })
                    },
                    Compaction::Snapshot(s) => {
                        proto::compaction_req::Compaction::Snapshot(s)
                    },
                };

                let req = CompactionReq {
                    from,
                    to,
                    compaction: Some(compaction),
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.compaction(req).await.unwrap();
                });
            },
            PaxosMsg::ForwardCompaction(compaction) => {
                let from = msg.from;
                let to = msg.to;

                let compaction = match compaction {
                    Compaction::Trim(vec) => {
                        proto::forward_compaction_req::Compaction::Trim(proto::forward_compaction_req::Trim {
                            trim: vec,
                        })
                    },
                    Compaction::Snapshot(s) => {
                        proto::forward_compaction_req::Compaction::Snapshot(s)
                    },
                };

                let req = ForwardCompactionReq {
                    from,
                    to,
                    compaction: Some(compaction),
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.forward_compaction(req).await.unwrap();
                });
            },
            PaxosMsg::AcceptStopSign(accept_stop_sign) => {
                let from = msg.from;
                let to = msg.to;

                let n = Some(proto_from_ballot(accept_stop_sign.n));
                let ss = Some(proto_from_stopsign(accept_stop_sign.ss));

                let req = AcceptStopSignReq {
                    from,
                    to,
                    n,
                    ss,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.accept_stop_sign(req).await.unwrap();
                });
            },
            PaxosMsg::AcceptedStopSign(accepted_stop_sign) => {
                let from = msg.from;
                let to = msg.to;

                let n = Some(proto_from_ballot(accepted_stop_sign.n));

                let req = AcceptedStopSignReq {
                    from,
                    to,
                    n,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.accepted_stop_sign(req).await.unwrap();
                });
            },
            PaxosMsg::DecideStopSign(decide_stop_sign) => {
                let from = msg.from;
                let to = msg.to;

                let n = Some(proto_from_ballot(decide_stop_sign.n));

                let req = DecideStopSignReq {
                    from,
                    to,
                    n,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.decide_stop_sign(req).await.unwrap();
                });
            },
            _ => panic!("Missing implementation for send message"),
        };
    }

    fn send_ble(&self, to_id: u64, msg: BLEMessage) {
        match msg.msg {
            HeartbeatMsg::Request(heartbeat_request) => {
                let from = msg.from;
                let to = msg.to;

                let round = heartbeat_request.round;

                let req = HeartbeatRequestReq {
                    from,
                    to,
                    round,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.heartbeat_request(req).await.unwrap();
                });
            },
            HeartbeatMsg::Reply(heartbeat_reply) => {
                let from = msg.from;
                let to = msg.to;

                let round = heartbeat_reply.round;
                let ballot = Some(proto_from_ballot(heartbeat_reply.ballot));
                let majority_connected = heartbeat_reply.majority_connected;

                let req = HeartbeatReplyReq {
                    from,
                    to,
                    round,
                    ballot,
                    majority_connected,
                };

                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let req = tonic::Request::new(req.clone());
                    client.conn.heartbeat_reply(req).await.unwrap();
                });
            },
        };
    }
}

/// RPC service.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RpcService {
    /// The ChiselStore server access via this RPC service.
    #[derivative(Debug = "ignore")]
    pub server: Arc<StoreServer<RpcTransport>>
}

impl RpcService {
    /// Creates a new RPC service.
    pub fn new(server: Arc<StoreServer<RpcTransport>>) -> Self {
        Self { server }
    }
}

#[tonic::async_trait]
impl Rpc for RpcService {
    async fn execute(
        &self,
        request: Request<Query>,
    ) -> Result<Response<QueryResults>, tonic::Status> {
        let mut query = request.into_inner();
        log(format!("Rpc execute: {:?}", query).to_string());

        let server = self.server.clone();
        let results = match server.query(query.sqls).await {
            Ok(results) => results,
            Err(e) => return Err(Status::internal(format!("{}", e))),
        };

        let mut rows = vec![];
        for row in results.rows {
            rows.push(QueryRow {
                values: row.values.clone(),
            })
        }

        Ok(Response::new(QueryResults { rows }))
    }

    async fn prepare(&self, request: Request<PrepareReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let n = ballot_from_proto(msg.n.unwrap());
        let n_accepted = ballot_from_proto(msg.n_accepted.unwrap());
        log(format!("{:?} received prepare from {:?}", to, from).to_string());

        let msg = Prepare {
            n,
            ld: msg.ld,
            n_accepted,
            la: msg.la,
        };

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::Prepare(msg),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn promise(&self, request: Request<PromiseReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let n = ballot_from_proto(msg.n.unwrap());
        let n_accepted = ballot_from_proto(msg.n_accepted.unwrap());
        log(format!("{:?} received promise from {:?}", to, from).to_string());

        let sync_item: Option<SyncItem<StoreCommand,()>> = match msg.sync_item {
            Some(si) => Some(sync_item_from_proto(si)),
            _ => None,
        };
        
        let ld = msg.ld;
        let la = msg.la;

        let stopsign: Option<omnipaxos_core::storage::StopSign> = match msg.stop_sign {
            Some(ss) => Some(stopsign_from_proto(ss)),
            _ => None,
        };

        let msg = Promise {
            n,
            n_accepted,
            sync_item,
            ld,
            la,
            stopsign,
        };

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::Promise(msg),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn accept_sync(&self, request: Request<AcceptSyncReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let n = ballot_from_proto(msg.n.unwrap());
        
        let sync_item = sync_item_from_proto(msg.sync_item.unwrap());
        let sync_idx = msg.sync_idx;

        let decide_idx = msg.decide_idx;
        
        let stopsign: Option<omnipaxos_core::storage::StopSign> = match msg.stop_sign {
            Some(ss) => Some(stopsign_from_proto(ss)),
            _ => None,
        };

        let cache = msg.cache;
        log(format!("{:?} received accept_sync: {:?} from {:?}", to, cache, from).to_string());

        let msg = AcceptSync {
            n,
            sync_item,
            sync_idx,
            decide_idx,
            stopsign,
            cache
        };

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::AcceptSync(msg),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn first_accept(&self, request: Request<FirstAcceptReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let n = ballot_from_proto(msg.n.unwrap());
        // let entries = msg.entries.into_iter().map(|sc| store_command_from_proto(sc)).collect();

        let msg = FirstAccept {
            n,
            // entries,
        };

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::FirstAccept(msg),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn accept_decide(&self, request: Request<AcceptDecideReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        {
            use prost::Message;
            let encoded_len = msg.encoded_len();

            log(format!("{:?}  of size {} is decoding entries: {:?}", thread::current().id(), encoded_len, msg.entries).to_string());    
        }

        let n = ballot_from_proto(msg.n.unwrap());
        let ld = msg.ld;
        let entries = msg.entries.into_iter().map(|sc| store_command_from_proto(sc)).collect();
        
        let msg = AcceptDecide {
            n,
            ld,
            entries,
        };

        let msg = omnipaxos_core::messages::Message {
            from,
            to,
            msg: PaxosMsg::AcceptDecide(msg),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn accepted(&self, request: Request<AcceptedReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let n = ballot_from_proto(msg.n.unwrap());
        let la = msg.la;

        let msg = Accepted {
            n,
            la,
        };

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::Accepted(msg),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn decide(&self, request: Request<DecideReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let n = ballot_from_proto(msg.n.unwrap());
        let ld = msg.ld;

        let msg = Decide {
            n,
            ld,
        };

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::Decide(msg),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn proposal_forward(&self, request: Request<ProposalForwardReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let entries = msg.entries.into_iter().map(|sc| store_command_from_proto(sc)).collect();

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::ProposalForward(entries),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn compaction(&self, request: Request<CompactionReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let compaction = match msg.compaction.unwrap() {
            proto::compaction_req::Compaction::Trim(trim) => {
                Compaction::Trim(trim.trim)
            },
            proto::compaction_req::Compaction::Snapshot(ss) => {
                Compaction::Snapshot(ss)
            },
        };

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::Compaction(compaction),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn forward_compaction(&self, request: Request<ForwardCompactionReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let compaction = match msg.compaction.unwrap() {
            proto::forward_compaction_req::Compaction::Trim(trim) => {
                Compaction::Trim(trim.trim)
            },
            proto::forward_compaction_req::Compaction::Snapshot(ss) => {
                Compaction::Snapshot(ss)
            },
        };

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::ForwardCompaction(compaction),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn accept_stop_sign(&self, request: Request<AcceptStopSignReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let n = ballot_from_proto(msg.n.unwrap());
        let ss = stopsign_from_proto(msg.ss.unwrap());

        let msg = AcceptStopSign {
            n,
            ss,
        };

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::AcceptStopSign(msg),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn accepted_stop_sign(&self, request: Request<AcceptedStopSignReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let n = ballot_from_proto(msg.n.unwrap());

        let msg = AcceptedStopSign {
            n,
        };

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::AcceptedStopSign(msg),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }
    
    async fn decide_stop_sign(&self, request: Request<DecideStopSignReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let n = ballot_from_proto(msg.n.unwrap());

        let msg = DecideStopSign {
            n,
        };

        let msg = Message {
            from,
            to,
            msg: PaxosMsg::DecideStopSign(msg),
        };

        let server = self.server.clone();
        server.recv_sp_msg(msg);
        
        Ok(Response::new(Void {}))
    }

    async fn heartbeat_request(&self, request: Request<HeartbeatRequestReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let round = msg.round;

        let msg = HeartbeatRequest {
            round,
        };

        let msg = BLEMessage {
            from,
            to,
            msg: HeartbeatMsg::Request(msg),
        };

        let server = self.server.clone();
        server.recv_ble_msg(msg);
        
        Ok(Response::new(Void {}))
    }

    async fn heartbeat_reply(&self, request: Request<HeartbeatReplyReq>) -> Result<Response<Void>, tonic::Status> {
        let msg = request.into_inner();
        let from = msg.from;
        let to = msg.to;

        let round = msg.round;
        let ballot = ballot_from_proto(msg.ballot.unwrap());
        let majority_connected = msg.majority_connected;

        let msg = HeartbeatReply {
            round,
            ballot,
            majority_connected,
        };

        let msg = BLEMessage {
            from,
            to,
            msg: HeartbeatMsg::Reply(msg),
        };

        let server = self.server.clone();
        server.recv_ble_msg(msg);
        
        Ok(Response::new(Void {}))
    }
}
