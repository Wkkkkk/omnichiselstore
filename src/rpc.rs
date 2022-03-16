//! ChiselStore RPC module.

use crate::rpc::proto::rpc_server::Rpc;
use crate::{Consistency, StoreCommand, StoreServer, StoreTransport};
use async_mutex::Mutex;
use async_trait::async_trait;
use crossbeam::queue::ArrayQueue;
use derivative::Derivative;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use omnipaxos_core::{
    ballot_leader_election::messages::{BLEMessage, HeartbeatMsg, HeartbeatRequest, HeartbeatReply},
    messages::{
        Message, PaxosMsg, Prepare, Promise, AcceptSync, 
        FirstAccept, AcceptDecide, Accepted, Decide, 
        ProposalForward, Compaction, ForwardCompaction, 
        AcceptStopSign, AcceptedStopSign, DecideStopSign,
    },
    util::{SyncItem}
};

#[allow(missing_docs)]
pub mod proto {
    tonic::include_proto!("proto");
}

use proto::rpc_client::RpcClient;
use proto::{
    Query, QueryResults, QueryRow, Void,
    Ballot, StopSign, PrepareReq, PromiseReq, 
    AcceptSyncReq, FirstAcceptReq, AcceptDecideReq, AcceptedReq, 
    DecideReq, AcceptStopSignReq, AcceptedStopSignReq, DecideStopSignReq,
    HeartbeatRequestReq, HeartbeatReplyReq,
};

type NodeAddrFn = dyn Fn(usize) -> String + Send + Sync;

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

/// RPC transport.
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

#[async_trait]
impl StoreTransport for RpcTransport {
    fn send_sp(&self, to_id: usize, msg: Message<StoreCommand, ()>) {
        match msg {
            Message::AppendEntryRequest {
                from_id,
                term,
                prev_log_index,
                prev_log_term,
                entries,
                commit_index,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let prev_log_index = prev_log_index as u64;
                let prev_log_term = prev_log_term as u64;
                let entries = entries
                    .iter()
                    .map(|entry| {
                        let id = entry.transition.id as u64;
                        let index = entry.index as u64;
                        let sql = entry.transition.sql.clone();
                        let term = entry.term as u64;
                        LogEntry {
                            id,
                            sql,
                            index,
                            term,
                        }
                    })
                    .collect();
                let commit_index = commit_index as u64;
                let request = AppendEntriesRequest {
                    from_id,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    commit_index,
                };
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client.conn.append_entries(request).await.unwrap();
                });
            }
            Message::AppendEntryResponse {
                from_id,
                term,
                success,
                last_index,
                mismatch_index,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let last_index = last_index as u64;
                let mismatch_index = mismatch_index.map(|idx| idx as u64);
                let request = AppendEntriesResponse {
                    from_id,
                    term,
                    success,
                    last_index,
                    mismatch_index,
                };
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let request = tonic::Request::new(request.clone());
                    client
                        .conn
                        .respond_to_append_entries(request)
                        .await
                        .unwrap();
                });
            }
            Message::VoteRequest {
                from_id,
                term,
                last_log_index,
                last_log_term,
            } => {
                let from_id = from_id as u64;
                let term = term as u64;
                let last_log_index = last_log_index as u64;
                let last_log_term = last_log_term as u64;
                let request = VoteRequest {
                    from_id,
                    term,
                    last_log_index,
                    last_log_term,
                };
                let peer = (self.node_addr)(to_id);
                let pool = self.connections.clone();
                tokio::task::spawn(async move {
                    let mut client = pool.connection(peer).await;
                    let vote = tonic::Request::new(request.clone());
                    client.conn.vote(vote).await.unwrap();
                });
            }
            Message::VoteResponse {
                from_id,
                term,
                vote_granted,
            } => {
                let peer = (self.node_addr)(to_id);
                tokio::task::spawn(async move {
                    let from_id = from_id as u64;
                    let term = term as u64;
                    let response = VoteResponse {
                        from_id,
                        term,
                        vote_granted,
                    };
                    if let Ok(mut client) = RpcClient::connect(peer.to_string()).await {
                        let response = tonic::Request::new(response.clone());
                        client.respond_to_vote(response).await.unwrap();
                    }
                });
            }
        }
    }

    fn send_ble(&self, to_id: usize, msg: BLEMessage) {

    }
}

/// RPC service.
#[derive(Debug)]
pub struct RpcService {
    /// The ChiselStore server access via this RPC service.
    pub server: Arc<StoreServer<RpcTransport>>,
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
        let query = request.into_inner();
        
        let server = self.server.clone();
        let results = match server.query(query.sql).await {
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
        
        let mut sync_item: Option<SyncItem<StoreCommand,()>> = match msg.sync_item {
            Some(si) => Some(sync_item_from_proto(si)),
            _ => None,
        };
        
        let ld = msg.ld;
        let la = msg.la;

        let mut stopsign: Option<omnipaxos_core::storage::StopSign> = match msg.stop_sign {
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
        
        let sync_item = sync_item_from_proto(message.sync_item.unwrap());
        let sync_idx = msg.sync_idx;

        let decide_idx = msg.decide_idx;
        
        let mut stopsign: Option<omnipaxos_core::storage::StopSign> = match msg.stop_sign {
            Some(ss) => Some(stopsign_from_proto(ss)),
            _ => None,
        };

        let msg = AcceptSync {
            n,
            sync_item,
            sync_idx,
            decide_idx,
            stopsign,
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
        let entries = msg.entries.into_iter().map(|sc| store_command_from_proto(sc)).collect();

        let msg = FirstAccept {
            n,
            entries,
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

        let n = ballot_from_proto(msg.n.unwrap());
        let ld = msg.ld;
        let entries = msg.entries.into_iter().map(|sc| store_command_from_proto(sc)).collect();

        let msg = AcceptDecide {
            n,
            ld,
            entries,
        };

        let msg = Message {
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
    let metadata = Some(ss.metadata.map(|md| => md as u8).collect());
    
    omnipaxos_core::storage::StopSign {
        config_id,
        nodes,
        metadata,
    }
}

fn sync_item_from_proto(si: proto::SyncItem) -> SyncItem<StoreCommand,()>{
    match si {
        proto::SyncItem::Item::Entries(entries) => {
            let entries = entries.store_commands.into_iter().map(|sc| store_command_from_proto(sc)).collect();
            return SyncItem::Entries(entries);
        },
        proto::SyncItem::Item::Snapshot(_) => return SyncItem::Snapshot(omnipaxos_core::storage::Snapshot<StoreCommand,()>),
        proto::SyncItem::Item::None(_) => return SyncItem::None,
    }
}