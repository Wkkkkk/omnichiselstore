//! ChiselStore server module.

use crate::errors::StoreError;
use async_notify::Notify;
use async_trait::async_trait;
use std::thread::sleep;
use std::time::Duration;
use crossbeam_channel as channel;
use crossbeam_channel::{Receiver, Sender};
use derivative::Derivative;
use sqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use omnipaxos_core::{
    ballot_leader_election::{BLEConfig, BallotLeaderElection, Ballot},
    ballot_leader_election::messages::BLEMessage,
    sequence_paxos::{SequencePaxos, SequencePaxosConfig},
    storage::{Storage, Snapshot, StopSignEntry},
    messages::Message,
};

/// ChiselStore transport layer.
///
/// Your application should implement this trait to provide network access
/// to the ChiselStore server.
#[async_trait]
pub trait StoreTransport {
    /// Send a store command message `msg` to `to_id` node.
    fn send_sp(&self, to_id: u64, msg: Message<StoreCommand, ()>);
    fn send_ble(&self, to_id: u64, msg: BLEMessage);
}

/// Store command.
///
/// A store command is a SQL statement that is replicated in the Raft cluster.
#[derive(Clone, Debug)]
pub struct StoreCommand {
    /// Unique ID of this command.
    pub id: u64,
    /// The SQL statement of this command.
    pub sql: String,
}

// Used for handling async queries
// #[derive(Clone, Debug)]
#[derive(Debug)]
pub struct QueryResultsHolder {
    query_completion_notifiers: HashMap<u64, Arc<Notify>>,
    results: HashMap<u64, Result<QueryResults, StoreError>>,
}

impl QueryResultsHolder {
    pub fn insert_notifier(&mut self, id: u64, notifier: Arc<Notify>) {
        self.query_completion_notifiers.insert(id, notifier);
    }

    pub fn push_result(&mut self, id: u64, result: Result<QueryResults, StoreError>) {
        if let Some(completion) = self.query_completion_notifiers.remove(&(id as u64)) {
            self.results.insert(id as u64, result);
            completion.notify();
        }
    }

    pub fn remove_result(&mut self, id: &u64) -> Option<Result<QueryResults, StoreError>> {
        self.results.remove(id)
    }
    
    fn default() -> Self {
        Self {
            query_completion_notifiers: HashMap::new(),
            results: HashMap::new(),
        }
    }
}

/// Store configuration.
#[derive(Debug)]
struct StoreConfig {
    /// Connection pool size.
    conn_pool_size: usize,
    query_results_holder: Arc<Mutex<QueryResultsHolder>>,
}

#[derive(Clone)]
#[derive(Derivative)]
#[derivative(Debug)]
struct SQLiteStore<S>
where
    S: Snapshot<StoreCommand>,
{
    /// Vector which contains all the replicated entries in-memory.
    log: Vec<StoreCommand>,
    /// Last promised round.
    n_prom: Ballot,
    /// Last accepted round.
    acc_round: Ballot,
    /// Length of the decided log.
    ld: u64,

    // TMP snapshots impl

    /// Garbage collected index.
    trimmed_idx: u64,
    /// Stored snapshot
    snapshot: Option<S>,
    /// Stored StopSign
    stopsign: Option<omnipaxos_core::storage::StopSignEntry>,

    /// SQLite
    this_id: u64,
    #[derivative(Debug = "ignore")]
    conn_pool: Vec<Arc<Mutex<Connection>>>,
    conn_idx: usize,
    query_results_holder: Arc<Mutex<QueryResultsHolder>>,
}

impl <S> SQLiteStore<S>
where
    S: Snapshot<StoreCommand>
{
    pub fn new(this_id: u64, config: StoreConfig) -> Self {
        let mut conn_pool = vec![];
        let conn_pool_size = config.conn_pool_size;
        for _ in 0..conn_pool_size {
            // FIXME: Let's use the 'memdb' VFS of SQLite, which allows concurrent threads
            // accessing the same in-memory database.
            let flags = OpenFlags::new()
                .set_read_write()
                .set_create()
                .set_no_mutex();
            let mut conn =
                Connection::open_with_flags(format!("node{}.db", this_id), flags).unwrap();
            conn.set_busy_timeout(5000).unwrap();
            conn_pool.push(Arc::new(Mutex::new(conn)));
        }
        let conn_idx = 0;
        SQLiteStore {
            log: Vec::new(),
            n_prom: Ballot::default(),
            acc_round: Ballot::default(),
            ld: 0,

            trimmed_idx: 0,
            snapshot: None,
            stopsign: None,

            this_id,
            conn_pool,
            conn_idx,

            query_results_holder: config.query_results_holder,
        }
    }

    pub fn get_connection(&mut self) -> Arc<Mutex<Connection>> {
        let idx = self.conn_idx % self.conn_pool.len();
        let conn = &self.conn_pool[idx];
        self.conn_idx += 1;
        conn.clone()
    }
}

fn query(conn: Arc<Mutex<Connection>>, sql: String) -> Result<QueryResults, StoreError> {
    let conn = conn.lock().unwrap();
    let mut rows = vec![];
    conn.iterate(sql, |pairs| {
        let mut row = QueryRow::new();
        for &(_, value) in pairs.iter() {
            row.values.push(value.unwrap().to_string());
        }
        rows.push(row);
        true
    })?;
    Ok(QueryResults { rows })
}

impl<S> Storage<StoreCommand, S> for SQLiteStore<S>
where
    S: Snapshot<StoreCommand>,
{
    fn append_entry(&mut self, entry: StoreCommand) -> u64 {
        self.log.push(entry);
        self.get_log_len()
    }

    fn append_entries(&mut self, entries: Vec<StoreCommand>) -> u64 {
        let mut e = entries;
        self.log.append(&mut e);
        self.get_log_len()
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<StoreCommand>) -> u64 {
        self.log.truncate(from_idx as usize);
        self.append_entries(entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) {
        self.n_prom = n_prom;
    }

    fn set_decided_idx(&mut self, ld: u64) {
        // run queries
        let queries_to_run = self.log[(self.ld as usize)..(ld as usize)].to_vec();
        for q in queries_to_run.iter() {
            let conn = self.get_connection();
            let results = query(conn, q.sql.clone());

            let mut query_results_holder = self.query_results_holder.lock().unwrap();
            query_results_holder.push_result(q.id, results);
        }

        self.ld = ld;
    }

    fn get_decided_idx(&self) -> u64 {
        self.ld
    }

    fn set_accepted_round(&mut self, na: Ballot) {
        self.acc_round = na;
    }

    fn get_accepted_round(&self) -> Ballot {
        self.acc_round
    }

    fn get_entries(&self, from: u64, to: u64) -> &[StoreCommand] {
        self.log.get(from as usize..to as usize).unwrap_or(&[])
    }

    fn get_log_len(&self) -> u64 {
        self.log.len() as u64
    }

    fn get_suffix(&self, from: u64) -> &[StoreCommand] {
        match self.log.get(from as usize..) {
            Some(s) => s,
            None => &[],
        }
    }

    fn get_promise(&self) -> Ballot {
        self.n_prom
    }

    // TEMP Snapshots impl
    fn set_stopsign(&mut self, s: StopSignEntry) {
        self.stopsign = Some(s);
    }

    fn get_stopsign(&self) -> Option<StopSignEntry> {
        self.stopsign.clone()
    }

    fn trim(&mut self, trimmed_idx: u64) {
        self.log.drain(0..trimmed_idx as usize);
    }

    fn set_compacted_idx(&mut self, trimmed_idx: u64) {
        self.trimmed_idx = trimmed_idx;
    }

    fn get_compacted_idx(&self) -> u64 {
        self.trimmed_idx
    }

    fn set_snapshot(&mut self, snapshot: S) {
        self.snapshot = Some(snapshot);
    }

    fn get_snapshot(&self) -> Option<S> {
        self.snapshot.clone()
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct StoreServer<T: StoreTransport + Send + Sync> {
    this_id: u64,
    next_cmd_id: AtomicU64,
    #[derivative(Debug = "ignore")]
    sequence_paxos: Arc<Mutex<SequencePaxos<StoreCommand, (), SQLiteStore<()>>>>,
    #[derivative(Debug = "ignore")]
    ballot_leader_election: Arc<Mutex<BallotLeaderElection>>,
    sp_notifier_rx: Receiver<Message<StoreCommand, ()>>,
    sp_notifier_tx: Sender<Message<StoreCommand, ()>>,
    ble_notifier_rx: Receiver<BLEMessage>,
    ble_notifier_tx: Sender<BLEMessage>,
    transport: T,
    query_results_holder: Arc<Mutex<QueryResultsHolder>>,
    halt: Arc<Mutex<bool>>,
}

/// Query row.
#[derive(Debug)]
pub struct QueryRow {
    /// Column values of the row.
    pub values: Vec<String>,
}

impl QueryRow {
    fn new() -> Self {
        QueryRow { values: Vec::new() }
    }
}

/// Query results.
#[derive(Debug)]
pub struct QueryResults {
    /// Query result rows.
    pub rows: Vec<QueryRow>,
}

const HEARTBEAT_TIMEOUT: u64 = 20; // ticks until timeout
impl<T: StoreTransport + Send + Sync> StoreServer<T> {
    /// Start a new server as part of a ChiselStore cluster.
    pub fn start(this_id: u64, peers: Vec<u64>, transport: T) -> Result<Self, StoreError> {
        // sequence paxos
        let configuration_id = 1;

        let mut sp_config = SequencePaxosConfig::default();
        sp_config.set_configuration_id(configuration_id);
        sp_config.set_pid(this_id);
        sp_config.set_peers(peers.to_vec());

        let query_results_holder = Arc::new(Mutex::new(QueryResultsHolder::default()));
        let store_config = StoreConfig { conn_pool_size: 20, query_results_holder: query_results_holder.clone() };
        let sqlite_store = SQLiteStore::new(this_id, store_config);
        
        let sp = Arc::new(Mutex::new(SequencePaxos::with(sp_config, sqlite_store)));

        // ballot leader election
        let mut ble_config = BLEConfig::default();
        ble_config.set_pid(this_id);
        ble_config.set_peers(peers);
        ble_config.set_hb_delay(HEARTBEAT_TIMEOUT);

        let ble = Arc::new(Mutex::new(BallotLeaderElection::with(ble_config)));

        // transport channels
        let (sp_notifier_tx, sp_notifier_rx) = channel::unbounded();
        let (ble_notifier_tx, ble_notifier_rx) = channel::unbounded();
        
        Ok(StoreServer {
            this_id,
            next_cmd_id: AtomicU64::new(0),
            sequence_paxos: sp,
            ballot_leader_election: ble,
            sp_notifier_rx,
            sp_notifier_tx,
            ble_notifier_rx,
            ble_notifier_tx,
            transport,
            query_results_holder,
            halt: Arc::new(Mutex::new(false)),
        })
    }

    /// Run the blocking event loop.
    pub fn run(&self) {
        loop {
            if *self.halt.lock().unwrap() {
                break
            }

            let mut sequence_paxos = self.sequence_paxos.lock().unwrap();
            let mut ballot_leader_election = self.ballot_leader_election.lock().unwrap();

            if let Some(leader) = ballot_leader_election.tick() {
                // a new leader is elected, pass it to SequencePaxos.
                sequence_paxos.handle_leader(leader);
            }

            // check incoming messages

            match self.sp_notifier_rx.try_recv() {
                Ok(msg) => {
                    sequence_paxos.handle(msg);
                },
                _ => {},
            };


            match self.ble_notifier_rx.try_recv() {
                Ok(msg) => {
                    ballot_leader_election.handle(msg);
                },
                _ => {},
            };

            // send outgoing messages

            for out_msg in sequence_paxos.get_outgoing_msgs() {
                let receiver = out_msg.to;
                self.transport.send_sp(receiver, out_msg);
            }

            for out_msg in ballot_leader_election.get_outgoing_msgs() {
                let receiver = out_msg.to;
                self.transport.send_ble(receiver, out_msg);
            }

            sleep(Duration::from_millis(10));
        }
    }

    /// Execute a SQL statement on the ChiselStore cluster.
    pub async fn query<S: AsRef<str>>(
        &self,
        stmt: S,
    ) -> Result<QueryResults, StoreError> {
        let results = {
            let (notify, id) = {
                let id = self.next_cmd_id.fetch_add(1, Ordering::SeqCst);
                let cmd = StoreCommand {
                    id: id,
                    sql: stmt.as_ref().to_string(),
                };
                
                let notify = Arc::new(Notify::new());

                let mut query_results_holder = self.query_results_holder.lock().unwrap();
                query_results_holder.insert_notifier(id, notify.clone());
                
                let mut sequence_paxos = self.sequence_paxos.lock().unwrap();
                sequence_paxos.append(cmd).expect("Failed to append");

                (notify, id)
            };

            // wait for append (and decide) to finish in background
            notify.notified().await;
            let results = self.query_results_holder.lock().unwrap().remove_result(&id).unwrap();
            results?
        };
        Ok(results)
    }

    /// Receive a sequence paxos message from the ChiselStore cluster.
    pub fn recv_sp_msg(&self, msg: Message<StoreCommand, ()>) {
        self.sp_notifier_tx.send(msg).unwrap();
    }
    
    /// Receive a ballot leader election message from the ChiselStore cluster.
    pub fn recv_ble_msg(&self, msg: BLEMessage) {
        self.ble_notifier_tx.send(msg).unwrap();
    }

    /// Used to shutdown replica
    pub fn set_halt(&self, new_halt: bool) {
        let mut halt = self.halt.lock().unwrap();
        *halt = new_halt;
    }

    pub fn is_leader(&self) -> bool {
        let sequence_paxos = self.sequence_paxos.lock().unwrap();
        sequence_paxos.get_current_leader() == (self.this_id as u64)
    }

    pub fn get_id(&self) -> u64 {
        self.this_id
    }
}
