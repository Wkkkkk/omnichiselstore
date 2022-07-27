use omnichiselstore::boost::*;

use std::{
    env,
    fs::File,
    io::{prelude::*, BufReader},
    time::Instant,
};

use tokio::main;
use tokio::signal;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("not enough arguments.");
    }
    
    let file = File::open(&args[1]).expect("no such file");
    let reader = BufReader::new(file);

    let replica_id: u64 = args[2].parse().unwrap();
    let mut starting_point: usize = 0;
    if args.len() == 4 {
        starting_point = args[3].parse().unwrap();
    }

    let now = Instant::now();
    tokio::task::spawn(async move {
        for (i, line) in reader.lines().enumerate() {
            if i < starting_point { continue; }
            let _res = query(replica_id, line.unwrap()).await.unwrap();
        }
    }).await.unwrap();
    let elapsed = now.elapsed();
    let time_to_finish_workload = elapsed.as_secs() as u64;

    println!("all queries are done in {} seconds", time_to_finish_workload);
}