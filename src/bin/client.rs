use omnichiselstore::boost::*;

use std::{
    env,
    fs::File,
    io::{prelude::*, BufReader},
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
    let mut reader = BufReader::new(file);

    let replica_id: u64 = args[2].parse().unwrap();

    tokio::task::spawn(async move {
        for line in reader.lines() {
            let _res = query(replica_id, line.unwrap()).await.unwrap();
        }
    }).await.unwrap();

    println!("all queries are done");
}