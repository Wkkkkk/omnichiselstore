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
    if args.len() < 2 {
        panic!("not enough arguments.");
    }
    
    let file = File::open(&args[1]).expect("no such file");
    let mut reader = BufReader::new(file);

    tokio::task::spawn(async {
        for line in reader.lines() {
            let _res = query(1, line.unwrap()).await.unwrap();
        }
    }).await.unwrap();

    println!("all queries are done");
}