use omnichiselstore::boost::*;

extern crate itertools;
use itertools::Itertools;

use std::{
    env,
    fs::{File, OpenOptions},
    io::{prelude::*, BufReader, Write},
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
    let mut chunk_size: usize = 1;
    if args.len() >= 4 {
        chunk_size = args[3].parse().unwrap();
    }
    let mut maximum_runtime: u64 = 60 * 30; 
    if args.len() >= 5 {
        maximum_runtime = args[4].parse().unwrap();
    }

    let mut f = OpenOptions::new()
        .append(true)
        .create(true) // Optionally create the file if it doesn't already exist
        .open("runtimes.txt")
        .expect("Unable to create file");    
    let now = Instant::now();
    let all_lines = reader.lines()
                        .map(|l| l.unwrap())
                        .collect::<Vec<_>>();

    for batch in all_lines.chunks(chunk_size) {
        let elapsed = now.elapsed();
        let has_run = elapsed.as_secs() as u64;
        if has_run > maximum_runtime { break };

        let start = Instant::now();
        let batch_size = batch.len();

        let _result = query(replica_id, batch.to_vec()).await.unwrap();

        let end = start.elapsed();
        let time_in_micros = end.as_micros() as u64;
        let throughput = 1000000 * batch_size as u64 / time_in_micros;
        println!("batch_size: {}, throughput: {}, time_in_micros: {} \n", batch_size, throughput, time_in_micros);
        let output_str = format!("{}, {}, {} \n", chunk_size, throughput, time_in_micros);
        f.write_all(output_str.as_bytes()).expect("Unable to write data");
    }

    let elapsed = now.elapsed();
    let time_to_finish_workload = elapsed.as_secs() as u64;

    println!("all queries are done in {} seconds", time_to_finish_workload);
}