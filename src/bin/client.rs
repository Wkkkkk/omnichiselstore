use omnichiselstore::boost::*;
use omnichiselstore::util::dataloader::Loader;

use std::env;

use tokio::main;
use tokio::signal;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("not enough arguments.");
    }

    let loader = Loader::new(&args[1]);
    let total_size = loader.size();
    println!("loader: {}", total_size);
    loader.print_head();

    tokio::task::spawn(async {
        for sql in loader.queries {
            let _res = query(1, sql).await.unwrap();
        }
    }).await.unwrap();

    println!("all queries are done");
}