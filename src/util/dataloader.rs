use std::{
    fs::File,
    io::{prelude::*, BufReader},
};

#[derive(Debug)]
pub struct Loader {
    pub queries:  Vec<String>,
}

impl Loader {
    pub fn new(filepath: &str) -> Loader {
        let file = File::open(filepath).expect("no such file");
        let buf = BufReader::new(file);
        let lines = buf.lines()
                    .map(|l| l.expect("Could not parse line"))
                    .collect();

        Loader {queries : lines}
    }

    pub fn size(&self) -> usize {
        self.queries.len()
    }

    pub fn print_head(&self) {
        println!("queries:");
        for i in 0..5 {
            println!("{:?}", self.queries[i]);
        }
    }
}