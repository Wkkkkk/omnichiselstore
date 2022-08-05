#! /bin/bash

for BATCH_SIZE in 1 5 10 15 25 50 75 100 500 1000 5000 10000 20000 50000 100000; do
  # run a fixed time
  ./target/release/client datasets/queries.txt 1 $BATCH_SIZE 60
done

