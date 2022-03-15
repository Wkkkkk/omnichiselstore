# OmniChiselStore

Replaces [Little Raft](https://github.com/andreev-io/little-raft) in [ChiselStore](https://github.com/chiselstrike/chiselstore) with [Omnipaxos](https://github.com/haraldng/omnipaxos).

## Tests

Several integration tests can be found in `tests/integration_test.rs`. However, because the tests run using multiple threads and by spawning new servers in each test, to run the tests reliably each test needs to be run by itself, e.g.:

```zsh
cargo test write_read -- --nocapture
cargo test sequential_writes -- --nocapture
```

## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in ChiselStore by you, shall be licensed as MIT, without any additional terms or conditions.
