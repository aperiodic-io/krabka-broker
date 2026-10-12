# krabka-raft

[API documentation](https://krabka-io.github.io/krabka-broker/krabka-raft/krabka_raft/index.html)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Metadata KRaft quorum (KIP-595 KraftController) for Krabka.

This crate is part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add krabka-raft --path ../krabka-broker/crates/raft
```

This crate is not published by this repository. Adjust the path to your local checkout. Workspace development uses the path dependencies declared in this repository.

## Usage example

Run the [typed-input example](examples/quorum_state.rs) without starting a controller:

<!-- snippet: raft/examples/quorum_state.rs#quorum_state -->
```rust
use assert2::assert;
use krabka_metadata::{KRaftVersionRange, Voter, VoterEndpoint, VoterSet};
use krabka_raft::kraft::{NodeId, QuorumState, QuorumStateMachine};
use krabka_units::prelude::millis;
use uuid::Uuid;

fn main() {
    let node = NodeId(1);
    let voters = VoterSet::from_voters([Voter {
        id: node,
        directory_id: Uuid::new_v4(),
        endpoints: vec![VoterEndpoint {
            name: "CONTROLLER".into(),
            host: "127.0.0.1".into(),
            port: 9093,
        }],
        kraft_version: KRaftVersionRange { min: 0, max: 1 },
    }]);
    let state = QuorumState::bootstrap(Uuid::new_v4(), voters);
    let machine = QuorumStateMachine::new(node, state, millis(1_000));
    assert!(machine.is_voter());
}
```
<!-- /snippet -->

## Documentation

- [Design](docs/design.md) — the sans-IO consensus core, the single-owner engine, submission and commit, snapshots, KIP-853 reconfiguration, and how the broker's controller logic plugs in.

Read the [krabka-raft API documentation](https://krabka-io.github.io/krabka-broker/krabka-raft/krabka_raft/index.html). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
