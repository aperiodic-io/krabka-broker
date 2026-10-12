# krabka-broker

[API documentation](https://krabka-io.github.io/krabka-broker/krabka-broker/krabka_broker/index.html)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Single-node Apache Kafka-compatible broker (MVP).

This crate is part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add krabka-broker --path ../krabka-broker/crates/broker
```

This crate is not published by this repository. Adjust the path to your local checkout. Workspace development uses the path dependencies declared in this repository.

## Usage example

Start a single-node broker with a local data directory:

```rust,no_run
use std::net::SocketAddr;
use krabka_broker::{Broker, BrokerConfig};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let listen_addr: SocketAddr = "127.0.0.1:9092".parse()?;
let config = BrokerConfig {
    listen_addr,
    advertised_listener: listen_addr.to_string(),
    log_dir: "./target/krabka-data".into(),
    ..BrokerConfig::default()
};

let broker = Broker::start(config).await?;
tokio::signal::ctrl_c().await?;
broker.shutdown().await;
# Ok(())
# }
```

For OAUTHBEARER on inter-broker and controller-listener connections, enable
`OAUTHBEARER` on the corresponding SASL listeners and point the shared
outbound credentials at a token file:

```toml
controller_listener_protocol = "SaslPlaintext"

[inter_broker_credentials]
type = "oauth-bearer"
token_path = "/var/run/secrets/krabka/inter-broker-token"
```

The broker validates the file at startup and re-reads it for every new
connection, so replacing the mounted token rotates credentials without a
broker restart. Inbound validation uses the existing `[oauthbearer]` policy.

## Design documents

- [Diskless WAL](docs/diskless-wal-design.md) — the quorum WAL, controller-assigned offsets, the object flusher, and the cold-read path behind `krabka.diskless=true`.
- [Replication and ISR](docs/replication-isr-design.md) — follower replication, the high watermark, ISR maintenance, and leader failover.
- [Transaction coordinator](docs/transaction-coordinator-design.md) — the `__transaction_state` coordinator, the `EndTxn` phases, and KIP-939 two-phase commit.
- [Witness broker role and stretch cluster](../../docs/KFCs/KFC-2-witness-broker-stretch-cluster.md) — the data-bearing witness role, and the three-site profile that keeps `acks=all` writes alive through the loss of any one site.
- [Consistent cross-topic snapshots](../../docs/KFCs/KFC-4-cross-topic-snapshots.md) — the barrier markers a coordinator injects across a topic set, and the cuts they define.

## Documentation

Read the [krabka-broker API documentation](https://krabka-io.github.io/krabka-broker/krabka-broker/krabka_broker/index.html). The repository README contains the project-wide setup, development, and release notes.

- [Configuration reference](../../docs/config-reference.md), generated from `krabka-broker --print-config-schema`; examples in [docs/examples/](../../docs/examples/).

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
