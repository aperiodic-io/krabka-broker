# krabka-records-legacy

[API documentation](https://krabka-io.github.io/krabka-broker/krabka-records-legacy/krabka_records_legacy/index.html)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Apache Kafka legacy (v0/v1) `MessageSet` codec, with bridges to and from the v2 `RecordBatch` types.

See the [Kafka message format docs](https://kafka.apache.org/43/implementation/message-format/) for the wire layout that this crate implements. v0 carries no per-message timestamp. v1 adds an `i64` timestamp to each message (KIP-32). Both formats signal compression in the low 3 bits of the per-message `attributes` byte. The compressed payload is a single outer message. The `value` of that outer message is a nested, uncompressed MessageSet.

This crate is part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add krabka-records-legacy --path ../krabka-broker/crates/records-legacy
```

This crate is not published by this repository. Adjust the path to your local checkout. Workspace development uses the path dependencies declared in this repository.

## Usage example

Encode and decode a Kafka v1 MessageSet:

```rust
use bytes::{Bytes, BytesMut};
use krabka_records_legacy::{
    Magic, ParsedRecord, decode_message_set, encode_flat_message_set,
};

let records = vec![ParsedRecord {
    offset: 42,
    timestamp: Some(1_713_000_000_000),
    key: Some(Bytes::from_static(b"order-42")),
    value: Some(Bytes::from_static(b"created")),
}];

let mut buf = BytesMut::new();
encode_flat_message_set(records, Magic::V1, &mut buf);
let decoded = decode_message_set(&mut &buf[..], buf.len()).unwrap();
assert_eq!(decoded[0].offset, 42);
```

## Compression

Every legacy compression codec is always available: the crate depends on
`krabka-compression` with `gzip`, `snappy`, `lz4` and `zstd` enabled, and
exposes no cargo features of its own.

## Documentation

Read the [krabka-records-legacy API documentation](https://krabka-io.github.io/krabka-broker/krabka-records-legacy/krabka_records_legacy/index.html). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
