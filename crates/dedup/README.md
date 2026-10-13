# krabka-dedup

The index for produce-time, windowed, first-wins record deduplication.

Part of [krabka-broker](../../README.md), an Apache Kafka-compatible broker written in Rust.

## Overview

The broker drops a plain produce record when a record with the same dedup identity was admitted within the topic's window. The identity is the Kafka key, or the value of one configured header. This crate holds the index that makes that decision. It parses no batches and reads no clock: the broker gives it digested identities, record timestamps, and its own time. Dedup is a Krabka extension. Kafka has no KIP for it, and idempotent and transactional batches bypass it.

## Features

- First-wins classification of a whole produce request, in record order, with an undo list. A failed replication reverts exactly its own request, compare-and-revert.
- A follower path, `populate`, that rebuilds the same index from the log. It is idempotent and keeps the highest timestamp per identity, and it sweeps at the same point as the leader path, so replicas hold the same entries in the same order.
- Fixed-size entries: a 128-bit xxh3 digest and a timestamp, whatever the size of the identity.
- Bounded work per record. A cursor visits a few entries for each new identity, drops expired ones, and near the entry limit drops the oldest quarter that its last pass measured.
- A per-partition entry limit and an optional broker-wide `DedupBudget` shared across threads. A full index admits a new identity unindexed and counts it.
- Record timestamps clamped to the broker's time, so one future-dated record cannot disable dedup for a partition.
- Counters for drops, clamped timestamps, unindexed records, capacity evictions, and partial drops that make the broker rebuild a batch.

## Usage

```rust
use krabka_dedup::{DedupRecord, DedupWindowFilter, IdentityDigest, IdentityLookup, identity_of};

let mut filter = DedupWindowFilter::new(60_000, DedupWindowFilter::DEFAULT_MAX_ENTRIES, None);
let now_ms = 1_700_000_000_000;

let record = |key: &[u8], timestamp_ms| {
    let identity = match identity_of(Some(key), [], None) {
        IdentityLookup::Identity(bytes) => Some(IdentityDigest::of(bytes)),
        IdentityLookup::NoIdentity | IdentityLookup::MissingHeader => None,
    };
    DedupRecord { identity, timestamp_ms }
};

let first = filter.classify(&[record(b"order-1", now_ms)], now_ms);
assert2::assert!(first.all_kept());

let again = filter.classify(&[record(b"order-1", now_ms)], now_ms);
assert2::assert!(again.none_kept());

// Replication of the first request failed: forget what it indexed.
filter.revert(&first.undo);
```

## Documentation

- [API documentation](https://krabka-io.github.io/krabka-broker/krabka-dedup/)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
