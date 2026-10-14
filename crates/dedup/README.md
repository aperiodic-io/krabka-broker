# krabka-dedup

The index for produce-time, windowed, first-wins record deduplication.

Part of [krabka-broker](../../README.md), an Apache Kafka-compatible broker written in Rust.

## Overview

The broker drops a plain produce record when a record with the same dedup identity was admitted within the topic's window. The identity is the Kafka key, or the value of one configured header. This crate holds the index that makes that decision. It parses no batches and reads no clock: the broker gives it digested identities, record timestamps, and its own time, and ticks it at a regular interval. Dedup is a Krabka extension. Kafka has no KIP for it. Plain and idempotent batches are deduplicated, and a dropped record leaves a gap in the offsets, so producer sequence numbers stay intact. Transactional batches bypass it.

## Features

- First-wins classification of a whole produce request, in record order, with an undo list. The leader classifies under the partition's log lock, immediately before its append, and a failed append reverts exactly its own request, compare-and-revert.
- Expiry against the broker's clock, as Kafka's `retention.ms` does. Each call moves a watermark that never moves back. An entry is live while it is at most the window behind the watermark, and a lookup treats any other entry as absent. A record that is already older than the window on arrival, such as a backfill, is admitted and not indexed. So an index rebuilt from the last window of the log agrees with one that has run all along.
- A follower path, `populate`, that rebuilds the same index from the log. It is idempotent and keeps the highest timestamp per identity, and it sweeps at the same point as the leader path, so replicas that make the same calls at the same times hold the same entries in the same order.
- `tick`, which moves the watermark and drops expired entries from a bounded slice of the index, so a partition with no traffic gives its entries, its heap and its share of the broker budget back.
- Fixed-size entries: a 128-bit xxh3 digest and a timestamp, whatever the size of the identity.
- Bounded work per call. A cursor visits a few entries for each new identity, and at most `TICK_VISITS` (1024) for each tick. It drops expired entries and, for a new identity near the entry limit, the oldest quarter that its last pass measured.
- A segmented index: 64 hash tables, chosen by digest bits. A table that grows or shrinks moves only its own entries, so the largest single move is about 1/32 of the entry limit and not the whole index. A table that falls to a quarter of its capacity shrinks, so the heap follows the live entries down after a burst. `largest_relocation` and `heap_bytes` report both.
- A per-partition entry limit and an optional broker-wide `DedupBudget` shared across threads. A full index admits a new identity unindexed and counts it.
- Record timestamps clamped to the broker's time, so an entry dated ahead of the broker cannot stay live past its window.
- Counters for drops, clamped timestamps, unindexed records, records that arrived older than the window, capacity evictions, and partial drops that make the broker rebuild a batch.

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

// The append of the first request failed: forget what it indexed.
filter.revert(&first.undo);

// Call this at a regular interval, with the broker's time.
filter.tick(now_ms);
```

## Documentation

- [API documentation](https://krabka-io.github.io/krabka-broker/krabka-dedup/)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
