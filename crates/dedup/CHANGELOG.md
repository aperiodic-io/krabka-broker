# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-dedup`.

## [Unreleased]

### Changed

- Expiry uses the broker's clock, as Kafka's `retention.ms` does, not the
  newest record timestamp. Each call moves a watermark to its `now_ms`, and the
  watermark never moves back. An entry is live while it is at most the window
  behind the watermark, and a lookup treats an entry that is not live as
  absent, before or after a sweep removes it. A running index and an index
  rebuilt from the last window of the log now agree for streams whose
  `CreateTime`s are old.
- A record that is already more than the window behind the broker's time when
  it arrives is admitted and not indexed, on the leader and on the follower
  path, and counted in `DedupCounters::expired_on_arrival_records`.
- `DedupWindowFilter::max_timestamp_ms` is replaced by `watermark_ms`.

- The index of a partition is 64 hash tables, not one. When a table grows or
  shrinks, it moves only its own entries. At two million identities, the
  slowest single `classify` falls from about 90 ms to a few milliseconds. Each
  table holds at most `max(2 * ceil(max_entries / 64), 4096)` entries, so
  identities chosen to share a table cannot bring the whole-index move back.
- A table that falls to a quarter of its capacity shrinks to twice its length,
  so the heap follows the live entries down after a burst expires.

### Added

- `DedupWindowFilter::SEGMENTS`, `heap_bytes` and `largest_relocation`.
- `DedupWindowFilter::tick` and `TICK_VISITS`. A tick moves the watermark and
  drops expired entries from a slice of at most 1024 entries. A partition with
  no traffic then gives back its expired entries, its heap and its share of
  the broker-wide `DedupBudget`. Before, an idle partition held them forever,
  and sixteen idle partitions of a million entries each filled the default
  budget.
- `DedupCounters::expired_on_arrival_records`.

### Fixed

- The docs said that idempotent batches bypass dedup and that only the leader
  consults the index, before replication. The broker deduplicates idempotent
  batches, and the leader classifies under the log lock immediately before
  its append. Only transactional batches bypass dedup.
