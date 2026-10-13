# KFC-10: Produce-Time Record Deduplication

A topic can ask the broker to drop, at produce time, a record whose key or chosen header was already written within a time window. The first record wins, and a dropped record leaves a gap in the offsets as compaction does.

## Status

**Draft.** The implementation is in pull request [aperiodic-io/krabka-broker#1](https://github.com/aperiodic-io/krabka-broker/pull/1).

The design is a port of the windowed deduplication in our Redpanda fork (`dream-faster/redpanda`, `docs/rfcs/20260730_log_derived_dedup_state.md`). That fork ran the feature under load and found most of the failure modes this document guards against. The krabka port keeps its index design, adapts its write path to a Kafka-style replicated log, and closes four of its documented gaps.

No KIP defines record deduplication. Kafka's idempotent producer (KIP-98) removes the duplicates that one producer session makes when it retries a batch. It does not remove a record that a second producer, a restarted producer, or a replayed upstream system writes again.

## Motivation

Many pipelines write the same record more than once, and the broker is the only component that sees every copy.

A connector that restarts from its last committed source offset writes again everything after that offset. A service that consumes, transforms and produces without transactions writes a record again after a rebalance. Two instances of a collector that scrape the same source write the same event twice. An upstream system that retries on timeout sends a message the broker already holds. Idempotent producers do not help in any of these cases, because each copy comes from a different producer session or carries a new sequence number.

Each consumer then has to deduplicate for itself. That means each consumer keeps state of its own, and each one gets the window, the key and the restart rules slightly wrong in its own way. The broker sees the records in log order, holds them durably, and can answer "was this identity written in the last N minutes" once for every consumer. A record that the broker drops costs no storage, no replication and no consumer work.

The feature is advisory, as a cache is. It is not exactly-once delivery. Its contract is "a duplicate inside the window is dropped, with stated exceptions near the edges", and the exceptions are listed below.

## Public Interfaces

The feature adds two topic configs, two broker settings and ten metrics. It adds no API key, no error code and no field to a request or a response.

### Topic Configs

| Config | Default | Values | Meaning |
| :--- | :--- | :--- | :--- |
| `dedup.window.ms` | `-1` | `-1`, or 1 and above | The window in milliseconds. `-1` turns deduplication off. |
| `dedup.key.header` | none | a non-empty header name | When set, the identity of a record is the value of its first header with this name, instead of the record key. |

`dedup.window.ms` is refused on a diskless topic (`krabka.diskless=true`). `dedup.key.header` is accepted on a topic whose window is `-1`, so an operator can choose the identity before turning deduplication on.

### Broker Settings

Both are `[runtime]` settings in `broker.toml`, read at startup.

| Setting | Default | Meaning |
| :--- | :--- | :--- |
| `dedup_max_entries_per_partition` | 1,000,000 | The most identities one partition replica's index holds. At most 10,000,000. |
| `dedup_max_entries` | 16,000,000 | The most identities all the indexes of one broker hold together. At about 64 bytes an entry, the default is about 1 GiB. |

### What a Producer Sees

A produce request that contains duplicates succeeds. The broker does not report which records it dropped: the response is the same as for a request that appended every record.

The `base_offset` in the response is exact. A dropped record keeps its offset, and the offset is left empty in the log, so the producer's `i`-th record is at `base_offset + i` whether or not it was dropped. A request whose records were all dropped still answers with a `base_offset`, and that offset range is empty in the log.

With `dedup.key.header` set, a record that has no header of that name, or whose header value is null, has no identity. The broker refuses the whole batch with `INVALID_RECORD` (87) and one `record_errors` entry for each such record. The broker does not fall back to the record key: a producer that chose a header and then forgot to set it would lose its deduplication without any signal.

With `dedup.key.header` unset, a record with a null key has no identity and is always appended. An empty key is an identity.

### What a Consumer Sees

A consumer of a deduplicated topic sees gaps in the offsets, as a consumer of a compacted topic does. Every Kafka client since 0.11 handles them, because compaction makes the same gaps. A request whose records were all dropped leaves an empty batch in the log: a batch header with no records, as the log cleaner writes to keep an idempotent producer's last sequence. Clients skip such a batch.

### Metrics

| Series | Type | Labels | Meaning |
| :--- | :--- | :--- | :--- |
| `krabka_broker_dedup_records_dropped_total` | counter | topic, partition | Records dropped as duplicates. |
| `krabka_broker_dedup_records_unindexed_total` | counter | topic, partition | Records appended without being indexed, because an entry limit was full. Their duplicates are not detected. |
| `krabka_broker_dedup_records_skew_clamped_total` | counter | topic, partition | Records whose timestamp was ahead of the broker clock and was clamped. |
| `krabka_broker_dedup_capacity_evicted_entries_total` | counter | topic, partition | Identities still inside the window that were removed to make room. Above zero, the effective window is narrower than the configured one. |
| `krabka_broker_dedup_batches_rebuilt_total` | counter | topic, partition | Batches re-encoded because some, but not all, of their records were dropped. This is the cost driver of the feature. |
| `krabka_broker_dedup_index_rebuilds_total` | counter | topic, partition | Times the index was rebuilt from the log. |
| `krabka_broker_dedup_index_entries` | gauge | topic, partition | Identities in the partition replica's index. |
| `krabka_broker_dedup_index_heap_bytes` | gauge | topic, partition | Estimated heap of the partition replica's index, from its table capacities. |
| `krabka_broker_dedup_broker_index_entries` | gauge | none | Identities in all indexes on the broker. |
| `krabka_broker_dedup_broker_index_max_entries` | gauge | none | The value of `dedup_max_entries`. |

## Proposed Changes

### The Rule

A record is dropped when an earlier record with the same identity was admitted and the two timestamps are at most `dedup.window.ms` apart. Within one batch the rule applies in record order, so a batch that holds the same key twice keeps the first record. The earliest record of an identity wins. A later copy never replaces it, and so a consumer reads the value that was written first.

The timestamp is the record's `CreateTime`, clamped to the broker clock. On a `LogAppendTime` topic it is the time of the append.

### The Index

Each partition replica holds an index from identity to the newest admitted timestamp. The index stores a 128-bit digest of the identity, not the identity itself, so an entry costs the same whatever the size of the key. At the largest supported index, the chance that two different identities share a digest is about 10⁻²⁵. That is far below the other edges of the feature. The digest is not resistant to a chosen-input attack, and does not need to be: a producer that wants to suppress another producer's record can already do so by writing that identity first.

The index is a function of the log. It holds what the records of the last window of the log say, read with the topic's current rule. Nothing in it comes from anywhere else, and nothing in it is written to disk.

### Where the Leader Decides

Each partition has one writer task, which makes every append to its log. The leader classifies a batch in that task, under the log lock, immediately before it appends the batch. So the decision and the append are one step: no other append can come between them, and a request never drops a record because of a write that was decided but not yet appended.

The produce handler reads the identities while it validates the batch. It already decompresses and walks every record for validation, so the identities cost a digest per record and no extra pass. The writer receives the digests and timestamps, not the records.

Three outcomes are possible for a batch:

- **Nothing dropped.** The batch appends unchanged, in the producer's own bytes. This is the common case, and it costs a hash lookup per record.
- **Some records dropped.** The batch is decoded, the duplicates are removed, and the rest is encoded again in the topic's codec. Each kept record keeps its offset delta, and the batch keeps its offset span and producer fields.
- **All records dropped.** An empty batch with the same offset span and producer fields appends.

Because every outcome appends a batch with the producer's offset span, the rest of the produce path does not change. The response reports the batch's real base offset. `acks=all` waits for the high watermark to pass the end of the batch, which is after the record that the batch duplicates. So a duplicate is never acknowledged ahead of the record it duplicates, and if that record is lost in a leader failover, the duplicate's batch is lost with it and the producer gets an error instead of a false success.

If the append fails, the writer gives back the identities that the batch added. The give-back is compare-and-revert: an identity that a later batch has since written keeps the later value.

### Followers and Failover

A follower appends the leader's batches in its own writer task, and indexes each one with the same rules. A follower that becomes leader therefore holds the index its leader held, and the first produce after a failover is deduplicated without delay.

The index records the log end offset it covers. It is current when that offset equals the log end. A truncation, a reset, a log-directory move, a change of `dedup.key.header`, a wider `dedup.window.ms`, and a broker restart all leave the index stale. Before the next append, the writer rebuilds a stale index from the log. It starts at the offset that `ListOffsets` gives for the time `now - dedup.window.ms`, and reads to the log end. It takes the log lock for each read of 1 MiB rather than for the whole rebuild, so fetches continue meanwhile.

### Idempotent and Transactional Producers

An idempotent producer's batches are deduplicated like any other batch. Because a dropped record keeps its offset and the batch keeps its sequence span, the producer's next sequence number is still the one it expects. An idempotent retry of a whole batch is recognized by the producer-state check before this feature sees it, so the retry gets its original offsets.

Transactional batches bypass deduplication on the leader and are not indexed on any replica. A transactional record can still abort. If it were indexed, it would drop a later record that duplicates a record that no `read_committed` consumer ever reads.

### Bounded Work

No step of the produce path or the follower path does work in proportion to the size of the index. The index removes expired entries with a cursor that advances a few entries for each new identity. A full pass over a multi-million-entry index takes long enough to stall a writer task. Every replica reaches the same pass at the same log position, so a whole-index pass would stall the leader and every follower together, and nobody would be free to complete an `acks=all` quorum.

### Growth Without Stalls

A hash table that grows moves every entry it holds. At two million identities, one such move in a single table took about 90 milliseconds, and every replica makes it at the same log position. So the index is 64 tables, chosen by the high bits of the digest, and each one grows on its own by doubling. The largest move is then one table, about 1/64 of the index. At two million identities, the slowest single call fell to about 4 milliseconds. A table also shrinks to twice its size when removals leave it a quarter full, so the heap follows the live identities back down after a burst. `dedup_index_heap_bytes` reports it.

A producer could choose keys that all land in one table, because the digest does not resist chosen input. Each table therefore holds at most twice its fair share of `dedup_max_entries_per_partition`. A new identity for a full table is appended without being indexed, like an identity at the partition limit, so no choice of keys brings the whole-index move back.

### Entry Limits

From 90% of `dedup_max_entries_per_partition`, the cursor also removes entries in the oldest quarter of the index. The index finds that quarter with a histogram of 512 buckets across the window, so it can tell apart only timestamps that are at least `dedup.window.ms / 512` apart. When more than a quarter of the index shares one bucket, for example when a producer writes many new keys within a few seconds, no entry is older than the others by that measure, and the index removes nothing it could still match. If a new identity arrives with the index at the limit, one bounded extra step looks for room. If it finds none, the record is appended without being indexed. That is counted in `dedup_records_unindexed`, and the broker logs a warning at most every five minutes. The broker-wide `dedup_max_entries` applies the same rule to the sum of all indexes, because the per-partition limit alone does not bound a broker that leads many busy partitions.

### Timestamps

A producer's `CreateTime` has no ordering guarantee. The index measures expiry against the newest timestamp it has seen. One record dated an hour ahead, which `message.timestamp.after.max.ms` admits by default, would move that point an hour forward. The next sweep would then empty the index, and every entry inserted after that would be expired as soon as it was written. Deduplication would stay off for the partition until a restart. So every timestamp is clamped to the broker clock before it reaches the index. A clamp does not change a timestamp that is at or behind the broker clock, which every timestamp in a rebuild is, so the index stays a function of the log. Clamped records are counted in `dedup_records_skew_clamped`.

### Best-Effort Edges

These edges are part of the contract. Each one is bounded by about one window.

- **The window edge is approximate.** Records that arrive out of timestamp order near the edge can be kept or dropped differently than their timestamps alone suggest.
- **The rebuild start is approximate.** A rebuild finds its first offset by timestamp, as `ListOffsets` does, and so has the same tolerance to unordered timestamps.
- **Entry limits narrow the window.** A partition at its limit whose identities are spread across the window keeps the most recent ones, so its effective window is the limit divided by the rate of new identities. `dedup_capacity_evicted_entries` shows when this happens. A partition that fills from a burst of identities created within one histogram bucket keeps those, and appends the identities that follow without indexing them until the burst expires. `dedup_records_unindexed` shows when that happens.
- **Replicas can trim differently under pressure.** The broker-wide limit depends on which other partitions a broker hosts. Under that pressure, a leader and its follower can remove different entries, so a failover can change which near-limit duplicate is caught.
- **A config change propagates per broker.** Each broker applies a new `dedup.window.ms` or `dedup.key.header` when its metadata image has it. For that short time, a leader can still apply the old rule.
- **Compaction removes records, not identities.** On a topic that is both compacted and deduplicated, an identity can stay in the index after compaction removed its record. Under the record-key rule that is the same identity as the newer record that replaced it, so nothing changes. Under the header rule, a rebuild can then forget it.

## Compatibility, Deprecation, and Migration Plan

**On disk.** The feature adds no persisted format. The log holds ordinary Kafka v2 batches. Offset gaps and empty batches are shapes that compaction already writes, and every 1.x broker reads them. The index is never written to disk. The two topic configs are ordinary `ConfigRecord` entries in the metadata log.

**Rolling upgrade.** The configs are not gated on a feature level, because no persisted format depends on them. During a rolling upgrade, an older broker keeps the two keys in its metadata image and ignores them. An older leader does not deduplicate, and an older broker refuses an `IncrementalAlterConfigs` request on a topic that carries them with "Unknown topic config name", because it validates the whole merged config. Set the two configs after every broker runs a version that knows them.

**Downgrade.** A downgraded broker reads the log as before and does not deduplicate. Remove the configs first, so that `IncrementalAlterConfigs` keeps working on those topics.

**Enabling on an existing topic.** The first append after the config arrives rebuilds the index from the last window of the log. So records written before deduplication was enabled count as already seen, if they are inside the window.

## Test Plan

- **Index unit tests** in `krabka-dedup`. They cover the rule, the window, every capacity path, the budget accounting, the clamp, give-back and its interaction with later writes, and a property test that holds the budget equal to the sum of the indexes. A leader-path and follower-path test given the same identities checks for the same contents in the same internal order. Bounds tests prove, without a clock, that no call moves more than one table, that the heap stays within a constant factor of the limit, that it shrinks after a burst, and that the work of every call is bounded at every size.
- **Writer-level tests** in the broker's `dedup` module, against a real log. They cover offset gaps, empty batches, a failed append, a rebuild from the window, truncation, a change of identity rule, a wider and a narrower window, and a follower that holds its leader's index.
- **Acceptance tests** that produce through the Kafka protocol. They cover duplicates across requests and within one request, exact base offsets, every codec, an idempotent producer, the header rule and its `INVALID_RECORD` refusal, transactional bypass, the clock-skew clamp, `LogAppendTime`, every `acks` value, and the config surface.
- **Recovery tests.** A restart, a torn log tail, a config change while a broker is down, a leader failover, a follower truncation and a rolling restart, each checked against what the recovered log holds.
- **Bounds and stall tests.** The per-partition and broker-wide limits hold under load. Produce latency stays flat when an index is full and while it grows. Fetches continue during a rebuild, because the rebuild takes the log lock per read.

## Rejected Alternatives

### Renumber the Kept Records

The Redpanda fork renumbers the kept records so that the log has no gaps. A producer's `i`-th record then has no stable offset, so the response can name only the first kept record. An idempotent producer's sequence numbers stop matching its offsets, which is why the fork had to exclude idempotent producers. Every modern Java client is idempotent by default, so that would exclude most traffic. Gaps cost nothing that compaction does not already cost.

### Answer an All-Duplicate Request Without an Append

The fork appends nothing for a request whose records were all dropped. It then needs a separate wait that holds an `acks=all` response until the commit point passes the duplicated record, plus a check that the leader still holds its term. An empty batch makes the normal high-watermark wait do the same job, and it advances an idempotent producer's sequence, which an absent batch cannot do.

### Classify in the Request Handler

The handler runs requests for one partition concurrently. A request that saw a duplicate would then have to wait until the request that introduced the identity had appended, and fail if that append failed. The fork carries such a fence. The writer task already orders every append, so a decision made there needs no fence.

### Persist the Index in a Sidecar File

A snapshot file would let a restarted broker skip the rebuild. It would also be a new persisted format, which needs a version marker, readers for every earlier version, a golden-bytes test and a feature level before any broker may write it. The fork's snapshots also caused its worst incident: a shard ran out of memory while it held a large snapshot several times over. A rebuild reads at most one window of the log, and only after a restart, a truncation or a config change. The index is advisory, so a slower rebuild never gives a wrong answer.

### Replicate the Index Changes in the Log

An earlier fork design wrote the index changes as their own batches in the log, so that every replica could rebuild the exact index at any offset. That required undo history, checkpoints, retention coordination and a speculative leader copy. The data batches already carry every key and timestamp, so the second copy bought only byte-exact history for a feature whose window edge is approximate by nature.

### Clear the Index on a Config Change

The fork bumps a generation counter in the topic config and clears the index when the identity rule changes. The cleared index then misses every duplicate of a record written before the change, and a restart later re-reads those records under the new rule anyway, so a running broker and a restarted broker disagree. Rebuilding from the log under the new rule gives every broker the same index. The same rebuild makes a wider window take effect at once, which in the fork had to wait for a restart.

### Store the Identity Bytes

Exact keys would remove the digest's collision chance. They would also make an entry cost the size of the key, and a header-based identity can be large. A 128-bit digest bounds the cost of an entry at a collision chance far below every other edge of the feature.

### Use the Broker Clock as the Watermark

Measuring expiry against the broker clock rather than the newest record timestamp avoids skew. But a rebuild would then depend on when it runs rather than only on the log, so two replicas could rebuild different indexes from the same log. The clamp keeps the timestamp-based watermark and removes only its failure.

### Use an LRU List for the Entry Limit

An LRU list removes exactly the oldest entry, but costs two pointers for every entry, which is about a third of the index. The capacity cursor finds the oldest quarter from a fixed histogram of 512 buckets and costs nothing per entry.
