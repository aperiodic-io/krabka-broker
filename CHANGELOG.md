# Changelog

This file records the releases of krabka-broker. Every version heading below
names an annotated git tag in this repository, and the tag is what a person
quotes in an incident. [Releasing](docs/releasing.md) gives the commands that
cut one.

krabka versions and tags the whole workspace as one unit. The version comes
from `[workspace.package]` in the root `Cargo.toml`, and each crate takes it
from there, so no crate has a release of its own. From 1.0.0 on, the version
number is a promise about on-disk compatibility: any 1.x broker reads every
artifact that an earlier 1.x broker wrote, so a rolling upgrade within 1.x
works, and a new major version is the only release that can break that.
[Persisted formats](docs/persisted_formats.md) states the contract. The number
makes no promise about the Rust API, so read the entries for that. Before
1.0.0, a minor bump was free to break any interface.

The layout follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
The history before krabka-broker became its own repository is in
[robot-head/crabka](https://github.com/robot-head/crabka). This repository
publishes `krabka-log`, `krabka-verified` and `krabka-macros` to crates.io from
the release tag; [Releasing](docs/releasing.md#cratesio) gives the procedure.

## [Unreleased]

## [1.0.4] - 2026-10-13

1.0.4 changes no on-disk format and adds no setting, so it upgrades from any
1.x release in place and rolls back to 1.0.3 the same way. Its changes apply
to partitions with `preallocate=true`, which write through `O_DIRECT`; other
partitions behave as in 1.0.3. After a crash, the active segment of such a
partition can now hold zeros between batches that were not yet synced, as
page-cache writeback can leave. The tail recovery of every 1.x release cuts
the file at the first bytes that do not decode as a batch, so a rollback
reads it too.

### Changed

- A partition with `preallocate=true` acknowledges a produce request once its
  `O_DIRECT` write is queued, not once the write is on the disk, as a
  partition without `preallocate` acknowledges once its bytes are in the page
  cache. Shared write threads put up to four of a segment's writes on the
  disk at the same time, and unwritten bytes stay in memory for reads. A write
  that fails afterwards fails every later append and sync of the segment.
  Before, the append waited for the write under the partition's log lock, so
  one slow device write stalled the producer, the followers and the consumers
  of the partition, which gave a long end-to-end latency tail at RF3 (#1371).
- The default `log_tail_cache_size` is 32 MiB, up from 1 MiB. A consumer that
  fell more than the tail cache behind the producer of a `preallocate=true`
  partition read from the disk and could fall further behind (#1370). Each
  such partition now holds up to 32 MiB of its newest data in memory. Set
  `log_tail_cache_size` to keep the old value.

### Fixed

- A fetch from a segment that wrote through `O_DIRECT`, sealed or not, no
  longer takes the `sendfile` path. Those bytes are not in the page cache, so
  walking their batch headers was one small synchronous disk read per batch
  under the log lock, and `sendfile` then read them from the disk again. The
  fetch now takes the newest bytes from memory without a copy and older ones
  in one large read, so a consumer that falls behind catches up (#1370).

## [1.0.3] - 2026-10-12

1.0.3 changes no on-disk format and adds no setting, so it upgrades from 1.0.2
or 1.0.1 in place and rolls back to 1.0.2 the same way. A rollback to 1.0.1
follows the 1.0.2 notes below.

### Fixed

- A consumer catching up on data that is no longer in the page cache no longer
  reads record payloads to find batch boundaries. In 1.0.2, a plaintext
  (`sendfile`) fetch found the batch boundaries of a run of large batches by
  reading 16 KiB at each one, with the partition's log mutex held. On a cold
  backlog each of those reads went to disk, which slowed the drain and held up
  appends to the same partition. A fetch now reads only the 61-byte header at
  a large batch, and keeps buffered scans for runs of small batches (#1359).
- Controller-side admission prevents stale topic UUID deletes and incremental
  config updates from affecting recreated topics or overwriting newer settings.
- Request byte limits reserve capacity before allocating and reading a frame
  body, including connections waiting for request-count capacity.
- Scheduled retention preserves pending records in both tiers. LATEST offsets
  account for pending archived batches, and timestamp lookup after reopen uses
  the durable deletion floor instead of the local cache boundary.
- Fetch offsets above the log end return `OFFSET_OUT_OF_RANGE`; fetching at the
  log end remains valid and empty.
- Compressed legacy batches preserve sparse offsets and reject unrepresentable
  offset or timestamp arithmetic instead of overflowing.
- Remote snapshot decoding checks entry counts before allocation. Restore and
  backup reads enforce caps against the fetched object, including overwrite
  races. Object-lock control-plane calls honor retry and timeout settings.
- OPA rejects HTTP error responses before interpreting decisions, and
  resource-type ACL authorization follows Kafka's pattern dominance rules.
- The public regex resolver exposes its result type, and public sealed-segment
  opens restore readable bounds while rejecting corrupt tails without trimming.
- Telemetry guard drop shuts down exporters and stops its heartbeat. Schema
  rendering bounds cyclic references, including recursive arrays.
- API comments, workflow comments, installation instructions and the runnable
  Raft README example match the implementation. CI checks quoted snippets in
  crate READMEs as well as documentation pages.
- **Controller admission:** CreatePartitions, legacy AlterConfigs, AlterPartition
  and automatic/manual leader elections use authoritative controller planning
  and guarded submission to preserve newer assignments, leadership, ISR and
  configuration. Controller role/epoch transitions invalidate admitted terms.
- **Share groups:** Initial state operations serialize absent-key fencing and
  deletion/recreation. Timed-out or cancelled writes fence the shard until a
  FIFO writer barrier and same-term replay restore its active generation.
  Replay includes oversized first batches instead of
  activating a partial prefix. ShareFetch, duration initialization and
  dead-letter retrieval include retained diskless WAL and archived history.
- **Fetch and offsets:** Response byte limits permit an oversized first batch
  only before any response records exist. ListOffsets timestamp lookup includes
  diskless WAL with exact logical-floor scans, and v0 scheduled LATEST includes
  archived pending batches. Stalled response writes close after the configured
  no-progress window and release request queue capacity.
- **Remote storage:** Index-cache fills publish atomically, multipart uploads
  apply bounded backpressure, and remote index reads enforce an independent
  byte cap. RLMM assignment uses the discovered metadata-topic partition count;
  initial consumer connections retry, epoch-list reads enforce readiness, and
  mutations report application errors or interrupted waits.
- **Backup and restore:** Unique sortable capture IDs and create-only writes
  preserve concurrent captures while keeping older captures discoverable.
  Latest selection ignores incomplete captures without committed manifests.
  Independent document caps bound backup reads. Continue-on-corrupt preserves
  surviving offsets on both sides of an interior gap, and authenticated
  snapshots are consumed from the exact retained bytes rather than mutable paths.
- **Authorization and libraries:** OPA uses the Kafka-plugin v1.5.1 action
  schema and its documented available-context subset. Generic derive expansions
  preserve type parameters and where clauses, and in-process formatter argument
  errors return exit codes without terminating the caller.
- **Workflows and documentation:** Trusted publication stops before its OIDC
  token expires and supports a full rerun with a refreshed pending plan. Missing
  known-failure system tests fail, snippet anchors match exactly, and release,
  benchmark, Clippy, profiling, stamp-helper and API comments describe the
  implemented behavior and preconditions.
- Queued ordinary Produce batches under `preallocate=true` share one aligned
  Direct I/O write when they fit in the active segment and total at most 1 MiB.
  This reduces synchronous writes and repeated block padding under load.

### Changed

- The unstable Rust API `krabka_records_legacy::parsed_from_v2` returns a
  `Result` so callers can handle invalid coordinates.
- Consumer-group reconciliation helpers that depend on internal state are
  crate-visible. `RemoteRetentionSegment` carries a `blocked` delivery fact
  for the verified retention decision.
- Live scheduled clock-bound changes apply prospectively without retracting the
  released delivery prefix. A restart rebuilds the unpersisted watermark using
  the current clock and bound, so changed assumptions can yield a lower prefix.

## [1.0.2] - 2026-10-10

1.0.2 changes no on-disk format, so it upgrades from 1.0.1 in place and
rolls back to it the same way. Before a rollback, remove `log_tail_cache_size`
and the `--log-tail-cache-size` flag from a configuration or service invocation
that sets it, because 1.0.1 refuses a `[runtime]` key and argument it does
not know. If `preallocate=true` was active during an unclean shutdown, the tail
recovery of 1.0.1 and 1.0.0 cuts the remaining zero padding as normal.

### Added

- The topic setting `preallocate` and the broker default `log.preallocate`
  reserve `segment.bytes` of disk blocks with `fallocate(FALLOC_FL_KEEP_SIZE)`
  when an active segment is opened, avoiding allocations during appends and
  releasing unused reserved blocks when the segment is sealed. On filesystems
  or platforms without block reservation support, the segment grows as it is
  written.
- On Linux 6.1+ where the underlying filesystem reports direct-I/O alignment,
  active segments under `preallocate=true` write through `O_DIRECT` to bypass
  the page cache, keeping the newest appends in memory to serve reads. Where
  unsupported, appends remain page-cache-backed.
- The `[runtime]` key `log_tail_cache_size`, with the `--log-tail-cache-size`
  flag and the `KRABKA_LOG_TAIL_CACHE_SIZE` variable, sets how much of the
  newest data a partition with `preallocate=true` keeps in memory to serve
  reads with. The default is 1 MiB, and `0` keeps only the newest batch.
- The previously inert topic settings `compression.gzip.level`,
  `compression.lz4.level`, `compression.zstd.level`, `segment.jitter.ms`,
  `file.delete.delay.ms`, `flush.messages`, and `flush.ms` now control log
  behavior. Compression levels apply when the broker re-encodes records;
  producer batches passed through unchanged keep their original encoding.
  Segment rolls use jitter, removed segment files are reclaimed after the
  configured delay, and message or time thresholds flush pending writes,
  including idle partitions. Topic overrides and broker defaults take effect
  through live configuration updates.

### Fixed

- Kafka 4.3.1 compatibility gaps in Fetch and ShareFetch byte limits,
  replication-quota progress, KIP-835 metadata health metrics, RE2J regex
  syntax and size handling, Java regex backreference documentation, and
  feature/config reporting.
- Zero-copy (`sendfile`) fetch descriptor planning adapts header scan
  buffers from 16 KiB up to 256 KiB based on batch size, eliminating
  repeated positioned read syscalls across small batches while preserving
  file-backed payloads for large batches.

## [1.0.1] - 2026-10-09

1.0.1 changes no on-disk format, so it upgrades from 1.0.0 in place and
rolls back to it the same way. Before a rollback, remove `log_read_ahead_max`
from a configuration that sets it, because 1.0.0 refuses a `[runtime]` key it
does not know.

### Added

- The `[runtime]` key `log_read_ahead_max`, with the `--log-read-ahead-max`
  flag and the `KRABKA_LOG_READ_AHEAD_MAX` variable, caps how far past its
  own window a fetch read asks the kernel to read ahead. The default is 4 MiB.
  `0` limits the hint to the fetch's own window. The hint has no effect on a
  host other than Linux.

### Fixed

- A consumer far behind the log end of a partition now reads at the speed of
  the disk. Before, each verbatim fetch read found its batch boundaries with
  one small `pread` per batch header, each up to a fetch window ahead of the
  data the kernel had already read. On a segment that was not in the page
  cache each of those reads was a disk read of its own, made one after another
  while the partition's log mutex was held, and the scattered pages they left
  kept the kernel's sequential readahead small. On a disk that charges per
  operation, such as a cloud block volume, that held a backlog drain below the
  publish rate of the OpenMessaging Benchmark's
  `backlog-1-topic-1-partition-1kb` workload. Each verbatim fetch read now
  gives the kernel one `POSIX_FADV_WILLNEED` hint for its window and the
  window after it before it walks the batch headers, so those reads become a
  few large ones in flight at once and the next fetch of a lagging consumer
  finds its data in the page cache. The response bytes do not change. Kafka
  needs no such hint, because its fetch sends a slice that may end in a
  partial batch and reads no batch header past the one it starts at.

## [1.0.0] - 2026-10-07

1.0.0 is the first release with a compatibility promise. Every 1.x broker
reads every on-disk artifact that an earlier 1.x broker wrote, so a rolling
upgrade within 1.x works without a reformat. A format change after 1.0.0 is
turned on by a feature level that the operator finalizes, as Kafka gates a
metadata change on `metadata.version`.
[Persisted formats](docs/persisted_formats.md) lists every artifact, its
version marker and the rules for changing it.

A data directory that a 0.x broker wrote is not covered. Format it again with
`krabka-format` before you start 1.0.0 on it.

Every krabka-owned on-disk artifact now carries a version marker, and every
reader refuses a version it does not know, or a layout without one, instead of
skipping, regenerating or treating the artifact as absent:

- `.stampindex`, `bootstrap.records.bin`, the audit spool and its three state
  files start with an `i16` version (the audit files after the magic `KAUD`).
  `high-watermark` and the diskless WAL `wal-durable-offset.checkpoint` start
  with a version line. The diskless WAL `quorum-state.json`, `WormChainRecord`,
  and the backup `manifest.json` and `group-offsets.json` have a required
  `version`. `bootstrap.json` names its field `version` instead of `schema`.
- `__diskless_wal_index` keys start with an `i16` key version that names their
  type, and both value types start with an `i16` version. `WalFlushRecord`
  keeps its number, 2, at the front of the value.
- The krabka-private records in `NoOpRecord` tags start with an `i16` version,
  and the reader checks that each tag carries its own variant. The metadata
  record reader refuses a KIP-631 `frameVersion` other than 1, as Kafka does.
- `quorum-state` with an unknown or missing `data_version`, or that does not
  parse, stops the node, as Kafka's `FileQuorumStateStore` does. Before, the
  node started as if it had never voted.
- `leader-epoch-checkpoint` refuses a header other than `0`, and transaction
  control records refuse a negative version or a short key or value, as
  Kafka's `CheckpointFile`, `ControlRecordType` and `EndTransactionMarker` do.
- The WORM manifest verifier accepts only `format_version` 2.
- The clean-shutdown proof is Kafka's `.kafka_cleanshutdown`, holding Kafka's
  `{"version":0,"brokerEpoch":N}`. A missing or unreadable one still makes the
  restart unclean, as in Kafka.
- The private controller RPCs 1003, 1004 and 1005 answer an unknown request
  version with `UNSUPPORTED_VERSION`.

A controller now stops on a committed metadata record that it cannot decode,
in live replay, restart recovery and the image walk, as Kafka's fatal fault
handler does. A broker-only node logs the record at error, counts it in
`metadata-load-error-count` and stops reading that batch, as Kafka's
`MetadataLoader` does. An invalid KRaft control record stops every node.
Before, every one of these was logged at debug level and skipped, so a node
could build a different metadata image from its peers. A record that names a
topic or partition the image no longer holds is still skipped on every
replica, because two racing writes can both commit; a field value that no
build accepts now stops the controller too.

`krabka.version` is the feature krabka owns for its own format changes.
Nodes advertise it at `[0, 1]`. Levels 0 and 1 both mean the 1.0.0 formats.
A fresh cluster carries no `krabka.version` record, so it runs at level 0
unless `krabka-format --feature krabka.version=N` overrides it. Once every
node is krabka, an operator finalizes level 1 with `kafka-features upgrade
--feature krabka.version=1`, under Kafka's rules for a feature other than
`metadata.version`. The level starts at 0 because a cluster can mix in Kafka
nodes, which support only level 0: a bootstrapped level 1 crashes a JVM
standby controller and keeps a JVM broker from joining.
The private controller RPCs 1003, 1004 and 1005 now negotiate their version
from the finalized `krabka.version`: a sender uses the version every peer
serves at that level, and the bytes stay v0 at levels 0 and 1. `krabka-raft`
drops `SUBMIT_CHANGE_VERSION`, `METADATA_FETCH_VERSION` and
`DELEGATION_TOKEN_MUTATION_VERSION` for `private_request_version`,
`private_api_highest_version` and `PRIVATE_BASELINE_VERSION`.

The group and share coordinators load `__consumer_offsets` and
`__share_group_state` as Kafka's `CoordinatorLoaderImpl` does: a record type
the broker does not know is logged and skipped, and any other bad record fails
the load. Before, an unknown type stopped the group load, and the share loader
skipped every bad record. The transaction coordinator skips an unknown key or
value version in `__transaction_state`, as Kafka's `TransactionStateManager`
does, and on any other bad record it logs an error and serves the
transactions it loaded before it, as Kafka does.

The records the coordinators write to `__consumer_offsets` now match Kafka
4.3.1 field for field:

- `MetadataHash` in the consumer, share and streams group metadata records is
  Kafka's `computeTopicHash` and `computeGroupHash` (XXH3-64, seed 0), not a
  krabka hash. A hash a 0.x broker wrote does not match.
- The target-assignment metadata records carry KIP-1263's
  `AssignmentTimestamp`, and the assignment interval runs from it.
- `StreamsGroupMetadataValue` carries `ValidatedTopologyEpoch` and
  `LastAssignmentConfigs`, and `StreamsGroupCurrentMemberAssignmentValue`
  carries KIP-1251's per-task `AssignmentEpochs`.
- `ConsumerGroupPartitionMetadata` (type 4) is read and replayed, and is
  tombstoned where Kafka tombstones it. krabka no longer writes
  `StreamsGroupPartitionMetadata` (type 18), which Kafka 4.3.1 does not know;
  a type-18 record in an existing log is skipped as an unknown type.
- Group deletion tombstones follow Kafka's `createGroupTombstoneRecords`
  order, and include the resolved regular expressions.
- Replay follows Kafka's `GroupMetadataManager.replay` record by record: a
  child record creates its group, a tombstone for a group the log does not
  hold is ignored, and an out-of-order tombstone fails the load.
- KIP-1331's `StoredDescriptionTopologyEpoch` and
  `FailedDescriptionTopologyEpoch` tags, which exist only on Kafka trunk, are
  written only while `group.streams.topology.description.plugin.class` is set,
  and are always read.

### Added

- KIP-1331 topology descriptions are stored when
  `group.streams.topology.description.plugin.class` in `[server_properties]`
  names Kafka's `org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin`,
  which krabka builds in. A version 1 `StreamsGroupHeartbeat` then asks one
  member of the group for the description, `StreamsGroupTopologyDescriptionUpdate`
  stores it in memory, and `StreamsGroupDescribe` v1 answers it. The group
  metadata record keeps Kafka's `StoredDescriptionTopologyEpoch` and
  `FailedDescriptionTopologyEpoch`. Any other class name stops the broker at
  startup, as Kafka stops on a class it cannot load.
- Kafka's `metadata.log.dir`: the `metadata_log_dir` key, the
  `--metadata-log-dir` flag and `KRABKA_METADATA_LOG_DIR` name the directory
  of the metadata log, `meta.properties` and the bootstrap records.
  Unset, it is `log_dir`. A metadata log directory that is not a data
  directory holds no partition and is not in `DescribeLogDirs`, and the node
  stops when it fails (KIP-858). `krabka-format --metadata-log-dir` formats it
  with the data directories, as `kafka-storage format` does.
- The `[runtime]` keys `metadata_log_segment_bytes`,
  `metadata_log_segment_roll_interval`, `metadata_max_retention_bytes`,
  `metadata_max_retention` and `metadata_max_idle_interval` are Kafka's
  `metadata.log.segment.bytes`, `metadata.log.segment.ms`,
  `metadata.max.retention.bytes`, `metadata.max.retention.ms` and
  `metadata.max.idle.interval.ms`. The leader appends a KIP-835 `NoOpRecord`
  every `metadata.max.idle.interval.ms`.
- `aspect kafka-system-tests` runs Apache Kafka's ducktape system tests
  against krabka. The task checks out the Kafka trunk revision that the
  manifest `tools/kafkatest/suite.json` pins, copies `krabka-broker` and
  `krabka-format` into it, and runs the tests in ducker-ak containers. The
  adapter `tools/kafkatest/krabka.py` replaces Kafka's `KafkaService`, so
  every broker and isolated controller of a test is `krabka-broker`. The
  adapter translates the `kafka.properties` of each node into a
  `krabka.toml`. The clients, the tools and the checks of each test stay
  Kafka's. The task gives each manifest entry one of four verdicts: pass,
  regression, known failure or fixed. It writes the verdicts to
  `target/kafka-system-tests/report.md`, and it exits with status 1 on a
  regression. `docs/kafka-system-tests.md` tells how to run it and which
  tests the manifest leaves out.
- The `[runtime]` keys `streams_group_min_session_timeout`,
  `streams_group_max_session_timeout`, `streams_group_min_heartbeat_interval`
  and `streams_group_max_heartbeat_interval` are Kafka's
  `group.streams.min.session.timeout.ms`,
  `group.streams.max.session.timeout.ms`,
  `group.streams.min.heartbeat.interval.ms` and
  `group.streams.max.heartbeat.interval.ms`. Each key also has a flag and an
  environment variable, for example `--streams-group-min-session-timeout` and
  `KRABKA_STREAMS_GROUP_MIN_SESSION_TIMEOUT`. The session timeout and the
  heartbeat interval of a streams group must stay inside these bounds, as
  Kafka's `GroupCoordinatorConfig` requires. Kafka's `BaseStreamsTest` sets
  `group.streams.min.session.timeout.ms` and
  `group.streams.session.timeout.ms` to 10 seconds.

- `krabka-format --controller-listener-name` names the voter endpoints of
  `--standalone` and `--initial-controllers` after the first entry of Kafka's
  `controller.listener.names`. The default is `CONTROLLER`. Thus
  `kafka-metadata-quorum add-controller` finds the endpoint of the default
  listener, and the leader accepts the new voter.

### Changed

- **On-disk compatibility from 1.0.0.** Any 1.x broker reads every on-disk
  and object-store artifact that an earlier 1.x broker wrote, so a cluster
  moves from 1.x to 1.y with a rolling upgrade and no reformat. A release that
  changes a persisted format keeps writing the old format until you finalize
  the feature level that introduces the new one, as Kafka does for
  `metadata.version`, so you can roll back until you finalize. Data that a 0.x
  broker wrote gets no promise: reformat a 0.x data directory with
  `krabka-format` before you start 1.0.0 on it. [Persisted
  formats](docs/persisted_formats.md) lists every format and the gaps that
  remain.
- The workspace builds on krabka-protocol 0.6.0 and krabka-client-rs 0.6.0.
  `krabka-security` now takes Kerberos from the crates.io `krabka-sspi` in
  place of the `robot-head/sspi-rs` fork. The metadata layer adds the
  `krabka.version` feature, which `ApiVersions` and broker registration
  advertise, and refuses a krabka-private metadata record written before
  0.6.0, so a metadata log from an earlier build must be formatted again.
- **Breaking, on-disk format.** `krabka-format` writes Kafka's
  `meta.properties` into each directory it formats, in place of
  `meta.properties.json`: the Java properties file of `kafka-storage format`,
  with `cluster.id`, `directory.id`, `node.id` and `version=1`, written
  through `meta.properties.tmp` and a rename. The Kafka tools now read a
  krabka directory, so `kafka-metadata-quorum add-controller` finds the
  directory id of a new krabka controller. `--node-id` is required, from 0 to
  2147483647, as Kafka's `node.id` is. At a start the broker reads the
  `meta.properties` of the metadata log directory and of every data
  directory, and applies the checks of Kafka's
  `KafkaRaftServer.initializeLogDirs` with Kafka's messages: a directory of
  another node stops the broker with `Stored node id 1 doesn't match previous
  node id 2 in ...`. The broker refuses a directory that has only
  `meta.properties.json` with `No readable meta.properties files found.`; run
  a fresh `krabka-format`. `docs/format-divergences.md` gives the format.
- `krabka-format --directory-id` refuses the 100 lowest ids, which Kafka
  reserves as directory-id sentinels. The broker refuses a reserved id at
  startup with `Invalid reserved directory ID ... found in <dir>`, as Kafka's
  `MetaPropertiesEnsemble.verify` does, so the format refuses it first and
  writes nothing.
- The metadata log is in `__cluster_metadata-0` under the metadata log
  directory, with its segments, its `<offset>-<epoch>.checkpoint` snapshots
  and its `quorum-state` file, as Kafka's is. It was in
  `__cluster_metadata/@metadata-0`. Reformat a node that has the old layout.
- A metadata snapshot no longer moves the log start by itself. The metadata
  log keeps the prefix a snapshot covers until `metadata.max.retention.bytes`
  or `metadata.max.retention.ms` lets the oldest snapshot go, as Kafka's
  `KafkaRaftLog.maybeClean` does, and its segments roll, so the cleaning
  deletes whole segment files.
- `krabka-format` writes the bootstrap files only into the metadata log
  directory. A data directory gets `meta.properties` only.
- The raft node id is the broker id, as Kafka's `node.id` is both, and the
  `broker_id` of `--config-file` now sets it. The seeded voter and the
  telemetry resource take the same id. A `--broker-id` other than the
  default 1 wins over the file's `broker_id`, and the file's `broker_id` wins
  over the default. Before, the raft node id came from `--broker-id` alone,
  so a node that set its id only in the file ran raft as node 1.
- A node makes itself the single voter of its own quorum only when it has
  the controller role and sets neither `controller_quorum_voters` nor
  `bootstrap_servers`. A node that sets `bootstrap_servers` seeds no voter,
  because its voters come from the KIP-853 `VotersRecord` in the log. A node
  without the controller role that sets neither key stops at startup with
  Kafka's message: `If using process.roles, either
  controller.quorum.bootstrap.servers must contain the set of bootstrap
  controllers or controller.quorum.voters must contain a parseable set of
  controllers.` Before, every node without `controller_quorum_voters` seeded
  itself as a voter, whatever its roles and its `bootstrap_servers`.
- At startup on Unix, the broker raises its soft limit on open files to the
  hard limit, as the JVM's default `-XX:+MaxFDLimit` does for a Kafka broker.
  A broker keeps the log and index files of each segment open. The soft
  limit of 1024 that a shell or a systemd unit often sets runs out at a few
  hundred partitions. The broker changes nothing when the soft limit is
  already at the hard limit, or when the hard limit is unlimited, because
  Linux refuses an unlimited soft limit on open files. It logs a warning when
  the change fails.
- **Breaking, group coordinator.** The consumer, share and streams group
  coordinators write `__consumer_offsets` records where Kafka 4.3.1's
  `GroupMetadataManager` writes them, in the same batches:
  - New consumer and share groups start at group epoch and assignment epoch 1,
    as Kafka's `ModernGroup` and `TargetAssignmentMetadata.INITIAL` do, so the
    first epoch bump of a new group writes epoch 2. Streams groups already
    started at 1.
  - A group record is written only when a transition changes what it holds:
    the member record when the member changed, the group metadata record when
    the group epoch moves, the targets that changed with the target metadata,
    and the member's current assignment when it changed. Before, each
    transition wrote the group epoch, and the records of every member it
    touched, again.
  - A leave or a fence bumps the group epoch and computes no target. Each
    session or rebalance timer fences its member in a batch of its own.
    Before, a leave installed a new target, and one batch fenced all expired
    members.
  - The resolutions of a regular expression are written in a batch of their
    own, after the heartbeat's batch. A member that joins with a regular
    expression gets its topics at its next heartbeat, as in Kafka, not in the
    response to its join.
  - The downgrade of a consumer group to a classic group happens inside the
    fence batch, not after it.
  - The upgrade of a classic group, and the replacement of a static member, go
    in the heartbeat's batch. The records of the replacement come before the
    heartbeat's own records.
  - `ConsumerGroupMemberMetadataValue` (type 5) lists its topics in sorted
    order and writes `""` when the member has no regular expression, as
    `newConsumerGroupMemberSubscriptionRecord` does. The share member record
    lists its topics in sorted order too.
  - A classic group that converts to a consumer group keeps its generation as
    its group epoch, and the last assignment of each member becomes its target
    and its current assignment, as `ConsumerGroup.fromClassicGroup` does.
  - A member with no target gets a target-assignment record with an empty
    value, not a tombstone.
  - The `ShareGroupStatePartitionMetadata` (type 15) record of the partitions
    that a heartbeat initializes is the last record of the heartbeat's batch.
    Before, it was a batch of its own.
  - Each share partition that a heartbeat initializes starts at offset -1, as
    Kafka's `buildInitializeShareGroupStateRequest` asks. So a new partition
    of a topic that a share group already consumes starts where
    `share.auto.offset.reset` says. Before, it started at offset 0.

### Fixed

- Fetch requests with a small byte budget retain records between sparse index
  entries, including records before a segment boundary. The reader locates the
  first eligible batch before spending the payload budget.

- A broker that becomes the leader of a partition decides idempotent and
  transactional produces from the producer state of its log, up to the log
  end. Thus it answers a retry of a batch that it replicated as a follower as
  a duplicate, with the offset of the first append, and it takes the next
  sequence after that batch. This is Kafka's behavior:
  `UnifiedLog.appendAsFollower` updates the producer state for each replicated
  batch, and the new leader's `ProducerStateEntry.findDuplicateBatch` finds
  the retry among the last five batches of the producer. A partition that the
  broker opens also takes the producer state of its log. Before, a follower
  did not keep the data batches that it replicated in the producer state of
  the produce path, and a promotion did not copy them from the log. In
  Kafka's `ReplicationTest`, with a clean bounce of the leader and an
  idempotent producer, the new leader appended a retry of a replicated batch
  a second time, and the consumer read six duplicates. A broker that led the
  partition again answered the next batch with `OUT_OF_ORDER_SEQUENCE_NUMBER`,
  because it still held the sequence of its previous term.
- A KIP-853 dynamic quorum grown from a `--standalone` controller works as
  Kafka's does. A broker-only node that names the quorum only through
  `controller.quorum.bootstrap.servers` finds the leader through those servers
  and reaches it on the endpoint of the voter set. A controller bound to a
  wildcard address advertises its host name in voter updates and in its
  `RegisterControllerRecord`, as Kafka's
  `ListenerInfo.withWildcardHostnamesResolved` does. Before, it committed
  `127.0.0.1` into the voter set, and brokers on other hosts stayed fenced and
  never finished starting. The first leader writes the voter set of the
  bootstrap checkpoint into the log, as Kafka's
  `LeaderState.appendStartOfEpochControlRecords` does, and writes
  `LeaderChange` at version 0, the only version Kafka reads. A controller
  formatted with `--no-initial-controllers` starts as an observer without
  auto-join, and its discovery takes the bootstrap servers in turn. A leader
  that removes itself waits for a remaining voter to commit the removal, and
  then observes its successor. `DescribeQuorum` lists a new observer of a
  leader that has made no commit since its election.
- The broker reads Kafka's `log.roll.ms` from `[server_properties]` as the
  roll interval of a topic that sets no `segment.ms`, and `DescribeConfigs`
  reports it at `STATIC_BROKER_CONFIG`. Before, the broker ignored the key and
  rolled every 7 days. Kafka's `LogDirFailureTest` sets it so that a broker
  writes into a failed log directory within seconds.
- A follower stops following a partition when an append to it fails on a log
  directory that is now offline, as Kafka's `ReplicaManager.handleLogDirFailure`
  does. Before, the follower fetched and failed the same records in a loop.
- A promotion whose leader-epoch checkpoint write fails takes the log
  directory offline, so the heartbeat reports it and the controller moves the
  leadership. Before, the partition kept that broker as its leader, and the
  broker answered `UNKNOWN_LEADER_EPOCH` to its followers.
- A read of the log goes on into the next segment only after it reads the
  segment before it to the end, and only its first batch can be larger than
  the fetch budget. Before, a fetch that its `partition_max_bytes` stopped
  inside a sealed segment went on into the next segment, served the first
  batch there, and skipped every offset between the two. A follower that
  caught up across segments lost those records, which is why a throttled
  reassignment in Kafka's `ThrottlingTest` finished in 7 seconds and not in
  131. A consumer fetch had the same gap.
- The transaction coordinator answers a request, and changes its state, only
  once the `__transaction_state` record of the request is committed: the
  record carries the leader epoch, the ISR holds `min.insync.replicas`
  members, and the high watermark covers the record while the coordinator
  still leads the partition. A write that does not commit answers
  `NOT_COORDINATOR` or `COORDINATOR_NOT_AVAILABLE`, as Kafka's
  `appendTransactionToLog` does. `TxnOffsetCommit` and `OffsetDelete` wait the
  same way for their `__consumer_offsets` records. Before, the coordinators
  answered at the local append, so a broker bounce could lose an `EndTxn`
  decision or a transaction's offsets that the client was told were written.
- A transaction completes only once each of its `COMMIT` or `ABORT` markers
  is committed on its partition. `WriteTxnMarkers` appends a marker as the
  partition leader with `acks=-1`, and answers the partition once the high
  watermark covers the marker. Otherwise it answers `NOT_LEADER_OR_FOLLOWER`,
  `NOT_ENOUGH_REPLICAS`, `NOT_ENOUGH_REPLICAS_AFTER_APPEND` or
  `REQUEST_TIMED_OUT`, as Kafka does, and the coordinator sends the marker
  again to the partition's leader. The coordinator's own marker appends wait
  the same way. Before, a marker counted at the local append, so a leader that
  died before a follower fetched the marker left the transaction open on the
  next leader permanently. Its last stable offset stopped there, and a
  `read_committed` consumer read nothing after it.
- A streams group that the broker loads from `__consumer_offsets` at startup
  assigns its tasks again. Before, the replay started the group before the
  broker connected the metadata image, and the group never read the image.
  Every target assignment after a restart was empty, so the members revoked
  all of their tasks, and in Kafka's `StreamsBrokerDownResilience` tests no
  instance processed a record after the broker came back. A streams group
  write that does not commit now answers `NOT_COORDINATOR` or
  `COORDINATOR_NOT_AVAILABLE`, as Kafka's `CoordinatorOperationExceptionHelper`
  does, and not `COORDINATOR_LOAD_IN_PROGRESS`.
- A classic consumer that joins a consumer group with live members (KIP-848
  online migration) joins as a follower, as Kafka's
  `classicGroupJoinToConsumerGroup` serves it. A dynamic member with no member
  id at `JoinGroup` v4 or later gets `MEMBER_ID_REQUIRED` and a new id. The
  response names no leader and lists no members, and its generation is the
  member epoch that the offset-commit fence checks. Before, the broker added a
  member with an empty id and named the member the leader of a list with no
  subscription metadata. The Java client failed to parse that list and
  stopped, so Kafka's `ConsumerProtocolMigrationTest.test_consumer_rolling_downgrade`
  timed out. A classic member that joined an upgraded group again got the
  same response.
- A tiered partition that stops taking writes moves its last records to the
  remote tier and off local disk. When the active segment breaches
  `local.retention.ms` or `local.retention.bytes`, and every sealed segment
  before it is copied and breached too, local retention rolls it, as Kafka's
  `UnifiedLog.deletableSegments` does. The next copy uploads the records, and
  the next pass deletes them from local disk. Before, the active segment stayed
  local until `segment.bytes` or `segment.ms` rolled it, so
  `ListOffsets(EARLIEST_LOCAL)` stayed at its base offset. Kafka's
  `ShareConsumerDLQTieredStorageTest` waits for that offset to reach the last
  record, and it timed out.
- A share group reads the records that only the remote tier holds. When the
  share-partition start offset is below the local log start of a tiered
  partition, `ShareFetch` reads the batches from the remote tier and acquires
  inside them, as Kafka's `DelayedShareFetch` does through
  `RemoteLogManager.asyncRead`. The control batches, the aborted transactional
  data under `read_committed` and the KFC-1 batches that are not due in that
  read stay out of the acquisition, as they do for a local read. A
  dead-letter copy (`errors.deadletterqueue.copy.record.enable`) reads its
  source record from the remote tier too, as Kafka's
  `ShareGroupDLQRecordFetcher` does. Before, the share fetch answered
  `UNKNOWN_SERVER_ERROR` for every tiered offset, so the consumer never got
  the records, and a copy of a tiered record carried headers alone.
- The active segment rolls when its offset index or its time index is full
  under `segment.index.bytes`, as Kafka's `LogSegment.shouldRoll` does. Before,
  krabka stored the key and did nothing with it, so a topic that sets
  `segment.index.bytes=12` and `index.interval.bytes=1` to put each batch in
  its own segment kept all of its batches in one segment.
- A controlled shutdown of a controller-only node stops the node at once, as
  Kafka stops a controller-only process. The node leads no partition and
  runs no heartbeat client, so no controller ever answers its drain. Before,
  the node waited for the whole drain timeout, and then stopped through the
  hard shutdown with `ShutdownTimeout`. So every stop of an isolated
  controller took the full drain timeout.
- The broker reads Kafka's `group.consumer.migration.policy` from
  `[server_properties]`, without regard to case, as Kafka does. A value other
  than `disabled`, `upgrade`, `downgrade` or `bidirectional` stops the broker
  at startup. `DescribeConfigs` reports the key at `STATIC_BROKER_CONFIG`.
  Before, the broker ignored the key and always ran the `bidirectional`
  policy. Kafka's `ConsumerProtocolMigrationTest` sets the key in each of its
  tests, for example to `disabled` in `test_consumer_offline_migration`.
- The share coordinator answers a share-state request only after its
  `__share_group_state` records commit, as Kafka's `CoordinatorRuntime` does.
  It appends a record only while it leads the partition at the leader epoch
  of its load, and it stamps that epoch on the batch. It then waits until the
  high watermark covers the last record that it wrote in that term. A read
  waits too. If the partition gets a new leader or a new leader epoch first,
  the coordinator answers `NOT_COORDINATOR`. If the wait takes longer than
  `share.coordinator.write.timeout.ms`, it answers
  `COORDINATOR_NOT_AVAILABLE`. The prune of the state log waits for the
  commit too. Before, the coordinator acknowledged a write when the record
  reached its local log, so a failover could lose acknowledged state.
- A follower `Fetch` that the leader answers from its first read reports the
  high watermark from before the leader recorded the follower's fetch
  offset, as Kafka's `Partition.fetchRecords` does. The leader records that
  position only after a successful read. A read that fails, or that finds a
  diverging epoch, records no position, as in Kafka. A follower fetch that
  parks reads again on a wake or at expiry, and then reports the high
  watermark that its fetch moved. Before, the leader recorded the position
  before the read, also for a read that then failed. So a follower learned a
  high watermark in the same fetch that moved it.
- The group coordinator answers a request only once its `__consumer_offsets`
  records are committed, as Kafka's `CoordinatorRuntime` does. This holds for
  `OffsetCommit`, `JoinGroup`, `SyncGroup`, `ConsumerGroupHeartbeat`,
  `ShareGroupHeartbeat` and every other group write. The coordinator appends
  only while it leads the partition, and it stamps the leader epoch on the
  batch. It then waits until the high watermark covers the batch while it
  still leads at that epoch. The wait lasts at most 5 seconds, the default
  of Kafka's `offsets.commit.timeout.ms`. When a write does not commit,
  `OffsetCommit` and `ShareGroupHeartbeat` answer `NOT_COORDINATOR` after a
  leader change, and `COORDINATOR_NOT_AVAILABLE` after the timeout. Before,
  the coordinator answered at the local append. In Kafka's
  `ShareConsumerTest.test_broker_failure`, a coordinator gave three members
  epochs that only its own log held, and then stopped. The next coordinator
  did not know the members, and it answered each heartbeat with
  `GROUP_ID_NOT_FOUND`, which a share consumer cannot recover from.
- In a cluster with isolated controllers, the first broker-only node to
  start creates the `__krabka_audit` topic, with one partition on each
  registered broker. The node submits the records itself, and its metadata
  source forwards them to the quorum leader. It then waits for the topic to
  reach its own metadata image, for at most the time that the `[runtime]`
  key `audit_partition_wait_timeout` sets. A controller-only node never
  places an audit partition on itself, and with no registered broker it
  creates nothing, as Kafka places replicas on registered brokers only.
  Before, only the quorum leader created the topic. A controller-only leader
  saw no registered broker at its start, and put the only partition on
  itself. No broker served that partition, so no broker wrote audit events.
- A partition leader sends `AlterPartition` to the CONTROLLER listener of
  the active controller, as Kafka's `AlterPartitionManager` does. It finds
  the endpoint in the KIP-853 voter set or in `controller_quorum_voters`, and
  it connects with the TLS and SASL settings of that listener. Before, the
  leader looked for the controller among the registered brokers, and sent
  the request to their broker listeners. A controller-only node has no
  broker registration, so with an isolated controller the request never
  reached it. The controller committed no ISR change, and a follower that
  restarted and caught up never rejoined the ISR.
- `CreateTopics` and `CreatePartitions` place a replica on a broker that a
  controlled shutdown stopped, as a fenced last resort, as Kafka does. Its
  registration keeps `InControlledShutdown` until it registers again, but
  Kafka's `BrokerHeartbeatManager.touch` takes a fenced broker out of
  controlled shutdown. The ISR leaves the stopped broker out. A broker that
  is in controlled shutdown and not fenced stays out of the placement.
  Before, the placement left out every broker whose registration had
  `InControlledShutdown`. After a clean stop of one of three brokers, a topic
  at replication factor 3 failed with `INVALID_REPLICATION_FACTOR`. In
  Kafka's `ShareConsumerTest.test_broker_failure`, the broker could not
  create `__share_group_state`, so the share group never initialized its
  partitions.
- `DescribeProducers` answers from the producer state of the partition log on
  the leader and on every follower, as Kafka's
  `ReplicaManager.activeProducerState` does through
  `UnifiedLog.activeProducers`. Before, it read the producer state of the
  produce path. A follower does not add the data batches that it replicates to
  that state, so a follower reported no idempotent producers and no open
  transactions. The leader reported the timestamp of the last data batch after
  a transaction marker, where Kafka reports the timestamp of the marker. A
  partition in an offline log directory now answers `KAFKA_STORAGE_ERROR`, and
  the producers come in producer id order. The broker removes a producer from
  the log when `producer.id.expiration.ms` has passed since its last write and
  it has no open transaction, as Kafka's
  `ProducerStateManager.removeExpiredProducers` does.
- A controller-only node (`[process] roles = ["controller"]`) opens no client
  listener, as Kafka's `ControllerServer` opens only the listeners that
  `controller.listener.names` names. Before, it also bound `--listen-addr`,
  `127.0.0.1:9092` by default, so a client reached it there and two
  controller-only nodes on one host competed for that port. A
  controller-only node that names `[[listeners]]` does not start: Kafka's
  `KafkaConfig` refuses a `listeners` config that names any listener outside
  `controller.listener.names` when `process.roles=controller`. The node closes
  a data-plane listener passed to `Broker::start_with_listeners`. It starts no KIP-405
  tiered storage, which Kafka runs on brokers only. Every node now logs
  `krabka-broker started` when its start is complete, as Kafka logs
  `Kafka Server started`. The line names `listen_addr` for the client
  listener of a node with the broker role, and `controller_listen_addr` for
  the controller listener of a node with the controller role. It replaces
  `krabka-broker listening`, and the ducktape adapter waits for it.
  `BrokerHandle::data_plane_addr` is `None` on a controller-only node, and
  `BrokerHandle::listen_addr` gives the controller listener there.
- `DescribeConfigs` for a broker reports the listener keys of the node's roles,
  as Kafka does. On a controller-only node, `listeners` names only the
  `CONTROLLER` listener and `advertised.listeners` is null at `DEFAULT_CONFIG`.
  On a broker-only node, `listeners` names only the data-plane listeners. On a
  combined node, it names both. `controller.listener.names` is `CONTROLLER` at
  `STATIC_BROKER_CONFIG` on each role, `listener.security.protocol.map` names
  the protocol of each listener and of the controller listener, and
  `inter.broker.listener.name` is at `STATIC_BROKER_CONFIG` when the operator
  named it. Before, a controller-only node reported data-plane listeners that
  it does not open, through `kafka-configs --bootstrap-controller` too.
- Log compaction on a follower keeps the last batch of each active producer,
  as it does on the leader. The cleaner reads the active producers from the
  producer state of the partition log, which a follower updates for each batch
  that it replicates, as Kafka's `Cleaner.cleanSegments` reads
  `UnifiedLog.lastRecordsOfActiveProducers` on every replica. It keeps the
  last data batch of each active producer as an empty batch, or the marker
  that started the producer's current epoch, as `Cleaner.cleanInto` does.
  Before, the cleaner read the producer state of the produce path, which on a
  follower holds only the transaction markers, so a follower that compacted
  its log removed the last batch of an idempotent or transactional producer
  when newer records replaced its keys. After a promotion, the log then held
  no batch with that producer's last sequence. The cleaner now keeps a
  producer until the periodic `producer.id.expiration.ms` check removes it,
  as Kafka does.
- The active controller writes the bootstrap records of a new cluster once,
  and a broker never writes them, as in Kafka. A leader whose metadata log
  holds no `metadata.version` writes the records directly after the
  `LeaderChange` batch of its epoch, as `QuorumController` does with
  `ActivationRecordsGenerator.recordsForEmptyLog`. When the records enable
  `eligible.leader.replicas.version`, the same batch ends with the
  cluster-level `min.insync.replicas` at the controller's static value, as in
  Kafka, so `DescribeConfigs` and `CreateTopics` report it at
  `DYNAMIC_DEFAULT_BROKER_CONFIG`. The controller takes the records from the
  bootstrap checkpoint of a dynamic format, or else from
  `bootstrap.records.bin`, and it does not apply the records of the bootstrap
  checkpoint to its image, as Kafka's `handleLoadBootstrap` keeps them for the
  activation. A controller that cannot write them stops with the fatal fault
  `exception while completing controller activation`, and the process exits
  with status 1, as Kafka's `fatalFaultHandler` halts it. A broker waits until
  its image finalizes `metadata.version`, and then registers. Before, a
  controller formatted with `--standalone` kept the records of its bootstrap
  checkpoint in its image and out of the log, so broker-only nodes saw no
  `metadata.version` and each one submitted its own copy, as Kafka's
  `quorum_reconfiguration_test.py` showed. `krabka-format` writes the
  bootstrap checkpoint as Kafka's `Formatter.writeBoostrapSnapshot` does: the
  control state, then the bootstrap records in their order. Bootstrap records
  that do not finalize `metadata.version` stop the start.
- A diskless Produce that arrives while the controller quorum elects a leader
  answers `NOT_LEADER_OR_FOLLOWER`, as Kafka's produce path does when a broker
  cannot append because leadership moves. The client refreshes its metadata
  and sends the batch again. The quorum refuses the offset reservation before
  it reserves an offset: it has no leader, or its new leader has not committed
  its epoch. A new leader now refuses the reservation with the same
  uncommitted-tail error that it gives a compare-and-set. Before, the broker
  answered `KAFKA_STORAGE_ERROR`, which says that a disk failed.

## [0.7.0] - 2026-10-02

### Added

- `group.streams.rack.aware.assignment.tags` is the `[runtime]` key
  `streams_group_rack_aware_assignment_tags`, with the flag
  `--streams-group-rack-aware-assignment-tags`. It is the default of each
  group's `streams.rack.aware.assignment.tags`, and it refuses an empty or a
  repeated tag key with Kafka's messages.
- Kafka's `share.coordinator.state.topic.compression.codec`,
  `share.coordinator.threads`, `share.coordinator.append.linger.ms` and
  `share.coordinator.cached.buffer.max.bytes` are accepted under `[runtime]`
  (#939). Only the codec has an effect: it compresses every
  `__share_group_state` batch.

### Changed

- **Strict Kafka 4.3.1 by default** (#784). With no opt-in, every listener,
  the broker's and the controller's alike, advertises and accepts exactly what
  a stock `apache/kafka:4.3.1` does. A version or api key only Kafka trunk
  has closes the connection, as 4.3.1 closes it: `TxnOffsetCommit` v6,
  `StreamsGroupHeartbeat` and `StreamsGroupDescribe` v1,
  `StreamsGroupTopologyDescriptionUpdate` (93), `UnregisterController` (94),
  and an `Envelope` that wraps one of them. `ApiVersions` v5 is answered
  `UNSUPPORTED_VERSION` with the v0-v4 range. The pre-4.0 `Fetch` v0-v3,
  `ListOffsets` v0 and `Produce` v0-v2 are refused too, and `Produce` is still
  advertised from v0 (KAFKA-18659). `metadata.version` is supported only up
  to `4.3-IV0` (30). That cap applies to `ApiVersions`, node registration,
  `UpdateFeatures` (Kafka's `Local controller N only supports versions 7-30`)
  and `krabka format`. CIDR ACL hosts (4.4-IV1) and controller unregistration
  (4.4-IV2) are therefore unreachable by default. A group resource carries
  4.3.1's 20 `GroupConfig` keys, and trunk's seven are `Unknown group config
name`. A topic resource carries 4.3.1's `LogConfig` keys (with krabka's
  own), so trunk's `remote.copy.lag.ms`, `remote.copy.lag.bytes`,
  `max.decompressed.message.bytes` and `errors.deadletterqueue.group.enable`
  are `Unknown topic config name` in `CreateTopics`, `AlterConfigs` and
  `IncrementalAlterConfigs` and are left out of `DescribeConfigs`, with the
  broker synonyms of the first three. An idempotent producer with no state may
  start at any sequence, as on 4.3.1. There are three opt-ins:
  - `server_properties` `unstable.api.versions.enable = "true"`, Kafka's own
    switch, serves the trunk versions, api keys, group keys and topic keys
    above, plus `InitProducerId` v6. It also applies trunk's KAFKA-15591 rule
    (#907): a producer with no state on a partition that has never held a
    record must start at sequence 0, and is otherwise answered
    `OUT_OF_ORDER_SEQUENCE_NUMBER` with nothing appended. With `transaction.two.phase.commit.enable` it also
    serves krabka's KIP-939 `keepPreparedTxn` recovery, which 4.3.1 answers
    `UNSUPPORTED_VERSION`.
  - `server_properties` `unstable.feature.versions.enable = "true"`, Kafka's
    own switch, supports `metadata.version` up to trunk's `4.4-IV2` (33).
    `krabka format --unstable-feature-versions-enable` is the same setting
    for a format.
  - `[runtime] legacy_request_versions_enable = true`, krabka-only, serves the
    pre-4.0 `Fetch`, `ListOffsets` and `Produce` versions again.
- **Breaking, config.** In `[runtime]`, the share-group lock limit is
  `share_group_partition_max_record_locks` and the delivery-attempt limit is
  `share_group_delivery_count_limit`, after the Kafka keys they set.
  `share_group_isolation_level`, `acl_max_principal`, `acl_max_resource_name`
  and `streams_internal_topic_replication_factor` are gone, with their flags.
  Kafka has none of them. A share group reads with its own
  `share.isolation.level`, `read_uncommitted` by default. A streams internal
  topic whose topology sets no replication factor gets
  `default.replication.factor`.
- **Breaking, on-disk format.** `krabka-format` writes the cluster id and the
  directory ids in Kafka's 22-character base64 form, in
  `meta.properties.json`, in `bootstrap.json` and on stdout, and the format
  stamp is now version 3. The broker refuses a version 2 directory with
  `unsupported meta.properties version`; run a fresh `krabka-format`.
  `--cluster-id`, `--directory-id` and `--initial-controllers` accept Kafka's
  form as `Uuid.fromString` does, and the hyphenated form.
- `krabka-format` formats every directory of a node in one run: `--log-dir`
  is repeatable and comma-separated, and the first directory is the metadata
  log directory. `--ignore-formatted` skips the formatted directories and
  formats the rest; without it, one formatted directory refuses the run with
  Kafka's message. A run that fails partway can be run again without an
  `rm -rf`. `docs/format-divergences.md` lists every difference from
  `kafka-storage format`.
- The broker share-group settings take Kafka 4.3.1's defaults, ranges and
  minimum and maximum keys, and refuse an out-of-order triple with Kafka's
  `require` messages (#959, #958). The record lock limit defaults to 2000.
- `unstable.api.versions.enable`, read from `[server_properties]`, gates the
  API versions Kafka marks unstable on both listeners. Off, its default, the
  broker advertises only stable versions and closes a connection that sends
  an unstable one (#646).
- The `share_group_enable` setting is removed. Share groups follow the finalized
  `share.version`.
- The private MetadataFetch (1004) request grew from 12 to 32 bytes, and the
  delegation token metadata record gained a requester. Delete local data
  directories, and use `krabka-protocol` from the commits named in
  `Cargo.toml`.
- `__remote_log_metadata` is created with `min.insync.replicas=2`; single-broker
  setups set `[remote_storage.kafka_metadata] min_isr = 1`.

### Fixed

- ApiVersions matches Kafka 4.3.1 on the broker and controller listeners: the
  unsupported-version answer, the api-key order, the supported and finalized
  feature rows, and `finalized_features_epoch` (#842, #783). Produce keeps
  serving v0 to v2 on purpose, and the KIP matrix records it (#863).
- UpdateFeatures applies the whole request or none of it, validates each row
  with Kafka's checks and messages, and decides a `metadata.version`
  downgrade on Kafka's `didMetadataChange` table (#779, #780, #781).
- ListOffsets fences as Kafka does while the high watermark trails the leader
  epoch's start (KIP-207, #879).
- WriteTxnMarkers retries, drops or cancels a partition by its error code as
  Kafka's completion handler does (#882). An AddPartitionsToTxn v4+ request
  that repeats a transactional id gets one result per entry (#883).
- StreamsGroupHeartbeat v1 carries `MISSING_CLIENT_TAGS` when a member does
  not send every configured rack-aware tag key (#972).
- CreateAcls has no length limit on a resource name or a principal, an IPv4
  CIDR ACL never matches `0.0.0.0`, and CreateDelegationToken with an owner
  name and no owner type answers `UNKNOWN_SERVER_ERROR` (#772, #652, #765).
- OffsetCommit and TxnOffsetCommit check an older member epoch against the
  epoch at which each partition was assigned (KIP-1251, #800).
- DescribeShareGroupOffsets answers an unknown topic, a failed share-state
  read and an explicit topic list as Kafka 4.3.1 does, and takes the lag's
  end offset from each partition's leader (#943).

The rest of this section comes from the Kafka compatibility audit (#1248).
It matches Kafka 4.3.1 by default and Kafka trunk under the unstable flags.

- Next-generation consumer groups follow Kafka's revocation protocol: a joining
  member gets a partition only after the incumbent reports it released, and a
  member is told when its assignment shrank (#1178). `SubscribedTopicRegex`
  must match a whole topic name, an empty pattern removes the subscription and
  a heartbeat without a pattern keeps the stored one (#1179, #1204). The
  resolved topics are persisted as `ConsumerGroupRegularExpression` records and
  refreshed after a topic creation, an ACL change and every 10 minutes, so a
  coordinator failover keeps them.
- `ConsumerGroupHeartbeat` on a missing group or on a share or streams group
  answers `GROUP_ID_NOT_FOUND` (#1205), and `ConsumerGroupDescribe` hides
  topics the caller cannot describe (#1206). `group.consumer.max.size` defaults
  to 2147483647 (#1207), and the server assignor is the one most members name
  (#1208). `StreamsGroupHeartbeat` v0 refuses static membership, task offsets
  and warm-up tasks unless `unstable.api.versions.enable` is on (#1209, #1247).
- Group config alters accept every 4.3.1 `GroupConfig` key with Kafka's bounds,
  and the consumer and share coordinators apply the per-group session,
  heartbeat and assignment-interval overrides. A new target assignment waits
  for `assignment.interval.ms` (#1186, #1236).
- After a session expiry the survivors of a classic group get the group's
  rebalance timeout. A member that never sends SyncGroup is removed, LeaveGroup
  for a pending member id answers `NONE`, unloading a group answers parked
  JoinGroup and SyncGroup with `NOT_COORDINATOR`, JoinGroup v0 uses the session
  timeout as its rebalance timeout, and offsets of an empty group expire from
  the moment it emptied (#1181, #1237). ListGroups answers
  `COORDINATOR_LOAD_IN_PROGRESS` while an offsets partition loads (#1182).
- The share APIs are gated on the finalized `share.version` and no longer on
  `group.share.enable`, which is gone (#1226). `group.share.partition.max.record.locks`
  caps the records in flight (#1224), a late acknowledgement or renewal gets
  `INVALID_RECORD_STATE` (#1225), and the trunk-only share state rules apply
  only under `unstable.api.versions.enable` (#1238). Under
  `unstable.feature.versions.enable`, `share.version` 2 is advertised and
  rejected or delivery-exhausted records go to the group's dead-letter topic
  (KIP-1191, #1227).
- Transactions choose their behavior from the request version, not the
  cluster's `transaction.version`, so older EndTxn and AddPartitionsToTxn
  clients can run consecutive transactions on one producer (#1180).
  TxnOffsetCommit is verified with the transaction coordinator at every
  version (#1228), a timed-out or re-initialised producer is fenced once at
  every transaction version (#1229, #1230), and the trunk-only
  `__transaction_state` tags are written only under
  `unstable.api.versions.enable` (#1239).
- Log compaction drops the records of aborted transactions and no longer lets
  them win the dedup map, and markers expire per transaction (#1177, #1196).
  `.index` and `.timeindex` files pass `kafka-dump-log` verification (#1198).
  `Log::truncate_to` below the log start resets the log (#1197), tiered copy
  stops at the last stable offset (#1200), `__remote_log_metadata` gets
  `min.insync.replicas=2` (#1199), and `min.compaction.lag.ms` holds back the
  oldest dirty segment (#1245).
- Cluster-wide and per-broker dynamic defaults of topic keys reach Produce and
  the partition log configuration (#1187), and a per-broker
  `min.insync.replicas` governs `acks=all` on that broker. Broker config alters
  refuse every non-dynamic `KafkaConfig` key and range-check every dynamic one
  (#1183), and DescribeConfigs types every stored key and reports the static
  layers as Kafka does (#1184, #1185). A config value above 32767 characters is
  `INVALID_CONFIG` (#1236).
- Disk errors answer `KAFKA_STORAGE_ERROR` per partition on Produce and Fetch
  (#1188), a Fetch for a known but unhosted partition answers
  `NOT_LEADER_OR_FOLLOWER` (#1189), a CRC mismatch answers `CORRUPT_MESSAGE`
  (#1190), the fetch-session cache evicts by Kafka's rule (#1191), and
  rack-aware replica selection prefers a leader in the client's rack and then
  the most caught-up follower (#1192, #1234).
- Metadata omits fenced brokers and brokers with no endpoint on the connection
  listener and answers `LEADER_NOT_AVAILABLE` or `LISTENER_NOT_FOUND` for them
  (#1202). Automatic placement starts at a random broker per topic and uses
  fenced brokers last (#1201, #1203), and denied cluster-level shortcut probes
  are no longer audited (#1235).
- GSSAPI `auth_to_local` follows `KerberosShortNamer`: `DEFAULT` maps
  `service/host@REALM`, `(match)` is a whole-string match and `/U` works
  (#1220). `ssl.principal.mapping.rules` use `java.util.regex` semantics
  (#1221). DescribeDelegationToken v3 reports the real requester, who can find
  and see the token (#1222). The token record holds the requester and no HMAC,
  as Kafka's `DelegationTokenRecord` does. Every use recomputes the HMAC-SHA-512
  from `delegation.token.secret.key` with `krabka-security`, so the token
  password is base64 of that HMAC and no token authenticates without a secret
  key (#1240). `allow.everyone.if.no.acl.found` covers non-transactional InitProducerId
  (#1223). SASL_SSL listeners no longer map the client certificate, CreateAcls
  and DeleteAcls have Kafka's 10000-ACL bound and message, and hosts are stored
  as text below `metadata.version` 4.4-IV1 (#1240).
- `(?i)` in a `java.util.regex` pattern folds ASCII case only, as Java's
  `CASE_INSENSITIVE` does, until `(?u)` or `(?U)` asks for Unicode case. That
  covers `ssl.principal.mapping.rules`, GSSAPI `auth_to_local` rules and the
  `match` of a client-metrics subscription, so `(?i)service-` no longer
  matches `ſervice-` (long s). Under `(?iu)` a letter, a class member and a
  class range match what Java's `Character.toLowerCase` of
  `Character.toUpperCase` accepts, and not what `fancy_regex`'s simple case
  folding does, in a character class as well: `\w`, `\W`, `\p{Lower}` and
  `\p{Upper}` in a class do not match long s or the Kelvin sign, a range that
  has `K` and not `k` does not match the Kelvin sign, and `İ` and `ı` match
  `i`. The POSIX classes `\p{Alpha}`, `\p{Alnum}`, `\p{Punct}` and the rest
  are ASCII unless `(?U)` is on, as in Java. The translation reads `\Q...\E`
  before the rest as Java does, so a quoted member can begin or end a class
  range, a `]` first in a class may start one, and a backslash before `<` or
  `>` is that character. The flag `x` ends with the group it is set in. A
  backreference under a case flag is accepted and compares as `fancy_regex`
  does, which differs from Java's for some non-ASCII letters. `\N{name}` is
  refused, and so is a `{` outside a class that does not start a quantifier's
  bounds, such as `a{x}` or a lone `{`, which is Java's `Illegal repetition`
  (#1248).
- Under `share.version` 2 the dead-letter records of every pending write to a
  destination leader go out in as few Produce requests as `max.message.bytes`
  allows, with one request in flight per leader, as Kafka's
  `ShareGroupDLQStateManager` sends them. The broker exports
  `krabka_broker_share_group_dlq_records_total`,
  `krabka_broker_share_group_dlq_produce_requests_total` and
  `krabka_broker_share_group_dlq_failed_produce_requests_total` per group. A
  write that races another creation of the topic goes on once the topic exists,
  and a copied record that the six headers push over `max.message.bytes` is
  written with its headers alone (#1227).
- With `unstable.api.versions.enable`, `max.decompressed.message.bytes` also
  applies to log compaction (a partition with a batch over the limit stays
  uncleaned until the limit changes), to by-timestamp ListOffsets (answered
  `INVALID_RECORD`) and to a share group's by-duration start offset (#1236).
- A combined broker whose controller refuses a feature level above its range
  now exits non-zero with the reason, at startup or when the record is applied
  later, as Kafka halts. The binary has a `--controller-listen-addr` flag (#1243).
- `transaction.partition.verification.enable` is read from the static
  configuration too: the `[runtime]` key
  `transaction_partition_verification_enable`, the flag
  `--transaction-partition-verification-enable` or the `server.properties`
  entry. A dynamic per-broker or cluster value wins over it (#1239).
- A group, member, instance or topic string over 32767 bytes no longer panics a
  task. A group request that carries one closes the connection, as Kafka's
  reader does, a broker-generated classic member id over that length answers
  `UNKNOWN_SERVER_ERROR`, `AlterBarrierGroups` and `WriteBarrierMarkers`
  answer `INVALID_REQUEST`, and a Fetch topic name that long is a protocol
  error below v12 (#1248).
- Quota buckets go into debt, so the throttled overage is not credited back
  (#1212). A shared bucket is re-rated from its own entity (#1213), `ip`
  entities match host names and non-canonical spellings (#1214), a null client
  id draws no client-level quota, a rate change keeps the bucket's balance and
  `throttle_time_ms` rounds to the nearest millisecond (#1241). A replication
  throttle follows the broker's own id and exempts in-sync and caught-up
  replicas (#1210, #1211).
- A response frame is limited only by its size prefix (#1231). The controller
  listener enforces the request-size, idle-timeout and connection limits
  (#1232), and `sasl.server.max.receive.size` bounds a frame before a SASL
  login completes (#1233). ApiVersions, UpdateFeatures and the KIP-590
  envelope match Kafka on the eight smaller points of #1242.
- DescribeQuorum lists every non-voter that fetches the metadata log, with real
  timestamps, and drops an observer after five minutes of silence (#1193).
  AddRaftVoter and the vote and pre-vote handlers follow `KafkaRaftClient`
  (#1194, #1243). `LastKnownElr` holds the last leader of a leaderless
  partition, and that leader is elected when it returns (#1195).
- A follower truncates to the intersection with its own epoch history
  (#1215), a reassignment that lowers the replication factor waits for the ISR
  (#1216), ElectLeaders avoids a replica on a dead log directory (#1217), the
  automatic leader rebalance repeats while it is capped (#1218), and a
  truncation cuts an in-flight log-directory move (#1219, #1244).
- KIP-714: a rejected terminating push locks the instance out, closing a
  connection drops the instance it created, and `client_source_address` is the
  JDK's text (#1246).

## [0.6.1] - 2026-09-26

A patch release that makes secured disaster recovery work on a diskless
cluster, and a wave of Kafka-conformance fixes to Fetch, Produce, ListOffsets,
transactions and log retention.

### Changed

- `krabka-backup capture` only reads `__diskless_wal_index`. It captures each
  partition from its low watermark up to the high watermark it saw when the
  capture started, and it no longer writes a replay fence of its own. The
  backup principal needs READ and DESCRIBE on the index, not WRITE.
- A segment rolls on Kafka's append-time rule: when the next batch would
  overflow `segment.bytes`, or when the gap since the active segment's first
  timestamp passes `segment.ms`. An empty active segment never rolls on age.
- `log.message.timestamp.before.max.ms` and
  `log.message.timestamp.after.max.ms` set in the broker config apply to every
  topic that does not override them.

### Fixed

- `krabka-backup capture` failed with `broker error_code 17` on any cluster
  with a `__diskless_wal_index` topic, because the broker refuses a Produce to
  an internal topic from any client other than `__admin_client`. The read-only
  capture streams the index one Fetch page at a time and follows a leader move
  within its 30 s bound.
- Fetch sessions are classified on `session_epoch` as Kafka does, so a
  reconnecting consumer gets a new session rather than
  `INVALID_FETCH_SESSION_EPOCH`.
- Fetch honors the request's `max_bytes` across all partitions, error rows
  carry Kafka's `-1` watermarks, a preferred read replica answers without
  reading the log, and a `read_committed` response lists only the aborted
  transactions in the range it served.
- Produce: `acks` outside `-1`, `0` and `1` answers `INVALID_REQUIRED_ACKS`; an
  `acks=0` request with a partition error closes the connection; zstd below
  Produce v7 answers `UNSUPPORTED_COMPRESSION_TYPE`; a nonzero `base_offset` is
  accepted; and keyless or out-of-window records get Kafka's per-record
  `record_errors` and message. `min.insync.replicas` is clamped to the replica
  count for `acks=all`.
- ListOffsets fills `leader_epoch`, answers `OFFSET_NOT_AVAILABLE` (KIP-207)
  while the high watermark trails the leader epoch's start, and clamps
  `EARLIEST_PENDING_UPLOAD` to the log start offset.
- Transactions: WriteTxnMarkers retries only retriable per-partition errors
  and no longer abandons the rest of a fan-out; completion persists when the
  log end offset does not move; AddPartitionsToTxn v4+ returns one row per
  partition; and concurrent partition registrations are serialized.
- Compaction writes each output segment's `.txnindex` from its own inputs,
  treats every output of a pass as clean, and closes consumed segments before
  it swaps the outputs in.

## [0.6.0] - 2026-09-26

Milestones 21 to 23: a release is now qualified against the exact image it
delivers, across installation, operator lifecycle, CLI administration,
observability, disaster recovery, schema evolution, registry migration and
snapshot retention. A long run of Kafka-conformance work changes on-disk
formats, error codes and authorization, so read the breaking entries before
you point an existing data directory at this build.

### Added

- Release images are multi-platform: one signed image index holds `linux/amd64`
  and `linux/arm64`, and its `linux/amd64` child is the digest CI tested.
- Ecosystem qualification binds its evidence to the delivered image digest and
  runs eight gates: installation, operator lifecycle, CLI administration,
  observability and recovery, secured disaster recovery, schema evolution,
  schema-registry migration and snapshot retention. A qualification release is
  published only when all eight pass.
- Secured disaster recovery: `krabka-backup` and `krabka-restore` work against
  SASL/TLS clusters, a signed WORM head is verified before a restore, diskless
  WAL batches replay under the restore predicates, and a real JBOD `ENOSPC`
  is covered.
- CIDR-range ACL hosts (KIP-1276), for example `10.0.0.0/8`, including
  IPv4-mapped IPv6 peers.
- `allow.everyone.if.no.acl.found`, with Kafka's semantics.
- `crates/docgen`, the benchmark harnesses and the diskless Jepsen suite live in
  this repository, and Bazel compiles the benches.

### Changed

- **Breaking, on disk.** `.txnindex` files use Kafka's 34-byte,
  version-prefixed `AbortedTxn` layout, `TransactionLogValue` records carry
  `LastProducerEpoch` and `ClientTransactionVersion`, and krabka's private
  metadata records ride as a tagged field of a `NoOpRecord`, so
  `kafka-dump-log` and `kafka-metadata-shell` read the log and its checkpoints.
  Delete data directories written by an earlier build and format them again.
- The cluster id is reported in Kafka's URL-safe base64 `Uuid` form in
  `Metadata` and `DescribeCluster`, not the hyphenated form.
- Every controller-listener request is authorized against the connection
  principal with the operation its API needs, as Kafka's `ControllerApis`
  does. Controller-scoped APIs are no longer reachable or advertised on a
  broker listener, whatever `inter.broker.listener.name` names.
- ACL operation implication (Read, Write, Delete or Alter implies Describe)
  widens ALLOW ACLs only. A DENY no longer implies a DENY on Describe.
- A transactional Produce is verified with the transaction coordinator before
  it is appended (KIP-890 part 1). A batch for a partition the client never
  added to the transaction is refused.
- A client Produce to an internal topic is refused with
  `INVALID_TOPIC_EXCEPTION` unless its `client_id` is `__admin_client`.
- The KIP-124 request quota is charged for every API Kafka charges, and a
  connection over `connection_creation_rate` is closed rather than delayed.

### Fixed

- About 150 request paths now answer with Kafka's error codes, row order and
  authorization checks, including `ListOffsets`, `Fetch`, `OffsetForLeaderEpoch`,
  `DeleteRecords`, `DescribeTopicPartitions`, `CreateTopics`, `DeleteTopics`,
  `AlterConfigs`, the partition-reassignment APIs, the KIP-853 voter APIs,
  every transaction API, and the consumer, share and streams group APIs.
  Unresolved topic ids answer `UNKNOWN_TOPIC_ID` across the topic-id versions.
- Idempotence: a retry of any of a producer's last five batches is answered as
  a duplicate, the first batch at a new producer epoch must be sequence 0, and
  `InitProducerId` rotates the producer id at the epoch ceiling.
- The last stable offset holds until the high watermark passes the transaction
  marker, the leader recomputes its high watermark before a produce is
  acknowledged, and aborted transaction data is archived for a
  `read_committed` share group.
- `__transaction_state` and `__share_group_state` are loaded and unloaded on a
  leadership change, and coordinator leadership follows the partition leader
  epoch.
- A raft leader no longer truncates its own log when it answers a diverging
  `Fetch`. A joining controller stays attached to the current leader, and a
  removed controller leader stays reachable until its removal commits.
- Time and size retention run only when `cleanup.policy` includes `delete`, and
  a record without a key is refused on a compacted topic.
- A deleted group's offsets are tombstoned so they do not come back on reload.
- The diskless flusher stops at shutdown when a flush outlasts its interval.
- The registry-migration qualification gate makes `host.docker.internal`
  resolve on the runner, since the handoff harness's in-process broker
  advertises that name to the host-side store as well as to the
  `cp-schema-registry` container. The gate had never passed.

## [0.5.4] - 2026-09-02

Milestones 5 and 6: a deployed cluster can now be probed, measured, watched and
operated without reading the source. Every hot path the design argues about
carries a benchmark, and metadata-backed metric series are released during
routine reassignment and topic deletion.

### Added

- `/healthz` and `/readyz` on their own listener, and reference Kubernetes
  manifests under `packaging/k8s/` that wire both probes and the format step.
  Readiness waits for log-dir recovery, bound listeners, and a metadata offset
  within `--readiness-max-metadata-lag` of the quorum's committed offset, which
  the KRaft engine and the metadata observer now track separately from the
  offset this node has applied.
- Per-partition follower lag and per-group consumer lag, for classic and
  KIP-848 groups, with a max-lag rollup. A stuck consumer is now visible before
  its data ages out.
- `krabka_broker_fetch_response_drain_total{path="sendfile"|"vectored"|"pread"}`
  and a kTLS gauge, so an operator can see which drain path a fetch took rather
  than inferring it. A regression that routed every fetch onto the copy path
  used to move no series at all.
- The container image includes `krabka-format`, `krabka-audit`,
  `krabka-barrier`, `krabka-guard`, `krabka-worm-verify` and `krabka-restore`.
  The image could not previously run the one mandatory
  pre-boot step, and has no shell to work around it with.
- Benchmarks for the produce hot path, the fetch hand-off, the KIP-227 session
  cache, the fetch drain's sendfile crossover, the per-observation metric label,
  and both documented PERF deferrals. Run them with `cargo bench -p krabka-broker`.
- Creusot proofs for the remaining safety-critical algorithms, and the catalog
  that records which algorithm each session covers.
- `krabka-broker --print-config-schema` prints the JSON schema of the config
  file, and [`docs/config-reference.md`](docs/config-reference.md) is
  generated from it, with example `broker.toml` files under `docs/examples/`.
- An operator guide under [`docs/operations/`](docs/operations/README.md):
  deploy, capacity and metrics, a reference Grafana dashboard, Prometheus
  alert rules, and one runbook per alert.
- A generated [KIP matrix](docs/KIP_MATRIX.md), and a design document per
  subsystem under each crate's `docs/`.
- CI gates for the KFC template, the design documents, the generated config
  reference and the metrics contract.
- A README for every crate, `SECURITY.md` and `CODEOWNERS`.
- The rustdoc set, published to GitHub Pages on every push to `main`.
- A reproducible real-cluster performance qualification now drives the same
  external workload against pinned Kafka and Krabka clusters under equal
  durability and resource settings. Its published three-run comparison and
  partition-envelope result include raw provenance, exact reconciliation,
  tail latency, resource snapshots, metrics cost, controller failover,
  readiness and reassignment; the measured 10,000-partition tier passed.
- The nightly Criterion lane now alternates three reference and candidate runs
  on one host and fails a machine-readable, raw-sample-backed verdict when a
  benchmark exceeds its variance-calibrated tolerance.
- A checked-in ecosystem qualification manifest records the exact broker,
  operator, CLI, observability, demo and client-stack revisions as one candidate
  set. CI validates the draft contract; the manual final gate additionally
  requires immutable artifact digests, passed evidence for installation,
  operator lifecycle, authenticated CLI administration and four-signal WAL
  recovery, plus a content-addressed published report.
- A three-worker kind lane now applies the reference Kubernetes manifests,
  proves quorum pods land on distinct nodes, produces and consumes through the
  bootstrap Service, and verifies the data again after a rolling restart. The
  manifests carry required hostname spreading and bounded init/broker
  resources. A scheduled previous-release lane also exercises the persisted
  surfaces in one log directory and expects either compatible recovery or the
  format refusal declared below. The new scaling guide and stalled-
  reassignment runbook cover broker addition, throttled data movement,
  decommission and the required break-glass approval.
- `krabka-backup`, the operator tool for the restore inputs a KIP-405 archive
  does not hold. `capture` copies a node's RLMM snapshot and its newest
  controller metadata checkpoint, and every consumer group's committed offsets,
  into the archive under `restore-inputs/<capture-id>/` with a manifest of
  sizes and SHA-256 digests. `verify` re-reads a capture and checks it against
  those digests, `list` names the captures, and `restore-offsets` commits a
  capture's offsets into a restored cluster so a group resumes where it stopped
  rather than at `auto.offset.reset`. The image has no shell, so `kubectl cp`
  cannot take those files off a broker; this binary ships beside the broker and
  runs from a `CronJob` with the volume mounted read-only.
  [Backup and restore](docs/operations/backup-restore.md) and the
  [restore-from-archive](docs/operations/runbooks/restore-from-archive.md)
  runbook say what to copy, how often, how to check a copy, and what to do on
  the day. `crates/restore/tests/dr_roundtrip.rs` runs the whole sequence.

### Changed

- The audit counters are exported as `krabka_broker_audit_events_total` and
  `krabka_broker_audit_write_failures_total`. The doubled `_total_total`
  suffix is gone.
- Per-partition and per-topic metric series are released when a reassignment
  drops this broker from a partition's replica set, or when a topic is deleted.
  `/metrics` previously grew for the life of the process on any cluster doing
  routine reassignment.
- Fetch-session allocation picks its victim in O(1) instead of scanning the
  whole cache under the global fetch mutex, which on a full cache blocked
  `classify` for every other in-flight fetch.
- A fetch hands one read to the thread pool without allocating a task per
  partition.
- Metric topic labels are held as `Arc<str>` shared with the partition
  registry, so a hot-path observation is a hash and a refcount bump rather than
  two `String` allocations. Those allocations cancelled out the registry's
  allocation-free design.
- Both documented PERF deferrals now carry a measured number and an explicit
  keep-or-fix decision, in the source, beside the code they describe.
- The broker's fourteen hand-rolled `MetadataSource` test doubles are one
  shared fake, so new metadata behaviour arrives testable rather than behind
  fourteen `unimplemented!()`s.

### Fixed

- A node redirected to a snapshot reports the quorum's committed offset rather
  than its own clamped watermark, so readiness cannot read a lag of zero while
  it is behind.
- Broker-only metadata observers now advance past every record offset in a
  multi-record metadata batch. Large reassignments previously applied the
  batch but left readiness permanently behind its high watermark.
- Topic and partition metric families now reconcile the labels their data
  paths actually created against the current metadata image. Invented topic
  names, invalid partition indexes, and writes racing a reassignment are
  collected without an ever-growing tombstone set.
- Controller bootstrap CLI and environment entries now accept unresolved DNS
  `host:port` names just like TOML, so a formatted joiner can discover a
  Kubernetes Service. Automatic `CreateTopics` and `CreatePartitions`
  placement excludes fenced and controller-dead brokers, and the reassignment
  acceptance case now moves real records onto its added replica before the
  source directory is pruned.
- The `meta.properties.json` on-disk format stamp is now version 2. This build
  refuses older or unknown stamps with instructions to run `krabka-format` on
  a fresh directory and restore topic data, and refuses a configured cluster
  id that disagrees with the formatted directory using
  `INCONSISTENT_CLUSTER_ID`. This is a declared on-disk format break, not a
  rolling-upgrade-compatible change.
- The diskless WAL index format is now version 2 for the
  `WalIndexEntry.max_timestamp_ms` layout. Replay refuses older, unknown or
  undecodable records, logs and counts the failure, clears the unsafe
  projection, and fails closed instead of serving an apparent data hole.
- `DeleteRecords` on a tiered topic (KIP-405) now takes the deleted prefix out
  of the remote tier as well as out of the local log. A partition keeps two
  floors the way Kafka does: `logStartOffset`, which `DeleteRecords`, retention
  and remote-segment deletion move, and `localLogStartOffset`, which follows
  the segments on disk. A fetch below the global floor answers
  `OFFSET_OUT_OF_RANGE` instead of being served from the archive, remote
  retention frees the segments that fell below it whatever `retention.ms` and
  `retention.bytes` say, and `ListOffsets(earliest)` follows the floor up after
  those deletes. Dropping a copied segment from local disk no longer moves the
  global floor, so the offsets the archive still holds stay readable, and the
  `log-start-offset-checkpoint` carries the global floor across a restart
  rather than the local one: a reopened tiered partition still refuses what a
  `DeleteRecords` deleted and still serves what only the archive holds. A log
  whose floor no checkpoint witnesses reports none at all, so neither the
  remote read nor the log-start breach acts on a floor that is only where the
  surviving segments happen to begin.
- A `DeleteRecords` trim now survives a broker restart even when it lands inside
  the active segment. Segment deletion records a trim that reaches a segment
  boundary, but the remainder used to live only in memory, so a restart served
  the deleted records again and `ListOffsets EARLIEST` moved back down. Every
  `krabka_log::Log` now checkpoints its log start to a
  `log-start-offset-checkpoint` file in the partition directory and reads it
  back on open, clamped to the offsets the log actually holds. Apache Kafka
  keeps the same value per log dir on a 60-second schedule; krabka writes it on
  the trim itself. The metadata log's private copy of this checkpoint is gone in
  favour of the shared one.
- Follower replicas of a tiered topic now enforce `local.retention.ms` /
  `local.retention.bytes` on their own disks, as KIP-405 has every replica do.
  The tiered-storage sweep used to skip a partition outright unless this broker
  led it, so a follower kept every segment it had ever fetched until it was
  elected: its disk grew to the topic's full `retention.*` footprint while the
  leader's held `local.retention.*` worth. The copy pass and remote retention
  stay leader-only, because one writer per partition owns the remote tier.
  Local retention now asks the remote-log metadata in offsets rather than in
  segment boundaries, so a replica whose segments do not line up with the
  leader's cannot drop a segment the tier holds only part of.
- A `krabka.diskless=true` topic now expires its object-store tier.
  `retention.ms`, `retention.bytes` and the `DeleteRecords` floor run against
  the committed WAL index on every flush tick, through the new proved
  `diskless_retention_prefix` kernel, and each expired range gets a keyed
  tombstone on `__diskless_wal_index`. The reclaimer then frees an object once
  no range in it is referenced. Before this the bucket and the index topic grew
  at the ingest rate for the life of the topic.
  `krabka_broker_diskless_wal_expired_ranges_total` counts the tombstones.
  `WalIndexEntry` gains the `max_timestamp_ms` field `retention.ms` reads, so
  the `__diskless_wal_index` record format changed: delete the topic and the
  local data directories rather than replaying an older one.
- `DeleteRecords` on a diskless partition now deletes. It measures against the
  offset the partition actually starts at rather than the flusher's local trim
  frontier, so a request below that frontier is no longer a silent no-op, and
  the object tier stops answering for the deleted offsets immediately: a fetch
  below the floor is `OFFSET_OUT_OF_RANGE` and `ListOffsets(EARLIEST)` reports
  the floor. The floor is a keyed record on `__diskless_wal_index`, published
  and projected before the trim is acknowledged, so it survives a restart and a
  leadership move on every broker. The range tombstones could not stand in for
  it: a range that straddles the floor still holds live records, and neither it
  nor the newest range may be expired.
- A KFC-9 write freeze now holds the diskless retention pass, as it already
  held the cleaner and the remote-log-manager's two retention passes. A frozen
  topic's prefix stays byte-identical in the object tier too.
- A broker-only node no longer stalls forever after a restart once the
  controller has snapshotted and pruned `__cluster_metadata` past offset 0. The
  observer metadata fetch now answers a pruned fetch offset with the KIP-630
  snapshot id that replaced those records, the observer installs that snapshot
  over `FetchSnapshot` before it resumes, and it keeps its own checkpoint in an
  `observer` directory under `__cluster_metadata` so a restart resumes there
  instead of at the log start. An observer that is answered but never applies
  anything now says so at warn level, with the log-start offset it was told.
- The KIP-590 row of the compatibility matrix said a Krabka broker-only node
  forwards admin writes through `Envelope`. It does not, and no such path
  exists: the controller listener serves `Envelope`, and a broker-only node
  reaches its controller over the krabka-private `SubmitChange` RPC. The row
  now claims the served half only, and says why the broker half is not needed.

## [0.5.3] - 2026-09-01

### Added

- KIP-966 eligible leader replicas. The controller maintains the ELR, elects
  from it on failover, and `DescribeTopicPartitions` reports its columns from
  the metadata image. A broker that cannot prove it shut down cleanly drops its
  membership rather than being re-derived into it from a stale ISR.
- The Kafka controller listener routes the whole Admin surface, and serves
  KIP-590 `Envelope`. The broker sends `BrokerHeartbeat` to it.
- `BROKER_LOGGER` as a config resource, so log levels change without a restart.
- Request latency split into its local, remote and throttle phases.

### Changed

- `ListOffsets` is fenced on the request leader epoch. `DescribeConfigs`
  returns typed config metadata, and reads an empty key list as a request for
  every config. Every broker-owned topic is marked internal.
- The KIP-219 throttle delay is applied after the response is sent, and the
  echoed delay is audited against every response schema.
- Cadence loops run on a `Timer` rather than an `AsyncSleeper`.
- The Kafka error-code table and the advertised `ApiVersions` table are derived
  from, and asserted against, the pinned Kafka image.

### Fixed

- `controller_id` advertises a reachable broker rather than the quorum leader.
- A refused produce partition answers with `base_offset = -1`, and an accepted
  one reports the partition's real log start offset.
- `max.message.bytes` is enforced, and an oversized batch refused.
- An open transaction's offsets answer `UNSTABLE_OFFSET_COMMIT`.
- Committed offsets expire for a group that went empty, idle and terminal
  transactional ids expire out of `__transaction_state`, and a connection idle
  past `connections.max.idle.ms` closes.
- A failed disk's partition reads as leaderless, the way `kafka-topics` reports
  it, and offline replicas appear in `Metadata` and `DescribeTopicPartitions`.
- The KIP-599 controller-mutation throttle is recorded as well as applied.

## [0.5.2] - 2026-08-31

### Added

- Diskless partitions. `krabka.diskless` is a create-only topic config that
  puts a partition's durability behind a quorum-replicated write-ahead log in
  front of object storage, with an index log that readers project. It is
  read-only in `DescribeConfigs`, pinned for the life of the topic, and refused
  alongside `remote.storage.enable=true` or `delivery.mode=scheduled`.
- A WAL fetch is authenticated as the broker node that sent it, through a
  configured principal-to-node-ID mapping (KIP-595).
- Compaction of the diskless WAL index, and reclamation of the objects it
  leaves stale.

### Fixed

- A diskless partition needs a distributed identity and a registry before it
  spawns.
- A diskless fetch honours the byte limits the request asks for.
- A stalled index replay recovers, the first flush waits for the index
  projection to catch up, and a divergent follower offset stays out of quorum
  accounting.

## [0.5.1] - 2026-08-30

### Added

- Broker audit logging, with a crash-safe spool replay.
- WORM verification and reporting for the S3 and GCS archive backends.
- Cluster metadata restore from a controller snapshot, with a topic-ID check.
- A Markdown link check in CI, and the verification ledger it reads.

### Changed

- The throttle kernel, the producer sequencing decisions, the sparse log index
  lookup and the leader epoch lookup moved into `krabka-verified`, behind
  Creusot contracts. Callers validate their inputs before they enter a kernel.

### Fixed

- The broker rejects an unsupported request version before dispatch and answers
  with `UNSUPPORTED_VERSION`, except for the `ApiVersions` fallback.
- A `read_committed` fetch keeps aborted-transaction metadata when replication
  trails the abort marker.
- A log append rolls back, and the rollback is durable, after a write failure or
  a sync failure.
- Archive verification aborts a failed multipart upload instead of leaving it
  orphaned.

## [0.5.0] - 2026-08-30

The first release of krabka-broker as its own repository. The broker, the log,
the KRaft layer and the crates that support them moved out of
robot-head/crabka.

### Added

- Cross-topic snapshots through log-embedded barrier markers (KFC-4).
- WORM archive mode with signed integrity manifests (KFC-5).
- Offline point-in-time restore from a KIP-405 archive (KFC-3).
- Deliver-at-time visibility, for records that become readable at their time
  (KFC-1).
- A data-bearing witness broker role, and a three-site stretch profile.
- Broker-side schema validation (KFC-7).
- A freeze registry and a break-glass state machine (KFC-9).
- `krabka format`, as a library as well as a binary, so a node can be formatted
  from this repository.
- A signed image, an SBOM attestation and a provenance attestation on every
  release tag.

### Changed

- Bazel is the build and test path. It runs the format check, the Clippy lint
  aspect, coverage, the rustdoc build, the Creusot proofs, the container suites
  and the image push.
- The `crabka` namespace became `krabka`, in crate names and in identifiers.
- Files over 500 lines in the broker became cohesive modules.

### Fixed

- `ListOffsets` honours `isolation_level`, and bounds every answer rather than
  only the `LATEST` one.
- An audit stamp carries the value that its freeze signature covers.
- The release publishes the image digest that cosign signed.

[Unreleased]: https://github.com/krabka-io/krabka-broker/compare/v1.0.4...HEAD
[1.0.4]: https://github.com/krabka-io/krabka-broker/releases/tag/v1.0.4
[1.0.3]: https://github.com/krabka-io/krabka-broker/releases/tag/v1.0.3
[1.0.2]: https://github.com/krabka-io/krabka-broker/releases/tag/v1.0.2
[1.0.1]: https://github.com/krabka-io/krabka-broker/releases/tag/v1.0.1
[1.0.0]: https://github.com/krabka-io/krabka-broker/releases/tag/v1.0.0
[0.7.0]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.7.0
[0.6.1]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.6.1
[0.6.0]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.6.0
[0.5.4]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.5.4
[0.5.3]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.5.3
[0.5.2]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.5.2
[0.5.1]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.5.1
[0.5.0]: https://github.com/krabka-io/krabka-broker/releases/tag/v0.5.0
