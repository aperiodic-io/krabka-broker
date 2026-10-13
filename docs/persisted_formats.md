# Persisted formats

This document is the authoritative list of every format that a krabka broker writes to disk or to an object store, and the compatibility contract that covers them from 1.0.0 on.

The contract covers krabka-broker and the persisted crates of [krabka-protocol](https://github.com/krabka-io/krabka-protocol): `krabka-metadata`, the record, metadata-envelope and checkpoint codecs of `krabka-protocol`, and every type in `krabka-ids`, `krabka-voters` and `krabka-security` that a persisted `MetadataRecord` contains. The other krabka repositories keep their greenfield rule.

## Design Goals

- **A 1.x broker reads all 1.x data.** Any 1.x broker reads every on-disk artifact that any earlier 1.x broker wrote. An operator never reformats a node or restores from a backup to move from 1.x to 1.y.
- **A rolling upgrade works.** A cluster moves from 1.x to 1.y one node at a time. Mixed-version nodes exchange the krabka-private records of the controller-forwarding RPCs during the roll, so those records are under the same contract as the disk.
- **A downgrade works until the operator finalizes.** A new broker writes the old format until the operator finalizes the feature level that introduces the new format. Until then, the operator can roll the cluster back to the earlier build.
- **The Kafka semantics are the model.** The gating follows Kafka's `metadata.version` model from [KIP-584](https://cwiki.apache.org/confluence/display/KAFKA/KIP-584%3A+Versioning+scheme+for+features) and [KIP-778](https://cwiki.apache.org/confluence/display/KAFKA/KIP-778%3A+KRaft+to+KRaft+Upgrades), so an operator who knows `kafka-features` knows the procedure.

## Architecture Overview

### The contract

1. **Read compatibility.** A 1.y broker reads every format in the tables below as any 1.x broker with x less than or equal to y wrote it. This covers partition logs and their sidecars, metadata log segments and snapshots, `quorum-state`, `meta.properties`, the bootstrap files, the internal-topic record formats, the krabka-private wincode records inside `NoOpRecord` tags, and the object-store formats: tiered segments, WORM manifests, diskless WAL objects, and backup captures.
2. **Gated writes.** A change to a persisted format ships behind a `metadata.version` level or another feature level. A broker that supports the new format keeps writing the old format until the operator finalizes that level, as Kafka's KIP-584 and KIP-778 semantics require.
3. **Downgrade.** Before finalization, the operator can downgrade to any earlier 1.x build, because nothing on disk uses the new format yet. After finalization, a downgrade follows Kafka's rules for the feature in question. For `metadata.version`, krabka implements the [KIP-1155](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1155%3A+Metadata+Version+Downgrades) snapshot path.
4. **No promise for 0.x data.** Data that a broker before 1.0.0 wrote gets no promise. An operator reformats a 0.x data directory with `krabka-format` and restores the topic data.

### What the contract does not cover

- **The Kafka wire protocol.** The Kafka compatibility rules in [`CLAUDE.md`](../CLAUDE.md) are unchanged. A request or a response follows Kafka's version negotiation, not this document.
- **The Rust API.** The contract is about bytes. It does not promise semver stability for a Rust type, a function or a crate interface.
- **Caches.** `remote-log-index-cache/` is emptied at startup. It is not durable state.
- **Configuration and command-line flags.** They follow the normal changelog process.

## Key Design Decisions

### Rules for a format change

These rules apply to every format in the tables below. [`CLAUDE.md`](../CLAUDE.md) gives the same rules as instructions to a contributor.

- **Wincode values are positional.** `wincode` writes an enum variant as its index and a struct as its fields in order, with no field names. So the order of the `MetadataRecord` variants, and the field layout of every type a variant contains, is part of the on-disk format. Add a new variant at the end only. Do not reorder, remove or insert a variant, and do not change the fields of a persisted wincode type. To change the shape of a record, add a new variant at the end and keep the old variant readable.
- **`NoOpRecord` private tags are permanent.** The tags 1001, 1003, 1004, 1005 and 1006 each carry one krabka-private record. Tag 1002 is a gap in the numbering. Never reuse it. A new private record takes a new tag above 1006.
- **A new or changed format carries a version marker.** Its reader decodes every earlier 1.x version of that format.
- **A new writer behavior is gated.** The broker writes the new format only after the operator finalizes the feature level that introduces it.
- **A golden-bytes fixture pins each format.** A test decodes bytes that an earlier release wrote and compares the result with the expected value.

### Gating

A `metadata.version` level is the gate when a krabka format change happens at the same level as a Kafka change. The `metadata.version` table mirrors Kafka's `MetadataVersion` enum exactly, because a JVM client throws on a level that its enum does not know. So krabka cannot add a `metadata.version` level of its own.

A krabka-only format change is gated on `krabka.version`, the feature that krabka owns (`krabka_metadata::krabka_version` in krabka-protocol). Levels 0 and 1 both mean the 1.0.0 formats: Kafka advertises a feature only when its highest supported level is above 0, so a 1.0.0 node advertises `krabka.version` at `[0, 1]`, and level 1 can never carry a new format. The first krabka-only format change takes level 2. A fresh cluster carries no `krabka.version` record: `krabka-format` bootstraps it at level 0, which means the same formats as level 1, unless `--feature krabka.version=N` overrides it. Once every node is krabka, an operator finalizes level 1 with `kafka-features upgrade --feature krabka.version=1`. The level stays at 0 until then because a cluster can mix in Kafka nodes, which support only level 0: a bootstrapped level 1 crashes a JVM standby controller and keeps a JVM broker from joining. The controller refuses a level that a registered node does not support and accepts a safe or unsafe downgrade, as Kafka's `FeatureControlManager` does for a feature other than `metadata.version` and `kraft.version`. `kafka-features upgrade --release-version` does not move it, because that walks only Kafka's production features.

### Partition log directory

Each partition directory is `<log_dir>/<topic>-<partition>/`.

| Artifact | Location | Encoding | Version marker | Unknown-version behavior | Gating |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Log segment | `<base>.log` | Kafka `RecordBatch` v2 | `magic` byte | A `magic` other than 2 is refused with `UnsupportedMagic`. | Kafka's rules |
| Transaction marker | Control batch in `.log` | Kafka `EndTransactionMarker` | Key version and value version, both `i16` 0 | As Kafka's `ControlRecordType.parseTypeId` and `EndTransactionMarker.deserializeValue`: a negative version, a key shorter than 4 bytes or a marker value shorter than 6 bytes is refused, and a higher version is read as version 0. Append, recovery and compaction all check. | Kafka's rules |
| Barrier marker | Control batch in `.log`, control type 1000 | krabka, big-endian | Key version and value version, both `i16` 0 | `parse_barrier_marker` refuses any other key or value version. | None |
| Offset index | `<base>.index` | Kafka, fixed width | None, as in Kafka | Not applicable | Kafka's rules |
| Time index | `<base>.timeindex` | Kafka, fixed width | None, as in Kafka | Not applicable | Kafka's rules |
| Transaction index | `<base>.txnindex` | Kafka | `i16` 0 per entry | A version other than 0 is refused. | Kafka's rules |
| Producer snapshot | `<offset>.snapshot` | Kafka, with CRC | `i16` 1 | A version other than 1 is refused. | Kafka's rules |
| Stamp index | `<base>.stampindex` | krabka, a header, then 24-byte big-endian entries `{base_offset, last_offset, stamp}` | `STAMP_INDEX_VERSION`, `i16` 0, at the front of the file | A file without the header, which a 0.x broker wrote, or with another version is refused. | None |
| Leader epoch checkpoint | `leader-epoch-checkpoint` | Kafka text | Header line `0` | A header other than `0` is refused, as Kafka's `CheckpointFile` refuses it. | Kafka's rules |
| Log start offset checkpoint | `log-start-offset-checkpoint` | krabka text, `0` then the offset, one file per partition | Header line `0` | A header other than `0` is refused as corrupt. | None |
| Replica move directory | `<topic>-<partition>-future` | krabka naming | None | Not applicable | None |

Kafka names a replica move directory `<topic>-<partition>.<uuid>-future`. [`format-divergences.md`](format-divergences.md) records the difference. The broker writes no recovery-point, replication-offset or cleaner-offset checkpoint, and no `partition.metadata`.

A sealed `.log` file is always exactly as long as the batches in it, and so is an active one unless `preallocate=true`. With `preallocate=true`, Kafka sets a new segment's length to `segment.bytes` and trims it when the segment closes, so after a crash its file can end in zeros. krabka reserves the blocks with `fallocate(FALLOC_FL_KEEP_SIZE)` without changing the length, and writes the active segment through `O_DIRECT`, which writes whole blocks: the active segment's file can run up to one block (at most 4 KiB on the filesystems the broker runs on) of zeros past its last batch. Sealing the segment, turning `preallocate` off, and a clean shutdown cut the zeros. From 1.0.4, several of a segment's direct writes can run at the same time and finish in any order, so a crash can also leave zeros between batches that were not yet synced, as page-cache writeback can. In both cases the tail recovery of every 1.x release cuts the file at the first bytes that do not decode as a batch, as Kafka cuts its own preallocated tail. `recovery_cuts_the_padding_a_crash_leaves` in `crates/log/src/log/direct_writes.rs` holds that reader to it.

### Log directory root

| Artifact | Location | Encoding | Version marker | Unknown-version behavior | Gating |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `meta.properties` | `<log_dir>/meta.properties` | Kafka V1 properties | `version=1` | A version other than 1 is refused. | None |
| Bootstrap records | `<metadata_log_dir>/bootstrap.records.bin` | krabka: a header, then for each record a `u32` little-endian length and a wincode `MetadataRecord` | `BOOTSTRAP_RECORDS_VERSION`, `i16` 0, at the front of the file | A file without the header, which a 0.x `krabka-format` wrote, or with another version is refused. The node must be formatted again. | None |
| Bootstrap manifest | `<metadata_log_dir>/bootstrap.json` | krabka JSON | `version: 1` | No reader exists. The broker never reads the file. | None |
| Incarnation id | `<log_dir>/incarnation_id` | krabka UUID text | None, on purpose | An unreadable value makes the broker generate and write a new id. The id is outside the strict contract: Kafka never persists it, and a new id only delays registration until the old heartbeat session expires. | None |
| Clean-shutdown proof | `<log_dir>/.kafka_cleanshutdown` | Kafka JSON `{"version":0,"brokerEpoch":N}` | `version` 0 | A missing, unreadable or other-version file makes the restart unclean, as Kafka's `CleanShutdownFileHandler.read` does. | None |

Kafka writes `bootstrap.checkpoint` instead. The bootstrap checkpoint at offset zero of the metadata log, described below, replaces `bootstrap.records.bin` for a controller of a dynamic quorum.

### Metadata log directory

The metadata partition directory is `<metadata_log_dir>/__cluster_metadata-0/`.

| Artifact | Location | Encoding | Version marker | Unknown-version behavior | Gating |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Metadata log segment | `<base>.log` | Kafka `RecordBatch` v2. Each value is the KIP-631 `ApiMessageAndVersion` frame: `frameVersion`, `apiKey`, `apiVersion`, body. | `frameVersion` 1 and the per-record `apiVersion` | A `frameVersion` other than 1 is refused, as Kafka's `MetadataRecordSerde` refuses it. On a controller, a committed record that does not decode (an unknown `apiKey`, an `apiVersion` above the supported one, a bad frame or private record, or a field value that no build accepts) stops the controller, in live replay, startup recovery and the image walk alike, as Kafka's fatal fault handler does. On a broker-only node the observer logs it at error, counts it in `metadata-load-error-count` and stops reading that batch, as Kafka's `MetadataLoader` does. A record that decodes but names a topic or ACL that is gone, or fails validation, is skipped on every replica, because a krabka leader validates against committed state and two racing writes can both commit. | `metadata.version` selects the `apiVersion` of each record, for example `DIRECTORY_ASSIGNMENT_MIN_LEVEL` 17, `ELR_MIN_LEVEL` 23 and `CORDONED_LOG_DIRS_MIN_LEVEL` 30. |
| Metadata snapshot | `<end>-<epoch>.checkpoint` | Kafka KIP-630 snapshot with the same record frame | As the log segment | Any record that does not decode or translate, an unknown `apiKey` included, stops the load with an error. | As the log segment |
| Bootstrap checkpoint | `00000000000000000000-0000000000.checkpoint` | Kafka KIP-630, with `KRaftVersionRecord` and `VotersRecord` | As the snapshot | As the snapshot | As the snapshot |
| Observer snapshot | `observer/<end>-<epoch>.checkpoint`, on a broker-only node | Kafka KIP-630 | As the snapshot | The observer discards a checkpoint it cannot read and fetches the metadata again. | As the snapshot |
| Quorum state | `quorum-state` | Kafka `QuorumStateData` JSON | `data_version` 0 or 1 | A file that does not parse, a missing `data_version` or one other than 0 or 1 stops the node, as Kafka's `FileQuorumStateStore` throws. A missing file means the node has not voted. | `kraft.version` selects `data_version` |
| High watermark | `high-watermark` | krabka text, `0` then the offset | Header line `0` | A file without the header (0.x) or with another version stops the node. A damaged version-0 file makes the broker use the log start offset: the file is only a restart shortcut, and Kafka keeps none. | None |

`FeatureLevelRecord` persists the finalized `metadata.version` and the other feature levels. A `metadata.version` downgrade writes a snapshot at the lower level and discards the incompatible log prefix, as KIP-1155 describes.

### Krabka-private metadata records

KIP-631 has no schema for some krabka records. Each one rides as the only tagged field of a Kafka `NoOpRecord` (apiKey 20), and the field body is a wincode `MetadataRecord`. Kafka's tools read the record as a `NoOpRecord` and keep the tag as an unknown tagged field. `crates/metadata/src/kraft_translate.rs` in krabka-protocol defines the tags.

| Tag | Variant | Version marker | Unknown-version behavior | Gating |
| :--- | :--- | :--- | :--- | :--- |
| 1001 | `V1FeaturesEpoch` | `PRIVATE_RECORD_VERSION`, `i16` 0, before the wincode body | A version other than 0, a variant that belongs to another tag, or bytes after the record is a decode error. See the metadata log segment row. | None |
| 1002 | None. The number is a gap. Do not reuse it. | Not applicable | Not applicable | Not applicable |
| 1003 | `V1PartitionOffsetAdvance` | As tag 1001 | As tag 1001 | None |
| 1004 | `V1TopicFreeze` | As tag 1001 | As tag 1001 | None |
| 1005 | `V1BreakGlassProposal` | As tag 1001 | As tag 1001 | None |
| 1006 | `V1DeleteBreakGlassProposal` | As tag 1001 | As tag 1001 | None |

The reader checks that each tag carries the variant assigned to it, and refuses a tag of 1001 or above that the table does not assign. A golden-bytes test in krabka-protocol pins the wincode encoding of every `MetadataRecord` variant and of every enum its fields reach.

`bootstrap.records.bin` holds wincode `MetadataRecord` values too, so every variant is on disk, not only the five above.

The broker also reads the topic config key `krabka.elr`, which a 0.x broker wrote, and removes it from the topic config. No 1.x broker writes it.

### Internal topics

| Topic | Encoding | Version marker | Unknown-version behavior | Gating |
| :--- | :--- | :--- | :--- | :--- |
| `__consumer_offsets` | Kafka 4.3.1 coordinator record schemas, field for field: `MetadataHash` is Kafka's `computeTopicHash`/`computeGroupHash`, and KIP-1331's trunk-only streams tags 2 and 3 are written only while `group.streams.topology.description.plugin.class` is set. Replay follows Kafka's `GroupMetadataManager.replay`. | Key version (the record type) and value version | As Kafka's `CoordinatorLoaderImpl`: a record type the broker does not know is logged at warn and skipped, value or tombstone, since it can be left over from an aborted upgrade. A known type with an unsupported value version, a key that does not decode, a missing key or a corrupt value fails the load. | Kafka's rules |
| `__transaction_state` | Kafka | Key version and value version | As Kafka's `TransactionStateManager`: an unknown key version or value version is logged at warn and skipped. Any other error is logged at error, and the transactions loaded before it are installed. | Kafka's rules |
| `__share_group_state` | Kafka `ShareSnapshot` and `ShareUpdate` | Key type and value version | As `__consumer_offsets`: an unknown record type is skipped, and any other bad record fails the load. | Kafka's rules |
| `__remote_log_metadata` | Kafka `RemoteLogMetadataSerde`. `CustomMetadata` holds a krabka JSON `WormChainRecord`. | Kafka's per-record version. `WormChainRecord` has a required `version`, 0. | A `WormChainRecord` of another version is refused. It uses `deny_unknown_fields`, so a new field needs a new version, gated on a feature level. JSON without `version` is not a chain record: another backend wrote it. | None for `WormChainRecord` |
| `__barrier_state` | krabka, big-endian | `i16` 0 | A version other than 0 is refused. | None |
| `__diskless_wal_index` | krabka. Keys are fixed-width binary; values are wincode. | Each key starts with an `i16` key version that names its type: 0 for a range key, 1 for a delete-floor key. Each value starts with an `i16` version: 2 for `WalFlushRecord`, 0 for `WalDeleteFloorRecord`. | An unknown key version or value version is refused, and the live index marks its projection invalid. | None |
| `__krabka_audit` | OCSF 1.3.0 JSON, with hash-chain headers and signed checkpoints in the signing domain `krabka-audit-ckpt-v1` | The OCSF schema version and the signing domain | Defined by `krabka-audit verify` | None |

The group coordinator writes a `__consumer_offsets` group record only where Kafka 4.3.1's `GroupMetadataManager` writes it. So a reader of the topic must accept two shapes that a write on every transition did not make:

- **A group can exist without a group metadata record.** A new consumer or share group starts at Kafka's group epoch 1, and the first `ConsumerGroupMetadataValue` (type 3) or `ShareGroupMetadataValue` (type 11) comes only with the first epoch bump. A member that joins without a change to the subscribed topic names writes its member record and its current assignment (types 5 and 8 for a consumer group) and no type 3. Replay creates the group from the first child record, as Kafka's `getOrMaybeCreatePersistedConsumerGroup` does, at epoch 1.
- **A target assignment can be empty.** `ConsumerGroupTargetAssignmentMemberValue` (type 7), `ShareGroupTargetAssignmentMemberValue` (type 13) and `StreamsGroupTargetAssignmentMemberValue` (type 21) can hold an empty assignment. That is a member with no target, which is not the same as a tombstone: the tombstone removes the member's target, and the empty value keeps it.

### Other local state

| Artifact | Location | Encoding | Version marker | Unknown-version behavior | Gating |
| :--- | :--- | :--- | :--- | :--- | :--- |
| WAL quorum membership | `<log_dir>/__diskless_wal_quorum/.../quorum-state.json` and its `.bak` | krabka JSON `{version, voters}` | `version` 0 | A missing or other version, in the file or its `.bak`, stops the shard from opening. | None |
| WAL durable offset | `.../wal-durable-offset.checkpoint` and its `.bak` | krabka text, `0` then the two offsets | Header line `0` | A missing or other version, in the file or its `.bak`, stops recovery with an error. | None |
| Audit spool | `<log_dir>/audit-spool/audit.spool` by default | krabka: magic `KAUD`, a version, then big-endian length-prefixed frames | `FORMAT_VERSION`, `i16` 0, after the magic | A file without the header (0.x) or with another version is refused. A torn last frame is still the end of the data. | None |
| Audit spool state | `audit.losses`, `audit.replay-offset`, `audit.replay-poison` | krabka, with the same header | As the audit spool | As the audit spool | None |
| RLMM snapshot | `<log_dir>/remote-log-metadata/snapshot` by default | krabka envelope over the `MetadataEvent` codec | `SNAPSHOT_FORMAT_VERSION` 0, `u16` | A version other than 0 is refused, and the broker replays `__remote_log_metadata` from the start. | None |

### Object store

| Artifact | Location | Encoding | Version marker | Unknown-version behavior | Gating |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Tiered segment | KIP-405 layout | Kafka segment, index and checkpoint files | Kafka's | Kafka's rules | Kafka's rules |
| WORM manifest | `<segment>.manifest` | krabka JSON, and a canonical big-endian layout that the chain hashes | `format_version` 2 | A version other than 2 is refused. | None. The writer always writes 2. |
| Diskless WAL object | WAL object key | krabka binary, magic `CKWL` | `OBJECT_VERSION` 1, `u16` little-endian | A version other than 1 is refused. | None |
| Backup capture manifest | `restore-inputs/<capture-id>/manifest.json` | krabka JSON | `version` 0 | A missing or other version is refused. | None |
| Captured metadata checkpoint | `restore-inputs/<capture-id>/cluster-metadata.checkpoint` | Kafka KIP-630 | As the metadata snapshot | As the metadata snapshot | As the metadata snapshot |
| Captured RLMM snapshot | `restore-inputs/<capture-id>/rlmm-snapshot` | As the RLMM snapshot | As the RLMM snapshot | As the RLMM snapshot | None |
| Captured group offsets | `restore-inputs/<capture-id>/group-offsets.json` | krabka JSON | `version` 0 | A missing or other version is refused. | None |
| Captured diskless WAL index | `restore-inputs/<capture-id>/diskless-wal-index.json` | krabka JSON `DisklessWalCapture` | `format_version` 1 | A version other than 1 is refused. | None |

### Rolling-upgrade RPCs

These records are not on disk, but nodes of two 1.x versions exchange them during a rolling upgrade. The contract covers them.

| RPC | API key | Body | Version marker | Unknown-version behavior |
| :--- | :--- | :--- | :--- | :--- |
| `SubmitChange` | 1003 | wincode `Vec<MetadataRecord>`. The response carries a wincode `SubmitChangeResult`. | The request header `api_version`, 0 | A request of another version is answered with `UNSUPPORTED_VERSION` (35) and not applied. |
| `MetadataFetch` | 1004 | Kafka record batches of `__cluster_metadata`, with the KIP-631 record frame | The request header `api_version`, 0 | As `SubmitChange`. The records are then read as the metadata log segment. |
| `DelegationTokenMutation` | 1005 | wincode `Vec<DelegationTokenMutation>` | The request header `api_version`, 0 | As `SubmitChange`. |

`crates/raft/src/wire.rs` defines the three codecs. Their versions are negotiated through `krabka.version`, as Kafka's inter-broker protocol versions follow `metadata.version`: the controller's `ApiVersions` answer stays Kafka's table, byte for byte. A sender sends the version that `krabka_metadata::private_rpc_version` gives for the level finalized in its metadata image, and v0 with no image or no finalized level. Because the controller finalizes only a level that every registered node supports, every peer serves that version. A receiver serves 0 up to the version at its own highest supported level and answers anything else with `UNSUPPORTED_VERSION`. Levels 0 and 1 give v0 for all three, so a new RPC version arrives with a new level.

## Known gaps

The items below are true of the code at 1.0.0.

### Fixture coverage

Every krabka-owned format in the tables above has a golden-bytes test that encodes a value, compares it with fixed bytes and decodes the bytes back. The Kafka-owned formats rely on the Kafka differential suites instead. The replay-fence key `__krabka_diskless_replay_fence` in `__diskless_wal_index` is a fixed literal with an empty value and has no version.

### Unknown tagged fields in group records

Kafka 4.3.1 builds each group record that it writes from its in-memory group, not from the record it replayed. So it drops the unknown tagged fields at the top level of a replayed record when it writes that record again, and krabka drops them too. KIP-1331's streams tags 2 and 3, which Kafka 4.3.1 does not know, follow the same rule: krabka writes them only while `group.streams.topology.description.plugin.class` is set.

Kafka keeps two nested structs of a replayed record as the generated objects, with their unknown tagged fields: `ConsumerGroupMemberMetadataValue.ClassicMemberMetadata`, with each of its `ClassicProtocol` entries, and `StreamsGroupMemberMetadataValue.Endpoint`. A member record that Kafka writes from a copy of the member, such as the record of a static member's replacement, carries those fields again. And because the generated `equals` compares them, a classic rejoin or a streams heartbeat that sends the same values as the replayed record still writes the member record again, and for the streams heartbeat also bumps the group epoch. krabka decodes these structs into its own types and drops their unknown tagged fields, so it writes the member record without them, and it writes no record and bumps no epoch for such a heartbeat. Kafka 4.3.1 defines no tagged field in these structs, so only a record from a later Kafka version can carry one.

## Integration

- [`CLAUDE.md`](../CLAUDE.md) gives the rules above as instructions to a contributor.
- [Deploy](operations/deploy.md#rolling-upgrade) gives the operator procedure for a rolling upgrade and for the finalization of a feature level.
- [Releasing](releasing.md) says how a release that changes a persisted format records the change.
- [`format-divergences.md`](format-divergences.md) lists where the files that `krabka-format` writes differ from the files that `kafka-storage format` writes.

## Kafka / KIP Compliance

- KIP-584 and KIP-778 define finalized feature levels and the `metadata.version` upgrade. The gating rule follows them.
- KIP-631 defines the metadata record frame, and KIP-630 defines the snapshot format.
- KIP-1155 defines the `metadata.version` downgrade that the controller implements.
- KIP-482 tagged fields let a Kafka tool read a krabka `NoOpRecord` and keep the private tag as an unknown tagged field.
- KIP-405 defines the tiered-segment layout in the object store.

## Testing

- `crates/metadata/src/kraft_translate.rs` in krabka-protocol pins the private tag list and the bytes of each private record, and `crates/metadata/src/wincode_contract.rs` pins the wincode encoding of every `MetadataRecord` variant and nested enum, so a test fails when a tag or a layout changes.
- Each krabka-owned format has a golden-bytes test next to its codec, and a table of refused versions, including the layout a 0.x broker wrote.
- [Verification](verification.md) lists the model checks and proofs, including the `quorum-state` load decision.
