# krabka-remote-storage

[API documentation](https://krabka-io.github.io/krabka-broker/krabka-remote-storage/krabka_remote_storage/index.html)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

KIP-405 tiered-storage SPI (RemoteStorageManager / RemoteLogMetadataManager) and reference implementations for Krabka.

This crate is part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add krabka-remote-storage --path ../krabka-broker/crates/remote-storage
```

This crate is not published by this repository. Adjust the path to your local checkout. Workspace development uses the path dependencies declared in this repository.

## Usage example

Copy a closed log segment into the filesystem-backed remote tier and fetch its offset index:

```rust,no_run
use std::{collections::BTreeMap, path::PathBuf};

use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    IndexType, LocalTieredStorage, LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId,
    RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageManager, TopicIdPartition,
};
use uuid::Uuid;

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let storage = LocalTieredStorage::new(PathBuf::from("/var/lib/krabka-remote"));
let topic_partition = TopicIdPartition::new(Uuid::new_v4(), "orders", 0);
let segment_id = RemoteLogSegmentId::new(topic_partition, Uuid::new_v4());
let mut leader_epochs = BTreeMap::new();
leader_epochs.insert(LeaderEpoch(0), 0);
let metadata = RemoteLogSegmentMetadata::new(
    segment_id,
    0,
    999,
    1_713_000_000_000,
    1,
    1_713_000_000_000,
    RemoteLogSegmentDetails::new(
        1_048_576,
        RemoteLogSegmentState::CopySegmentStarted,
        leader_epochs,
    ),
)?;
let segment = LogSegmentData {
    log_segment: PathBuf::from("/var/lib/krabka/orders-0/00000000000000000000.log"),
    offset_index: PathBuf::from("/var/lib/krabka/orders-0/00000000000000000000.index"),
    time_index: PathBuf::from("/var/lib/krabka/orders-0/00000000000000000000.timeindex"),
    transaction_index: None,
    producer_snapshot_index: None,
    leader_epoch_index: Bytes::new(),
};
let _custom_metadata = storage.copy_log_segment_data(&metadata, &segment)?;
let _offset_index = storage.fetch_index(&metadata, IndexType::Offset)?;
# Ok(())
# }
```

## WORM archive mode

WORM archive mode turns an object-store tier into a write-once compliance archive. The broker still tiers segments the way KIP-405 describes, but it never rewrites and never deletes what it wrote, and it leaves proof that an auditor can check years later without the cluster.

The mode changes four things:

- **Every object is a conditional create.** The backend writes with `PutMode::Create`, so a second write to the same key fails instead of replacing the first.
- **Every segment copy seals a manifest.** The `.manifest` object beside the segment names each object the copy wrote, records a SHA-256 digest for each one, and carries the partition's hash-chain head. An Ed25519 signature over that head binds the chain to a named key.
- **Every delete is refused.** The backend returns an error rather than remove an object, whatever asks it to.
- **Remote retention is disabled.** The `RemoteLogManager` does not run its remote-retention pass for a WORM partition, so `retention.bytes` and `retention.ms` never reach the archive.

Local retention is unaffected. The broker still evicts a local segment once the archive holds it, so a WORM deployment does not need disk for the whole history.

### Configuration

The `[remote_storage.worm]` table turns the mode on. Its presence is the switch, and the table applies to whichever object store `[remote_storage.s3]` or `[remote_storage.gcs]` selects. A `storage_dir` backend is rejected at load time, because a local directory cannot enforce write-once.

```toml
[remote_storage.s3]
bucket = "krabka-archive"
region = "us-east-1"

[remote_storage.worm]
signing_key_path = "/etc/krabka/worm-signing.pk8"
signing_key_id = "worm-2026-q3"
write_only = false
```

| Key | Default | Description |
|-----|---------|-------------|
| `signing_key_path` | unset | Path to the PKCS#8 Ed25519 key that signs each manifest. Set it together with `signing_key_id`. |
| `signing_key_id` | unset | Stable id recorded in every signature, so a chain stays verifiable across a key rotation. Set it together with `signing_key_path`. |
| `write_only` | `false` | Refuse every remote fetch from this archive. |

An unsigned archive is legal: leave both key fields unset and the archive keeps the per-object digests and the hash chain. It then proves continuity but not authorship, and `krabka-worm-verify` grades it as an incomplete attestation.

### The bucket enforces WORM, not the broker

The retention that stops an administrator with delete rights lives on the bucket. Configure S3 Object Lock in compliance mode with a default retention period, or GCS versioning with a locked retention policy. Krabka verifies that policy before enabling WORM mode; it does not set the lock.

There is an untyped way through — `ClientOptions::with_default_headers` — and it was considered and rejected rather than missed. `x-amz-object-lock-retain-until-date` is an absolute timestamp, so a default header pins one date for the lifetime of the process instead of holding each object for a period measured from its own write, and the header would ride every request the client makes, reads included. A bucket default expresses the policy correctly and outlives any broker that writes to it.

The broker's contribution is narrower and still worth having. It never issues a delete, it never overwrites, and it leaves a signed chain that shows what the archive held. A bucket with Object Lock and a broker that writes once are the two halves of the guarantee. Neither half is enough alone.

### Multipart writes reserve their key first

`PutMode::Create` applies directly to a single-PUT write. `object_store` 0.13's multipart completion has no precondition, so a large body first claims its key with an atomic empty-object create. Only that winner starts multipart upload; the completed version is then read back by version id and checked against the uploaded digest. A failed upload leaves its reservation behind rather than making an ambiguously owned key retryable.

Manifests are small and always take the single-PUT path, so every manifest is a conditional create. "One manifest per segment, and never rewritten" holds whatever the segment size is.

### `write_only = true` disables remote fetch

With `write_only = true` the archive is a compliance sink and not a storage tier. A consumer that asks for an offset whose local segment was already evicted gets an error, not a slow read. This is a cliff and not a slowdown: the data is in the bucket, and the broker refuses to serve it.

Set `write_only = true` only when the archive exists for the auditor and the read path never needs it. Leave it `false` when consumers still read history through the tier.

### Topic deletion leaves the archive intact

A `DeleteTopics` call removes the partition from the cluster. The objects and the manifests stay in the bucket, and the broker deletes nothing.

That is intended. Erasing a compliance archive must not be a side effect of a topic delete, and a retention rule the archive never agreed to must not arrive through the admin API. Removal of archived data is a bucket-side action, taken deliberately after the Object Lock retention period ends.

### Chain continuity across restarts needs a topic-backed RLMM

The broker reads its chain tip back from the remote-log metadata manager. With `RlmmKind::InMemory` that metadata does not survive a restart, so the broker starts a new epoch at genesis rather than continue a chain it cannot read. Nothing binds the manifests before the restart to the manifests after it.

`krabka-worm-verify` reports that as an attestation hole, and it is one. A WORM deployment should use the topic-backed `[remote_storage.kafka_metadata]` manager, which recovers its state from `__remote_log_metadata` after a restart.

### Tail truncation is not detectable from the archive alone

An attacker who deletes the newest manifests of a partition, and the objects they name, leaves a shorter chain that verifies perfectly. Every remaining head chains correctly and every remaining signature is valid. Nothing inside the archive says how long the chain should be, so no amount of reading the archive reveals what is gone.

Close that gap outside the archive. A successful verify prints each partition's tip head; record it somewhere the bucket's writers cannot reach, and pass it to the next run as `--expect-head`. A run that holds no expected head proves internal consistency and says nothing about completeness.

### Verify an archive with `krabka-worm-verify`

`krabka-worm-verify` audits an archive with read-only credentials, with no broker and no cluster. It takes its credentials from the ambient AWS chain, and it has no `--access-key` flag on purpose: an auditor should hold a read-only role and not a copy of the writer's keys.

```sh
krabka-worm-verify verify \
  --bucket krabka-archive \
  --region us-east-1 \
  --key-id worm-2026-q3 \
  --public-key /etc/krabka/worm-2026-q3.pub \
  --expect-head 9f2c1b0a... \
  --deep
```

Use `--local-dir PATH` in place of `--bucket` for a mounted copy, and `--endpoint URL` with `--allow-http` for an S3-compatible endpoint served without TLS. Narrow a run with `--prefix`, `--topic`, and `--partition`. `--allow-epoch-restarts` accepts a chain restart instead of grading it as a hole, and `--strict-orphans` turns an orphan object into a failure.

`--deep` downloads every object and recomputes its SHA-256. It is the only check that catches a body replaced with different bytes of the same length: a shallow run checks that each object exists with the recorded size, and a same-size substitution passes that check.

The verdict goes to stdout and the diagnostics go to stderr, so a script can read the grade without parsing the explanation.

| Verdict | Exit code | What it means |
|---------|-----------|---------------|
| `OK: N manifests over M partition(s), …` | 0 | The chain is continuous, every signature is valid, and every object is present. One line per partition follows, and it names the tip for the next `--expect-head`. |
| `OK: empty archive` | 0 | Nothing to verify. |
| `TAMPER DETECTED at KEY (seq N)` | 1 | A manifest was rewritten, reordered, or removed, or an object does not match what its manifest records. |
| `HEAD MISMATCH: expected X, archive tip Y` | 1 | The chain is internally perfect but stops short of the head the run was given. This is what tail truncation looks like. |
| `ORPHAN OBJECTS: N object(s) with no manifest` | 0, or 1 under `--strict-orphans` | An object that no manifest names makes no integrity claim. Reported either way, and counted on the `OK:` line. |
| `INCOMPLETE ATTESTATION: chain restarted N time(s)` | 1 | The chain has holes between epochs. Use a topic-backed RLMM, or accept them with `--allow-epoch-restarts`. |
| `INCOMPLETE ATTESTATION: N manifest(s) unsigned, M signed by an untrusted key` | 1 | The archive is not attested to the key the run trusts. |

A failure to read the archive is a different outcome from a broken archive, and the tool keeps them apart. "I could not look" exits with an error message and no verdict.

### Why an orphan does not fail the run

The archiver writes the manifest **last**, as the commit point, so a copy that died before sealing one leaves its objects behind with nothing naming them. The retry runs under a fresh segment id, so those objects are inert: no manifest claims them and no reader reaches them.

A WORM archive refuses deletes, so they can never be cleared. Grading them a failure would mean one interrupted copy condemns the archive on every run from then on, with no action any operator could take to get back to green — and a verdict nobody can act on is one they stop reading. They are reported in full instead, on stderr and in the `OK:` line, and `--strict-orphans` restores the hard grade for a deployment that wants the bucket to hold nothing but the archive.

What that costs is worth stating: orphans are also what a *removed* manifest leaves behind. Only `--expect-head` settles tail truncation, and it is the check to run if that is the concern.

### When a body has been overwritten

On a versioned bucket an overwrite does not replace the locked original; it stacks a new current version over it. `--deep` reads the current version, because that is what a reader gets, so the overwrite is reported as tampering. When the manifest recorded a version id, the run then re-reads that pinned version and says whether it still matches — the difference between restoring the segment and writing it off. The pinned version is never read first: a check that always read it would confirm bytes no reader can reach by key any more, and would be blind to the overwrite itself.

## Documentation

Read the [krabka-remote-storage API documentation](https://krabka-io.github.io/krabka-broker/krabka-remote-storage/krabka_remote_storage/index.html). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
