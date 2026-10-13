//! Fixtures the materialize unit tests share: a parsed `RestoreArgs` for a
//! dummy archive and a given target, the records and batches a test builds a
//! segment from, and the `VerifiedSegment` and `PartitionInventory` values that
//! stand in for what an archive scan recovered.

use {
    crate::{
        Cli,
        args::RestoreArgs,
        bound::Predicates,
        discover::PartitionInventory,
        verify::{SegmentFacts, VerifiedSegment},
    },
    bytes::{Bytes, BytesMut},
    clap::Parser as _,
    krabka_ids::Offset,
    krabka_log::{Log, LogConfig, name},
    krabka_protocol::records::{Attributes, RecordBatch},
    krabka_remote_storage::TopicIdPartition,
    uuid::Uuid,
};

/// Parses `RestoreArgs` the same way the binary does, with a fixed dummy
/// archive source and the given `log_dir`, so a test only has to state
/// the target and bound flags under test.
pub(super) fn args_from(extra: &[&str], log_dir: &std::path::Path) -> RestoreArgs {
    let mut argv = vec![
        "krabka-restore".to_owned(),
        "--archive-local".to_owned(),
        "/archive".to_owned(),
        "--log-dir".to_owned(),
        log_dir.display().to_string(),
    ];
    argv.extend(extra.iter().map(|s| (*s).to_owned()));
    Cli::try_parse_from(argv).expect("valid command line").args
}

/// A fresh orders partition and its bound predicates, with the target kept alive.
pub(super) fn materialization_context(
    extra: &[&str],
) -> (tempfile::TempDir, RestoreArgs, TopicIdPartition, Predicates) {
    let target = tempfile::tempdir().expect("tempdir");
    let args = args_from(extra, target.path());
    let partition = topic_id_partition("orders", 0);
    let predicates = Predicates::from_args(&args).expect("predicates");
    (target, args, partition, predicates)
}

pub(super) fn reopen_orders(args: &RestoreArgs) -> Log {
    let dir = name::partition_dir(&args.target.log_dir, "orders", 0);
    Log::open(&dir, LogConfig::default()).expect("reopen")
}

pub(super) fn topic_id_partition(topic: &str, partition: i32) -> TopicIdPartition {
    TopicIdPartition {
        topic_id: Uuid::new_v4(),
        topic: topic.to_owned(),
        partition,
    }
}

/// A minimal record at `offset_delta`, with no key or headers.
pub(super) fn record(offset_delta: i32, value: &str) -> krabka_protocol::records::Record {
    krabka_protocol::records::Record {
        attributes: 0,
        timestamp_delta: i64::from(offset_delta),
        offset_delta,
        key: None,
        value: Some(Bytes::copy_from_slice(value.as_bytes())),
        headers: Vec::new(),
    }
}

/// A record like [`record`], but with a key an `--exclude-key` pattern
/// can match.
pub(super) fn record_with_key(offset_delta: i32, key: &str) -> krabka_protocol::records::Record {
    krabka_protocol::records::Record {
        key: Some(Bytes::copy_from_slice(key.as_bytes())),
        ..record(offset_delta, "v")
    }
}

/// A batch at `base_offset` holding `records`, with `last_offset_delta`
/// derived from the highest `offset_delta` among them, matching how a
/// real producer batch is shaped.
pub(super) fn batch(
    base_offset: i64,
    records: Vec<krabka_protocol::records::Record>,
) -> RecordBatch {
    let delta = records.iter().map(|r| r.offset_delta).max().unwrap_or(0);
    let ts = 1_700_000_000_000;
    RecordBatch {
        base_offset,
        partition_leader_epoch: 7,
        attributes: Attributes::default(),
        last_offset_delta: delta,
        base_timestamp: ts,
        max_timestamp: ts + i64::from(delta),
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records,
    }
}

fn encode(batches: &[RecordBatch]) -> Bytes {
    let mut buf = BytesMut::new();
    for b in batches {
        b.encode(&mut buf).expect("encode");
    }
    buf.freeze()
}

/// Build a [`VerifiedSegment`] from already-encoded `batches`, the way
/// `verify_segment` would have handed it to `write_segment`.
pub(super) fn verified_segment(base_offset: i64, batches: &[RecordBatch]) -> VerifiedSegment {
    let log = encode(batches);
    let last = batches.last().expect("at least one batch");
    let end_offset = last.base_offset + i64::from(last.last_offset_delta);
    let records: u64 = batches
        .iter()
        .map(|b| u64::try_from(b.records.len()).unwrap_or(0))
        .sum();
    VerifiedSegment {
        facts: SegmentFacts {
            segment_id: Uuid::new_v4(),
            base_offset: Offset(base_offset),
            end_offset: Offset(end_offset),
            max_timestamp_ms: batches.iter().map(|b| b.max_timestamp).max().unwrap_or(-1),
            batches: u64::try_from(batches.len()).unwrap_or(0),
            records,
            log_bytes: u64::try_from(log.len()).unwrap_or(0),
            leader_epochs: Vec::new(),
        },
        log,
    }
}

pub(super) fn partition_inventory(
    topic: &str,
    topic_id: Uuid,
    partition: i32,
) -> PartitionInventory {
    PartitionInventory {
        partition: TopicIdPartition {
            topic_id,
            topic: topic.to_owned(),
            partition,
        },
        segments: Vec::new(),
    }
}
