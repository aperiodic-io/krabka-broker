//! Tests for the metadata log's Kafka lifecycle: segments that roll by
//! `metadata.log.segment.bytes`, snapshots that leave the log start alone
//! until `metadata.max.retention.bytes` or `metadata.max.retention.ms` lets
//! the oldest one go, and the KIP-835 no-op records a leader appends.

use assert2::check;
use krabka_units::prelude::{bytes, days, gibibytes, millis};

use super::*;
use crate::kraft::controller::{
    checkpoint::{checkpoint_ids, write_checkpoint},
    records::{is_kip835_noop, noop_record_value},
    test_support::{EngineSetup, build_engine_only, elect_single_voter_engine, topic_record_named},
};

/// A metadata log whose every batch gets its own segment, so a cleaning that
/// moves the log start shows up as deleted segment files.
fn one_batch_segments(
    max_retention_size: Option<ByteSize>,
    max_retention: Option<Time>,
) -> MetadataLogConfig {
    MetadataLogConfig {
        segment_size: bytes(1),
        max_retention_size,
        max_retention,
        max_idle_interval: millis(0),
        ..MetadataLogConfig::default()
    }
}

fn engine(metadata_log: MetadataLogConfig) -> (Engine, tempfile::TempDir) {
    let (mut engine, dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        metadata_log,
        ..Default::default()
    });
    elect_single_voter_engine(&mut engine);
    (engine, dir)
}

fn commit_topic(engine: &mut Engine, name: &str, id: u128) {
    let mut rx = super::test_support::submit_on_engine(engine, &topic_record_named(name, id));
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))), "commit {name}");
}

/// The snapshot ids in the partition directory, oldest first.
fn snapshot_ids(dir: &std::path::Path) -> Vec<(i64, i32)> {
    let mut ids = checkpoint_ids(dir);
    ids.sort_unstable();
    ids
}

/// What a cleaning left: the log start, whether the first segment file is
/// still on disk, and the snapshots in the directory.
#[derive(Debug, PartialEq, Eq)]
struct Cleaned {
    log_start: i64,
    first_segment: bool,
    snapshots: Vec<(i64, i32)>,
}

fn cleaned(engine: &Engine) -> Cleaned {
    Cleaned {
        log_start: engine.log.log_start_offset().0,
        first_segment: engine.data_dir.join("00000000000000000000.log").exists(),
        snapshots: snapshot_ids(&engine.data_dir),
    }
}

/// Two snapshots in, each cleaning rule takes the log start up to the newer
/// snapshot and deletes the segments wholly below it, as Kafka's
/// `deleteBeforeSnapshot` does. With one snapshot, or inside the limits,
/// nothing moves. The snapshot the log start left behind stays for a reader
/// that is part way through it.
#[test]
fn a_cleaning_moves_the_log_start_up_to_the_newest_snapshot_once_a_limit_lets_it() {
    // (what, retention size, retention age, whether the second snapshot cleans)
    let cases = [
        ("the size rule", Some(bytes(0)), None, true),
        ("the age rule", None, Some(millis(0)), true),
        (
            "inside both limits",
            Some(gibibytes(1)),
            Some(days(365)),
            false,
        ),
        ("no limit", None, None, false),
    ];
    for (what, max_retention_size, max_retention, cleans) in cases {
        let (mut engine, _dir) = engine(one_batch_segments(max_retention_size, max_retention));
        commit_topic(&mut engine, "first", 1);
        engine.write_snapshot_and_clean().expect("first snapshot");
        let first = engine.log.hwm().0;
        let after_one = cleaned(&engine);

        // The age rule compares the snapshot's last record with the wall
        // clock, which must move past it.
        std::thread::sleep(std::time::Duration::from_millis(5));
        commit_topic(&mut engine, "second", 2);
        engine.write_snapshot_and_clean().expect("second snapshot");
        let second = engine.log.hwm().0;

        check!(
            after_one
                == Cleaned {
                    log_start: 0,
                    first_segment: true,
                    snapshots: vec![(first, 1)],
                },
            "{what}: one snapshot cleans nothing"
        );
        let want = if cleans {
            Cleaned {
                log_start: second,
                first_segment: false,
                snapshots: vec![(first, 1), (second, 1)],
            }
        } else {
            Cleaned {
                log_start: 0,
                first_segment: true,
                snapshots: vec![(first, 1), (second, 1)],
            }
        };
        check!(cleaned(&engine) == want, "{what}");
    }
}

/// A third snapshot under the size rule deletes the snapshot the second
/// cleaning kept for in-flight readers, so the directory holds the newest
/// snapshot and the one before it.
#[test]
fn a_later_cleaning_deletes_the_snapshot_an_earlier_one_kept() {
    let (mut engine, _dir) = engine(one_batch_segments(Some(bytes(0)), None));
    let mut ids = Vec::new();
    for (name, id) in [("first", 1), ("second", 2), ("third", 3)] {
        commit_topic(&mut engine, name, id);
        engine.write_snapshot_and_clean().expect("snapshot");
        ids.push((engine.log.hwm().0, 1));
    }
    check!(
        cleaned(&engine)
            == Cleaned {
                log_start: ids[2].0,
                first_segment: false,
                snapshots: vec![ids[1], ids[2]],
            }
    );
}

/// A cleaning keeps the log start at a committed snapshot: it never moves it
/// past the high watermark, whatever the limits say.
#[test]
fn a_cleaning_never_moves_the_log_start_past_the_high_watermark() {
    let (mut engine, _dir) = engine(one_batch_segments(Some(bytes(0)), None));
    commit_topic(&mut engine, "first", 1);
    engine.write_snapshot_and_clean().expect("first snapshot");
    // A snapshot file whose boundary nothing committed yet.
    let beyond = engine.log.hwm().0 + 100;
    write_checkpoint(&engine.data_dir, beyond, 1, b"not committed").expect("write");

    check!(
        (
            engine.maybe_clean().expect("clean"),
            engine.log.log_start_offset()
        ) == (false, Offset(0))
    );
}

/// KIP-835: the leader's no-op records advance the committed log and leave
/// the metadata image as it was.
#[test]
fn a_leader_appends_kip835_no_ops_that_change_nothing() {
    let (mut engine, _dir) = engine(one_batch_segments(None, None));
    commit_topic(&mut engine, "topic", 1);
    let image = engine.image.clone();
    let end = engine.log.log_end_offset();

    engine.append_noop().expect("first no-op");
    engine.append_noop().expect("second no-op");

    check!(
        (
            engine.log.log_end_offset(),
            engine.log.hwm(),
            engine.image == image,
        ) == (end + 2, end + 2, true)
    );
}

/// Only an empty `NoOpRecord` is a KIP-835 no-op. A krabka record that rides
/// a `NoOpRecord` carrier is not one, and neither is any other record.
#[test]
fn only_an_empty_no_op_record_is_a_kip835_no_op() {
    let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    let carried = krabka_metadata::to_kraft_values(
        &krabka_metadata::MetadataRecord::V1FeaturesEpoch(krabka_metadata::FeaturesEpochRecord {
            epoch: 7,
        }),
        &image,
    )
    .expect("encode a carried record");
    let topic = krabka_metadata::to_kraft_values(&topic_record_named("t", 1)[0], &image)
        .expect("encode a topic record");
    let cases: [(&str, &[u8], bool); 4] = [
        (
            "an empty no-op",
            &noop_record_value().expect("encode"),
            true,
        ),
        ("a carried krabka record", &carried[0], false),
        ("a topic record", &topic[0], false),
        ("garbage", b"\xff\xff", false),
    ];
    for (what, value, want) in cases {
        check!(is_kip835_noop(value) == want, "{what}");
    }
}

/// The no-op timer runs only while this node leads and the interval is not
/// zero.
#[test]
fn the_no_op_timer_runs_only_on_a_leader_with_an_interval() {
    for (what, interval, lead, armed) in [
        ("a leader", millis(500), true, true),
        ("a leader with no interval", millis(0), true, false),
        ("a node that does not lead", millis(500), false, false),
    ] {
        let (mut engine, _dir) = build_engine_only(EngineSetup {
            ids: &[NodeId(1)],
            metadata_log: MetadataLogConfig {
                max_idle_interval: interval,
                ..MetadataLogConfig::default()
            },
            ..Default::default()
        });
        if lead {
            elect_single_voter_engine(&mut engine);
        }
        engine.reconcile_noop_timer();
        check!(engine.noop_at.is_some() == armed, "{what}");
    }
}
