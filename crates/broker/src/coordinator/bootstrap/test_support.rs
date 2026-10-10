//! Fixtures shared by the bootstrap unit tests: an in-process controller that
//! has already elected a leader, the two `GroupCoordinator` flavours the tests
//! drive, and the classic record builder they replay.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::assert;
use krabka_raft::ControllerHandle;

use crate::{
    coordinator::{GroupCoordinator, persistence::GroupMetadataValue},
    partition_registry::PartitionRegistry,
};

/// Start a controller, wait until it reports a leader, and return the
/// handle.
pub(super) async fn controller_with_leader(log_dir: std::path::PathBuf) -> Arc<ControllerHandle> {
    let cfg = krabka_raft::ControllerConfig {
        election_timeout: krabka_units::millis(200),
        heartbeat_interval: Some(krabka_units::millis(50)),
        client_id: "test".into(),
        ..krabka_raft::ControllerConfig::for_tests(krabka_raft::NodeId(1), log_dir)
    };
    let handle = Arc::new(krabka_raft::Controller::start(cfg).await.unwrap());
    let mut rx = handle.watch_leader();
    let deadline = Instant::now() + Duration::from_secs(5);
    while rx.borrow().is_none() {
        assert!(Instant::now() < deadline, "no leader elected in 5s");
        let _ = tokio::time::timeout(Duration::from_millis(100), rx.changed()).await;
    }
    handle
}

pub(super) fn test_coordinator(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    partitions: &Arc<PartitionRegistry>,
) -> Arc<GroupCoordinator> {
    let offsets_log: Arc<dyn crate::coordinator::unified::offsets_log::OffsetsLog> = Arc::new(
        crate::coordinator::unified::offsets_log::ProductionOffsetsLog::new(
            partitions.clone(),
            controller.clone(),
            krabka_raft::NodeId(1),
        ),
    );
    crate::coordinator::test_support::default_coordinator(
        Arc::new(crate::coordinator::unified::ImageMetadataProvider {
            controller: controller.clone(),
        }),
        offsets_log,
    )
}

/// Build a bare `GroupCoordinator` with no metadata wiring and no
/// persister wiring.
///
/// It has the same shape as the coordinator in the share and streams
/// replay tests. A test can drive the `apply_record`, `apply_tombstone`,
/// and `finalize` replay path directly with it.
pub(super) fn bare_coordinator() -> Arc<GroupCoordinator> {
    bare_coordinator_with_mailbox(
        crate::coordinator::unified::config::NextGenConfig::default().actor_mailbox_capacity,
    )
}

/// A [`bare_coordinator`] whose group actors have a mailbox of exactly
/// `actor_mailbox_capacity` messages, so a test can drive replay past what one
/// actor can take at once.
pub(super) fn bare_coordinator_with_mailbox(
    actor_mailbox_capacity: usize,
) -> Arc<GroupCoordinator> {
    crate::coordinator::test_support::coordinator_with_config(
        crate::coordinator::unified::config::NextGenConfig {
            actor_mailbox_capacity,
            ..crate::coordinator::unified::config::NextGenConfig::default()
        },
        crate::coordinator::unified::actor::test_support::empty_metadata(),
        Arc::new(crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog::default()),
    )
}

/// Encode a classic k2 `GroupMetadata` key-value record pair for group
/// `g` with a single member `m1`.
pub(super) fn classic_group_record(
    group_id: &str,
    member_id: &str,
) -> (bytes::Bytes, bytes::Bytes) {
    use crate::coordinator::persistence::MemberMetadata;
    let key = GroupMetadataValue::encode_key(group_id).unwrap();
    let value = GroupMetadataValue {
        protocol_type: "consumer".into(),
        generation: 3,
        protocol_name: Some("range".into()),
        leader: Some(member_id.into()),
        current_state_timestamp_ms: 0,
        members: vec![MemberMetadata {
            member_id: member_id.into(),
            group_instance_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            rebalance_timeout_ms: 60_000,
            session_timeout_ms: 30_000,
            subscription: bytes::Bytes::new(),
            assignment: bytes::Bytes::from_static(b"asn"),
        }],
    }
    .encode_value()
    .unwrap();
    (key, value)
}

/// Group and member metadata written by an upgrade, in log order.
pub(super) fn consumer_group_records(
    epoch: i32,
    classic: Option<crate::coordinator::unified::persistence_next_gen::ClassicMemberMetadata>,
) -> [(bytes::Bytes, bytes::Bytes); 2] {
    use crate::coordinator::unified::persistence_next_gen as ng;
    [
        (
            ng::encode_key(&ng::NextGenKey::GroupMetadata {
                group_id: "g".into(),
            })
            .unwrap(),
            ng::GroupMetadataValue {
                epoch,
                metadata_hash: 0,
            }
            .encode(),
        ),
        (
            ng::encode_key(&ng::NextGenKey::MemberMetadata {
                group_id: "g".into(),
                member_id: "m1".into(),
            })
            .unwrap(),
            ng::MemberMetadataValue {
                instance_id: None,
                rack_id: None,
                client_id: "c1".into(),
                client_host: "/127.0.0.1".into(),
                subscribed_topic_names: vec!["t".into()],
                subscribed_topic_regex: None,
                server_assignor: None,
                rebalance_timeout_ms: 60_000,
                classic,
            }
            .encode(),
        ),
    ]
}

/// Replay ordinary values through the same parser and record application path.
pub(super) fn replay_values(
    coordinator: &Arc<GroupCoordinator>,
    records: impl IntoIterator<Item = (bytes::Bytes, bytes::Bytes)>,
) -> super::replay::Replayed {
    replay_stream(
        coordinator,
        records.into_iter().map(|(key, value)| (key, Some(value))),
    )
}

/// Apply a stream of record values and tombstones in log order.
pub(super) fn replay_stream(
    coordinator: &Arc<GroupCoordinator>,
    stream: impl IntoIterator<Item = (bytes::Bytes, Option<bytes::Bytes>)>,
) -> super::replay::Replayed {
    use super::replay::{Replayed, apply_record, apply_tombstone};
    let mut replayed = Replayed::default();
    let batch = krabka_protocol::records::RecordBatch::default();
    for (key, value) in stream {
        let key = crate::coordinator::persistence::parse_key(&key).unwrap();
        match value {
            Some(value) => apply_record(coordinator, &mut replayed, key, &value, &batch).unwrap(),
            None => apply_tombstone(coordinator, &mut replayed, key).unwrap(),
        }
    }
    replayed
}

/// Compacted downgrade residue: one k6 record, then the authoritative classic
/// k2 snapshot. The caller supplies the literal k6 value or tombstone.
pub(super) fn replay_classic_residue(
    target_metadata: Option<bytes::Bytes>,
) -> (Arc<GroupCoordinator>, super::replay::Replayed) {
    use crate::coordinator::unified::persistence_next_gen::{NextGenKey, encode_key};
    let coordinator = bare_coordinator();
    let (key, value) = classic_group_record("g", "m1");
    let stream = [
        (
            encode_key(&NextGenKey::TargetAssignmentMetadata {
                group_id: "g".into(),
            })
            .unwrap(),
            target_metadata,
        ),
        (key, Some(value)),
    ];
    let replayed = replay_stream(&coordinator, stream);
    (coordinator, replayed)
}

pub(super) async fn assert_classic_replayed(coordinator: &GroupCoordinator) {
    use crate::coordinator::unified::{GroupType, actor::GroupKindTag};
    assert!(coordinator.group_type("g") != Some(GroupType::NextGen));
    let snapshot = coordinator
        .describe_group("g")
        .await
        .expect("classic group present");
    assert!(
        snapshot
            .members
            .iter()
            .any(|member| member.member_id == "m1")
    );
    assert!(
        coordinator
            .find("g")
            .is_some_and(|handle| handle.kind == GroupKindTag::Classic)
    );
}

/// Build a persisted commit with a distinct offset per partition.
pub(super) fn commit_record(partition: i32, offset: i64) -> krabka_protocol::records::Record {
    commit_record_for_group("g", partition, offset)
}

pub(super) fn commit_record_for_group(
    group: &str,
    partition: i32,
    offset: i64,
) -> krabka_protocol::records::Record {
    crate::coordinator::test_support::offset_record(
        crate::coordinator::test_support::OffsetRecordSetup {
            group,
            partition: krabka_ids::PartitionIndex(partition),
            offset: krabka_log::Offset(offset),
            ..Default::default()
        },
    )
}

/// Write each `(key, value)` record (a `None` value is a tombstone) to a fresh
/// log as a batch of its own, then replay that log into a [`bare_coordinator`].
///
/// Returns the coordinator the replay seeded and what [`replay_records`]
/// returned.
///
/// [`replay_records`]: super::replay::replay_records
pub(super) fn replay_log(
    records: impl IntoIterator<Item = (bytes::Bytes, Option<bytes::Bytes>)>,
) -> (
    Arc<GroupCoordinator>,
    Result<super::replay::Replayed, crate::error::BrokerError>,
) {
    use krabka_protocol::records::{Record, RecordBatch};
    let coordinator = bare_coordinator();
    let dir = tempfile::tempdir().unwrap();
    let mut log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
    for (key, value) in records {
        log.append(&mut RecordBatch {
            records: vec![Record {
                key: Some(key),
                value,
                ..Record::default()
            }],
            ..RecordBatch::default()
        })
        .unwrap();
    }
    let replayed = super::replay::replay_records(&log, &coordinator);
    (coordinator, replayed)
}
