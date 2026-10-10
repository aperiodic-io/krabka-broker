//! Tests for the controller activation: the bootstrap records a new leader
//! writes to a log that holds no `metadata.version`, and the bootstrap
//! checkpoint whose records reach the image only through that write.

use std::collections::BTreeMap;

use assert2::check;
use krabka_metadata::{
    BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, FeatureLevelRecord,
    group_version::GROUP_VERSION_FEATURE,
    metadata_version::{ELR_VERSION_FEATURE, METADATA_VERSION_FEATURE},
};
use krabka_units::prelude::secs;

use super::*;
use crate::{
    config::{DEFAULT_METADATA_RAFT_FETCH_MAX, LATEST_PRODUCTION_METADATA_VERSION},
    kraft::controller::{
        activation::{Activation, activation_records, check_bootstrap_records},
        checkpoint::write_checkpoint,
        records::metadata_record_batch,
        test_support::{EngineSetup, await_leader, build_engine_only, voter_set},
    },
};

/// A `metadata.version` level below the latest production one, which an
/// earlier leader finalized.
const EARLIER_METADATA_VERSION: i16 = 21;

fn feature(name: &str, level: i16) -> MetadataRecord {
    MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: name.to_owned(),
        level,
    })
}

/// The cluster-level `min.insync.replicas` at `value`, as Kafka's
/// activation writes it: a `ConfigRecord` of the `BROKER` resource `""`.
fn cluster_min_isr(value: i32) -> MetadataRecord {
    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
        config_name: "min.insync.replicas".to_owned(),
        config_value: Some(value.to_string()),
    })
}

/// Bootstrap records as a format writes them: `metadata.version` first, then
/// another feature level.
fn bootstrap_records() -> Vec<MetadataRecord> {
    vec![
        feature(METADATA_VERSION_FEATURE, LATEST_PRODUCTION_METADATA_VERSION),
        feature(GROUP_VERSION_FEATURE, 1),
    ]
}

/// One batch of the metadata log, as a replica reads it.
#[derive(Debug, Clone, PartialEq)]
enum Batch {
    /// A control batch: a `LeaderChange` marker and the KIP-853 controls.
    Control,
    /// A batch of metadata records.
    Metadata(Vec<MetadataRecord>),
}

/// Every batch of the engine's log, from its start, with each metadata value
/// decoded against the image that the values before it produce.
fn log_batches(engine: &Engine) -> Vec<Batch> {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    engine
        .log
        .read_decoded(
            engine.log.log_start_offset(),
            DEFAULT_METADATA_RAFT_FETCH_MAX,
        )
        .expect("read the log")
        .iter()
        .map(|batch| {
            if batch.attributes.is_control_batch() {
                return Batch::Control;
            }
            Batch::Metadata(
                batch
                    .records
                    .iter()
                    .filter_map(|record| record.value.as_ref())
                    .map(|value| {
                        let record = from_kraft_value(value, &image).expect("decode a value");
                        image.apply(&record);
                        record
                    })
                    .collect(),
            )
        })
        .collect()
}

/// What the log holds before the first election.
#[derive(Debug, Clone)]
enum Before {
    /// Nothing.
    Nothing,
    /// A batch of these records that a quorum committed.
    Committed(Vec<MetadataRecord>),
    /// A batch of these records that an earlier leader appended and did not
    /// commit.
    Uncommitted(Vec<MetadataRecord>),
}

/// Append `records` to the log of `engine` as one batch of leader epoch 0,
/// as a follower takes it from that leader, and leave it uncommitted.
fn append_uncommitted(engine: &mut Engine, records: &[MetadataRecord]) {
    let mut image = engine.image.clone();
    let mut blobs = Vec::new();
    for record in records {
        blobs.extend(to_kraft_values(record, &image).expect("encode a record"));
        image.apply(record);
    }
    let mut batch = metadata_record_batch(0, &blobs).expect("build a batch");
    engine.log.append(&mut batch, 0).expect("append the batch");
}

/// Elect node 1 leader of `voters`, with node 2 granting the votes a
/// multi-voter quorum needs.
fn elect(engine: &mut Engine, voters: &[NodeId]) {
    engine.on_event(Event::ElectionTimeout);
    if voters.len() > 1 {
        for epoch in [0, 1] {
            engine.on_event(Event::ReceiveVoteResponse {
                from: NodeId(2),
                epoch,
                vote_granted: true,
            });
        }
    }
    assert2::assert!(engine.core.role().is_leader());
}

/// Commit the log of the leader `engine`: node 2 fetches at its log end, which
/// makes a majority of three. A single voter has committed its log already.
fn acknowledge(engine: &mut Engine, voters: &[NodeId]) {
    if voters.len() > 1 {
        engine.on_event(Event::ReceiveFetch {
            from: NodeId(2),
            fetch_epoch: engine.core.quorum_state().leader_epoch,
            fetch_offset: engine.log.log_end_offset().0,
        });
    }
}

/// A new leader writes its bootstrap records once, directly after the
/// `LeaderChange` batch of its epoch, and only to a log that holds no
/// `metadata.version`. A `metadata.version` that an earlier leader appended
/// and did not commit counts, because it commits with the new epoch. The
/// records reach the image when the quorum commits them, and a later epoch on
/// the same log writes nothing more. A multi-voter election sends its vote
/// requests on the runtime, so the test runs on one.
#[tokio::test]
async fn a_new_leader_writes_the_bootstrap_records_only_to_a_log_without_a_metadata_version() {
    let single: &[NodeId] = &[NodeId(1)];
    let three: &[NodeId] = &[NodeId(1), NodeId(2), NodeId(3)];
    let earlier = vec![feature(METADATA_VERSION_FEATURE, EARLIER_METADATA_VERSION)];
    let latest = Some(LATEST_PRODUCTION_METADATA_VERSION);
    // (what, voters, bootstrap records, the log before the election, the
    // finalized `metadata.version` of the image after the election, the log
    // after the quorum commits it and a later epoch starts, and the finalized
    // `metadata.version` of the image then)
    let mut with_elr = bootstrap_records();
    with_elr.push(feature(ELR_VERSION_FEATURE, 1));
    let mut with_elr_and_min_isr = with_elr.clone();
    with_elr_and_min_isr.push(cluster_min_isr(2));
    let cases = [
        (
            "an empty log",
            single,
            bootstrap_records(),
            Before::Nothing,
            latest,
            vec![
                Batch::Control,
                Batch::Metadata(bootstrap_records()),
                Batch::Control,
            ],
            latest,
        ),
        (
            "an empty log and bootstrap records that enable ELR",
            single,
            with_elr,
            Before::Nothing,
            latest,
            vec![
                Batch::Control,
                Batch::Metadata(with_elr_and_min_isr),
                Batch::Control,
            ],
            latest,
        ),
        (
            "an empty log of three voters",
            three,
            bootstrap_records(),
            Before::Nothing,
            None,
            vec![
                Batch::Control,
                Batch::Metadata(bootstrap_records()),
                Batch::Control,
            ],
            latest,
        ),
        (
            "no bootstrap records",
            single,
            Vec::new(),
            Before::Nothing,
            None,
            vec![Batch::Control, Batch::Control],
            None,
        ),
        (
            "a committed metadata.version",
            single,
            bootstrap_records(),
            Before::Committed(earlier.clone()),
            Some(EARLIER_METADATA_VERSION),
            vec![
                Batch::Metadata(earlier.clone()),
                Batch::Control,
                Batch::Control,
            ],
            Some(EARLIER_METADATA_VERSION),
        ),
        (
            "an uncommitted metadata.version of an earlier leader",
            three,
            bootstrap_records(),
            Before::Uncommitted(earlier.clone()),
            None,
            vec![Batch::Metadata(earlier), Batch::Control, Batch::Control],
            Some(EARLIER_METADATA_VERSION),
        ),
    ];
    for (what, voters, bootstrap, before, elected, log, committed) in cases {
        let (mut engine, _dir) = build_engine_only(EngineSetup {
            ids: voters,
            ..Default::default()
        });
        engine.activation = Activation {
            bootstrap_records: bootstrap,
            default_min_insync_replicas: 2,
        };
        match &before {
            Before::Nothing => {}
            Before::Committed(records) => {
                engine.test_append_and_commit(records);
            }
            Before::Uncommitted(records) => append_uncommitted(&mut engine, records),
        }

        elect(&mut engine, voters);
        let after_election = engine.image.finalized_metadata_version();
        acknowledge(&mut engine, voters);
        let epoch = engine.core.quorum_state().leader_epoch;
        engine.execute_one_local(Action::AppendLeaderChange { epoch });

        check!(
            (
                after_election,
                log_batches(&engine),
                engine.image.finalized_metadata_version()
            ) == (elected, log, committed),
            "{what}"
        );
    }
}

/// Bootstrap metadata that holds records but does not finalize
/// `metadata.version` is refused: the leader would write it to every new epoch
/// of a log that never gets a `metadata.version`. A record that sets level 0
/// removes the feature, so it does not finalize one either.
#[test]
fn bootstrap_metadata_without_a_metadata_version_is_refused() {
    // (what, records, the refusal)
    let cases = [
        ("no records", Vec::new(), None),
        ("a metadata.version", bootstrap_records(), None),
        (
            "no metadata.version",
            vec![feature(GROUP_VERSION_FEATURE, 1)],
            Some(
                "startup misconfiguration: No FeatureLevelRecord for metadata.version was found \
                 in the bootstrap metadata from the bootstrap checkpoint",
            ),
        ),
        (
            "a metadata.version that a later record removes",
            vec![
                feature(METADATA_VERSION_FEATURE, LATEST_PRODUCTION_METADATA_VERSION),
                feature(METADATA_VERSION_FEATURE, 0),
            ],
            Some(
                "startup misconfiguration: No MetadataVersion with feature level 0 in the \
                 bootstrap metadata from the bootstrap checkpoint",
            ),
        ),
    ];
    for (what, records, refusal) in cases {
        check!(
            check_bootstrap_records(&records, "the bootstrap checkpoint")
                .map_err(|error| error.to_string())
                == refusal.map_or(Ok(()), |refusal| Err(refusal.to_owned())),
            "{what}"
        );
    }
}

/// The records of the bootstrap checkpoint stay out of the image of a node
/// that opens it, as Kafka's `MetadataLoader` ignores the bootstrap
/// checkpoint. They replace the configured bootstrap records, and reach the
/// image through the log once the node leads. The checkpoint's voters are in
/// the image from the start.
#[tokio::test]
async fn the_records_of_the_bootstrap_checkpoint_reach_the_image_through_the_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let voters = voter_set(&[NodeId(1)]);
    let bytes = crate::serialize_bootstrap_snapshot(1, &voters, &bootstrap_records(), 0)
        .expect("serialize");
    write_checkpoint(dir.path(), 0, 0, &bytes).expect("write the bootstrap checkpoint");

    // The election timeout is long, so only the injected timeout elects.
    let ctrl = crate::kraft::controller::test_support::open_test_controller_with(
        dir.path().to_path_buf(),
        crate::kraft::controller::test_support::ControllerOpenSetup {
            voters: voters.clone(),
            election_timeout: secs(60),
            activation: Activation {
                bootstrap_records: vec![feature(
                    METADATA_VERSION_FEATURE,
                    EARLIER_METADATA_VERSION,
                )],
                ..Activation::default()
            },
            ..Default::default()
        },
    )
    .expect("open");
    let opened = ctrl.current_image();
    let before = (opened.finalized_features().clone(), opened.voters().clone());

    ctrl.inject_event(Event::ElectionTimeout)
        .await
        .expect("inject the election timeout");
    await_leader(&ctrl, Some(NodeId(1))).await;
    let mut images = ctrl.watch_image();
    let led = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        images.wait_for(|image| image.finalized_metadata_version().is_some()),
    )
    .await
    .expect("the bootstrap records commit")
    .expect("the image channel stays open")
    .finalized_features()
    .clone();

    let expected: BTreeMap<String, i16> = [
        (
            METADATA_VERSION_FEATURE.to_owned(),
            LATEST_PRODUCTION_METADATA_VERSION,
        ),
        (GROUP_VERSION_FEATURE.to_owned(), 1),
    ]
    .into();
    check!((before, led) == ((BTreeMap::new(), voters), expected));
    ctrl.shutdown().await;
}

/// The activation of an empty log writes the bootstrap records, and after
/// them the cluster-level `min.insync.replicas` at the static value when the
/// bootstrap records leave `eligible.leader.replicas.version` above 0, as
/// Kafka's `ActivationRecordsGenerator.recordsForEmptyLog` does. The last
/// level of the feature counts.
#[test]
fn the_activation_sets_the_cluster_min_insync_replicas_when_the_bootstrap_enables_elr() {
    let mv = feature(METADATA_VERSION_FEATURE, LATEST_PRODUCTION_METADATA_VERSION);
    let elr = |level| feature(ELR_VERSION_FEATURE, level);
    // (what, bootstrap records, static `min.insync.replicas`, the records)
    let cases = [
        ("no ELR record", vec![mv.clone()], 2, vec![mv.clone()]),
        (
            "ELR at level 1",
            vec![mv.clone(), elr(1)],
            2,
            vec![mv.clone(), elr(1), cluster_min_isr(2)],
        ),
        (
            "ELR at level 1 and Kafka's default",
            vec![mv.clone(), elr(1)],
            1,
            vec![mv.clone(), elr(1), cluster_min_isr(1)],
        ),
        (
            "ELR at level 0",
            vec![mv.clone(), elr(0)],
            2,
            vec![mv.clone(), elr(0)],
        ),
        (
            "ELR enabled and then disabled",
            vec![mv.clone(), elr(1), elr(0)],
            2,
            vec![mv, elr(1), elr(0)],
        ),
        ("no bootstrap records", vec![], 2, vec![]),
    ];
    for (what, bootstrap_records, default_min_insync_replicas, records) in cases {
        let activation = Activation {
            bootstrap_records,
            default_min_insync_replicas,
        };
        check!(activation_records(&activation) == records, "{what}");
    }
}

/// A new leader that refuses its own activation records stops its engine
/// over the fault, as Kafka's `QuorumController` gives the failure of its
/// activation to the `fatalFaultHandler`, and it writes nothing after its
/// `LeaderChange` batch. A partition record for a topic that the records do
/// not create is one the leader refuses.
#[test]
fn a_refused_activation_is_a_fatal_fault() {
    let (mut engine, _dir) = build_engine_only(EngineSetup {
        ids: &[NodeId(1)],
        ..Default::default()
    });
    engine.activation = Activation {
        bootstrap_records: vec![
            feature(METADATA_VERSION_FEATURE, LATEST_PRODUCTION_METADATA_VERSION),
            MetadataRecord::V1Partition(crate::test_support::single_replica_partition(
                "missing",
                krabka_ids::PartitionIndex(0),
                NodeId(1),
            )),
        ],
        ..Activation::default()
    };
    let mut faults = engine.fault_tx.subscribe();

    elect(&mut engine, &[NodeId(1)]);
    let stopped = engine.publish_fault();

    let refusal =
        "exception while completing controller activation: metadata: unknown topic 'missing'"
            .to_owned();
    check!(
        (
            stopped,
            engine.activation_fault.clone(),
            faults.borrow_and_update().clone(),
            log_batches(&engine),
            engine.image.finalized_metadata_version(),
        ) == (
            true,
            Some(refusal.clone()),
            Some(refusal),
            vec![Batch::Control],
            None
        )
    );
}
