//! Tests for KIP-903 broker registration: the broker epoch is the offset the
//! registration commits at, and a re-registration of an unchanged incarnation
//! keeps the epoch it was already assigned.

use assert2::{assert, check};

use super::*;
use crate::kraft::controller::test_support::{
    ControllerSetup, build, submit_change_with_timeout, topic_record,
};

#[test]
fn broker_registration_epoch_is_assigned_from_appended_offset() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();

    let mut rx = super::test_support::submit_on_engine(&mut engine, &topic_record("anchor"));
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));

    let base = engine.log.log_end_offset();
    let reg = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        fenced: false,
        log_dirs: vec![],
        ..new_registration()
    });
    let mut rx = super::test_support::submit_on_engine(&mut engine, &[reg]);

    assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    assert!(engine.image.broker_epoch(NodeId(7)) == Some(base.0));
}

#[test]
fn broker_registration_projection_preserves_existing_epoch() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let registration = BrokerRegistrationRecord {
        fenced: false,
        ..new_registration()
    };
    let mut rx = super::test_support::submit_on_engine(
        &mut engine,
        &[MetadataRecord::V1BrokerRegistration(registration)],
    );
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    let mut projection = engine.image.broker(NodeId(7)).unwrap().clone();
    let assigned_epoch = projection.broker_epoch;
    projection.log_dirs.clear();

    let mut rx = super::test_support::submit_on_engine(
        &mut engine,
        &[MetadataRecord::V1BrokerRegistration(projection)],
    );

    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    let stored = engine.image.broker(NodeId(7)).unwrap();
    assert2::assert!(stored.broker_epoch == assigned_epoch);
    assert2::assert!(stored.log_dirs.is_empty());
}

#[tokio::test]
async fn broker_registration_epoch_equals_commit_offset() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};
    let (ctrl, _dir) = super::test_support::single_voter_leader().await;

    let reg = |id: u64| {
        vec![MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                fenced: false,
                in_controlled_shutdown: false,
                cordoned_log_dirs: None,
                node_id: NodeId(id),
                broker_epoch: -1, // stamped by the leader at append
                incarnation_id: uuid::Uuid::from_u128(u128::from(id)),
                host: "h".into(),
                port: 9092,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            },
        )]
    };

    let base1 = ctrl.quorum_state().await.unwrap().log_end_offset;
    submit_change_with_timeout(&ctrl, reg(7), "first broker registration")
        .await
        .expect("first registration");
    let e1 = ctrl.current_image().broker_epoch(NodeId(7));
    assert2::assert!(e1 == Some(base1));

    let base2 = ctrl.quorum_state().await.unwrap().log_end_offset;
    submit_change_with_timeout(&ctrl, reg(7), "broker re-registration")
        .await
        .expect("re-registration");
    let e2 = ctrl.current_image().broker_epoch(NodeId(7));
    assert2::assert!(e2 == Some(base2));
    assert2::assert!(base2 > base1 && e2 > e1);

    ctrl.shutdown().await;
}

/// A new registration of broker 7 at no epoch yet, for the leader to stamp.
fn new_registration() -> krabka_metadata::BrokerRegistrationRecord {
    krabka_metadata::BrokerRegistrationRecord {
        fenced: true,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
        node_id: NodeId(7),
        broker_epoch: -1,
        incarnation_id: uuid::Uuid::from_u128(7),
        host: "broker-7".into(),
        port: 9092,
        rack: None,
        endpoints: vec![],
        log_dirs: vec![uuid::Uuid::from_u128(0xD1)],
        features: std::collections::BTreeMap::new(),
    }
}

/// Codex review of krabka-io/krabka-broker#1166: a registration change names
/// the epoch of the registration it was built from and applies only while
/// the broker is still registered at that epoch, as Kafka's
/// `ClusterControlManager.replayRegistrationChange` requires of a
/// `BrokerRegistrationChangeRecord`. A change built at epoch E that reaches
/// the leader after the broker registered again at E2 is dropped: it neither
/// overwrites the new registration nor registers the broker at a third epoch.
#[test]
fn a_registration_change_applies_only_at_the_epoch_it_names() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let submit = |engine: &mut Engine, record: BrokerRegistrationRecord| {
        let mut rx = super::test_support::submit_on_engine(
            engine,
            &[MetadataRecord::V1BrokerRegistration(record)],
        );
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
        engine.image.broker(NodeId(7)).cloned()
    };

    let registered = submit(&mut engine, new_registration()).expect("broker 7 registered");
    let unfenced = BrokerRegistrationRecord {
        fenced: false,
        ..registered.clone()
    };
    let after_unfence = submit(&mut engine, unfenced.clone());
    // Built from the image at the first epoch, before the broker restarts.
    let stale_shutdown = BrokerRegistrationRecord {
        in_controlled_shutdown: true,
        ..unfenced.clone()
    };
    let reregistered = submit(&mut engine, new_registration()).expect("broker 7 registered again");
    let after_stale_change = submit(&mut engine, stale_shutdown);

    assert!(after_unfence == Some(unfenced));
    assert!(reregistered.broker_epoch > registered.broker_epoch);
    assert!(
        reregistered
            == BrokerRegistrationRecord {
                broker_epoch: reregistered.broker_epoch,
                ..new_registration()
            }
    );
    assert!(after_stale_change == Some(reregistered));
}

/// Codex review of krabka-io/krabka-broker#1166: the leader decides a
/// registration change against its committed image, so while a new
/// registration of the same broker is appended but uncommitted, a change
/// waits with `UncommittedTail` rather than being appended behind it and
/// overwriting it on replay. Once the registration commits, the stale change
/// is dropped.
#[tokio::test]
async fn a_registration_change_waits_for_an_uncommitted_registration() {
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};

    let (ctrl, _dir) = build(ControllerSetup::default());
    crate::kraft::controller::test_support::elect_leader_with_helper(&ctrl, NodeId(1), NodeId(2))
        .await;
    let commit = || async {
        super::test_support::commit_pending(&ctrl, NodeId(2)).await;
    };
    let register = || {
        let ctrl = ctrl.clone();
        tokio::spawn(async move {
            ctrl.submit_change(vec![MetadataRecord::V1BrokerRegistration(
                new_registration(),
            )])
            .await
        })
    };

    let first = register();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    commit().await;
    first.await.unwrap().unwrap();
    let registered = ctrl.current_image().broker(NodeId(7)).cloned().unwrap();
    let stale_unfence = BrokerRegistrationRecord {
        fenced: false,
        ..registered
    };

    let second = register();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let while_uncommitted = ctrl
        .submit_change(vec![MetadataRecord::V1BrokerRegistration(
            stale_unfence.clone(),
        )])
        .await;
    commit().await;
    second.await.unwrap().unwrap();
    let reregistered = ctrl.current_image().broker(NodeId(7)).cloned().unwrap();
    let once_committed = ctrl
        .submit_change(vec![MetadataRecord::V1BrokerRegistration(stale_unfence)])
        .await;

    assert!(matches!(while_uncommitted, Err(RaftError::UncommittedTail)));
    assert!(once_committed.is_ok());
    assert!(ctrl.current_image().broker(NodeId(7)) == Some(&reregistered));
    assert!(
        reregistered
            == BrokerRegistrationRecord {
                broker_epoch: reregistered.broker_epoch,
                ..new_registration()
            }
    );
    ctrl.shutdown().await;
}

/// Every value the log holds from `start`, decoded with Kafka's metadata
/// record schemas: what `kafka-dump-log --cluster-metadata-decoder` or a JVM
/// controller in a mixed quorum reads.
fn kafka_records_from(
    engine: &Engine,
    start: Offset,
) -> Vec<(krabka_protocol::records::metadata::KraftMetadataRecord, i16)> {
    engine
        .log
        .read_decoded(start, DEFAULT_METADATA_RAFT_FETCH_MAX)
        .expect("read the appended batches")
        .iter()
        .filter(|batch| !batch.attributes.is_control_batch())
        .flat_map(|batch| batch.records.iter())
        .filter_map(|record| record.value.as_ref())
        .map(|value| {
            krabka_protocol::records::metadata::KraftMetadataRecord::decode_value(value)
                .expect("a Kafka metadata record")
        })
        .collect()
}

/// krabka-io/krabka-broker#1009: the controller writes a broker's
/// registration and its fence, unfence, controlled shutdown and
/// unregistration in Kafka's record shapes, as
/// `ClusterControlManager.registerBroker` (`RegisterBrokerRecord`, `Fenced`
/// true), `ReplicationControlManager.handleBrokerUnfenced`,
/// `handleBrokerInControlledShutdown` and `handleBrokerFenced`
/// (`BrokerRegistrationChangeRecord` v0, v1 and v0) and `unregisterBroker`
/// (`UnregisterBrokerRecord` with the registration's epoch) write them. A
/// replica that replays each record arrives at the registration the leader
/// holds.
#[test]
fn registration_transitions_are_written_as_kafkas_records() {
    use krabka_metadata::{
        BrokerRegistrationChangeRecord, FencingChange, MetadataRecord, UnregisterBrokerRecord,
    };
    use krabka_protocol::{
        owned::{
            broker_registration_change_record::BrokerRegistrationChangeRecord as KChange,
            unregister_broker_record::UnregisterBrokerRecord as KUnregister,
        },
        records::metadata::KraftMetadataRecord,
    };

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let submit = |engine: &mut Engine, record: MetadataRecord| {
        let start = engine.log.log_end_offset();
        let mut rx = super::test_support::submit_on_engine(engine, &[record]);
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
        (
            kafka_records_from(engine, start),
            engine.image.broker(NodeId(7)).cloned(),
        )
    };

    let (registered_records, registered) = submit(
        &mut engine,
        MetadataRecord::V1BrokerRegistration(new_registration()),
    );
    let registered = registered.expect("broker 7 registered");
    let epoch = registered.broker_epoch;
    let change = |fenced: FencingChange, in_controlled_shutdown: bool| {
        MetadataRecord::V1BrokerRegistrationChange(BrokerRegistrationChangeRecord {
            fenced,
            in_controlled_shutdown,
            ..BrokerRegistrationChangeRecord::no_change(NodeId(7), epoch)
        })
    };
    let kafka_change = |fenced: i8, in_controlled_shutdown: i8, version: i16| {
        vec![(
            KraftMetadataRecord::BrokerRegistrationChange(KChange {
                broker_id: 7,
                broker_epoch: epoch,
                fenced,
                in_controlled_shutdown,
                ..KChange::default()
            }),
            version,
        )]
    };
    let with = |fenced: bool, in_controlled_shutdown: bool| {
        Some(krabka_metadata::BrokerRegistrationRecord {
            fenced,
            in_controlled_shutdown,
            ..registered.clone()
        })
    };

    let steps = [
        (
            "unfence",
            change(FencingChange::Unfence, false),
            kafka_change(-1, 0, 0),
            with(false, false),
        ),
        (
            "enter controlled shutdown",
            change(FencingChange::None, true),
            kafka_change(0, 1, 1),
            with(false, true),
        ),
        (
            "fence",
            change(FencingChange::Fence, false),
            kafka_change(1, 0, 0),
            with(true, true),
        ),
        (
            "unregister",
            MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
                node_id: NodeId(7),
                broker_epoch: epoch,
            }),
            vec![(
                KraftMetadataRecord::UnregisterBroker(KUnregister {
                    broker_id: 7,
                    broker_epoch: epoch,
                    ..KUnregister::default()
                }),
                0,
            )],
            None,
        ),
    ];

    assert!(let [(KraftMetadataRecord::RegisterBroker(written), _)] = registered_records.as_slice());
    check!(
        (
            written.broker_epoch,
            written.fenced,
            written.in_controlled_shutdown
        ) == (epoch, true, false)
    );
    for (what, record, want_records, want_registration) in steps {
        let (records, registration) = submit(&mut engine, record);
        check!(records == want_records, "{what}");
        check!(registration == want_registration, "{what}");
    }
}

/// krabka-io/krabka-broker#1009: a `BrokerRegistrationChangeRecord` built
/// before the broker registered again names an epoch the broker is no longer
/// registered at. Kafka's `replayRegistrationChange` would refuse it, so the
/// leader drops it instead of appending it: nothing reaches the log, the new
/// registration is untouched, and the batch it travels with still commits.
#[test]
fn a_registration_change_at_a_replaced_epoch_is_dropped() {
    use krabka_metadata::{BrokerRegistrationChangeRecord, FencingChange, MetadataRecord};

    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    let submit = |engine: &mut Engine, records: &[MetadataRecord]| {
        let mut rx = super::test_support::submit_on_engine(engine, records);
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    };
    submit(
        &mut engine,
        &[MetadataRecord::V1BrokerRegistration(new_registration())],
    );
    let first_epoch = engine.image.broker_epoch(NodeId(7)).expect("registered");
    submit(
        &mut engine,
        &[MetadataRecord::V1BrokerRegistration(new_registration())],
    );
    let reregistered = engine.image.broker(NodeId(7)).cloned();
    let start = engine.log.log_end_offset();

    let stale_unfence =
        MetadataRecord::V1BrokerRegistrationChange(BrokerRegistrationChangeRecord {
            fenced: FencingChange::Unfence,
            ..BrokerRegistrationChangeRecord::no_change(NodeId(7), first_epoch)
        });
    let mut batch = topic_record("travels-with-the-stale-change");
    batch.push(stale_unfence);
    submit(&mut engine, &batch);

    // Only the topic's `TopicRecord` (2) and `PartitionRecord` (3) are
    // written, and no `BrokerRegistrationChangeRecord` (17).
    let written: Vec<_> = kafka_records_from(&engine, start)
        .into_iter()
        .map(|(record, _)| record.api_key())
        .collect();
    check!(written == vec![2, 3]);
    check!(
        engine
            .image
            .topic("travels-with-the-stale-change")
            .is_some()
    );
    check!(engine.image.broker(NodeId(7)).cloned() == reregistered);
    check!(reregistered.is_some_and(|registration| registration.fenced));
}

/// krabka-io/krabka-broker#825: a controller that restarts recovers every
/// broker's fence and controlled shutdown from the log, as Kafka replays
/// `RegisterBrokerRecord` and `BrokerRegistrationChangeRecord` into
/// `ClusterControlManager`.
#[test]
fn replay_recovers_fencing_and_controlled_shutdown() {
    use krabka_metadata::{
        BrokerRegistrationChangeRecord, BrokerRegistrationRecord, FencingChange, MetadataImage,
        MetadataRecord,
    };

    let registration = |node: u64| {
        MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: NodeId(node),
            incarnation_id: uuid::Uuid::from_u128(u128::from(node)),
            ..new_registration()
        })
    };
    // Broker 7 unfences, broker 8 unfences and enters controlled shutdown,
    // broker 9 stays fenced.
    let (mut engine, _dir) = super::test_support::single_voter_leader_engine();
    for node in [7, 8, 9] {
        let mut rx = super::test_support::submit_on_engine(&mut engine, &[registration(node)]);
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    }
    let changes = [
        (7, FencingChange::Unfence, false),
        (8, FencingChange::Unfence, false),
        (8, FencingChange::None, true),
    ];
    for (node, fenced, in_controlled_shutdown) in changes {
        let epoch = engine.image.broker_epoch(NodeId(node)).expect("registered");
        let (reply, mut rx) = oneshot::channel();
        engine.on_submit_change(
            &[MetadataRecord::V1BrokerRegistrationChange(
                BrokerRegistrationChangeRecord {
                    fenced,
                    in_controlled_shutdown,
                    ..BrokerRegistrationChangeRecord::no_change(NodeId(node), epoch)
                },
            )],
            reply,
        );
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    }

    let mut recovered = MetadataImage::new(uuid::Uuid::nil());
    crate::kraft::controller::recovery::replay_committed(
        &engine.log,
        &mut recovered,
        Offset(0),
        MetadataRaftFetchMax::default(),
    )
    .expect("replay");

    let registrations = |image: &MetadataImage| {
        let mut brokers: Vec<BrokerRegistrationRecord> = image.brokers().cloned().collect();
        brokers.sort_by_key(|broker| broker.node_id);
        brokers
    };
    let flags: Vec<_> = registrations(&recovered)
        .iter()
        .map(|broker| {
            (
                broker.node_id.0,
                broker.fenced,
                broker.in_controlled_shutdown,
            )
        })
        .collect();
    check!(flags == vec![(7, false, false), (8, false, true), (9, true, false)]);
    check!(registrations(&recovered) == registrations(&engine.image));
}
