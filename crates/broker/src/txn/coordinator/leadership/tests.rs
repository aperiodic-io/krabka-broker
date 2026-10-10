use assert2::assert;
use uuid::Uuid;

use super::*;

const THIS_BROKER: NodeId = NodeId(1);
const OTHER_BROKER: NodeId = NodeId(2);
const P0: PartitionIndex = PartitionIndex(0);

/// An image with no `__transaction_state` topic, as the image before the
/// topic was created is.
fn image_without_topic() -> MetadataImage {
    MetadataImage::new(Uuid::nil())
}

fn image(topic_id: Uuid, leader: NodeId, leader_epoch: LeaderEpoch) -> MetadataImage {
    crate::txn::coordinator::test_support::state_image(
        crate::txn::coordinator::test_support::StateImageSetup {
            topic_id,
            leader,
            leader_epoch,
            replicas: &[leader],
        },
    )
}

#[derive(Clone, Copy)]
struct LoadGeneration(u64);

#[derive(Clone, Copy, Default)]
enum LocalLog {
    #[default]
    Open,
    Unopened,
}

fn leadership(
    topic_id: Uuid,
    leader_epoch: LeaderEpoch,
    term: Option<(LoadGeneration, LoadStatus)>,
) -> StatePartitionLeadership {
    StatePartitionLeadership {
        topic_id,
        leader_epoch,
        term: term.map(|(generation, status)| LeaderTerm {
            generation: generation.0,
            status,
        }),
    }
}

/// One step: the image applied, whether the log is local, the changes it asks
/// for, and the leadership of partition 0 afterwards.
#[derive(krabka_macros::FieldDefaults)]
struct Step {
    #[default("leadership step")]
    name: &'static str,
    #[default(image_without_topic())]
    image: MetadataImage,
    #[default(LocalLog::Open)]
    local: LocalLog,
    #[default(changes(&[], &[]))]
    changes: LeadershipChanges,
    after: Option<StatePartitionLeadership>,
    /// A load that ends before the next step, with its result.
    load_ends: Option<LoadStatus>,
}

/// Election is known, but the local log cannot be replayed until it opens.
fn unopened_step(name: &'static str) -> Step {
    Step {
        name,
        image: image(Uuid::from_u128(1), THIS_BROKER, LeaderEpoch(0)),
        local: LocalLog::Unopened,
        after: Some(leadership(
            Uuid::from_u128(1),
            LeaderEpoch(0),
            Some((LoadGeneration(1), LoadStatus::Pending)),
        )),
        ..Default::default()
    }
}

fn changes(
    unload: &[PartitionIndex],
    load: &[(PartitionIndex, LoadGeneration)],
) -> LeadershipChanges {
    LeadershipChanges {
        unload: unload.to_vec(),
        load: load
            .iter()
            .map(|&(partition, generation)| (partition, generation.0))
            .collect(),
    }
}

fn run(steps: Vec<Step>) {
    let mut known = StatePartitionLeaders::new();
    let mut generation = 0;
    for step in steps {
        let applied = apply_image(
            &mut known,
            THIS_BROKER,
            &step.image,
            |_| matches!(step.local, LocalLog::Open),
            || {
                generation += 1;
                generation
            },
        );
        assert!(applied == step.changes, "{}: changes", step.name);
        assert!(
            known.get(&P0).copied() == step.after,
            "{}: leadership",
            step.name
        );
        if let Some(result) = step.load_ends
            && let Some(term) = known.get_mut(&P0).and_then(|entry| entry.term.as_mut())
        {
            term.status = result;
        }
    }
}

#[test]
fn an_election_loads_and_a_resignation_unloads() {
    run(vec![
        Step {
            name: "an image before the topic exists",
            ..Default::default()
        },
        Step {
            name: "this broker is elected at epoch 0",
            image: image(Uuid::from_u128(1), THIS_BROKER, LeaderEpoch(0)),
            changes: changes(&[], &[(PartitionIndex(0), LoadGeneration(1))]),
            after: Some(leadership(
                Uuid::from_u128(1),
                LeaderEpoch(0),
                Some((LoadGeneration(1), LoadStatus::Loading)),
            )),
            load_ends: Some(LoadStatus::Loaded),
            ..Default::default()
        },
        Step {
            name: "the same image again changes nothing",
            image: image(Uuid::from_u128(1), THIS_BROKER, LeaderEpoch(0)),
            after: Some(leadership(
                Uuid::from_u128(1),
                LeaderEpoch(0),
                Some((LoadGeneration(1), LoadStatus::Loaded)),
            )),
            ..Default::default()
        },
        Step {
            name: "a stale image from before the topic existed (#975)",
            after: Some(leadership(
                Uuid::from_u128(1),
                LeaderEpoch(0),
                Some((LoadGeneration(1), LoadStatus::Loaded)),
            )),
            ..Default::default()
        },
        Step {
            name: "another broker is elected at epoch 1",
            image: image(Uuid::from_u128(1), OTHER_BROKER, LeaderEpoch(1)),
            changes: changes(&[PartitionIndex(0)], &[]),
            after: Some(leadership(Uuid::from_u128(1), LeaderEpoch(1), None)),
            ..Default::default()
        },
        Step {
            name: "a stale image of epoch 0",
            image: image(Uuid::from_u128(1), THIS_BROKER, LeaderEpoch(0)),
            after: Some(leadership(Uuid::from_u128(1), LeaderEpoch(1), None)),
            ..Default::default()
        },
        Step {
            name: "this broker is elected again at epoch 2",
            image: image(Uuid::from_u128(1), THIS_BROKER, LeaderEpoch(2)),
            changes: changes(&[], &[(PartitionIndex(0), LoadGeneration(2))]),
            after: Some(leadership(
                Uuid::from_u128(1),
                LeaderEpoch(2),
                Some((LoadGeneration(2), LoadStatus::Loading)),
            )),
            ..Default::default()
        },
        Step {
            name: "a new term during a load drops the load and starts another",
            image: image(Uuid::from_u128(1), THIS_BROKER, LeaderEpoch(3)),
            changes: changes(
                &[PartitionIndex(0)],
                &[(PartitionIndex(0), LoadGeneration(3))],
            ),
            after: Some(leadership(
                Uuid::from_u128(1),
                LeaderEpoch(3),
                Some((LoadGeneration(3), LoadStatus::Loading)),
            )),
            load_ends: Some(LoadStatus::Failed),
            ..Default::default()
        },
        Step {
            name: "a failed load waits for the next election",
            image: image(Uuid::from_u128(1), THIS_BROKER, LeaderEpoch(3)),
            after: Some(leadership(
                Uuid::from_u128(1),
                LeaderEpoch(3),
                Some((LoadGeneration(3), LoadStatus::Failed)),
            )),
            ..Default::default()
        },
        Step {
            name: "the next election loads again",
            image: image(Uuid::from_u128(1), THIS_BROKER, LeaderEpoch(4)),
            changes: changes(
                &[PartitionIndex(0)],
                &[(PartitionIndex(0), LoadGeneration(4))],
            ),
            after: Some(leadership(
                Uuid::from_u128(1),
                LeaderEpoch(4),
                Some((LoadGeneration(4), LoadStatus::Loading)),
            )),
            load_ends: Some(LoadStatus::Loaded),
            ..Default::default()
        },
        Step {
            name: "a stale image from before the topic existed, while the log is open",
            after: Some(leadership(
                Uuid::from_u128(1),
                LeaderEpoch(4),
                Some((LoadGeneration(4), LoadStatus::Loaded)),
            )),
            ..Default::default()
        },
        Step {
            name: "the topic is deleted and its log is removed",
            local: LocalLog::Unopened,
            changes: changes(&[PartitionIndex(0)], &[]),
            ..Default::default()
        },
        Step {
            name: "the topic is created again and another broker leads it at epoch 0",
            image: image(Uuid::from_u128(2), OTHER_BROKER, LeaderEpoch(0)),
            after: Some(leadership(Uuid::from_u128(2), LeaderEpoch(0), None)),
            ..Default::default()
        },
        Step {
            name: "the topic is created a third time and this broker leads it at epoch 0",
            image: image(Uuid::from_u128(3), THIS_BROKER, LeaderEpoch(0)),
            changes: changes(&[], &[(PartitionIndex(0), LoadGeneration(5))]),
            after: Some(leadership(
                Uuid::from_u128(3),
                LeaderEpoch(0),
                Some((LoadGeneration(5), LoadStatus::Loading)),
            )),
            ..Default::default()
        },
    ]);
}

#[test]
fn an_election_before_the_log_opens_loads_once_it_opens() {
    run(vec![
        unopened_step("elected, the log is not open"),
        unopened_step("still not open"),
        Step {
            name: "the log opened",
            image: image(Uuid::from_u128(1), THIS_BROKER, LeaderEpoch(0)),
            changes: changes(&[], &[(PartitionIndex(0), LoadGeneration(2))]),
            after: Some(leadership(
                Uuid::from_u128(1),
                LeaderEpoch(0),
                Some((LoadGeneration(2), LoadStatus::Loading)),
            )),
            ..Default::default()
        },
    ]);
}

#[test]
fn the_load_status_gives_the_kafka_error_code() {
    let cases = [
        (None, Some(crate::codes::NOT_COORDINATOR)),
        (
            Some(LoadStatus::Pending),
            Some(crate::codes::COORDINATOR_LOAD_IN_PROGRESS),
        ),
        (
            Some(LoadStatus::Loading),
            Some(crate::codes::COORDINATOR_LOAD_IN_PROGRESS),
        ),
        (Some(LoadStatus::Loaded), None),
        (
            Some(LoadStatus::Failed),
            Some(crate::codes::NOT_COORDINATOR),
        ),
    ];
    for (status, expected) in cases {
        assert!(coordinator_error(status) == expected, "{status:?}");
    }
}
