use {
    super::*,
    crate::{
        codes::{COORDINATOR_LOAD_IN_PROGRESS, NOT_COORDINATOR},
        txn::coordinator::test_support::state_image,
    },
    assert2::assert,
    uuid::Uuid,
};

const THIS_BROKER: NodeId = NodeId(1);
const OTHER_BROKER: NodeId = NodeId(2);
const P0: PartitionIndex = PartitionIndex(0);

/// An image with no `__transaction_state` topic, as the image before the
/// topic was created is.
fn image_without_topic() -> MetadataImage { MetadataImage::new(Uuid::nil()) }

fn image(topic_id: Uuid, leader: NodeId, leader_epoch: LeaderEpoch) -> MetadataImage {
    state_image()
        .topic_id(topic_id)
        .leader(leader)
        .leader_epoch(leader_epoch)
        .replicas(&[leader])
        .call()
}

#[derive(Clone, Copy)]
struct LoadGeneration(u64);

/// The term of a leadership: the generation of its load and the load's status.
#[derive(Clone, Copy)]
struct TermSpec {
    generation: LoadGeneration,
    status: LoadStatus,
}

/// A partition to load, with the generation of its new load.
#[derive(Clone, Copy)]
struct LoadSpec {
    partition: PartitionIndex,
    generation: LoadGeneration,
}

/// A step where this broker is elected for partition 0 of topic `topic` at
/// `leader_epoch`, unloads `unload`, and starts load `generation`.
fn elected_and_loading(
    name: &'static str,
    topic: u128,
    leader_epoch: i32,
    unload: &[PartitionIndex],
    generation: u64,
) -> Step {
    Step {
        name,
        image: image(
            Uuid::from_u128(topic),
            THIS_BROKER,
            LeaderEpoch(leader_epoch),
        ),
        changes: changes(unload, &[LoadSpec {
            partition: PartitionIndex(0),
            generation: LoadGeneration(generation),
        }]),
        after: Some(leadership(
            Uuid::from_u128(topic),
            LeaderEpoch(leader_epoch),
            Some(TermSpec {
                generation: LoadGeneration(generation),
                status: LoadStatus::Loading,
            }),
        )),
        ..Default::default()
    }
}

#[derive(Clone, Copy, Default)]
enum LocalLog {
    #[default]
    Open,
    Unopened,
}

fn leadership(
    topic_id: Uuid,
    leader_epoch: LeaderEpoch,
    term: Option<TermSpec>,
) -> StatePartitionLeadership {
    StatePartitionLeadership {
        topic_id,
        leader_epoch,
        term: term.map(|TermSpec { generation, status }| LeaderTerm {
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
            Some(TermSpec {
                generation: LoadGeneration(1),
                status: LoadStatus::Pending,
            }),
        )),
        ..Default::default()
    }
}

fn changes(unload: &[PartitionIndex], load: &[LoadSpec]) -> LeadershipChanges {
    LeadershipChanges {
        unload: unload.to_vec(),
        load: load
            .iter()
            .map(
                |&LoadSpec {
                     partition,
                     generation,
                 }| (partition, generation.0),
            )
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
    let loaded_epoch_0 = Some(leadership(
        Uuid::from_u128(1),
        LeaderEpoch(0),
        Some(TermSpec {
            generation: LoadGeneration(1),
            status: LoadStatus::Loaded,
        }),
    ));
    run(vec![
        Step {
            name: "an image before the topic exists",
            ..Default::default()
        },
        Step {
            load_ends: Some(LoadStatus::Loaded),
            ..elected_and_loading("this broker is elected at epoch 0", 1, 0, &[], 1)
        },
        Step {
            name: "the same image again changes nothing",
            image: image(Uuid::from_u128(1), THIS_BROKER, LeaderEpoch(0)),
            after: loaded_epoch_0,
            ..Default::default()
        },
        Step {
            name: "a stale image from before the topic existed (#975)",
            after: loaded_epoch_0,
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
        elected_and_loading("this broker is elected again at epoch 2", 1, 2, &[], 2),
        Step {
            load_ends: Some(LoadStatus::Failed),
            ..elected_and_loading(
                "a new term during a load drops the load and starts another",
                1,
                3,
                &[PartitionIndex(0)],
                3,
            )
        },
        Step {
            name: "a failed load waits for the next election",
            image: image(Uuid::from_u128(1), THIS_BROKER, LeaderEpoch(3)),
            after: Some(leadership(
                Uuid::from_u128(1),
                LeaderEpoch(3),
                Some(TermSpec {
                    generation: LoadGeneration(3),
                    status: LoadStatus::Failed,
                }),
            )),
            ..Default::default()
        },
        Step {
            load_ends: Some(LoadStatus::Loaded),
            ..elected_and_loading(
                "the next election loads again",
                1,
                4,
                &[PartitionIndex(0)],
                4,
            )
        },
        Step {
            name: "a stale image from before the topic existed, while the log is open",
            after: Some(leadership(
                Uuid::from_u128(1),
                LeaderEpoch(4),
                Some(TermSpec {
                    generation: LoadGeneration(4),
                    status: LoadStatus::Loaded,
                }),
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
        elected_and_loading(
            "the topic is created a third time and this broker leads it at epoch 0",
            3,
            0,
            &[],
            5,
        ),
    ]);
}

#[test]
fn an_election_before_the_log_opens_loads_once_it_opens() {
    run(vec![
        unopened_step("elected, the log is not open"),
        unopened_step("still not open"),
        elected_and_loading("the log opened", 1, 0, &[], 2),
    ]);
}

#[test]
fn the_load_status_gives_the_kafka_error_code() {
    let cases = [
        (None, Some(NOT_COORDINATOR)),
        (
            Some(LoadStatus::Pending),
            Some(COORDINATOR_LOAD_IN_PROGRESS),
        ),
        (
            Some(LoadStatus::Loading),
            Some(COORDINATOR_LOAD_IN_PROGRESS),
        ),
        (Some(LoadStatus::Loaded), None),
        (Some(LoadStatus::Failed), Some(NOT_COORDINATOR)),
    ];
    for (status, expected) in cases {
        assert!(coordinator_error(status) == expected, "{status:?}");
    }
}
