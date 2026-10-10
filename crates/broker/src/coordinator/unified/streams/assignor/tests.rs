//! Behaviour tests for [`assign`](super::assign).
//!
//! The rows come from Kafka's `StickyTaskAssignorTest`. Where Kafka accepts
//! several results because it breaks load ties in hash-map order, the row
//! expects the one that this port's tie-break on the process id and the member
//! id gives, and the comment says which of Kafka's results that is.

use std::collections::HashMap;

use assert2::assert;

use super::*;

/// A member built from `(subtopology, partitions)` lists for each role, and
/// `(subtopology, partition, offset sum)` reported offsets.
struct M {
    id: &'static str,
    process: &'static str,
    active: &'static [(&'static str, &'static [i32])],
    standby: &'static [(&'static str, &'static [i32])],
    warmup: &'static [(&'static str, &'static [i32])],
    offsets: &'static [(&'static str, i32, i64)],
}

const fn m(id: &'static str, process: &'static str) -> M {
    M {
        id,
        process,
        active: &[],
        standby: &[],
        warmup: &[],
        offsets: &[],
    }
}

const fn m_active(
    id: &'static str,
    process: &'static str,
    active: &'static [(&'static str, &'static [i32])],
) -> M {
    M {
        active,
        ..m(id, process)
    }
}

fn role(tasks: &[(&str, &[i32])]) -> BTreeMap<String, Vec<i32>> {
    tasks
        .iter()
        .map(|(sub, parts)| ((*sub).to_owned(), parts.to_vec()))
        .collect()
}

fn member(spec: &M) -> AssignorMember {
    AssignorMember {
        member_id: spec.id.to_owned(),
        process_id: spec.process.to_owned(),
        current_active: role(spec.active),
        current_standby: role(spec.standby),
        current_warmup: role(spec.warmup),
        task_offsets: spec
            .offsets
            .iter()
            .map(|(sub, part, sum)| (((*sub).to_owned(), *part), *sum))
            .collect(),
    }
}

/// `member -> subtopology "s" -> partitions`.
fn roles(entries: &[(&str, &[i32])]) -> HashMap<String, BTreeMap<String, Vec<i32>>> {
    entries
        .iter()
        .map(|(member, parts)| ((*member).to_owned(), role(&[("s", parts)])))
        .collect()
}

struct Row {
    name: &'static str,
    members: Vec<M>,
    partitions: i32,
    stateful: bool,
    standby_replicas: i32,
    active: &'static [(&'static str, &'static [i32])],
    standby: &'static [(&'static str, &'static [i32])],
}

/// Rows about active placement.
fn active_rows() -> Vec<Row> {
    vec![
        Row {
            name: "no members",
            members: vec![],
            partitions: 2,
            stateful: false,
            standby_replicas: 0,
            active: &[],
            standby: &[],
        },
        Row {
            // Kafka's range-like first assignment gives the least loaded
            // process each task in turn.
            name: "empty group, stateless",
            members: vec![m("A", "p1"), m("B", "p2")],
            partitions: 4,
            stateful: false,
            standby_replicas: 0,
            active: &[("A", &[0, 2]), ("B", &[1, 3])],
            standby: &[],
        },
        Row {
            // A current owner keeps only its quota; the rest move at once.
            name: "sticky within quota",
            members: vec![m_active("A", "p1", &[("s", &[0, 1, 2, 3])]), m("B", "p2")],
            partitions: 4,
            stateful: true,
            standby_replicas: 0,
            active: &[("A", &[0, 1]), ("B", &[2, 3])],
            standby: &[],
        },
        Row {
            name: "shouldAssignTasksToClientWithPreviousStandbyTasks",
            members: vec![
                M {
                    standby: &[("s", &[2])],
                    ..m("member1", "process1")
                },
                M {
                    standby: &[("s", &[1])],
                    ..m("member2", "process2")
                },
                M {
                    standby: &[("s", &[0])],
                    ..m("member3", "process3")
                },
            ],
            partitions: 3,
            stateful: false,
            standby_replicas: 0,
            active: &[("member1", &[2]), ("member2", &[1]), ("member3", &[0])],
            standby: &[],
        },
        Row {
            name: "shouldAssignTasksToClientWithPreviousWarmupTasks",
            members: vec![
                m_active("member1", "process1", &[("s", &[0, 1, 2])]),
                m_active("member2", "process2", &[("s", &[3, 4, 5])]),
                M {
                    active: &[("s", &[6, 7])],
                    warmup: &[("s", &[8])],
                    ..m("member3", "process3")
                },
                m_active("member4", "process4", &[("s", &[9])]),
            ],
            partitions: 12,
            stateful: true,
            standby_replicas: 0,
            active: &[
                ("member1", &[0, 1, 2]),
                ("member2", &[3, 4, 5]),
                ("member3", &[6, 7, 8]),
                ("member4", &[9, 10, 11]),
            ],
            standby: &[],
        },
        Row {
            name: "shouldAssignStandbyTaskToClientWithPreviousWarmupTaskOverLessLoadedClient",
            members: vec![
                m_active("member1", "process1", &[("s", &[0])]),
                m_active("member2", "process2", &[("s", &[1])]),
                M {
                    active: &[("s", &[2])],
                    warmup: &[("s", &[1])],
                    ..m("member3", "process3")
                },
                m("member4", "process4"),
            ],
            partitions: 3,
            stateful: true,
            standby_replicas: 1,
            active: &[("member1", &[0]), ("member2", &[1]), ("member3", &[2])],
            standby: &[("member2", &[0]), ("member3", &[1]), ("member4", &[2])],
        },
        Row {
            name: "shouldPreferMoreCaughtUpCandidateRegardlessOfPrevStandbyOrPrevWarmupRole",
            members: vec![
                m_active("member1", "process1", &[("s", &[0])]),
                m_active("member2", "process2", &[("s", &[1])]),
                M {
                    standby: &[("s", &[0])],
                    offsets: &[("s", 0, 100)],
                    ..m("member3", "process3")
                },
                M {
                    warmup: &[("s", &[0])],
                    offsets: &[("s", 0, 10)],
                    ..m("member4", "process4")
                },
                M {
                    standby: &[("s", &[1])],
                    offsets: &[("s", 1, 10)],
                    ..m("member5", "process5")
                },
                M {
                    warmup: &[("s", &[1])],
                    offsets: &[("s", 1, 100)],
                    ..m("member6", "process6")
                },
            ],
            partitions: 2,
            stateful: true,
            standby_replicas: 1,
            active: &[("member1", &[0]), ("member2", &[1])],
            standby: &[("member3", &[0]), ("member6", &[1])],
        },
    ]
}

/// Rows about standby placement and cold starts.
fn standby_rows() -> Vec<Row> {
    vec![
        Row {
            // Kafka's third accepted result.
            name: "shouldAssignStandbyToPreviousStandbyThatDoesNotHoldTheActiveTask",
            members: vec![
                M {
                    standby: &[("s", &[2])],
                    ..m("member1", "process1")
                },
                M {
                    active: &[("s", &[0])],
                    standby: &[("s", &[2])],
                    ..m("member2", "process2")
                },
                m_active("member3", "process3", &[("s", &[1])]),
                m("member4", "process4"),
            ],
            partitions: 3,
            stateful: true,
            standby_replicas: 1,
            active: &[("member1", &[2]), ("member2", &[0]), ("member3", &[1])],
            standby: &[("member1", &[0]), ("member2", &[2]), ("member4", &[1])],
        },
        Row {
            name: "shouldRankCurrentOwnersAheadOfReportedTaskOffsetsAndMostCaughtUpFirst",
            members: vec![
                m_active("member1", "process1", &[("s", &[0])]),
                M {
                    standby: &[("s", &[0, 1])],
                    ..m("member2", "process2")
                },
                M {
                    offsets: &[("s", 0, 1_000_000), ("s", 1, 1_000_000), ("s", 2, 100)],
                    ..m("member3", "process3")
                },
                M {
                    offsets: &[("s", 2, 50)],
                    ..m("member4", "process4")
                },
            ],
            partitions: 4,
            stateful: true,
            standby_replicas: 0,
            active: &[
                ("member1", &[0]),
                ("member2", &[1]),
                ("member3", &[2]),
                ("member4", &[3]),
            ],
            standby: &[],
        },
        Row {
            name: "shouldAssignStandbysAwayFromProcessHoldingStateOnColdStart",
            members: vec![
                M {
                    offsets: &[("s", 0, 100)],
                    ..m("member1", "process1")
                },
                M {
                    offsets: &[("s", 1, 100)],
                    ..m("member2", "process2")
                },
            ],
            partitions: 2,
            stateful: true,
            standby_replicas: 1,
            active: &[("member1", &[0]), ("member2", &[1])],
            standby: &[("member1", &[1]), ("member2", &[0])],
        },
        Row {
            name: "shouldAssignTasksToProcessesReportingTaskOffsetsOnColdStartWithMultipleMembersPerProcess",
            members: vec![
                M {
                    offsets: &[("s", 0, 100), ("s", 1, 100)],
                    ..m("member1_1", "process1")
                },
                M {
                    offsets: &[("s", 0, 100), ("s", 1, 100)],
                    ..m("member1_2", "process1")
                },
                M {
                    offsets: &[("s", 2, 100), ("s", 3, 100)],
                    ..m("member2_1", "process2")
                },
                M {
                    offsets: &[("s", 2, 100), ("s", 3, 100)],
                    ..m("member2_2", "process2")
                },
            ],
            partitions: 4,
            stateful: true,
            standby_replicas: 0,
            active: &[
                ("member1_1", &[0]),
                ("member1_2", &[1]),
                ("member2_1", &[2]),
                ("member2_2", &[3]),
            ],
            standby: &[],
        },
        Row {
            // The issue's first case: the sticky assignor places standbys.
            name: "empty group, stateful, one standby",
            members: vec![m("A", "p1"), m("B", "p2")],
            partitions: 2,
            stateful: true,
            standby_replicas: 1,
            active: &[("A", &[0]), ("B", &[1])],
            standby: &[("A", &[1]), ("B", &[0])],
        },
        Row {
            // The owner of task 0 left; m2 held its standby and takes it.
            name: "orphan active goes to its previous standby",
            members: vec![
                M {
                    active: &[("s", &[1])],
                    standby: &[("s", &[0])],
                    ..m("m2", "p2")
                },
                M {
                    active: &[("s", &[2])],
                    standby: &[("s", &[1])],
                    ..m("m3", "p3")
                },
            ],
            partitions: 3,
            stateful: true,
            standby_replicas: 1,
            active: &[("m2", &[0, 1]), ("m3", &[2])],
            standby: &[("m2", &[2]), ("m3", &[0, 1])],
        },
        Row {
            // Two members of one process: a standby never shares a process
            // with its active copy, so no standby can be placed.
            name: "one process gets no standby",
            members: vec![m("A", "p1"), m("B", "p1")],
            partitions: 2,
            stateful: true,
            standby_replicas: 1,
            active: &[("A", &[0]), ("B", &[1])],
            standby: &[],
        },
    ]
}

#[test]
fn assign_matches_kafka_sticky_task_assignor() {
    for row in active_rows().into_iter().chain(standby_rows()) {
        let members: Vec<AssignorMember> = row.members.iter().map(member).collect();
        let input = AssignorInput {
            tasks: maplit::btreemap! {"s".to_owned() => (0..row.partitions).collect()},
            stateful: if row.stateful {
                maplit::btreeset! {"s".to_owned()}
            } else {
                BTreeSet::new()
            },
            num_standby_replicas: row.standby_replicas,
        };
        let want = StreamsAssignment {
            active: roles(row.active),
            standby: roles(row.standby),
        };
        let got = assign(&members, &input);
        assert!(got == want, "{}", row.name);
        // The same inputs in another order give the same assignment.
        let reversed: Vec<AssignorMember> = members.iter().rev().cloned().collect();
        assert!(assign(&reversed, &input) == want, "{} (reversed)", row.name);
    }
}

#[test]
fn range_assigns_each_member_a_task_of_every_subtopology() {
    // Kafka's `shouldRangeAssignTasksWhenScalingUp`: member2 is new, and each
    // member ends with one active and one standby task of each subtopology.
    let members = [
        member(&m_active(
            "member1",
            "process1",
            &[("s1", &[0, 1]), ("s2", &[0, 1])],
        )),
        member(&m("member2", "process2")),
    ];
    let input = AssignorInput {
        tasks: maplit::btreemap! {
            "s1".to_owned() => vec![0, 1],
            "s2".to_owned() => vec![0, 1],
        },
        stateful: maplit::btreeset! {"s1".to_owned(), "s2".to_owned()},
        num_standby_replicas: 1,
    };
    assert!(
        assign(&members, &input)
            == StreamsAssignment {
                active: maplit::hashmap! {
                    "member1".to_owned() => role(&[("s1", &[0]), ("s2", &[0])]),
                    "member2".to_owned() => role(&[("s1", &[1]), ("s2", &[1])]),
                },
                standby: maplit::hashmap! {
                    "member1".to_owned() => role(&[("s1", &[1]), ("s2", &[1])]),
                    "member2".to_owned() => role(&[("s1", &[0]), ("s2", &[0])]),
                },
            }
    );
}
