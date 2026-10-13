//! Behaviour tests for [`assign`](super::assign).
//!
//! The rows come from Kafka's `StickyTaskAssignorTest`. Where Kafka accepts
//! several results because it breaks load ties in hash-map order, the row
//! expects the one that this port's tie-break on the process id and the member
//! id gives, and the comment says which of Kafka's results that is.

use {super::*, assert2::assert, std::collections::HashMap};

/// The partitions of one subtopology that a member holds in a role.
struct TaskPartitions {
    subtopology: &'static str,
    partitions: &'static [i32],
}

/// The partitions a member is expected to be assigned in a role.
struct MemberTasks {
    member: &'static str,
    partitions: &'static [i32],
}

/// The offset sum that a member reported for one task.
struct ReportedOffset {
    subtopology: &'static str,
    partition: i32,
    offset_sum: i64,
}

/// A member built from the `TaskPartitions` of each role and its
/// `ReportedOffset`s.
struct M {
    id: &'static str,
    process: &'static str,
    active: Vec<TaskPartitions>,
    standby: Vec<TaskPartitions>,
    warmup: Vec<TaskPartitions>,
    offsets: Vec<ReportedOffset>,
}

const fn task(subtopology: &'static str, partitions: &'static [i32]) -> TaskPartitions {
    TaskPartitions {
        subtopology,
        partitions,
    }
}

const fn member_tasks(member: &'static str, partitions: &'static [i32]) -> MemberTasks {
    MemberTasks { member, partitions }
}

const fn reported(partition: i32, offset_sum: i64) -> ReportedOffset {
    ReportedOffset {
        subtopology: "s",
        partition,
        offset_sum,
    }
}

fn first_two_active() -> Vec<MemberTasks> {
    vec![member_tasks("member1", &[0]), member_tasks("member2", &[1])]
}

fn m(id: &'static str, process: &'static str) -> M {
    M {
        id,
        process,
        active: Vec::new(),
        standby: Vec::new(),
        warmup: Vec::new(),
        offsets: Vec::new(),
    }
}

fn m_active(id: &'static str, process: &'static str, active: Vec<TaskPartitions>) -> M {
    M {
        active,
        ..m(id, process)
    }
}

fn role(tasks: &[TaskPartitions]) -> BTreeMap<String, Vec<i32>> {
    tasks
        .iter()
        .map(|entry| (entry.subtopology.to_owned(), entry.partitions.to_vec()))
        .collect()
}

fn member(spec: &M) -> AssignorMember {
    AssignorMember {
        member_id: spec.id.to_owned(),
        process_id: spec.process.to_owned(),
        current_active: role(&spec.active),
        current_standby: role(&spec.standby),
        current_warmup: role(&spec.warmup),
        task_offsets: spec
            .offsets
            .iter()
            .map(|reported| {
                (
                    (reported.subtopology.to_owned(), reported.partition),
                    reported.offset_sum,
                )
            })
            .collect(),
    }
}

/// `member -> subtopology "s" -> partitions`.
fn roles(entries: &[MemberTasks]) -> HashMap<String, BTreeMap<String, Vec<i32>>> {
    entries
        .iter()
        .map(|entry| {
            (
                entry.member.to_owned(),
                role(&[TaskPartitions {
                    subtopology: "s",
                    partitions: entry.partitions,
                }]),
            )
        })
        .collect()
}

struct Row {
    name: &'static str,
    members: Vec<M>,
    partitions: i32,
    stateful: bool,
    standby_replicas: i32,
    active: Vec<MemberTasks>,
    standby: Vec<MemberTasks>,
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
            active: Vec::new(),
            standby: Vec::new(),
        },
        Row {
            // Kafka's range-like first assignment gives the least loaded
            // process each task in turn.
            name: "empty group, stateless",
            members: vec![m("A", "p1"), m("B", "p2")],
            partitions: 4,
            stateful: false,
            standby_replicas: 0,
            active: vec![member_tasks("A", &[0, 2]), member_tasks("B", &[1, 3])],
            standby: Vec::new(),
        },
        Row {
            // A current owner keeps only its quota; the rest move at once.
            name: "sticky within quota",
            members: vec![
                m_active("A", "p1", vec![task("s", &[0, 1, 2, 3])]),
                m("B", "p2"),
            ],
            partitions: 4,
            stateful: true,
            standby_replicas: 0,
            active: vec![member_tasks("A", &[0, 1]), member_tasks("B", &[2, 3])],
            standby: Vec::new(),
        },
        Row {
            name: "shouldAssignTasksToClientWithPreviousStandbyTasks",
            members: vec![
                M {
                    standby: vec![task("s", &[2])],
                    ..m("member1", "process1")
                },
                M {
                    standby: vec![task("s", &[1])],
                    ..m("member2", "process2")
                },
                M {
                    standby: vec![task("s", &[0])],
                    ..m("member3", "process3")
                },
            ],
            partitions: 3,
            stateful: false,
            standby_replicas: 0,
            active: vec![
                member_tasks("member1", &[2]),
                member_tasks("member2", &[1]),
                member_tasks("member3", &[0]),
            ],
            standby: Vec::new(),
        },
        Row {
            name: "shouldAssignTasksToClientWithPreviousWarmupTasks",
            members: vec![
                m_active("member1", "process1", vec![task("s", &[0, 1, 2])]),
                m_active("member2", "process2", vec![task("s", &[3, 4, 5])]),
                M {
                    active: vec![task("s", &[6, 7])],
                    warmup: vec![task("s", &[8])],
                    ..m("member3", "process3")
                },
                m_active("member4", "process4", vec![task("s", &[9])]),
            ],
            partitions: 12,
            stateful: true,
            standby_replicas: 0,
            active: vec![
                member_tasks("member1", &[0, 1, 2]),
                member_tasks("member2", &[3, 4, 5]),
                member_tasks("member3", &[6, 7, 8]),
                member_tasks("member4", &[9, 10, 11]),
            ],
            standby: Vec::new(),
        },
    ]
}

fn first_two_active_members() -> Vec<M> {
    vec![
        m_active("member1", "process1", vec![task("s", &[0])]),
        m_active("member2", "process2", vec![task("s", &[1])]),
    ]
}

/// Active rows about stateful placement.
fn active_rows_stateful() -> Vec<Row> {
    vec![
        Row {
            name: "shouldAssignStandbyTaskToClientWithPreviousWarmupTaskOverLessLoadedClient",
            members: {
                let mut members = first_two_active_members();
                members.extend([
                    M {
                        active: vec![task("s", &[2])],
                        warmup: vec![task("s", &[1])],
                        ..m("member3", "process3")
                    },
                    m("member4", "process4"),
                ]);
                members
            },
            partitions: 3,
            stateful: true,
            standby_replicas: 1,
            active: vec![
                member_tasks("member1", &[0]),
                member_tasks("member2", &[1]),
                member_tasks("member3", &[2]),
            ],
            standby: vec![
                member_tasks("member2", &[0]),
                member_tasks("member3", &[1]),
                member_tasks("member4", &[2]),
            ],
        },
        Row {
            name: "shouldPreferMoreCaughtUpCandidateRegardlessOfPrevStandbyOrPrevWarmupRole",
            members: {
                let mut members = first_two_active_members();
                members.extend([
                    M {
                        standby: vec![task("s", &[0])],
                        offsets: vec![reported(0, 100)],
                        ..m("member3", "process3")
                    },
                    M {
                        warmup: vec![task("s", &[0])],
                        offsets: vec![reported(0, 10)],
                        ..m("member4", "process4")
                    },
                    M {
                        standby: vec![task("s", &[1])],
                        offsets: vec![reported(1, 10)],
                        ..m("member5", "process5")
                    },
                    M {
                        warmup: vec![task("s", &[1])],
                        offsets: vec![reported(1, 100)],
                        ..m("member6", "process6")
                    },
                ]);
                members
            },
            partitions: 2,
            stateful: true,
            standby_replicas: 1,
            active: first_two_active(),
            standby: vec![member_tasks("member3", &[0]), member_tasks("member6", &[1])],
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
                    standby: vec![task("s", &[2])],
                    ..m("member1", "process1")
                },
                M {
                    active: vec![task("s", &[0])],
                    standby: vec![task("s", &[2])],
                    ..m("member2", "process2")
                },
                m_active("member3", "process3", vec![task("s", &[1])]),
                m("member4", "process4"),
            ],
            partitions: 3,
            stateful: true,
            standby_replicas: 1,
            active: vec![
                member_tasks("member1", &[2]),
                member_tasks("member2", &[0]),
                member_tasks("member3", &[1]),
            ],
            standby: vec![
                member_tasks("member1", &[0]),
                member_tasks("member2", &[2]),
                member_tasks("member4", &[1]),
            ],
        },
        Row {
            name: "shouldRankCurrentOwnersAheadOfReportedTaskOffsetsAndMostCaughtUpFirst",
            members: vec![
                m_active("member1", "process1", vec![task("s", &[0])]),
                M {
                    standby: vec![task("s", &[0, 1])],
                    ..m("member2", "process2")
                },
                M {
                    offsets: vec![
                        ReportedOffset {
                            subtopology: "s",
                            partition: 0,
                            offset_sum: 1_000_000,
                        },
                        ReportedOffset {
                            subtopology: "s",
                            partition: 1,
                            offset_sum: 1_000_000,
                        },
                        reported(2, 100),
                    ],
                    ..m("member3", "process3")
                },
                M {
                    offsets: vec![reported(2, 50)],
                    ..m("member4", "process4")
                },
            ],
            partitions: 4,
            stateful: true,
            standby_replicas: 0,
            active: vec![
                member_tasks("member1", &[0]),
                member_tasks("member2", &[1]),
                member_tasks("member3", &[2]),
                member_tasks("member4", &[3]),
            ],
            standby: Vec::new(),
        },
        Row {
            name: "shouldAssignStandbysAwayFromProcessHoldingStateOnColdStart",
            members: vec![
                M {
                    offsets: vec![reported(0, 100)],
                    ..m("member1", "process1")
                },
                M {
                    offsets: vec![reported(1, 100)],
                    ..m("member2", "process2")
                },
            ],
            partitions: 2,
            stateful: true,
            standby_replicas: 1,
            active: first_two_active(),
            standby: vec![member_tasks("member1", &[1]), member_tasks("member2", &[0])],
        },
    ]
}

/// Standby rows about cold starts.
fn standby_rows_cold_start() -> Vec<Row> {
    vec![
        Row {
            name: "shouldAssignTasksToProcessesReportingTaskOffsetsOnColdStartWithMultipleMembersPerProcess",
            members: vec![
                M {
                    offsets: vec![reported(0, 100), reported(1, 100)],
                    ..m("member1_1", "process1")
                },
                M {
                    offsets: vec![reported(0, 100), reported(1, 100)],
                    ..m("member1_2", "process1")
                },
                M {
                    offsets: vec![reported(2, 100), reported(3, 100)],
                    ..m("member2_1", "process2")
                },
                M {
                    offsets: vec![reported(2, 100), reported(3, 100)],
                    ..m("member2_2", "process2")
                },
            ],
            partitions: 4,
            stateful: true,
            standby_replicas: 0,
            active: vec![
                member_tasks("member1_1", &[0]),
                member_tasks("member1_2", &[1]),
                member_tasks("member2_1", &[2]),
                member_tasks("member2_2", &[3]),
            ],
            standby: Vec::new(),
        },
        Row {
            // The issue's first case: the sticky assignor places standbys.
            name: "empty group, stateful, one standby",
            members: vec![m("A", "p1"), m("B", "p2")],
            partitions: 2,
            stateful: true,
            standby_replicas: 1,
            active: vec![member_tasks("A", &[0]), member_tasks("B", &[1])],
            standby: vec![member_tasks("A", &[1]), member_tasks("B", &[0])],
        },
        Row {
            // The owner of task 0 left; m2 held its standby and takes it.
            name: "orphan active goes to its previous standby",
            members: vec![
                M {
                    active: vec![task("s", &[1])],
                    standby: vec![task("s", &[0])],
                    ..m("m2", "p2")
                },
                M {
                    active: vec![task("s", &[2])],
                    standby: vec![task("s", &[1])],
                    ..m("m3", "p3")
                },
            ],
            partitions: 3,
            stateful: true,
            standby_replicas: 1,
            active: vec![member_tasks("m2", &[0, 1]), member_tasks("m3", &[2])],
            standby: vec![member_tasks("m2", &[2]), member_tasks("m3", &[0, 1])],
        },
        Row {
            // Two members of one process: a standby never shares a process
            // with its active copy, so no standby can be placed.
            name: "one process gets no standby",
            members: vec![m("A", "p1"), m("B", "p1")],
            partitions: 2,
            stateful: true,
            standby_replicas: 1,
            active: vec![member_tasks("A", &[0]), member_tasks("B", &[1])],
            standby: Vec::new(),
        },
    ]
}

#[test]
fn assign_matches_kafka_sticky_task_assignor() {
    let rows = active_rows()
        .into_iter()
        .chain(active_rows_stateful())
        .chain(standby_rows())
        .chain(standby_rows_cold_start());
    for row in rows {
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
            active: roles(&row.active),
            standby: roles(&row.standby),
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
        member(&m_active("member1", "process1", vec![
            task("s1", &[0, 1]),
            task("s2", &[0, 1]),
        ])),
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
                    "member1".to_owned() => role(&[TaskPartitions { subtopology: "s1", partitions: &[0] }, TaskPartitions { subtopology: "s2", partitions: &[0] }]),
                    "member2".to_owned() => role(&[TaskPartitions { subtopology: "s1", partitions: &[1] }, TaskPartitions { subtopology: "s2", partitions: &[1] }]),
                },
                standby: maplit::hashmap! {
                    "member1".to_owned() => role(&[TaskPartitions { subtopology: "s1", partitions: &[1] }, TaskPartitions { subtopology: "s2", partitions: &[1] }]),
                    "member2".to_owned() => role(&[TaskPartitions { subtopology: "s1", partitions: &[0] }, TaskPartitions { subtopology: "s2", partitions: &[0] }]),
                },
            }
    );
}
