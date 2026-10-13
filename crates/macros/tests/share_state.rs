//! Share-state schemas retain native operations, concurrent row order and commit boundaries.

use {
    assert2::assert,
    futures_util::future::poll_fn,
    krabka_macros::{
        share_fixture_schemas, share_partition_results, share_state_operation, share_state_request,
        share_state_results,
    },
    krabka_protocol::{
        owned::{
            initialize_share_group_state_request::{
                InitializeShareGroupStateRequest, InitializeStateData, PartitionData,
            },
            initialize_share_group_state_response::{
                InitializeShareGroupStateResponse, InitializeStateResult,
                PartitionResult as InitializationResult,
            },
            read_share_group_state_response::{
                PartitionResult as ReadingResult, ReadShareGroupStateResponse, ReadStateResult,
            },
        },
        primitives::uuid::Uuid as ProtoUuid,
        tagged_fields::UnknownTaggedFields,
    },
    std::{
        cell::{Cell, RefCell},
        task::{Poll, Waker},
        time::Duration,
    },
};

// The generated protocol conversion needs only these two UUID operations.
mod uuid {
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct Uuid {
        pub bytes: [u8; 16],
    }

    impl Uuid {
        pub const fn from_bytes(bytes: [u8; 16]) -> Self { Self { bytes } }

        pub const fn as_bytes(&self) -> &[u8; 16] { &self.bytes }
    }
}

#[test]
fn singleton_and_row_requests_evaluate_values_once_and_keep_native_patterns() {
    #[derive(Clone, Copy)]
    struct Fields {
        partition: i32,
        state_epoch: i32,
    }
    let observations = RefCell::new(Vec::new());
    let fields = [
        Fields {
            partition: 2,
            state_epoch: 3,
        },
        Fields {
            partition: 4,
            state_epoch: 5,
        },
    ];
    let request = share_state_request! {
        InitializeShareGroupStateRequest, InitializeStateData, PartitionData;
        group [{ observations.borrow_mut().push("group"); "borrowed" }];
        topic [{ observations.borrow_mut().push("topic"); uuid::Uuid { bytes: [7; 16] } }];
        fields {
            partition: { observations.borrow_mut().push("partition"); fields.iter().map(|&Fields { partition, state_epoch }| partition + state_epoch).sum() },
            state_epoch: 12,
            start_offset: 91,
        };
    };
    assert!(*observations.borrow() == ["group", "topic", "partition"]);
    assert!(request.group_id == "borrowed");
    assert!(request.topics[0].topic_id == ProtoUuid([7; 16]));
    assert!(request.topics[0].partitions[0].partition == 14);
    assert!(request.topics[0].partitions[0].state_epoch == 12);
    assert!(request.topics[0].partitions[0].start_offset == 91);
    assert!(request.unknown_tagged_fields.0.is_empty());
    let request = share_state_request! {
        InitializeShareGroupStateRequest, InitializeStateData;
        group ["rows"];
        topic [ProtoUuid([8; 16])];
        partitions [fields.iter().map(|&Fields { partition, state_epoch }| PartitionData {
            partition, state_epoch, ..Default::default()
        }).collect()];
    };
    assert!(
        request.topics[0]
            .partitions
            .iter()
            .map(|p| p.partition)
            .collect::<Vec<_>>()
            == [2, 4]
    );
}

trait StateReply: Sized {
    type Partition;
    type Topic;
    fn topic(id: ProtoUuid, rows: Vec<Self::Partition>) -> Self::Topic;
    fn from_results(topics: Vec<Self::Topic>) -> Self;
    fn error_row(index: i32, code: i16, text: Option<&str>) -> Self::Partition;
}

share_fixture_schemas! {
    InitializeShareGroupStateResponse => InitializeStateResult, InitializationResult;
    ReadShareGroupStateResponse => ReadStateResult, ReadingResult;
}

#[derive(Debug, PartialEq)]
struct PartitionRow {
    partition: i32,
    error_code: i16,
    error_message: Option<String>,
}

trait PartitionResults {
    fn partition_results(self) -> Vec<PartitionRow>;
}

share_partition_results!(
    InitializeShareGroupStateResponse,
    ReadShareGroupStateResponse
);

#[test]
fn external_schemas_own_messages_and_flatten_in_topic_then_partition_order() {
    let message = String::from("refused");
    let first = InitializeShareGroupStateResponse::error_row(3, 16, Some(&message));
    drop(message);
    let response = InitializeShareGroupStateResponse::from_results(vec![
        InitializeShareGroupStateResponse::topic(ProtoUuid([1; 16]), vec![first]),
        InitializeShareGroupStateResponse::topic(ProtoUuid([2; 16]), vec![
            InitializeShareGroupStateResponse::error_row(1, 0, None),
        ]),
    ]);
    let rows = response.partition_results();
    assert!(
        rows[0]
            == PartitionRow {
                partition: 3,
                error_code: 16,
                error_message: Some("refused".into())
            }
    );
    assert!(
        rows[1]
            == PartitionRow {
                partition: 1,
                error_code: 0,
                error_message: None
            }
    );
    let read = ReadShareGroupStateResponse::from_results(vec![]);
    assert!(read.partition_results().is_empty());
}

#[derive(Debug, Default)]
struct TopicResult {
    topic_id: ProtoUuid,
    partitions: Vec<String>,
    unknown_tagged_fields: UnknownTaggedFields,
}

#[tokio::test]
async fn all_partitions_start_concurrently_and_results_keep_request_order() {
    let req = InitializeShareGroupStateRequest {
        group_id: "ordered".into(),
        topics: [9, 4]
            .map(|id| InitializeStateData {
                topic_id: ProtoUuid([id; 16]),
                partitions: [3, 1]
                    .map(|partition| PartitionData {
                        partition,
                        ..Default::default()
                    })
                    .to_vec(),
                ..Default::default()
            })
            .to_vec(),
        ..Default::default()
    };
    let arrivals = &Cell::new(0);
    let waiters = &RefCell::<Vec<Waker>>::new(Vec::new());
    let operation = async {
        share_state_results!(req, TopicResult, |group, topic, row| {
            let mut arrived = false;
            poll_fn(|cx| {
                if !arrived {
                    arrivals.set(arrivals.get() + 1);
                    arrived = true;
                    for waiter in waiters.borrow_mut().drain(..) {
                        waiter.wake();
                    }
                }
                if arrivals.get() == 4 {
                    Poll::Ready(())
                } else {
                    waiters.borrow_mut().push(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await;
            format!("{group}:{}:{}", topic.bytes[0], row.partition)
        })
    };
    let results = tokio::time::timeout(Duration::from_secs(1), operation)
        .await
        .expect("all four rows must reach the barrier");
    assert!(arrivals.get() == 4);
    assert!(results[0].partitions == ["ordered:9:3", "ordered:9:1"]);
    assert!(results[1].partitions == ["ordered:4:3", "ordered:4:1"]);
    assert!(results[0].topic_id == ProtoUuid([9; 16]));
    assert!(results[1].topic_id == ProtoUuid([4; 16]));
    assert!(
        results
            .iter()
            .all(|topic| topic.unknown_tagged_fields.0.is_empty())
    );
}

#[derive(Debug, PartialEq)]
enum ShareStateError {
    Inactive,
    Refused,
    Commit,
}

impl ShareStateError {
    const fn inactive((): ()) -> Self { Self::Inactive }
}

struct AdmissionDomain {
    admitted: bool,
    committed: bool,
    events: RefCell<Vec<&'static str>>,
}

struct Active<'a> {
    domain: &'a AdmissionDomain,
}

impl Drop for Active<'_> {
    fn drop(&mut self) { self.domain.events.borrow_mut().push("release"); }
}

impl AdmissionDomain {
    fn state_partition_for(&self, group: &str, topic: &[u8; 16], partition: i32) -> usize {
        assert!(group == "group" && topic[0] == 1 && partition == 2);
        self.events.borrow_mut().push("key");
        0
    }

    async fn active(&self, partition: usize) -> Result<Active<'_>, ()> {
        assert!(partition == 0);
        self.events.borrow_mut().push("acquire");
        tokio::task::yield_now().await;
        if self.admitted {
            Ok(Active { domain: self })
        } else {
            Err(())
        }
    }

    async fn answer<T>(
        &self,
        guard: Active<'_>,
        result: Result<T, ShareStateError>,
    ) -> Result<T, ShareStateError> {
        guard.domain.events.borrow_mut().push("answer");
        tokio::task::yield_now().await;
        let answer = if self.committed {
            result
        } else {
            Err(ShareStateError::Commit)
        };
        drop(guard);
        answer
    }

    #[share_state_operation(active)]
    async fn serve(
        &self,
        image: &str,
        group: &str,
        topic: [u8; 16],
        partition: i32,
    ) -> Result<usize, ShareStateError> {
        active.domain.events.borrow_mut().push("operation");
        tokio::task::yield_now().await;
        if image == "refuse" {
            Err(ShareStateError::Refused)
        } else {
            Ok(image.len())
        }
    }
}

#[tokio::test]
async fn admission_and_commit_errors_keep_the_guard_and_original_operation_boundary() {
    struct Case {
        admitted: bool,
        committed: bool,
        image: &'static str,
        expected: Result<usize, ShareStateError>,
    }
    for case in [
        Case {
            admitted: false,
            committed: true,
            image: "value",
            expected: Err(ShareStateError::Inactive),
        },
        Case {
            admitted: true,
            committed: true,
            image: "value",
            expected: Ok(5),
        },
        Case {
            admitted: true,
            committed: true,
            image: "refuse",
            expected: Err(ShareStateError::Refused),
        },
        Case {
            admitted: true,
            committed: false,
            image: "value",
            expected: Err(ShareStateError::Commit),
        },
    ] {
        let domain = AdmissionDomain {
            admitted: case.admitted,
            committed: case.committed,
            events: RefCell::new(Vec::new()),
        };
        assert!(domain.serve(case.image, "group", [1; 16], 2).await == case.expected);
        let expected_events: &[&str] = if case.admitted {
            &["key", "acquire", "operation", "answer", "release"]
        } else {
            &["key", "acquire"]
        };
        assert!(domain.events.borrow().as_slice() == expected_events);
    }
}
