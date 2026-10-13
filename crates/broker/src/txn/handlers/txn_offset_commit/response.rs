//! Builders for the `TxnOffsetCommitResponse`.
//!
//! [`build_response`] is Kafka's `TxnOffsetCommitResponse.Builder` as
//! `KafkaApis.handleTxnOffsetCommitRequest` fills it: the topic sweep's rows
//! go in first, with `UNKNOWN_TOPIC_ID`, `TOPIC_AUTHORIZATION_FAILED` or
//! `UNKNOWN_TOPIC_OR_PARTITION`, and the group coordinator's answer for the
//! rows that survived the sweep is merged after them. [`error_response`] is
//! `TxnOffsetCommitRequest.getErrorResponse`, one code on every row of the
//! request in request order.

use {
    crate::codes,
    krabka_protocol::owned::{
        txn_offset_commit_request::{TxnOffsetCommitRequest, TxnOffsetCommitRequestTopic},
        txn_offset_commit_response::{
            TxnOffsetCommitResponse, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
        },
    },
};

/// The response to a request whose topic sweep is done: the sweep's rows
/// first, then `code` on every row that survived it.
///
/// `topic_ids` is the v6+ shape. A topic whose id the image could not name,
/// so whose name is still empty, answers `UNKNOWN_TOPIC_ID` on every row,
/// ahead of the ACL and existence codes, and the response keys its topics by
/// id, as Kafka's `TopicIdBuilder` does. Below v6 it keys them by name, as
/// `TopicNameBuilder` does. Rows of one key go to one response topic, the
/// sweep's before the coordinator's.
pub(super) fn build_response(
    req: &TxnOffsetCommitRequest,
    code: i16,
    topic_ids: bool,
    denied_topics: &std::collections::HashSet<String>,
    unknown_rows: &std::collections::HashSet<(String, i32)>,
) -> TxnOffsetCommitResponse {
    let sweep_code = |topic: &TxnOffsetCommitRequestTopic, partition: i32| {
        if topic_ids && topic.name.is_empty() {
            Some(codes::UNKNOWN_TOPIC_ID)
        } else if denied_topics.contains(&topic.name) {
            Some(codes::TOPIC_AUTHORIZATION_FAILED)
        } else if unknown_rows.contains(&(topic.name.clone(), partition)) {
            Some(codes::UNKNOWN_TOPIC_OR_PARTITION)
        } else {
            None
        }
    };
    let mut builder = ResponseBuilder {
        topic_ids,
        topics: Vec::new(),
    };
    for topic in &req.topics {
        for partition in &topic.partitions {
            if let Some(row_code) = sweep_code(topic, partition.partition_index) {
                builder.add(topic, partition.partition_index, row_code);
            }
        }
    }
    // The coordinator answers one topic per request topic with a surviving
    // row, and Kafka's `Builder.merge` takes its answer whole when the sweep
    // left nothing.
    let coordinator: Vec<TxnOffsetCommitResponseTopic> = req
        .topics
        .iter()
        .filter_map(|topic| {
            let partitions: Vec<TxnOffsetCommitResponsePartition> = topic
                .partitions
                .iter()
                .filter(|partition| sweep_code(topic, partition.partition_index).is_none())
                .map(|partition| row(partition.partition_index, code))
                .collect();
            (!partitions.is_empty()).then(|| TxnOffsetCommitResponseTopic {
                partitions,
                ..response_topic(topic)
            })
        })
        .collect();
    builder.merge(coordinator);
    TxnOffsetCommitResponse {
        throttle_time_ms: 0,
        topics: builder.topics,
        ..Default::default()
    }
}

/// Kafka's `TxnOffsetCommitResponse.Builder`: the response topics, each
/// found again by its id at v6+ and by its name below.
struct ResponseBuilder {
    topic_ids: bool,
    topics: Vec<TxnOffsetCommitResponseTopic>,
}

impl ResponseBuilder {
    fn position(&self, topic: &TxnOffsetCommitResponseTopic) -> Option<usize> {
        self.topics.iter().position(|held| {
            if self.topic_ids {
                held.topic_id == topic.topic_id
            } else {
                held.name == topic.name
            }
        })
    }

    /// `Builder.addPartition`: the row goes to the response topic of
    /// `topic`'s key, which is created at the end when there is none.
    fn add(&mut self, topic: &TxnOffsetCommitRequestTopic, partition: i32, code: i16) {
        let wanted = response_topic(topic);
        let at = self.position(&wanted).unwrap_or_else(|| {
            self.topics.push(wanted);
            self.topics.len() - 1
        });
        self.topics[at].partitions.push(row(partition, code));
    }

    /// `Builder.merge`: the coordinator's topics replace an empty response,
    /// and otherwise each one joins the response topic of its key or goes at
    /// the end.
    fn merge(&mut self, coordinator: Vec<TxnOffsetCommitResponseTopic>) {
        if self.topics.is_empty() {
            self.topics = coordinator;
            return;
        }
        for topic in coordinator {
            match self.position(&topic) {
                Some(at) => self.topics[at].partitions.extend(topic.partitions),
                None => self.topics.push(topic),
            }
        }
    }
}

/// The response topic of a request topic, with no rows yet. It carries both
/// the id and the name; the version decides which one goes on the wire.
fn response_topic(topic: &TxnOffsetCommitRequestTopic) -> TxnOffsetCommitResponseTopic {
    TxnOffsetCommitResponseTopic {
        name: topic.name.clone(),
        topic_id: topic.topic_id,
        ..Default::default()
    }
}

fn row(partition_index: i32, error_code: i16) -> TxnOffsetCommitResponsePartition {
    TxnOffsetCommitResponsePartition {
        partition_index,
        error_code,
        ..Default::default()
    }
}

/// Kafka's `TxnOffsetCommitRequest.getErrorResponse`: `code` on every row, one
/// response topic per request topic, in request order. It answers a
/// whole-request error that precedes the topic sweep: the transactional id
/// `Write` and group `Read` gates. Every later exit goes through
/// [`build_response`] with the sweep's rows.
pub(super) fn error_response(req: &TxnOffsetCommitRequest, code: i16) -> TxnOffsetCommitResponse {
    TxnOffsetCommitResponse {
        throttle_time_ms: 0,
        topics: req
            .topics
            .iter()
            .map(|topic| TxnOffsetCommitResponseTopic {
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| row(partition.partition_index, code))
                    .collect(),
                ..response_topic(topic)
            })
            .collect(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            handlers::encode_response,
            test_support::decode_response,
            txn::handlers::txn_offset_commit::test_support::{
                request, request_topic as request_topic_fixture,
            },
        },
        assert2::assert,
        krabka_protocol::primitives::uuid::Uuid as WireUuid,
        std::collections::HashSet,
    };

    fn request_topic(name: &str, id: u128, partitions: &[i32]) -> TxnOffsetCommitRequestTopic {
        request_topic_fixture()
            .name(name)
            .topic_id(WireUuid(uuid::Uuid::from_u128(id).into_bytes()))
            .partitions(partitions.to_vec())
            .call()
    }

    fn response_topic(name: &str, id: u128, rows: &[(i32, i16)]) -> TxnOffsetCommitResponseTopic {
        TxnOffsetCommitResponseTopic {
            name: name.into(),
            topic_id: WireUuid(uuid::Uuid::from_u128(id).into_bytes()),
            partitions: rows
                .iter()
                .map(|&(partition_index, error_code)| row(partition_index, error_code))
                .collect(),
            ..Default::default()
        }
    }

    struct Case {
        name: &'static str,
        topic_ids: bool,
        topics: Vec<TxnOffsetCommitRequestTopic>,
        code: i16,
        denied: &'static [&'static str],
        unknown: &'static [(&'static str, i32)],
        expected: Vec<TxnOffsetCommitResponseTopic>,
    }

    /// Kafka's `TxnOffsetCommitResponse.Builder`: the sweep's rows first, the
    /// coordinator's merged after them into the topic of the same key, which
    /// is the id at v6 and the name below.
    #[test]
    fn build_response_puts_the_sweep_rows_first_and_merges_by_key() {
        let cases = [
            Case {
                name: "no sweep row takes the coordinator answer whole",
                topic_ids: false,
                topics: vec![request_topic("orders", 0, &[2, 3])],
                code: codes::INVALID_TXN_STATE,
                denied: &[],
                unknown: &[],
                expected: vec![response_topic("orders", 0, &[
                    (2, codes::INVALID_TXN_STATE),
                    (3, codes::INVALID_TXN_STATE),
                ])],
            },
            Case {
                name: "a denied topic is 29 on every row",
                topic_ids: false,
                topics: vec![request_topic("orders", 0, &[2, 3])],
                code: codes::NONE,
                denied: &["orders"],
                unknown: &[],
                expected: vec![response_topic("orders", 0, &[
                    (2, codes::TOPIC_AUTHORIZATION_FAILED),
                    (3, codes::TOPIC_AUTHORIZATION_FAILED),
                ])],
            },
            Case {
                name: "an unknown partition leads its own topic",
                topic_ids: false,
                topics: vec![request_topic("orders", 0, &[2, 3])],
                code: codes::NONE,
                denied: &[],
                unknown: &[("orders", 3)],
                expected: vec![response_topic("orders", 0, &[
                    (3, codes::UNKNOWN_TOPIC_OR_PARTITION),
                    (2, codes::NONE),
                ])],
            },
            Case {
                name: "failed topics lead the committed ones",
                topic_ids: false,
                topics: vec![
                    request_topic("a", 0, &[0]),
                    request_topic("denied", 0, &[0]),
                    request_topic("missing", 0, &[0]),
                ],
                code: codes::NONE,
                denied: &["denied"],
                unknown: &[("missing", 0)],
                expected: vec![
                    response_topic("denied", 0, &[(0, codes::TOPIC_AUTHORIZATION_FAILED)]),
                    response_topic("missing", 0, &[(0, codes::UNKNOWN_TOPIC_OR_PARTITION)]),
                    response_topic("a", 0, &[(0, codes::NONE)]),
                ],
            },
            Case {
                name: "v6 keys each unresolved id apart and answers it 100",
                topic_ids: true,
                topics: vec![
                    request_topic("", 7, &[0]),
                    request_topic("a", 1, &[0, 5]),
                    request_topic("", 8, &[1]),
                ],
                code: codes::NONE,
                denied: &[],
                unknown: &[("a", 5)],
                expected: vec![
                    response_topic("", 7, &[(0, codes::UNKNOWN_TOPIC_ID)]),
                    response_topic("a", 1, &[
                        (5, codes::UNKNOWN_TOPIC_OR_PARTITION),
                        (0, codes::NONE),
                    ]),
                    response_topic("", 8, &[(1, codes::UNKNOWN_TOPIC_ID)]),
                ],
            },
            Case {
                name: "below v6 an empty name is a name like any other",
                topic_ids: false,
                topics: vec![request_topic("", 7, &[0])],
                code: codes::NONE,
                denied: &[],
                unknown: &[("", 0)],
                expected: vec![response_topic("", 7, &[(
                    0,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                )])],
            },
        ];
        for case in cases {
            let req = TxnOffsetCommitRequest {
                topics: case.topics,
                ..request()
            };
            let denied: HashSet<String> = case.denied.iter().map(|&t| t.to_string()).collect();
            let unknown: HashSet<(String, i32)> = case
                .unknown
                .iter()
                .map(|&(t, p)| (t.to_string(), p))
                .collect();
            let response = build_response(&req, case.code, case.topic_ids, &denied, &unknown);
            assert!(
                response
                    == TxnOffsetCommitResponse {
                        throttle_time_ms: 0,
                        topics: case.expected,
                        ..Default::default()
                    },
                "{}",
                case.name
            );
        }
    }

    /// v6 puts the topic id on the wire and drops the name; v5 does the
    /// reverse. The whole-request error keeps request order at both.
    #[test]
    fn responses_encode_the_topic_key_of_their_version() {
        let req = TxnOffsetCommitRequest {
            topics: vec![request_topic("orders", 9, &[2, 3])],
            ..request()
        };
        for (version, name, id) in [(5, "orders", 0), (6, "", 9)] {
            let rows = [(2, codes::INVALID_TXN_STATE), (3, codes::INVALID_TXN_STATE)];
            let expected = TxnOffsetCommitResponse {
                throttle_time_ms: 0,
                topics: vec![response_topic(name, id, &rows)],
                ..Default::default()
            };
            let built = build_response(
                &req,
                codes::INVALID_TXN_STATE,
                version >= 6,
                &HashSet::new(),
                &HashSet::new(),
            );
            let bytes = encode_response(&built, version).expect("encode response");
            let decoded: TxnOffsetCommitResponse = decode_response(&bytes, version);
            assert!(decoded == expected, "build v{version}");

            let bytes = encode_response(&error_response(&req, codes::INVALID_TXN_STATE), version)
                .expect("encode all-error response");
            let decoded: TxnOffsetCommitResponse = decode_response(&bytes, version);
            assert!(decoded == expected, "error v{version}");
        }
    }
}
