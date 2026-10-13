//! KIP-516: `OffsetCommit` v10 and `OffsetFetch` v8+ keyed by `topic_id`.
use {
    crate::support::{
        offsets::{
            offset_commit_topic, offset_fetch_group, offset_fetch_request, offset_fetch_topic,
        },
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
};
mod support;

use {
    krabka_protocol::{
        owned::{
            offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestPartition},
            offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            },
            offset_fetch_response::{
                OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartitions,
                OffsetFetchResponseTopics,
            },
        },
        primitives::uuid::Uuid as WireUuid,
    },
    support::topic_id_for,
};

/// Kafka's `UNKNOWN_TOPIC_ID` error code.
const UNKNOWN_TOPIC_ID: i16 = 100;

use crate::support::start_ready_group as start;

#[derive(Clone, Copy)]
enum FetchTopicShape {
    Listed,
    All,
}

#[yare::parameterized(
    listed_topic = { "g1", "o_topic", 42, FetchTopicShape::Listed },
    fetch_all = { "g3", "fa_topic", 7, FetchTopicShape::All },
)]
#[test_macro(tokio::test)]
async fn offset_commit_and_fetch_by_topic_id_round_trip(
    group_id: &str,
    topic_name: &str,
    committed_offset: i64,
    shape: FetchTopicShape,
) {
    let p = start().await;
    p.client
        .send(create_topic_request(
            creatable_topic().name(topic_name.into()).call(),
        ))
        .await
        .expect("create topic");
    let id = topic_id_for(&p.client, topic_name).await;

    // Commit by topic_id (v10: name empty, id set). Empty member_id
    // skips the membership check.
    p.client
        .send(OffsetCommitRequest {
            group_id: group_id.into(),
            topics: vec![offset_commit_topic(String::new(), id, vec![
                OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset,
                    ..Default::default()
                },
            ])],
            ..Default::default()
        })
        .await
        .expect("offset commit");

    let topics = match shape {
        FetchTopicShape::Listed => Some(vec![offset_fetch_topic(String::new(), id, vec![0])]),
        FetchTopicShape::All => None,
    };
    let resp = p
        .client
        .send(offset_fetch_request(offset_fetch_group(group_id, topics)))
        .await
        .expect("offset fetch");

    let grp = resp
        .groups
        .iter()
        .find(|g| g.group_id == group_id)
        .expect("requested group");
    let t = grp
        .topics
        .iter()
        .find(|t| t.topic_id == id)
        .expect("topic by id");
    let part = t.partitions.first().expect("partition 0");
    check!(part.committed_offset == committed_offset);
    check!(part.error_code == 0);
    check!(t.topic_id == id); // id echoed
}

/// `OffsetFetch` v10 with a `topic_id` that does not resolve answers
/// `UNKNOWN_TOPIC_ID` with offset -1 and the id echoed. The zero id is such an
/// id.
#[tokio::test]
async fn offset_fetch_unresolved_topic_id_returns_unknown_topic_id() {
    /// Kafka's `UNKNOWN_TOPIC_ID` error code.
    const UNKNOWN_TOPIC_ID: i16 = 100;

    let p = start().await;
    let cases = support::topics::unresolved_topic_ids(0xabad_1dea);
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (label, topic_id) in cases {
        let resp = p
            .client
            .send(offset_fetch_request(offset_fetch_group(
                "g2",
                Some(vec![offset_fetch_topic(String::new(), topic_id, vec![0])]),
            )))
            .await
            .expect("offset fetch");
        actual.push((label, resp));
        expected.push((label, OffsetFetchResponse {
            groups: vec![OffsetFetchResponseGroup {
                group_id: "g2".into(),
                topics: vec![OffsetFetchResponseTopics {
                    name: String::new(),
                    topic_id,
                    partitions: vec![OffsetFetchResponsePartitions {
                        partition_index: 0,
                        committed_offset: -1,
                        committed_leader_epoch: -1,
                        metadata: Some(String::new()),
                        error_code: UNKNOWN_TOPIC_ID,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }));
    }
    assert!(actual == expected);
}

/// `OffsetCommit` v10 with a known `topic_id` and a `topic_id` that does not
/// resolve. The known topic commits and answers 0. The other row commits
/// nothing and answers `UNKNOWN_TOPIC_ID` with its id echoed. The zero id is
/// such an id. Kafka's `OffsetCommitResponse.Builder` puts the refused row
/// ahead of the committed row.
#[tokio::test]
async fn offset_commit_unresolved_topic_id_returns_unknown_topic_id() {
    let p = start().await;
    p.client
        .send(create_topic_request(
            creatable_topic().name(("oc_known").into()).call(),
        ))
        .await
        .expect("create topic");
    let known = topic_id_for(&p.client, "oc_known").await;

    let cases = [
        (
            "non-zero id",
            WireUuid(uuid::Uuid::from_u128(0x0bad_0bad).into_bytes()),
        ),
        ("zero id", WireUuid::ZERO),
    ];
    let row = |topic_id, committed_offset| {
        offset_commit_topic(String::new(), topic_id, vec![
            OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset,
                ..Default::default()
            },
        ])
    };
    let answer = |topic_id, error_code| OffsetCommitResponseTopic {
        name: String::new(),
        topic_id,
        partitions: vec![OffsetCommitResponsePartition {
            partition_index: 0,
            error_code,
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (label, unresolved) in cases {
        let resp = p
            .client
            .send(OffsetCommitRequest {
                group_id: "gc".into(),
                topics: vec![row(known, 5), row(unresolved, 9)],
                ..Default::default()
            })
            .await
            .expect("offset commit");
        actual.push((label, resp));
        expected.push((label, OffsetCommitResponse {
            topics: vec![answer(unresolved, UNKNOWN_TOPIC_ID), answer(known, 0)],
            ..Default::default()
        }));
    }
    assert!(actual == expected);
}
