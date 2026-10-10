//! KIP-516: `OffsetCommit` v10 and `OffsetFetch` v8+ keyed by `topic_id`.
use assert2::{assert, check};

use crate::support::{
    offsets::{offset_commit_topic, offset_fetch_group, offset_fetch_request, offset_fetch_topic},
    topics::{creatable_topic, create_topic_request},
};
mod support;

use krabka_protocol::{
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
};
use support::topic_id_for;

/// Kafka's `UNKNOWN_TOPIC_ID` error code.
const UNKNOWN_TOPIC_ID: i16 = 100;

use crate::support::start_ready_group as start;

#[tokio::test]
async fn offset_commit_and_fetch_by_topic_id_round_trip() {
    let p = start().await;
    p.client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("o_topic").into(),
                ..Default::default()
            },
        )))
        .await
        .expect("create topic");
    let id = topic_id_for(&p.client, "o_topic").await;

    // Commit offset 42 by topic_id (v10: name empty, id set). Empty member_id
    // skips the membership check.
    p.client
        .send(OffsetCommitRequest {
            group_id: "g1".into(),
            topics: vec![offset_commit_topic(
                String::new(),
                id,
                vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 42,
                    ..Default::default()
                }],
            )],
            ..Default::default()
        })
        .await
        .expect("offset commit");

    // Fetch back via v8+ multi-group shape keyed by topic_id.
    let resp = p
        .client
        .send(offset_fetch_request(offset_fetch_group(
            "g1",
            Some(vec![offset_fetch_topic(String::new(), id, vec![0])]),
        )))
        .await
        .expect("offset fetch");

    let grp = resp
        .groups
        .iter()
        .find(|g| g.group_id == "g1")
        .expect("group g1");
    let t = grp
        .topics
        .iter()
        .find(|t| t.topic_id == id)
        .expect("topic by id");
    let part = t.partitions.first().expect("partition 0");
    check!(part.committed_offset == 42);
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
        expected.push((
            label,
            OffsetFetchResponse {
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
            },
        ));
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
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("oc_known").into(),
                ..Default::default()
            },
        )))
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
        offset_commit_topic(
            String::new(),
            topic_id,
            vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset,
                ..Default::default()
            }],
        )
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
        expected.push((
            label,
            OffsetCommitResponse {
                topics: vec![answer(unresolved, UNKNOWN_TOPIC_ID), answer(known, 0)],
                ..Default::default()
            },
        ));
    }
    assert!(actual == expected);
}

/// A fetch-all with null `topics` at v10 must echo each topic's `topic_id`,
/// because v10 drops the name from the wire and the client matches by id.
#[tokio::test]
async fn offset_fetch_all_echoes_topic_id() {
    let p = start().await;
    p.client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("fa_topic").into(),
                ..Default::default()
            },
        )))
        .await
        .expect("create topic");
    let id = topic_id_for(&p.client, "fa_topic").await;

    p.client
        .send(OffsetCommitRequest {
            group_id: "g3".into(),
            topics: vec![offset_commit_topic(
                String::new(),
                id,
                vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 7,
                    ..Default::default()
                }],
            )],
            ..Default::default()
        })
        .await
        .expect("offset commit");

    // Fetch-all: `topics: None` for the group.
    let resp = p
        .client
        .send(offset_fetch_request(offset_fetch_group("g3", None)))
        .await
        .expect("offset fetch");
    let grp = resp
        .groups
        .iter()
        .find(|g| g.group_id == "g3")
        .expect("group g3");
    let t = grp
        .topics
        .iter()
        .find(|t| t.topic_id == id)
        .expect("topic row with echoed id");
    assert!(t.partitions.first().expect("a partition").committed_offset == 7);
}
