//! KIP-966 `DescribeTopicPartitions` (`api_key` 75): the paginated topic
//! listing the JVM admin client uses for `kafka-topics --describe`
//! against Kafka 3.7+ brokers.
//!
//! Covered:
//!   * named-request, fetch-all, and unknown-topic paths
//!   * the `is_internal` flag lives in `tests/internal_topics.rs`, which
//!     pins it across this API and `Metadata` at once
//!   * `topic_authorized_operations` populated (KIP-430 helper) on every
//!     Allow row. This API's v0 schema has no opt-in flag.
//!   * Pagination through `response_partition_limit` and the `cursor` /
//!     `next_cursor` round-trip
//!   * Stable sort order on fetch-all (alphabetical)

use assert2::{assert, check};

use crate::support::acl::TOPIC_FULL_MASK;
mod support;

use krabka_protocol::owned::{
    describe_topic_partitions_request::{
        Cursor as RequestCursor, DescribeTopicPartitionsRequest, TopicRequest,
    },
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

fn describe_topics_request(
    names: &[&str],
    limit: i32,
    cursor: Option<RequestCursor>,
) -> DescribeTopicPartitionsRequest {
    DescribeTopicPartitionsRequest {
        topics: names
            .iter()
            .map(|name| TopicRequest {
                name: (*name).into(),
                ..Default::default()
            })
            .collect(),
        response_partition_limit: limit,
        cursor,
        ..Default::default()
    }
}

async fn described_topic(
    name: &str,
    partitions: i32,
) -> (
    support::InProcess,
    krabka_protocol::owned::describe_topic_partitions_response::DescribeTopicPartitionsResponse,
) {
    let p = support::start().await;
    create_topic(
        &p,
        crate::support::topics::CreateTopicSetup {
            topic: name,
            num_partitions: crate::support::topics::TopicPartitionCount(partitions),
            ..Default::default()
        },
    )
    .await;
    let response = p
        .client
        .send(describe_topics_request(&[name], 2000, None))
        .await
        .expect("DescribeTopicPartitions");
    (p, response)
}

// Bit positions (subset; cross-check'd with the KIP-430 unit tests).

async fn create_topic(p: &support::InProcess, setup: crate::support::topics::CreateTopicSetup<'_>) {
    support::client::create_topic_named(&p.client, setup).await;
}

#[tokio::test]
async fn named_request_returns_listed_topics_with_partitions() {
    let p = support::start().await;
    create_alpha_beta(
        &p,
        AlphaBetaTopicsSetup {
            alpha_partitions: crate::support::topics::TopicPartitionCount(2),
        },
    )
    .await;

    let resp = p
        .client
        .send(describe_topics_request(&["alpha", "beta"], 2000, None))
        .await
        .expect("DescribeTopicPartitions");

    assert!(resp.topics.len() == 2);
    // Named-request order preserves the request order.
    check!(resp.topics[0].name.as_deref() == Some("alpha"));
    check!(resp.topics[0].error_code == 0);
    check!(resp.topics[0].partitions.len() == 2);
    for (i, part) in resp.topics[0].partitions.iter().enumerate() {
        check!(part.error_code == 0);
        check!(part.partition_index == i32::try_from(i).unwrap());
        check!(part.leader_id == 1);
    }
    check!(resp.topics[1].name.as_deref() == Some("beta"));
    check!(resp.topics[1].partitions.len() == 1);

    // No truncation expected — every partition fits under the default
    // 2000-partition budget.
    check!(
        resp.next_cursor.is_none(),
        "no cursor expected: {:?}",
        resp.next_cursor,
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn fetch_all_returns_topics_in_alphabetical_order() {
    let p = support::start().await;
    // Create in non-alphabetical order to prove the broker sorts.
    create_topic(
        &p,
        crate::support::topics::CreateTopicSetup {
            topic: "gamma",
            ..Default::default()
        },
    )
    .await;
    create_alpha_beta(&p, AlphaBetaTopicsSetup::default()).await;

    let resp = p
        .client
        .send(describe_topics_request(&[], 2000, None)) // empty → fetch-all
        .await
        .expect("DescribeTopicPartitions");

    let names: Vec<&str> = resp
        .topics
        .iter()
        .filter_map(|t| t.name.as_deref())
        .collect();
    // Internal topics may appear too — filter to the user-created ones.
    let user_topics: Vec<&str> = names
        .iter()
        .copied()
        .filter(|n| !n.starts_with("__"))
        .collect();
    assert!(user_topics == vec!["alpha", "beta", "gamma"]);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn unknown_topic_in_named_request_returns_error_row() {
    let p = support::start().await;
    create_topic(
        &p,
        crate::support::topics::CreateTopicSetup {
            topic: "real-topic",
            ..Default::default()
        },
    )
    .await;

    let resp = p
        .client
        .send(describe_topics_request(
            &["ghost", "real-topic"],
            2000,
            None,
        ))
        .await
        .expect("DescribeTopicPartitions");
    assert!(resp.topics.len() == 2);
    // Unknown topic row carries UNKNOWN_TOPIC_OR_PARTITION (3).
    check!(resp.topics[0].name.as_deref() == Some("ghost"));
    check!(resp.topics[0].error_code == 3);
    check!(resp.topics[0].partitions.is_empty());
    // Known sibling still served on the same response.
    check!(resp.topics[1].name.as_deref() == Some("real-topic"));
    check!(resp.topics[1].error_code == 0);
    check!(resp.topics[1].partitions.len() == 1);

    p.broker.shutdown().await;
}

/// The JVM 3.8 admin client NPEs when `eligibleLeaderReplicas` or
/// `lastKnownElr` decode as `null`. The NPE happens in
/// `DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo`. The
/// schema marks both nullable, but real Kafka brokers always emit empty
/// lists. This test pins the empty-list shape so the broker does not
/// regress.
#[tokio::test]
async fn elr_lists_are_empty_not_null_for_jvm_3_8_admin_compatibility() {
    let (p, resp) = Box::pin(described_topic("t", 1)).await;

    assert!(resp.topics.len() == 1);
    assert!(resp.topics[0].partitions.len() == 1);
    let part = &resp.topics[0].partitions[0];
    // MUST be Some(_), not None. Both fields stay as empty vecs so the
    // JVM 3.8 admin client's unconditional `.stream()` call doesn't NPE.
    assert!(
        part.eligible_leader_replicas.as_deref() == Some(&[][..]),
        "eligible_leader_replicas must be empty list, not null"
    );
    assert!(
        part.last_known_elr.as_deref() == Some(&[][..]),
        "last_known_elr must be empty list, not null"
    );

    p.broker.shutdown().await;
}

/// The `DescribeTopicPartitions` request the ELR-downgrade case sends twice,
/// once on either side of the downgrade.
fn describe_request() -> DescribeTopicPartitionsRequest {
    describe_topics_request(&["t"], 2000, None)
}

/// KIP-966: a downgrade of `eligible.leader.replicas.version` to 0 clears the
/// memberships the feature published, so `DescribeTopicPartitions` reports an
/// empty `Elr` afterwards.
///
/// This is the read side of Kafka's `generateRecordsForCleaningElr`. The
/// membership is seeded as the topic-config override the controller publishes,
/// which is how krabka carries ELR, and the downgrade goes through
/// `UpdateFeatures` exactly as `kafka-features downgrade` sends it.
#[tokio::test]
async fn a_feature_downgrade_empties_the_reported_elr() {
    let p = support::start().await;
    create_topic(
        &p,
        crate::support::topics::CreateTopicSetup {
            topic: "t",
            ..Default::default()
        },
    )
    .await;

    p.broker
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: "eligible.leader.replicas.version".into(),
                level: 1,
            },
        ))
        .await
        .expect("finalize eligible.leader.replicas.version");
    p.broker
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1PartitionElr(
            krabka_metadata::PartitionElrRecord {
                topic: "t".into(),
                partition: 0,
                eligible_leader_replicas: vec![
                    krabka_metadata::NodeId(2),
                    krabka_metadata::NodeId(3),
                ],
                last_known_elr: vec![],
            },
        ))
        .await
        .expect("publish an ELR");

    let before = p
        .client
        .send(describe_request())
        .await
        .expect("describe before");
    assert!(
        before.topics[0].partitions[0]
            .eligible_leader_replicas
            .as_deref()
            == Some(&[2, 3][..]),
        "precondition: the published membership is reported: {:?}",
        before.topics[0].partitions[0]
    );

    let downgrade = p
        .client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "eligible.leader.replicas.version".into(),
                max_version_level: 0,
                // 2 = SAFE_DOWNGRADE, what `kafka-features downgrade` sends.
                upgrade_type: 2,
                ..Default::default()
            }],
            timeout_ms: 10_000,
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(
        downgrade.error_code == 0,
        "UpdateFeatures rejected the request: {downgrade:?}"
    );

    // The clearing records ride in the same batch as the feature record, so
    // they are committed together; the describe below waits only for this
    // broker to have applied that batch.
    //
    // intentional poll: the only signal that the batch applied is the state
    // it changes, which is what is being asserted.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let after = loop {
        let resp = p
            .client
            .send(describe_request())
            .await
            .expect("describe after");
        let cleared = resp.topics[0].partitions[0]
            .eligible_leader_replicas
            .as_deref()
            == Some(&[][..]);
        if cleared || std::time::Instant::now() > deadline {
            break resp;
        }
        tokio::task::yield_now().await;
    };
    let part = &after.topics[0].partitions[0];
    check!(
        part.eligible_leader_replicas.as_deref() == Some(&[][..]),
        "the downgrade must clear the ELR, got {:?}",
        part.eligible_leader_replicas
    );
    check!(part.last_known_elr.as_deref() == Some(&[][..]));
    // KIP-584 treats level 0 as a delete, so the feature leaves the finalized
    // map rather than sitting at 0; `feature_enabled` reads both as off.
    check!(
        p.broker
            .controller_image_for_test()
            .finalized_feature("eligible.leader.replicas.version")
            == None,
        "the downgrade must remove the finalized feature"
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn topic_authorized_operations_populated_for_super_user() {
    let (p, resp) = Box::pin(described_topic("t", 1)).await;

    assert!(resp.topics.len() == 1);
    let row = &resp.topics[0];
    assert!(row.error_code == 0);
    // `support::start` uses `AllowAllAuthorizer` by default, so every
    // supported topic operation is authorized → full mask.
    assert!(
        row.topic_authorized_operations == TOPIC_FULL_MASK,
        "expected full topic mask, got 0b{:b}",
        row.topic_authorized_operations
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn pagination_caps_response_at_partition_limit_and_returns_next_cursor() {
    let p = support::start().await;
    create_topic(
        &p,
        crate::support::topics::CreateTopicSetup {
            topic: "big",
            num_partitions: crate::support::topics::TopicPartitionCount(5),
            ..Default::default()
        },
    )
    .await;

    // Cap response to 3 partitions; expect 3 returned + a cursor pointing
    // at "big" / partition 3.
    let resp = p
        .client
        .send(describe_topics_request(&["big"], 3, None))
        .await
        .expect("DescribeTopicPartitions");

    assert!(resp.topics.len() == 1);
    check!(resp.topics[0].name.as_deref() == Some("big"));
    check!(resp.topics[0].partitions.len() == 3);
    let cursor = resp.next_cursor.expect("next_cursor must be set");
    assert!(cursor.topic_name == "big");
    assert!(cursor.partition_index == 3);

    // Resume from the cursor — should return partitions 3 and 4 only.
    let resp2 = p
        .client
        .send(describe_topics_request(
            &["big"],
            2000,
            Some(RequestCursor {
                topic_name: cursor.topic_name.clone(),
                partition_index: cursor.partition_index,
                ..Default::default()
            }),
        ))
        .await
        .expect("DescribeTopicPartitions (resume)");
    assert!(resp2.topics.len() == 1);
    let parts: Vec<i32> = resp2.topics[0]
        .partitions
        .iter()
        .map(|p| p.partition_index)
        .collect();
    assert!(parts == vec![3, 4]);
    assert!(
        resp2.next_cursor.is_none(),
        "no more data should remain after the resume: {:?}",
        resp2.next_cursor,
    );

    p.broker.shutdown().await;
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct AlphaBetaTopicsSetup {
    #[default(crate::support::topics::TopicPartitionCount(1))]
    alpha_partitions: crate::support::topics::TopicPartitionCount,
}

/// Keep the shared alpha-before-beta creation order with each case's alpha partition count.
async fn create_alpha_beta(p: &support::InProcess, setup: AlphaBetaTopicsSetup) {
    create_topic(
        p,
        crate::support::topics::CreateTopicSetup {
            topic: "alpha",
            num_partitions: setup.alpha_partitions,
            ..Default::default()
        },
    )
    .await;
    create_topic(
        p,
        crate::support::topics::CreateTopicSetup {
            topic: "beta",
            ..Default::default()
        },
    )
    .await;
}
