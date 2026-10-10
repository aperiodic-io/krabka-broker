//! The KFC-9 write-freeze gate that `AddPartitionsToTxn` runs beside its
//! per-topic `Write` ACL sweep.
//!
//! A topic that a write freeze covers never joins the transaction's partition
//! set. [`frozen_topics`] reads the registry once per transaction entry, and
//! [`topic_refusal`] is where the ACL gate and the freeze meet: the ACL deny
//! outranks the freeze, so a caller with no right to read a topic learns
//! nothing about its freeze state.

use krabka_metadata::MetadataImage;
use krabka_protocol::owned::common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic;
use krabka_verified::{FreezeMutationDecision, FreezeMutationKind, freeze_mutation_decision};

use crate::{
    codes,
    freeze::resolve::{FreezeMutationResolution, resolve_freeze_mutation},
};

/// Builds the set of topic names that a KFC-9 write freeze covers.
///
/// It runs once per transaction entry: a freeze is a property of the topic,
/// not of a partition, so each partition row then costs one set lookup. On a
/// cluster with no freeze the image answers every topic in two emptiness
/// tests and the set stays empty.
pub(super) fn frozen_topics(
    image: &MetadataImage,
    topics: &[AddPartitionsToTxnTopic],
    denied: &std::collections::HashSet<String>,
) -> std::collections::HashSet<String> {
    topics
        .iter()
        .filter(|t| {
            matches!(
                resolve_freeze_mutation(
                    image,
                    &t.name,
                    !denied.contains(&t.name),
                    FreezeMutationKind::TransactionEnlistment,
                ),
                FreezeMutationResolution::Frozen(_)
            )
        })
        .map(|t| t.name.clone())
        .collect()
}

/// The refusal that one topic's partition rows carry before the transaction
/// state machine has any say, or `None` when the topic is free to proceed.
///
/// The `Write` ACL deny outranks the freeze. An unauthorized principal learns
/// that it is unauthorized and nothing more: answering `POLICY_VIOLATION`
/// instead would leak the topic's freeze state to a caller with no right to
/// read it.
pub(super) fn topic_refusal(
    name: &str,
    denied: &std::collections::HashSet<String>,
    frozen: &std::collections::HashSet<String>,
) -> Option<i16> {
    match freeze_mutation_decision(
        !denied.contains(name),
        frozen.contains(name),
        FreezeMutationKind::TransactionEnlistment,
    ) {
        FreezeMutationDecision::AuthorizationDenied => Some(codes::TOPIC_AUTHORIZATION_FAILED),
        FreezeMutationDecision::Frozen => Some(codes::POLICY_VIOLATION),
        FreezeMutationDecision::Admit => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use assert2::{assert, check};
    use krabka_ids::PartitionIndex;
    use krabka_metadata::{
        AclOperation, MetadataRecord, PatternType, ResourceType, TopicFreezeRecord,
    };
    use krabka_protocol::owned::{
        add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
        add_partitions_to_txn_response::AddPartitionsToTxnResponse,
        common::add_partitions_to_txn_response::add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
    };

    use super::*;
    use crate::{
        authorizer::AuthorizationResult,
        test_support::{FreezeSetup, test_ctx},
        txn::{
            handlers::add_partitions_to_txn::{
                handle,
                test_support::{topic, topic_result},
            },
            state::TxnEntry,
        },
    };

    crate::test_support::context_helper!(client_id = "producer-client");

    /// The transactional id every freeze case drives.
    const TID: &str = "tid-freeze";
    /// The frozen topic in every freeze case, and the unfrozen control that
    /// travels in the same request.
    const FROZEN_TOPIC: &str = "tenant-a.orders";
    const UNFROZEN_TOPIC: &str = "events";

    fn freeze_record(scope: &str, pattern_type: PatternType) -> TopicFreezeRecord {
        crate::test_support::topic_freeze_record(FreezeSetup {
            scope,
            pattern_type,
            ..Default::default()
        })
    }

    use crate::test_support::frozen_topics_image as image_with_freezes;

    #[test]
    fn frozen_topics_keeps_the_covered_names_and_nothing_else() {
        let image = image_with_freezes(
            &[
                ("orders", PatternType::Literal),
                ("tenant-a.", PatternType::Prefixed),
            ],
            crate::test_support::FrozenTopicsImageSetup::default(),
        );

        // (label, the topics the request names, the names the gate keeps)
        let cases: [(&str, &[&str], &[&str]); 5] = [
            (
                "a literal freeze covers the one topic it names",
                &["orders"],
                &["orders"],
            ),
            (
                "a prefix freeze covers every topic under it",
                &["tenant-a.billing"],
                &["tenant-a.billing"],
            ),
            ("an unfrozen topic is left out", &["events"], &[]),
            (
                "an internal topic is never frozen",
                &["__consumer_offsets"],
                &[],
            ),
            (
                "one request mixing all of them keeps only the covered names",
                &["orders", "tenant-a.billing", "events"],
                &["orders", "tenant-a.billing"],
            ),
        ];

        for (label, names, want) in cases {
            let topics: Vec<_> = names.iter().map(|name| topic(name, &[0])).collect();
            let expected: HashSet<String> = want.iter().map(|name| (*name).to_owned()).collect();
            check!(
                frozen_topics(&image, &topics, &HashSet::new()) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn frozen_topics_is_empty_on_a_cluster_with_no_freeze() {
        let image = image_with_freezes(&[], crate::test_support::FrozenTopicsImageSetup::default());
        let topics = [topic("orders", &[0]), topic("tenant-a.billing", &[1])];

        check!(frozen_topics(&image, &topics, &HashSet::new()) == HashSet::new());
    }

    #[test]
    fn topic_refusal_ranks_the_acl_deny_above_the_freeze() {
        let denied = maplit::hashset! {"denied".to_string(), "both".to_string()};
        let frozen = maplit::hashset! {"frozen".to_string(), "both".to_string()};

        for (label, name, want) in [
            ("a topic under neither refusal proceeds", "clean", None),
            (
                "a denied topic reports the ACL deny",
                "denied",
                Some(codes::TOPIC_AUTHORIZATION_FAILED),
            ),
            (
                "a frozen topic reports the freeze",
                "frozen",
                Some(codes::POLICY_VIOLATION),
            ),
            (
                "a denied and frozen topic never leaks the freeze state",
                "both",
                Some(codes::TOPIC_AUTHORIZATION_FAILED),
            ),
        ] {
            check!(topic_refusal(name, &denied, &frozen) == want, "{label}");
        }
    }

    /// Allows every operation except `Write` on a `Topic`. The
    /// transactional-id preamble passes, so a test that installs it reaches
    /// the per-topic gates that follow.
    #[derive(Debug)]
    struct DenyTopicWrites;

    krabka_macros::test_authorizer! { DenyTopicWrites, req; {
        if req.resource_type == ResourceType::Topic && req.operation == AclOperation::Write {
            AuthorizationResult::Deny
        } else {
            AuthorizationResult::Allow
        }
    }}

    async fn wait_until(label: &str, mut ready: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !ready() {
            assert!(std::time::Instant::now() <= deadline, "timed out: {label}");
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    /// A broker that coordinates every transactional id, holds one open
    /// transaction under [`TID`], and carries one live freeze in its image.
    ///
    /// `transaction_state_num_partitions = 1` puts every tid on partition 0,
    /// so the coordinator check passes and the freeze gate behind it runs.
    async fn start_frozen_coordinator(
        authorizer: Arc<dyn crate::authorizer::Authorizer>,
        freeze: (&str, PatternType),
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let (handle, dir) = crate::test_support::start_broker_no_audit_with(move |cfg| {
            crate::test_support::configure_single_partition_transactions(cfg, authorizer);
        })
        .await;
        let broker = handle.broker_arc_for_test();
        wait_until("the broker becomes controller leader", || {
            broker
                .controller
                .watch_leader()
                .borrow()
                .is_some_and(|node| node == broker.config.node_id)
        })
        .await;
        wait_until("the broker registers itself", || {
            broker.controller.current_image().brokers().next().is_some()
        })
        .await;
        handle.wait_until_transaction_coordinator_ready().await;
        // The two topics exist, so the existence check lets them through to
        // the freeze gate.
        crate::txn::handlers::add_partitions_to_txn::test_support::seed_topic(
            &broker,
            FROZEN_TOPIC,
            2,
        )
        .await;
        crate::txn::handlers::add_partitions_to_txn::test_support::seed_topic(
            &broker,
            UNFROZEN_TOPIC,
            1,
        )
        .await;
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1TopicFreeze(freeze_record(
                freeze.0, freeze.1,
            ))])
            .await
            .expect("submit the freeze");
        wait_until("__transaction_state-0 becomes local", || {
            broker
                .partitions
                .get(crate::txn::bootstrap::TOPIC, PartitionIndex(0))
                .is_some()
        })
        .await;
        let image = broker.controller.current_image();
        drop(
            broker
                .txn_coordinator
                .refresh_leader_partitions(&image)
                .await,
        );
        assert!(
            broker
                .txn_coordinator
                .wait_for_load(PartitionIndex(0), std::time::Duration::from_secs(30))
                .await
                == Some(crate::txn::coordinator::leadership::LoadStatus::Loaded),
            "__transaction_state-0 loads"
        );
        let txnv = crate::txn::version::resolve_txn_version(&image);
        broker
            .txn_coordinator
            .put(
                TxnEntry::new_empty(TID.to_owned(), krabka_log::ProducerId(11), 2, 30_000, 0),
                txnv,
            )
            .await
            .expect("seed the open transaction");
        (handle, dir)
    }

    /// The two-topic request every freeze case sends, in the shape the given
    /// wire version carries it.
    fn freeze_case_request(version: i16) -> AddPartitionsToTxnRequest {
        let topics = vec![topic(FROZEN_TOPIC, &[0, 1]), topic(UNFROZEN_TOPIC, &[0])];
        if version >= 4 {
            AddPartitionsToTxnRequest {
                transactions: vec![AddPartitionsToTxnTransaction {
                    transactional_id: TID.into(),
                    producer_id: 11,
                    producer_epoch: 2,
                    verify_only: false,
                    topics,
                    ..Default::default()
                }],
                ..Default::default()
            }
        } else {
            AddPartitionsToTxnRequest {
                v3_and_below_transactional_id: TID.into(),
                v3_and_below_producer_id: 11,
                v3_and_below_producer_epoch: 2,
                v3_and_below_topics: topics,
                ..Default::default()
            }
        }
    }

    /// The topic rows a response carries, whichever half of the wire format
    /// the version puts them in.
    fn topic_rows(
        resp: &AddPartitionsToTxnResponse,
        version: i16,
    ) -> Vec<AddPartitionsToTxnTopicResult> {
        if version >= 4 {
            resp.results_by_transaction
                .iter()
                .flat_map(|txn| txn.topic_results.clone())
                .collect()
        } else {
            resp.results_by_topic_v3_and_below.clone()
        }
    }

    async fn anonymous_freeze_request(
        broker: &crate::broker::Broker,
        version: i16,
    ) -> AddPartitionsToTxnResponse {
        test_ctx!(ctx, "ANONYMOUS");
        handle(broker, freeze_case_request(version), version, &ctx)
            .await
            .expect("handle")
    }

    #[tokio::test]
    async fn handle_refuses_every_partition_row_of_a_frozen_topic_and_adds_the_unfrozen_one() {
        // (label, wire version, freeze scope, pattern type). A prefix scope
        // covering the topic has to refuse exactly what a literal one does,
        // on both the v0-3 and the v4+ path.
        for (label, version, scope, pattern_type) in [
            (
                "v3, a literal freeze",
                3,
                FROZEN_TOPIC,
                PatternType::Literal,
            ),
            ("v3, a prefix freeze", 3, "tenant-a.", PatternType::Prefixed),
            (
                "v4, a literal freeze",
                4,
                FROZEN_TOPIC,
                PatternType::Literal,
            ),
            ("v4, a prefix freeze", 4, "tenant-a.", PatternType::Prefixed),
        ] {
            let (broker_handle, _dir) = start_frozen_coordinator(
                Arc::new(crate::authorizer::AllowAllAuthorizer),
                (scope, pattern_type),
            )
            .await;
            let broker = broker_handle.broker_arc_for_test();
            let resp = anonymous_freeze_request(&broker, version).await;

            let expected = vec![
                topic_result(
                    FROZEN_TOPIC,
                    &[(0, codes::POLICY_VIOLATION), (1, codes::POLICY_VIOLATION)],
                ),
                topic_result(UNFROZEN_TOPIC, &[(0, codes::NONE)]),
            ];
            check!(topic_rows(&resp, version) == expected, "{label}");

            // The refusal is the enlistment itself and not only the code: no
            // partition of the frozen topic joins the transaction, while the
            // unfrozen one does.
            let entry_mutex = broker.txn_coordinator.get(TID).expect("open transaction");
            let enrolled: HashSet<String> = entry_mutex
                .lock()
                .await
                .partitions
                .iter()
                .map(|tp| tp.topic.clone())
                .collect();
            check!(
                enrolled == maplit::hashset! {UNFROZEN_TOPIC.to_string()},
                "{label}"
            );

            broker_handle.shutdown().await;
        }
    }

    #[tokio::test]
    async fn handle_tells_an_unauthorized_principal_it_is_unauthorized_and_not_that_it_is_frozen() {
        // Only a client version checks topic `Write`. A v4+ request comes from
        // a broker that holds `ClusterAction`, and it checks no topic ACL.
        for (label, version) in [("v3", 3)] {
            let (broker_handle, _dir) = start_frozen_coordinator(
                Arc::new(DenyTopicWrites),
                (FROZEN_TOPIC, PatternType::Literal),
            )
            .await;
            let broker = broker_handle.broker_arc_for_test();
            let resp = anonymous_freeze_request(&broker, version).await;

            // Both topics report the ACL deny. The frozen one reports 29 and
            // not 44: its freeze state never reaches a caller with no right
            // to read it.
            let expected = vec![
                topic_result(
                    FROZEN_TOPIC,
                    &[
                        (0, codes::TOPIC_AUTHORIZATION_FAILED),
                        (1, codes::TOPIC_AUTHORIZATION_FAILED),
                    ],
                ),
                topic_result(UNFROZEN_TOPIC, &[(0, codes::TOPIC_AUTHORIZATION_FAILED)]),
            ];
            check!(topic_rows(&resp, version) == expected, "{label}");

            broker_handle.shutdown().await;
        }
    }
}
