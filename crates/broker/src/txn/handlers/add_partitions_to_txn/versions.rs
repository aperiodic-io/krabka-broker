//! The two request shapes of `AddPartitionsToTxn` and the authorization each
//! of them runs.
//!
//! v0-3 carries a single transaction inline on the request and answers with
//! `results_by_topic_v3_and_below`. It comes from a client, which needs
//! `Write` on the transactional id and on each topic. v4-5 carries a
//! `transactions` array and answers with `results_by_transaction`. It comes
//! from a broker, which [`super::handle`] has already checked for
//! `ClusterAction`, and it runs no client check. Below the authorization
//! the work is identical, so both paths funnel into
//! [`process_one_txn`](super::registration::process_one_txn).

use {
    super::{
        authz::{TopicAuthorization, failed_partitions},
        registration::{TransactionRequest, process_one_txn},
        results::{dedup_topics, topic_error},
        write_freeze::frozen_topics,
    },
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        codes,
        config::BrokerConfig,
        txn::{coordinator::TxnCoordinator, version::TxnVersion},
    },
    krabka_metadata::{AclOperation, MetadataImage, ResourceType},
    krabka_protocol::owned::{
        add_partitions_to_txn_request::AddPartitionsToTxnRequest,
        add_partitions_to_txn_response::{AddPartitionsToTxnResponse, AddPartitionsToTxnResult},
        common::{
            add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
            add_partitions_to_txn_response::add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
        },
    },
    krabka_security::Principal,
    std::{
        collections::{HashMap, HashSet},
        net::SocketAddr,
    },
};

/// The request-independent collaborators both version paths need: the
/// coordinator they drive, the metadata image the checks read, the
/// negotiated transaction version, and the caller's identity.
#[derive(Clone, Copy)]
pub(super) struct HandlerDependencies<'a> {
    pub(super) coord: &'a TxnCoordinator,
    pub(super) image: &'a MetadataImage,
    pub(super) txnv: TxnVersion,
    pub(super) authorizer: &'a dyn Authorizer,
    pub(super) principal: &'a Principal,
    pub(super) peer: &'a SocketAddr,
    /// The broker config, which names the internal topics a client can never
    /// add.
    pub(super) config: &'a BrokerConfig,
}

/// One transaction of a request, in the form both version paths share.
struct Transaction<'a> {
    transactional_id: &'a str,
    producer_id: i64,
    producer_epoch: i16,
    /// The partitions to check and add.
    topics: &'a [AddPartitionsToTxnTopic],
    /// The request topics an answer that carries one code for the whole
    /// transaction lists, as sent. See
    /// [`TransactionRequest::response_topics`].
    response_topics: &'a [AddPartitionsToTxnTopic],
    verify_only: bool,
}

/// Runs the checks of Kafka's `KafkaApis.handleAddPartitionsToTxnRequest` for
/// one transaction, then hands it to the coordinator.
///
/// A client (v0-3) needs `Write` on the transactional id, else every
/// partition answers `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`. Then every
/// partition must be authorized and exist, else the whole transaction fails
/// and nothing is added.
async fn process_transaction(
    dependencies: &HandlerDependencies<'_>,
    client: bool,
    txn: &Transaction<'_>,
    version: i16,
) -> Vec<AddPartitionsToTxnTopicResult> {
    let &HandlerDependencies {
        coord,
        image,
        txnv,
        authorizer,
        principal,
        peer,
        config,
    } = dependencies;
    // Kafka checks and adds a set of partitions, so a request that names a
    // topic or partition twice checks and adds it once, and an answer keyed
    // by partition -- a failed check, or a verify-only answer -- lists it
    // once (#883). Deduping here, before either authorization check, means
    // the ACL sweep, the freeze gate, and the coordinator call all work from
    // the collapsed list. An answer that carries one code for the whole
    // transaction lists `txn.response_topics` instead.
    let topics = dedup_topics(txn.topics);
    let authorization = if client {
        let tid_req = AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::TransactionalId,
            resource_name: txn.transactional_id,
            operation: AclOperation::Write,
        };
        if authorizer.authorize(image, &tid_req) == AuthorizationResult::Deny {
            return topic_error(
                txn.response_topics,
                codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            );
        }
        TopicAuthorization::Client {
            authorizer,
            principal,
            peer,
            config,
        }
    } else {
        TopicAuthorization::Broker
    };
    if let Some(rows) = failed_partitions(image, authorization, &topics) {
        return rows;
    }
    let authorized = HashSet::new();
    let frozen = frozen_topics(image, &topics, &authorized);
    process_one_txn(coord, TransactionRequest {
        transactional_id: txn.transactional_id,
        producer_id: krabka_log::ProducerId(txn.producer_id),
        producer_epoch: txn.producer_epoch,
        topics: &topics,
        response_topics: txn.response_topics,
        denied: &authorized,
        frozen: &frozen,
        txnv,
        verify_only: txn.verify_only,
        version,
    })
    .await
}

// ── v4+ path ─────────────────────────────────────────────────────────────────

pub(super) async fn handle_v4(
    dependencies: &HandlerDependencies<'_>,
    version: i16,
    req: &AddPartitionsToTxnRequest,
) -> AddPartitionsToTxnResponse {
    // A request that names one transactional id twice is answered the way
    // Kafka 4.3.1's `KafkaApis.handleAddPartitionsToTxnRequest` answers it
    // (#883). `TransactionalId` is a `mapKey`, but the generated collections
    // are multi-collections that keep every entry, and the handler walks
    // every request entry and adds one result for each. Each entry runs with
    // its own producer id, epoch and `verify_only`. The partitions it checks
    // and adds come from `partitionsByTransaction()`, a map the last entry
    // for the id overwrote, and an answer carrying one code lists the topics
    // of the first entry, the one `errorResponseForTransaction` finds.
    let mut added_topics: HashMap<&str, &[AddPartitionsToTxnTopic]> = HashMap::new();
    let mut answered_topics: HashMap<&str, &[AddPartitionsToTxnTopic]> = HashMap::new();
    for txn in &req.transactions {
        added_topics.insert(txn.transactional_id.as_str(), &txn.topics);
        answered_topics
            .entry(txn.transactional_id.as_str())
            .or_insert(&txn.topics);
    }

    let mut results_by_transaction = Vec::with_capacity(req.transactions.len());
    for txn in &req.transactions {
        let transactional_id = txn.transactional_id.as_str();
        let topic_results = process_transaction(
            dependencies,
            false,
            &Transaction {
                transactional_id,
                producer_id: txn.producer_id,
                producer_epoch: txn.producer_epoch,
                topics: added_topics[transactional_id],
                response_topics: answered_topics[transactional_id],
                verify_only: txn.verify_only,
            },
            version,
        )
        .await;
        results_by_transaction.push(AddPartitionsToTxnResult {
            transactional_id: txn.transactional_id.clone(),
            topic_results,
            ..Default::default()
        });
    }

    AddPartitionsToTxnResponse {
        results_by_transaction,
        ..Default::default()
    }
}

// ── v0-3 path ─────────────────────────────────────────────────────────────────

pub(super) async fn handle_v3(
    dependencies: &HandlerDependencies<'_>,
    version: i16,
    req: &AddPartitionsToTxnRequest,
) -> AddPartitionsToTxnResponse {
    let topic_results = process_transaction(
        dependencies,
        true,
        &Transaction {
            transactional_id: req.v3_and_below_transactional_id.as_str(),
            producer_id: req.v3_and_below_producer_id,
            producer_epoch: req.v3_and_below_producer_epoch,
            topics: &req.v3_and_below_topics,
            response_topics: &req.v3_and_below_topics,
            // v0-3 has no `verify_only` field (predates KIP-890); always add.
            verify_only: false,
        },
        version,
    )
    .await;

    AddPartitionsToTxnResponse {
        results_by_topic_v3_and_below: topic_results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            test_support::{
                ControllerPeerAllowed, DenyAll,
                start_broker_with_authorizer_no_audit as start_broker, test_ctx,
            },
            txn::handlers::add_partitions_to_txn::{
                handle,
                test_support::{
                    CoordinatorBroker, PartitionCode, PartitionSpec, TopicRows, TopicSpec,
                    enlisted, partition_set, seed_transaction, start_coordinator, topic,
                    topic_result,
                },
            },
        },
        assert2::assert,
        krabka_protocol::owned::add_partitions_to_txn_request::AddPartitionsToTxnTransaction,
        std::sync::Arc,
    };

    krabka_macros::wire_codec_helpers! {
        test_context ["producer-client"];
    }

    /// A v4+ request comes from a broker, so a principal without
    /// `ClusterAction` gets Kafka's top-level `CLUSTER_AUTHORIZATION_FAILED`
    /// and no transaction row.
    #[tokio::test]
    async fn handle_v4_without_cluster_action_returns_the_top_level_error() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        test_ctx!(ctx, "ANONYMOUS");
        let req = AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: "tid-4".into(),
                producer_id: 11,
                producer_epoch: 2,
                verify_only: false,
                topics: vec![topic("alpha", &[1, 2])],
                ..Default::default()
            }],
            ..Default::default()
        };
        let resp = handle(&broker_handle.broker_arc_for_test(), req, 4, &ctx)
            .await
            .expect("handle");

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            results_by_transaction: vec![],
            results_by_topic_v3_and_below: vec![],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    struct Entry {
        epoch: i16,
        verify_only: bool,
        topics: &'static [TopicSpec],
    }
    fn entry(epoch: i16, verify_only: bool, topics: &'static [TopicSpec]) -> Entry {
        Entry {
            epoch,
            verify_only,
            topics,
        }
    }
    /// The topic rows one entry answers.
    type Rows = &'static [TopicRows];
    struct Case {
        name: &'static str,
        entries: [Entry; 2],
        /// Per entry, the rows it answers.
        results: [Rows; 2],
        /// The partitions in the transaction after the request.
        enlisted: &'static [PartitionSpec],
    }
    const EPOCH: i16 = 2;
    const NONE: i16 = codes::NONE;
    const FENCED: i16 = codes::PRODUCER_FENCED;
    const UNKNOWN: i16 = codes::UNKNOWN_TOPIC_OR_PARTITION;
    const NOT_ATTEMPTED: i16 = codes::OPERATION_NOT_ATTEMPTED;

    const ONLY_A: &[TopicSpec] = &[TopicSpec {
        name: "a",
        partitions: &[0],
    }];
    const ONLY_B: &[TopicSpec] = &[TopicSpec {
        name: "b",
        partitions: &[0],
    }];
    const B_AND_MISSING: &[TopicSpec] = &[
        TopicSpec {
            name: "b",
            partitions: &[0],
        },
        TopicSpec {
            name: "missing",
            partitions: &[0],
        },
    ];
    const A_NONE: Rows = &[TopicRows {
        name: "a",
        rows: &[PartitionCode {
            partition: 0,
            code: NONE,
        }],
    }];
    const B_NONE: Rows = &[TopicRows {
        name: "b",
        rows: &[PartitionCode {
            partition: 0,
            code: NONE,
        }],
    }];
    const A_FENCED: Rows = &[TopicRows {
        name: "a",
        rows: &[PartitionCode {
            partition: 0,
            code: FENCED,
        }],
    }];
    const B_NOT_ATTEMPTED_MISSING_UNKNOWN: Rows = &[
        TopicRows {
            name: "b",
            rows: &[PartitionCode {
                partition: 0,
                code: NOT_ATTEMPTED,
            }],
        },
        TopicRows {
            name: "missing",
            rows: &[PartitionCode {
                partition: 0,
                code: UNKNOWN,
            }],
        },
    ];
    const ENLISTED_B: &[PartitionSpec] = &[PartitionSpec {
        topic: "b",
        partition: 0,
    }];

    /// The cases of the repeated transactional id: two entries each, the rows
    /// each answers, and the partitions enlisted afterwards.
    fn repeated_id_cases() -> [Case; 4] {
        [
            Case {
                name: "both entries add the second entry's partitions",
                entries: [entry(EPOCH, false, ONLY_A), entry(EPOCH, false, ONLY_B)],
                results: [A_NONE, A_NONE],
                enlisted: ENLISTED_B,
            },
            Case {
                name: "each entry is checked with its own producer epoch",
                entries: [entry(EPOCH - 1, false, ONLY_A), entry(EPOCH, false, ONLY_B)],
                results: [A_FENCED, A_NONE],
                enlisted: ENLISTED_B,
            },
            Case {
                name: "a failed partition check lists the checked partitions",
                entries: [
                    entry(EPOCH, false, ONLY_A),
                    entry(EPOCH, false, B_AND_MISSING),
                ],
                results: [
                    B_NOT_ATTEMPTED_MISSING_UNKNOWN,
                    B_NOT_ATTEMPTED_MISSING_UNKNOWN,
                ],
                enlisted: &[],
            },
            Case {
                name: "the verify-only entry verifies what the add entry added",
                entries: [entry(EPOCH, false, ONLY_A), entry(EPOCH, true, ONLY_B)],
                results: [A_NONE, B_NONE],
                enlisted: ENLISTED_B,
            },
        ]
    }

    /// #883: a v4+ request that names one transactional id twice is answered
    /// the way Kafka 4.3.1's `KafkaApis.handleAddPartitionsToTxnRequest`
    /// answers it. The generated collections keep every entry, so each entry
    /// gets its own result, run with its own producer epoch and
    /// `verify_only`. Each adds the partitions of the *last* entry for the id
    /// (`partitionsByTransaction()` is a map the last entry overwrote), and a
    /// one-code answer lists the topics of the *first* entry (the one
    /// `errorResponseForTransaction` finds).
    #[tokio::test]
    async fn handle_v4_answers_every_entry_of_a_repeated_transactional_id() {
        let cases = repeated_id_cases();

        let CoordinatorBroker {
            handle: broker_handle,
            dir: _dir,
        } = start_coordinator(Arc::new(ControllerPeerAllowed(DenyAll))).await;
        let broker = broker_handle.broker_arc_for_test();
        test_ctx!(ctx, "ANONYMOUS");
        for (index, case) in (0_i64..).zip(&cases) {
            let tid = format!("dup-tid-{index}");
            let producer_id = 100 + index;
            seed_transaction(&broker, &tid, producer_id).await;
            let req = AddPartitionsToTxnRequest {
                transactions: case
                    .entries
                    .iter()
                    .map(|entry| AddPartitionsToTxnTransaction {
                        transactional_id: tid.clone(),
                        producer_id,
                        producer_epoch: entry.epoch,
                        verify_only: entry.verify_only,
                        topics: entry
                            .topics
                            .iter()
                            .map(|&TopicSpec { name, partitions }| topic(name, partitions))
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };

            let resp = handle(&broker, req, 4, &ctx).await.expect("handle");

            let expected = AddPartitionsToTxnResponse {
                results_by_transaction: case
                    .results
                    .iter()
                    .map(|rows| AddPartitionsToTxnResult {
                        transactional_id: tid.clone(),
                        topic_results: rows
                            .iter()
                            .map(|&TopicRows { name, rows }| topic_result(name, rows))
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            assert!(resp == expected, "{}", case.name);
            assert!(
                enlisted(&broker, &tid).await == partition_set(case.enlisted),
                "{}",
                case.name
            );
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_v3_transactional_id_deny_returns_topic_rows() {
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        test_ctx!(ctx, "ANONYMOUS");
        let req = AddPartitionsToTxnRequest {
            v3_and_below_transactional_id: "tid-3".into(),
            v3_and_below_producer_id: 11,
            v3_and_below_producer_epoch: 2,
            v3_and_below_topics: vec![topic("alpha", &[3, 4])],
            ..Default::default()
        };
        let resp = handle(&broker_handle.broker_arc_for_test(), req, 3, &ctx)
            .await
            .expect("handle");

        let expected = AddPartitionsToTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            results_by_transaction: vec![],
            results_by_topic_v3_and_below: vec![topic_result("alpha", &[
                PartitionCode {
                    partition: 3,
                    code: codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                },
                PartitionCode {
                    partition: 4,
                    code: codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                },
            ])],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
