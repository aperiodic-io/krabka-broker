//! End-to-end tests for the `InitProducerId` handler entry point.
//!
//! They drive a live broker, because the transactional path only becomes
//! reachable once the coordinator owns the `__transaction_state` partition for
//! the transactional id. KIP-939 needs the broker config
//! `transaction.two.phase.commit.enable`, not a `transaction.version` level.
//! Keeping them out of the module root leaves the request flow readable.

use {
    super::{handle as init_producer_id_handle, identity::ProducerIdentity, *},
    crate::{
        api_catalog::UnstableApiVersions,
        authorizer::SimpleAclAuthorizer,
        broker::{Broker, BrokerHandle},
        error::BrokerError,
        handlers::{
            RequestContext as HandlerRequestContext, context::RequestContext,
            find_coordinator::handle as find_coordinator_handle,
        },
        test_support::{
            ControllerPeerAllowed, peer, principal, start_broker_no_audit,
            start_broker_no_audit_with, start_broker_with_authorizer_no_audit,
            wait_for_controller_leader,
        },
        txn::{
            coordinator::TxnCoordinator,
            handlers::end_txn::handle as end_txn_handle,
            state::{TxnEntry, TxnState},
            version::TxnVersion,
        },
    },
    assert2::assert,
    krabka_metadata::{
        AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
    },
    krabka_units::secs,
    std::{collections::HashSet, sync::Arc},
};

/// Persist the held transaction as ongoing after releasing its entry lock.
async fn persist_ongoing(
    coordinator: &TxnCoordinator,
    cell: &Arc<tokio::sync::Mutex<TxnEntry>>,
    expectation: &str,
) -> ProducerIdentity {
    let (producer_id, producer_epoch, snapshot) = {
        let mut entry = cell.lock().await;
        entry.state = TxnState::Ongoing;
        (entry.producer_id, entry.producer_epoch, entry.clone())
    };
    coordinator
        .put(snapshot, TxnVersion::Verified)
        .await
        .expect(expectation);
    ProducerIdentity {
        id: producer_id,
        epoch: producer_epoch,
    }
}

/// Waits for the controller, and checks the cluster finalized `TV_2`, the
/// highest `transaction.version` Kafka defines, which a self-bootstrapped
/// broker finalizes. KIP-939 needs no higher level.
async fn wait_for_transaction_version_2(broker_handle: &BrokerHandle) {
    wait_for_controller_leader(&broker_handle.broker_arc_for_test()).await;
    broker_handle
        .wait_for_image(|image| {
            image.finalized_feature(
                krabka_metadata::transaction_version::TRANSACTION_VERSION_FEATURE,
            ) == Some(krabka_metadata::transaction_version::TRANSACTION_VERSION_MAX)
        })
        .await;
    let image = broker_handle
        .broker_arc_for_test()
        .controller
        .current_image();
    assert!(
        crate::txn::version::resolve_txn_version(&image)
            == crate::txn::version::TxnVersion::Verified
    );
}

/// The timeout table of Kafka's `validateTransactionTimeoutMs`, checked
/// against a live broker whose `transaction.max.timeout.ms` is 8 s.
async fn check_timeout_answers(
    broker: &std::sync::Arc<Broker>,
    context: &HandlerRequestContext<'_>,
    tids: [&str; 4],
) {
    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    for (tid, requested_ms, enable_2pc, expected) in [
        (tids[0], 500, false, Ok(500)),
        (
            tids[1],
            10_000,
            false,
            Err(codes::INVALID_TRANSACTION_TIMEOUT),
        ),
        (tids[2], 500, true, Ok(i32::MAX)),
        (tids[3], 0, false, Err(codes::INVALID_TRANSACTION_TIMEOUT)),
    ] {
        let request = InitProducerIdRequest {
            transactional_id: Some(tid.to_string()),
            transaction_timeout_ms: requested_ms,
            enable2_pc: enable_2pc,
            ..Default::default()
        };
        let response = init_producer_id_handle(broker, request, version, context)
            .await
            .expect("initialize transactional producer");
        match expected {
            Ok(expected_ms) => {
                assert!(
                    response
                        == InitProducerIdResponse {
                            producer_id: response.producer_id,
                            producer_epoch: 0,
                            ..Default::default()
                        },
                    "{tid}: {response:?}"
                );
                let entry = broker
                    .txn_coordinator
                    .get(tid)
                    .expect("persisted transaction entry");
                assert!(entry.lock().await.txn_timeout_ms == expected_ms, "{tid}");
            }
            Err(error_code) => {
                assert!(
                    response
                        == InitProducerIdResponse {
                            error_code,
                            producer_id: -1,
                            producer_epoch: -1,
                            ..Default::default()
                        },
                    "{tid}: {response:?}"
                );
                assert!(broker.txn_coordinator.get(tid).is_none(), "{tid}");
            }
        }
    }
}

macro_rules! transaction_v2_broker {
    (($handle:ident, $directory:ident, $broker:ident)) => {
        broker_fixture!(
            ($handle, $directory, $broker),
            start_broker_no_audit_with(|config| {
                config.transaction_state_num_partitions = 7;
                config.transaction_max_timeout = secs(8);
                config.features.transaction_two_phase_commit_enable = true;
                config.features.unstable_api_versions =
                    crate::api_catalog::UnstableApiVersions::Enabled;
            })
        );
    };
}

#[tokio::test]
async fn handler_refuses_a_timeout_kafka_refuses_and_stores_the_rest_as_sent() {
    transaction_v2_broker!((broker_handle, _dir, broker));
    request_identity!(
        (principal, peer, context),
        principal("admin"),
        client_id = "txn-client"
    );
    let tids = ["txn-small", "txn-above-max", "txn-2pc", "txn-zero"];

    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    wait_for_transaction_version_2(&broker_handle).await;
    broker_handle
        .wait_until_transaction_coordinator_ready()
        .await;

    let find_version = krabka_protocol::owned::find_coordinator_response::MAX_VERSION;
    let find_request = krabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest {
        key_type: 1,
        coordinator_keys: tids.iter().map(ToString::to_string).collect(),
        ..Default::default()
    };
    let find_response = find_coordinator_handle(&broker, find_request, find_version, &context)
        .await
        .expect("find transaction coordinators");
    assert!(
        find_response
            .coordinators
            .iter()
            .all(|coordinator| coordinator.error_code == codes::NONE)
    );

    // Kafka `validateTransactionTimeoutMs`: a timeout above
    // `transaction.max.timeout.ms`, or one that is not positive, is refused.
    // Every other value is stored as the client sent it, and 2PC stores the
    // sentinel whatever the request asks for.
    check_timeout_answers(&broker, &context, tids).await;

    let ongoing = broker
        .txn_coordinator
        .get(tids[2])
        .expect("2PC transaction entry");
    let ProducerIdentity {
        id: ongoing_pid,
        epoch: ongoing_epoch,
    } = persist_ongoing(
        &broker.txn_coordinator,
        &ongoing,
        "persist ongoing 2PC transaction",
    )
    .await;

    let recovery_request = InitProducerIdRequest {
        transactional_id: Some(tids[2].to_string()),
        transaction_timeout_ms: 500,
        enable2_pc: true,
        keep_prepared_txn: true,
        ..Default::default()
    };
    let recovery_response =
        init_producer_id_handle(&broker, recovery_request.clone(), version, &context)
            .await
            .expect("recover prepared transaction");
    assert!(recovery_response.error_code == codes::NONE);
    assert!(recovery_response.ongoing_txn_producer_id == ongoing_pid.get());
    assert!(recovery_response.ongoing_txn_producer_epoch == ongoing_epoch);

    let second_recovery_response =
        init_producer_id_handle(&broker, recovery_request, version, &context)
            .await
            .expect("recover prepared transaction again");
    assert!(second_recovery_response.error_code == codes::NONE);
    assert!(second_recovery_response.producer_id == recovery_response.producer_id);
    assert!(second_recovery_response.producer_epoch == recovery_response.producer_epoch + 1);
    assert!(second_recovery_response.ongoing_txn_producer_id == ongoing_pid.get());
    assert!(second_recovery_response.ongoing_txn_producer_epoch == ongoing_epoch);

    let end_version = krabka_protocol::owned::end_txn_response::MAX_VERSION;
    let fenced_end_request = krabka_protocol::owned::end_txn_request::EndTxnRequest {
        transactional_id: tids[2].to_string(),
        producer_id: recovery_response.producer_id,
        producer_epoch: recovery_response.producer_epoch,
        committed: true,
        ..Default::default()
    };
    let fenced_end_response = end_txn_handle(&broker, fenced_end_request, end_version, &context)
        .await
        .expect("reject fenced recovery client");
    // Kafka `endTransaction` fences a stale identity with PRODUCER_FENCED at
    // `EndTxn` v2 and above.
    assert!(
        fenced_end_response
            == unthrottled_wire!(krabka_protocol::owned::end_txn_response::EndTxnResponse {
                error_code: codes::PRODUCER_FENCED,
                producer_id: -1,
                producer_epoch: -1,
            })
    );

    let end_request = krabka_protocol::owned::end_txn_request::EndTxnRequest {
        transactional_id: tids[2].to_string(),
        producer_id: second_recovery_response.producer_id,
        producer_epoch: second_recovery_response.producer_epoch,
        committed: true,
        ..Default::default()
    };
    let end_response = end_txn_handle(&broker, end_request.clone(), end_version, &context)
        .await
        .expect("complete recovered transaction");
    assert!(
        end_response
            == unthrottled_wire!(krabka_protocol::owned::end_txn_response::EndTxnResponse {
                error_code: codes::NONE,
                producer_id: second_recovery_response.producer_id,
                producer_epoch: second_recovery_response.producer_epoch + 1,
            })
    );

    let retry_response = end_txn_handle(&broker, end_request, end_version, &context)
        .await
        .expect("retry recovered transaction completion");
    assert!(retry_response == end_response);
    let completed = broker
        .txn_coordinator
        .get(tids[2])
        .expect("completed 2PC transaction entry");
    let completed = completed.lock().await;
    assert!(completed.state == TxnState::CompleteCommit);
    assert!(completed.next_producer_id.is_none());
    assert!(completed.next_producer_epoch == -1);
    broker_handle.shutdown().await;
}

/// krabka-io/krabka-broker#784: Kafka 4.3.1 gates KIP-939 on
/// `transaction.two.phase.commit.enable`, not on a `transaction.version`
/// level. With the config off (its default),
/// `TransactionCoordinator.handleInitProducerId` answers `enable2Pc` with
/// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` and `keepPreparedTxn` with
/// `UNSUPPORTED_VERSION`. At `TV_2`, with the config on, neither is refused on
/// version grounds, and a broker that coordinates nothing answers the
/// coordinator check.
#[tokio::test]
async fn kip939_fields_follow_the_two_phase_commit_config_at_transaction_version_2() {
    use crate::api_catalog::UnstableApiVersions::{Disabled, Enabled};
    let refused = |error_code| InitProducerIdResponse {
        error_code,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    };
    // `keepPreparedTxn` is refused as Kafka 4.3.1 refuses it unless 2PC and
    // `unstable.api.versions.enable` are both on (#784).
    for (two_phase_commit, unstable, enable_2pc, keep_prepared_txn, want) in [
        (
            false,
            Enabled,
            true,
            false,
            refused(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
        ),
        (
            false,
            Enabled,
            false,
            true,
            refused(codes::UNSUPPORTED_VERSION),
        ),
        (true, Disabled, true, false, refused(codes::NOT_COORDINATOR)),
        (
            true,
            Disabled,
            false,
            true,
            refused(codes::UNSUPPORTED_VERSION),
        ),
        (true, Enabled, true, false, refused(codes::NOT_COORDINATOR)),
        (true, Enabled, false, true, refused(codes::NOT_COORDINATOR)),
    ] {
        let (broker_handle, _dir) = start_broker_no_audit_with(|config| {
            config.features.transaction_two_phase_commit_enable = two_phase_commit;
            config.features.unstable_api_versions = unstable;
        })
        .await;
        wait_for_transaction_version_2(&broker_handle).await;
        let broker = broker_handle.broker_arc_for_test();
        request_identity!(
            (principal, peer, context),
            principal("admin"),
            client_id = "txn-client"
        );
        let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
        // No FindCoordinator ran, so this broker coordinates nothing.
        let request = InitProducerIdRequest {
            transactional_id: Some("txn-tv2".to_string()),
            transaction_timeout_ms: 500,
            enable2_pc: enable_2pc,
            keep_prepared_txn,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("answer the KIP-939 request");
        assert!(
            response == want,
            "config {two_phase_commit}, {unstable:?}, enable2Pc {enable_2pc}, \
             keepPreparedTxn {keep_prepared_txn}: {response:?}"
        );
        broker_handle.shutdown().await;
    }
}

#[tokio::test]
async fn keep_prepared_txn_without_enable_2pc_preserves_finite_timeout() {
    transaction_v2_broker!((broker_handle, _dir, broker));
    wait_for_transaction_version_2(&broker_handle).await;
    broker_handle
        .wait_until_transaction_coordinator_ready()
        .await;
    request_identity!(
        (principal, peer, context),
        principal("admin"),
        client_id = "txn-client"
    );
    let tid = "txn-recover-finite";

    let response = find_transaction_coordinator(&broker, &context, tid)
        .await
        .expect("find transaction coordinator");
    assert!(response.coordinators[0].error_code == codes::NONE);

    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    let request = InitProducerIdRequest {
        transactional_id: Some(tid.to_string()),
        transaction_timeout_ms: 500,
        ..Default::default()
    };
    let response = init_producer_id_handle(&broker, request, version, &context)
        .await
        .expect("initialize finite-timeout transaction");
    assert!(response.error_code == codes::NONE);

    let finite = broker
        .txn_coordinator
        .get(tid)
        .expect("finite-timeout transaction entry");
    let ProducerIdentity {
        id: finite_pid,
        epoch: finite_epoch,
    } = persist_ongoing(
        &broker.txn_coordinator,
        &finite,
        "persist finite ongoing transaction",
    )
    .await;

    let recovery_request = InitProducerIdRequest {
        transactional_id: Some(tid.to_string()),
        transaction_timeout_ms: 500,
        keep_prepared_txn: true,
        ..Default::default()
    };
    let response = init_producer_id_handle(&broker, recovery_request, version, &context)
        .await
        .expect("recover finite-timeout transaction without enable2Pc");
    assert!(response.error_code == codes::NONE);
    assert!(response.ongoing_txn_producer_id == finite_pid.get());
    assert!(response.ongoing_txn_producer_epoch == finite_epoch);
    assert!(finite.lock().await.txn_timeout_ms == 500);
    broker_handle.shutdown().await;
}

/// Kafka validates the timeout in `TransactionCoordinator.handleInitProducerId`
/// before it looks the coordinator up, so a broker that does not coordinate the
/// id answers `INVALID_TRANSACTION_TIMEOUT` too.
#[tokio::test]
async fn the_timeout_check_runs_before_the_coordinator_check() {
    let (broker_handle, _dir) =
        start_broker_no_audit_with(|config| config.transaction_max_timeout = secs(8)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_transaction_version_2(&broker_handle).await;
    request_identity!(
        (principal, peer, context),
        principal("admin"),
        client_id = "txn-client"
    );
    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    // No FindCoordinator ran, so `__transaction_state` does not exist and this
    // broker coordinates nothing.
    let tid = "txn-no-coordinator";

    let answers = [
        (
            "a valid timeout reaches the coordinator check",
            5_000,
            codes::NOT_COORDINATOR,
        ),
        (
            "an invalid timeout answers before it",
            9_000,
            codes::INVALID_TRANSACTION_TIMEOUT,
        ),
    ];
    for (name, requested_ms, expected) in answers {
        let request = InitProducerIdRequest {
            transactional_id: Some(tid.to_string()),
            transaction_timeout_ms: requested_ms,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("initialize transactional producer");
        assert!(
            response
                == InitProducerIdResponse {
                    error_code: expected,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                },
            "{name}: {response:?}"
        );
    }
    broker_handle.shutdown().await;
}

/// Kafka `KafkaApis.handleInitProducerIdRequest` refuses half an identity with
/// `INVALID_REQUEST`, and answers a client below version 4 with
/// `INVALID_PRODUCER_EPOCH` in place of `PRODUCER_FENCED`.
#[tokio::test]
async fn half_an_identity_is_invalid_and_an_old_client_gets_invalid_producer_epoch() {
    let (broker_handle, _dir) = start_broker_no_audit().await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_transaction_version_2(&broker_handle).await;
    broker_handle
        .wait_until_transaction_coordinator_ready()
        .await;
    request_identity!(
        (principal, peer, context),
        principal("admin"),
        client_id = "txn-client"
    );
    let tid = "txn-half-identity";
    let max = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    let found = find_transaction_coordinator(&broker, &context, tid)
        .await
        .expect("find the transaction coordinator");
    assert!(found.coordinators[0].error_code == codes::NONE);
    // An entry to be fenced against.
    let created = init_producer_id_handle(
        &broker,
        InitProducerIdRequest {
            transactional_id: Some(tid.to_string()),
            transaction_timeout_ms: 60_000,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        },
        max,
        &context,
    )
    .await
    .expect("create the transaction entry");
    assert!(created.error_code == codes::NONE, "{created:?}");
    let zombie = (created.producer_id + 1_000, created.producer_epoch);

    // (name, version, transactional id, request identity, expected code)
    let cases = [
        (
            "a producer id with no epoch",
            max,
            Some(tid),
            (7, -1),
            codes::INVALID_REQUEST,
        ),
        (
            "an epoch with no producer id",
            max,
            Some(tid),
            (-1, 3),
            codes::INVALID_REQUEST,
        ),
        (
            "an idempotent producer with an epoch and no producer id",
            max,
            None,
            (-1, 5),
            codes::INVALID_REQUEST,
        ),
        (
            "an idempotent producer with a producer id and no epoch",
            max,
            None,
            (42, -1),
            codes::INVALID_REQUEST,
        ),
        (
            "an empty transactional id with half an identity",
            max,
            Some(""),
            (42, -1),
            codes::INVALID_REQUEST,
        ),
        (
            "a fenced identity at the newest version",
            max,
            Some(tid),
            zombie,
            codes::PRODUCER_FENCED,
        ),
        (
            "a fenced identity below version 4",
            3,
            Some(tid),
            zombie,
            codes::INVALID_PRODUCER_EPOCH,
        ),
    ];
    let mut expected = Vec::new();
    let mut actual = Vec::new();
    for (name, version, transactional_id, (producer_id, producer_epoch), code) in cases {
        let request = InitProducerIdRequest {
            transactional_id: transactional_id.map(str::to_string),
            transaction_timeout_ms: 60_000,
            producer_id,
            producer_epoch,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("initialize transactional producer");
        expected.push((name, InitProducerIdResponse {
            throttle_time_ms: 0,
            error_code: code,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        }));
        actual.push((name, response));
    }
    assert!(actual == expected);
    broker_handle.shutdown().await;
}

/// A stripped-down [`AclEntry`] builder for the ACL-preamble table below: only
/// the resource type/name/pattern, operation, and Allow/Deny vary per row.
fn acl(
    permission_type: PermissionType,
    resource_type: ResourceType,
    resource_name: &str,
    pattern_type: PatternType,
    operation: AclOperation,
) -> AclEntry {
    AclEntry {
        resource_type,
        resource_name: resource_name.into(),
        pattern_type,
        principal: "User:alice".into(),
        host: "*".into(),
        operation,
        permission_type,
    }
}

/// #685: the null-transactional-id and empty-transactional-id branches of the
/// `InitProducerId` ACL preamble, matching Kafka's
/// `KafkaApis.handleInitProducerIdRequest`.
///
/// Neither branch reaches the transaction coordinator (null allocates
/// directly; empty is rejected before dispatch), so each case starts its own
/// broker with only the authorizer configured, with no coordinator bring-up.
#[tokio::test]
async fn acl_preamble_for_null_and_empty_transactional_id() {
    struct Case {
        label: &'static str,
        transactional_id: Option<&'static str>,
        acls: Vec<AclEntry>,
        expected_error: i16,
    }

    let cases = [
        Case {
            label: "null id: cluster IdempotentWrite allows",
            transactional_id: None,
            acls: vec![acl(
                PermissionType::Allow,
                ResourceType::Cluster,
                crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                PatternType::Literal,
                AclOperation::IdempotentWrite,
            )],
            expected_error: codes::NONE,
        },
        Case {
            label: "null id: literal Write on one topic allows",
            transactional_id: None,
            acls: vec![acl(
                PermissionType::Allow,
                ResourceType::Topic,
                "orders",
                PatternType::Literal,
                AclOperation::Write,
            )],
            expected_error: codes::NONE,
        },
        Case {
            label: "null id: prefixed Write on topics allows",
            transactional_id: None,
            acls: vec![acl(
                PermissionType::Allow,
                ResourceType::Topic,
                "ord",
                PatternType::Prefixed,
                AclOperation::Write,
            )],
            expected_error: codes::NONE,
        },
        Case {
            label: "null id: topic Write fully covered by a Deny on Topic \"*\" denies",
            transactional_id: None,
            acls: vec![
                acl(
                    PermissionType::Allow,
                    ResourceType::Topic,
                    "orders",
                    PatternType::Literal,
                    AclOperation::Write,
                ),
                acl(
                    PermissionType::Deny,
                    ResourceType::Topic,
                    "*",
                    PatternType::Literal,
                    AclOperation::Write,
                ),
            ],
            expected_error: codes::CLUSTER_AUTHORIZATION_FAILED,
        },
        Case {
            label: "null id: Read (not Write) on a topic denies",
            transactional_id: None,
            acls: vec![acl(
                PermissionType::Allow,
                ResourceType::Topic,
                "orders",
                PatternType::Literal,
                AclOperation::Read,
            )],
            expected_error: codes::CLUSTER_AUTHORIZATION_FAILED,
        },
        Case {
            label: "empty id: no ACLs denies with TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
            transactional_id: Some(""),
            acls: vec![],
            expected_error: codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        },
        Case {
            label: "empty id: Write on TransactionalId \"*\" passes the ACL gate, \
                     but the coordinator still rejects the empty id",
            transactional_id: Some(""),
            acls: vec![acl(
                PermissionType::Allow,
                ResourceType::TransactionalId,
                "*",
                PatternType::Literal,
                AclOperation::Write,
            )],
            expected_error: codes::INVALID_REQUEST,
        },
    ];

    let mut expected = Vec::new();
    let mut actual = Vec::new();
    for case in cases {
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker_with_authorizer_no_audit(Arc::new(SimpleAclAuthorizer::new(
                HashSet::new()
            ),))
        );
        crate::handlers::acl_test_support::seed_case_acls!(broker, case.acls);

        request_identity!(
            (principal, peer, context),
            principal("alice"),
            client_id = "idempotent-client"
        );
        let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
        let request = InitProducerIdRequest {
            transactional_id: case.transactional_id.map(ToString::to_string),
            transaction_timeout_ms: 60_000,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("handle InitProducerId");

        expected.push((case.label, case.expected_error));
        actual.push((case.label, response.error_code));
        if case.expected_error != codes::NONE {
            assert!(response.producer_id == -1, "{}", case.label);
        }
        broker_handle.shutdown().await;
    }
    assert!(actual == expected);
}

/// #685: `keep_prepared_txn` alone must not run the KIP-939 two-phase-commit
/// gate -- only `enable_2pc` does. Both cases share a Write ACL on the
/// transactional id (which the ACL preamble above already covers) and differ
/// only in which KIP-939 flag is set and whether a `TwoPhaseCommit` ACL
/// exists.
#[tokio::test]
async fn two_phase_commit_gate_is_scoped_to_enable_2pc_not_keep_prepared_txn() {
    struct Case {
        label: &'static str,
        tid: &'static str,
        enable_2pc: bool,
        keep_prepared_txn: bool,
        two_phase_commit_acl: bool,
        // `None` means "any success code (not TRANSACTIONAL_ID_AUTHORIZATION_FAILED)".
        expected_error: Option<i16>,
    }

    let cases = [
        Case {
            label: "keep_prepared_txn alone skips the 2PC ACL/config gate",
            tid: "tx-keep-only",
            enable_2pc: false,
            keep_prepared_txn: true,
            two_phase_commit_acl: false,
            expected_error: None,
        },
        Case {
            label: "enable_2pc without a TwoPhaseCommit ACL is denied",
            tid: "tx-enable-2pc",
            enable_2pc: true,
            keep_prepared_txn: false,
            two_phase_commit_acl: false,
            expected_error: Some(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED),
        },
    ];

    for case in cases {
        let (broker_handle, _dir) = start_broker_no_audit_with(|config| {
            config.transaction_state_num_partitions = 7;
            config.transaction_max_timeout = secs(8);
            config.features.transaction_two_phase_commit_enable = true;
            config.features.unstable_api_versions = UnstableApiVersions::Enabled;
            config.authorizer = Arc::new(ControllerPeerAllowed(SimpleAclAuthorizer::new(
                HashSet::new(),
            )));
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_transaction_version_2(&broker_handle).await;
        broker_handle
            .wait_until_transaction_coordinator_ready()
            .await;

        let mut acls = vec![acl(
            PermissionType::Allow,
            ResourceType::TransactionalId,
            case.tid,
            PatternType::Literal,
            AclOperation::Write,
        )];
        if case.two_phase_commit_acl {
            acls.push(acl(
                PermissionType::Allow,
                ResourceType::TransactionalId,
                case.tid,
                PatternType::Literal,
                AclOperation::TwoPhaseCommit,
            ));
        }
        broker
            .controller
            .submit_change(
                acls.into_iter()
                    .map(MetadataRecord::V1AccessControlEntry)
                    .collect(),
            )
            .await
            .expect("seed acls");

        request_identity!(
            (principal, peer, context),
            principal("alice"),
            client_id = "txn-client"
        );

        let find_response = find_transaction_coordinator(&broker, &context, case.tid)
            .await
            .expect("find transaction coordinator");
        assert!(
            find_response.coordinators[0].error_code == codes::NONE,
            "{}",
            case.label
        );

        let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
        let request = InitProducerIdRequest {
            transactional_id: Some(case.tid.to_string()),
            transaction_timeout_ms: 500,
            enable2_pc: case.enable_2pc,
            keep_prepared_txn: case.keep_prepared_txn,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("handle InitProducerId");

        match case.expected_error {
            Some(code) => assert!(response.error_code == code, "{}", case.label),
            None => assert!(
                response.error_code != codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                "{}: {:?}",
                case.label,
                response
            ),
        }
        broker_handle.shutdown().await;
    }
}

/// #1015: Kafka's `RPCProducerIdManager.generateProducerId` throws
/// `COORDINATOR_LOAD_IN_PROGRESS` while it has no producer ID block, and
/// `TransactionCoordinator.handleInitProducerId` answers that code for an
/// idempotent producer and for a new transactional id alike. The client
/// retries; the broker keeps the connection.
#[tokio::test]
async fn a_failed_block_allocation_answers_coordinator_load_in_progress() {
    let (broker_handle, _dir) = start_broker_no_audit().await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_controller_leader(&broker).await;
    broker_handle
        .wait_until_transaction_coordinator_ready()
        .await;
    request_identity!(
        (principal, peer, context),
        principal("admin"),
        client_id = "txn-client"
    );
    let tid = "txn-no-block";

    let find_response = find_transaction_coordinator(&broker, &context, tid)
        .await
        .expect("find transaction coordinator");
    assert!(
        find_response
            .coordinators
            .iter()
            .all(|coordinator| coordinator.error_code == codes::NONE)
    );

    // The last frontier that cannot fit another block: the controller refuses
    // every allocation from here on.
    let broker_epoch = broker
        .controller
        .current_image()
        .broker_epoch(broker.config.node_id)
        .expect("registered broker epoch");
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1ProducerIds(
            krabka_metadata::ProducerIdsRecord {
                broker_id: broker.config.node_id,
                broker_epoch,
                next_producer_id: i64::MAX - 999,
            },
        )])
        .await
        .expect("seed exhausted producer ID space");

    let version = krabka_protocol::owned::init_producer_id_response::MAX_VERSION;
    let cases = [
        ("idempotent producer", None),
        ("new transactional id", Some(tid)),
    ];
    for (name, transactional_id) in cases {
        let request = InitProducerIdRequest {
            transactional_id: transactional_id.map(ToString::to_string),
            transaction_timeout_ms: 5_000,
            producer_id: -1,
            producer_epoch: -1,
            ..Default::default()
        };
        let response = init_producer_id_handle(&broker, request, version, &context)
            .await
            .expect("answered, not a closed connection");
        assert!(
            response
                == InitProducerIdResponse {
                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                    producer_id: -1,
                    producer_epoch: -1,
                    ..Default::default()
                },
            "{name}"
        );
    }
    broker_handle.shutdown().await;
}

/// Route one transaction id through the real coordinator handler at its latest version.
async fn find_transaction_coordinator(
    broker: &Broker,
    context: &RequestContext<'_>,
    tid: &str,
) -> Result<krabka_protocol::owned::find_coordinator_response::FindCoordinatorResponse, BrokerError>
{
    find_coordinator_handle(
        broker,
        krabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest {
            key_type: 1,
            coordinator_keys: vec![tid.to_owned()],
            ..Default::default()
        },
        krabka_protocol::owned::find_coordinator_response::MAX_VERSION,
        context,
    )
    .await
}
