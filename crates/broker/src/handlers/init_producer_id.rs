//! `InitProducerId` (`api_key=22`). This handler hands out
//! `(producer_id, producer_epoch)` to a producer, or it initialises /
//! re-initialises a transactional producer.
//!
//! Non-transactional path: idempotent-producer support.
//! Transactional path:     coordinator routing.
//!
//! ## ACL preamble
//!
//! Two distinct authorize gates branch off `req.transactional_id`, matching
//! Kafka's `KafkaApis.handleInitProducerIdRequest`: any non-null id, empty
//! string included, is a transactional id, and only `None` is idempotent-only.
//!
//! * `Some(_)` (empty or not) → `Write` on
//!   `TransactionalId(transactional_id)`. Deny →
//!   `error_code = TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)`. An empty id
//!   that passes this check still allocates no producer id: the transaction
//!   coordinator rejects it with `error_code = INVALID_REQUEST (42)`.
//! * `None` (idempotent-only producer) → `IdempotentWrite` on
//!   `Cluster("kafka-cluster")`, OR `Write` on at least one `Topic` resource
//!   pattern (`authorize_by_resource_type`). Deny of both →
//!   `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`.

use {
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::owned::{
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse,
    },
    krabka_units::convert::TimeExt as _,
};

mod identity;
mod transactional;

#[cfg(test)]
mod tests;

use {
    self::transactional::handle_transactional,
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult},
        codes,
        error::BrokerError,
        replicator_supervisor::materialize_partition,
        txn::coordinator::leadership::LoadStatus,
    },
};

/// How long a transactional `InitProducerId` waits for the load of its
/// `__transaction_state` partition before it answers
/// `COORDINATOR_LOAD_IN_PROGRESS`.
const INIT_LOAD_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

context_handler! {
    InitProducerIdRequest => InitProducerIdResponse,
    (broker, req, version, ctx),
    {
        let producer_ids = broker.producer_ids.clone();
        let coord = broker.txn_coordinator.clone();
        let controller = broker.controller.clone();
        let log_dirs = broker.config.all_log_dirs();
        let log_config = broker.config.log_config.clone();
        let log_dir_status = broker.log_dir_status.clone();

        // ── ACL preamble ────────────────────────────────────────
        // Branch on whether this is an idempotent-only or transactional
        // request and gate on the appropriate resource/operation.
        {
            let image = controller.current_image();
            let authorizer = broker.config.authorizer.as_ref();
            if let Some(tid) = req.transactional_id.as_deref() {
                // Any non-null id, empty string included, is transactional.
                let acl_req = AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::TransactionalId,
                    resource_name: tid,
                    operation: AclOperation::Write,
                };
                if authorizer.authorize(&*image, &acl_req) == AuthorizationResult::Deny {
                    return Ok(refusal(
                        version,
                        codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                    ));
                }
                // KIP-939: `enable_2pc` also needs `TwoPhaseCommit` on the id.
                // Kafka checks it here, before the identity and 2PC-config checks.
                if req.enable2_pc {
                    let two_pc_req = AuthorizationRequest {
                        operation: AclOperation::TwoPhaseCommit,
                        ..acl_req
                    };
                    if authorizer.authorize(&*image, &two_pc_req) == AuthorizationResult::Deny {
                        return Ok(refusal(
                            version,
                            codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                        ));
                    }
                }
            } else {
                // Idempotent-only producer: cluster-wide IdempotentWrite, or
                // Write on at least one topic resource pattern (KIP-599).
                let cluster_req = AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::Cluster,
                    resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                    operation: AclOperation::IdempotentWrite,
                };
                let cluster_allowed =
                    authorizer.authorize(&*image, &cluster_req) == AuthorizationResult::Allow;
                let topic_write_allowed = cluster_allowed
                    || authorizer.authorize_by_resource_type(
                        &*image,
                        ctx.principal,
                        ctx.peer,
                        ResourceType::Topic,
                        AclOperation::Write,
                    ) == AuthorizationResult::Allow;
                if !topic_write_allowed {
                    return Ok(refusal(version, codes::CLUSTER_AUTHORIZATION_FAILED));
                }
            }
        }

        // Kafka `KafkaApis.handleInitProducerIdRequest`: a request carries both
        // halves of the producer identity or neither, whatever the transactional
        // id, and the check runs before the coordinator sees the request.
        if half_identity(req.producer_id, req.producer_epoch) {
            return Ok(refusal(version, codes::INVALID_REQUEST));
        }

        let resp = match req.transactional_id.as_deref() {
            None => {
                // Non-transactional path (idempotence). Kafka's
                // `TransactionCoordinator.handleInitProducerId` answers a failed
                // `generateProducerId` with its error code, which is
                // `COORDINATOR_LOAD_IN_PROGRESS` while no block is ready, and
                // keeps the connection.
                let (pid, epoch) = match producer_ids.allocate().await {
                    Ok(identity) => identity,
                    Err(error) => {
                        let error = BrokerError::from(error);
                        tracing::warn!(%error, "InitProducerId: no producer ID available");
                        return Ok(refusal(version, codes::from_broker_error(&error)));
                    }
                };
                InitProducerIdResponse {
                    throttle_time_ms: 0,
                    error_code: codes::NONE,
                    // Unwrap the allocated `ProducerId` into the raw-`i64` wire field.
                    producer_id: pid.get(),
                    producer_epoch: epoch,
                    ..Default::default()
                }
            }
            Some("") => {
                // An empty transactional id passes the Write-on-TransactionalId("")
                // ACL check above like any other transactional id, but the
                // transaction coordinator itself rejects the empty id outright.
                // No producer id is allocated either way.
                return Ok(refusal(version, codes::INVALID_REQUEST));
            }
            Some(tid) => {
                // Refresh the coordinator's leader-partition view from the
                // current metadata image. This is a cheap idempotent read,
                // and it ensures we don't race with the replicator-supervisor
                // loop when a `FindCoordinator(TRANSACTION)` call that
                // triggered `__transaction_state` bootstrap just happened.
                let image = controller.current_image();
                let txnv = crate::txn::version::resolve_txn_version(&image);

                // ── KIP-939 two-phase-commit gate ───────────────────────────
                // Kafka's `TransactionCoordinator.handleInitProducerId` gates 2PC
                // on the broker config `transaction.two.phase.commit.enable`, and
                // `KafkaApis` on the `TwoPhaseCommit` ACL, checked in the preamble
                // above. No `transaction.version` level is involved: Kafka's
                // `TransactionVersion` stops at `TV_2`. These checks run before the
                // coordinator-ness check, in Kafka's order. A cluster with 2PC
                // disabled answers TRANSACTIONAL_ID_AUTHORIZATION_FAILED (not an
                // UNSUPPORTED_*), so a client can't probe the config.
                let two_phase_commit = broker.config.features.transaction_two_phase_commit_enable;
                if req.enable2_pc && !two_phase_commit {
                    return Ok(refusal(
                        version,
                        codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                    ));
                }
                // Kafka 4.3.1's `TransactionCoordinator.handleInitProducerId`
                // answers every `keepPreparedTxn` with UNSUPPORTED_VERSION, after
                // the 2PC gate above, because it has not implemented the recovery
                // (trunk still has not). krabka does implement it, and serves it
                // only where the operator opted into both 2PC and Kafka's unstable
                // api versions; otherwise the answer is Kafka's. Without
                // `unstable.api.versions.enable` the v6 request cannot reach this
                // handler at all, so the second condition only matters to a
                // future path that does.
                let prepared_txn_recovery = two_phase_commit
                    && broker.config.features.unstable_api_versions
                        == crate::api_catalog::UnstableApiVersions::Enabled;
                if req.keep_prepared_txn && !prepared_txn_recovery {
                    return Ok(refusal(version, codes::UNSUPPORTED_VERSION));
                }
                if req.keep_prepared_txn && (req.producer_id != -1 || req.producer_epoch != -1) {
                    return Ok(refusal(version, codes::INVALID_REQUEST));
                }
                // Kafka validates the timeout before the coordinator lookup, so a
                // broker that does not coordinate the id answers the same code.
                let txn_timeout = match crate::txn::two_pc::resolve_txn_timeout(
                    req.enable2_pc,
                    req.transaction_timeout_ms,
                    broker.config.transaction_max_timeout.millis_i32(),
                ) {
                    Ok(timeout) => timeout,
                    Err(error_code) => return Ok(refusal(version, error_code)),
                };
                drop(coord.refresh_leader_partitions(&image).await);
                let txn_partition = coord.partition_for(tid);
                if coord.load_status(txn_partition).await == Some(LoadStatus::Pending) {
                    // This broker leads the `__transaction_state` partition, but
                    // its log is not open yet. The replicator supervisor opens it
                    // asynchronously, and this request can race it when
                    // `FindCoordinator` just created the topic. Open it here, and
                    // refresh again so the load starts. `materialize_partition`
                    // uses `DashMap::entry()` to check and insert atomically, so
                    // two concurrent calls cannot both spawn a writer task.
                    materialize_partition(crate::replicator_supervisor::MaterializePartitionConfig {
                        partitions: &coord.partitions,
                        topic: crate::txn::bootstrap::TOPIC,
                        topic_id: None,
                        partition: txn_partition.get(),
                        log_dirs: &log_dirs,
                        log_config: &log_config,
                        log_dir_status: &log_dir_status,
                        producer_state: &broker.producer_state,
                        runtime: crate::partition::PartitionRuntimeConfig::from_broker(
                            &broker.config,
                            false,
                            (None, None, None),
                        ),
                    })
                    .map_err(BrokerError::Txn)?;
                    drop(coord.refresh_leader_partitions(&image).await);
                }
                // The load of a partition that was just elected usually takes
                // milliseconds. `InitProducerId` is the first coordinator call of
                // a producer, so it waits a short time for that load before it
                // answers `COORDINATOR_LOAD_IN_PROGRESS`. Later calls do not wait.
                coord.wait_for_load(txn_partition, INIT_LOAD_WAIT).await;
                if let Some(error_code) = coord.coordinator_error(tid).await {
                    return Ok(refusal(version, error_code));
                }
                let handled = handle_transactional(
                    &coord,
                    tid,
                    txnv,
                    txn_timeout,
                    req.enable2_pc,
                    req.keep_prepared_txn,
                    // KIP-360: the identity the caller believes it holds. It
                    // is `(-1, -1)` below v3 and for a first initialisation.
                    (req.producer_id, req.producer_epoch),
                )
                .await;
                match handled {
                    Ok(response) => response,
                    // A `__transaction_state` write that did not commit answers
                    // the coordinator error Kafka's `appendTransactionToLog`
                    // answers for it, which the client retries.
                    Err(BrokerError::TransactionStateWriteUncommitted { code, .. }) => {
                        tracing::warn!(
                            tid,
                            error_code = code,
                            "InitProducerId: the state write did not commit"
                        );
                        return Ok(refusal(version, code));
                    }
                    // An append that ends in a newer coordinator term fails. Kafka
                    // answers the coordinator error in that case.
                    Err(error) => match coord.coordinator_error(tid).await {
                        Some(error_code) => {
                            tracing::warn!(tid, %error, error_code, "InitProducerId: coordinator term changed");
                            return Ok(refusal(version, error_code));
                        }
                        // `generateProducerId` failed for a new or rotated
                        // producer id. Kafka answers its error code.
                        None if matches!(error, BrokerError::ProducerIdBlockUnavailable(_)) => {
                            tracing::warn!(tid, %error, "InitProducerId: no producer ID available");
                            return Ok(refusal(version, codes::from_broker_error(&error)));
                        }
                        None => return Err(error),
                    },
                }
            }
        };

        Ok(downgrade_producer_fenced(resp, version))
    }
}

/// Whether exactly one half of the `(producer_id, producer_epoch)` pair is
/// its `-1` sentinel, which Kafka answers with `INVALID_REQUEST`.
fn half_identity(producer_id: i64, producer_epoch: i16) -> bool {
    (producer_id == -1) != (producer_epoch == -1)
}

/// Kafka `KafkaApis.handleInitProducerIdRequest`: a client below version 4
/// does not know `PRODUCER_FENCED`, so it gets `INVALID_PRODUCER_EPOCH`.
fn downgrade_producer_fenced(
    mut response: InitProducerIdResponse,
    version: i16,
) -> InitProducerIdResponse {
    if version < 4 && response.error_code == codes::PRODUCER_FENCED {
        response.error_code = codes::INVALID_PRODUCER_EPOCH;
    }
    response
}

/// The response that refuses the request with `error_code`.
fn refusal(version: i16, error_code: i16) -> InitProducerIdResponse {
    let resp = InitProducerIdResponse {
        throttle_time_ms: 0,
        error_code,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    };
    downgrade_producer_fenced(resp, version)
}
