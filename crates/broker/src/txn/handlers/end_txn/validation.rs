//! The checks `EndTxn` runs before it touches the transaction: the ACL on the
//! transactional id, coordinator ownership, and the producer identity and state
//! the coordinator entry holds. The outcome is either an entry to finalise, the
//! already-complete answer for an idempotent retry, or a Kafka error code.

use krabka_log::ProducerId;
use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
use krabka_protocol::owned::end_txn_request::EndTxnRequest;

use super::state_table::{EndTxnDecision, end_txn_decision};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
    codes,
    handlers::RequestContext,
    txn::{coordinator::TxnCoordinator, state::TxnEntry, version::TxnVersion},
};

pub(super) enum EndTxnValidation {
    /// Finalise the transaction. `no_partition_added` marks Kafka's
    /// `prepareAbortOrCommit(..., noPartitionAdded = true)`.
    Proceed {
        entry: std::sync::Arc<tokio::sync::Mutex<TxnEntry>>,
        no_partition_added: bool,
    },
    /// Answer `NONE` with this identity and write nothing.
    AlreadyComplete(ProducerId, i16),
}

pub(super) async fn validate_end_txn(
    coordinator: &TxnCoordinator,
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    context: &RequestContext<'_>,
    request: &EndTxnRequest,
    (client_version, trunk_rules): (TxnVersion, bool),
) -> Result<EndTxnValidation, i16> {
    let transactional_id = request.transactional_id.as_str();
    let authorization = AuthorizationRequest {
        principal: context.principal,
        host: context.peer,
        resource_type: ResourceType::TransactionalId,
        resource_name: transactional_id,
        operation: AclOperation::Write,
    };
    if authorizer.authorize(image, &authorization) == AuthorizationResult::Deny {
        return Err(codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    }
    // Kafka `endTransaction` refuses an empty transactional id before it looks
    // the coordinator up.
    if transactional_id.is_empty() {
        return Err(codes::INVALID_REQUEST);
    }
    if let Some(code) = coordinator.coordinator_error(transactional_id).await {
        return Err(code);
    }
    // A leadership change can unload this partition between the check above
    // and the lookup below. Recheck the coordinator status before answering
    // INVALID_PRODUCER_ID_MAPPING, so a client retries a failover with the
    // retriable COORDINATOR_LOAD_IN_PROGRESS or NOT_COORDINATOR instead of
    // seeing its valid transaction reported as unknown.
    let Some(entry) = coordinator.get(transactional_id) else {
        return Err(coordinator.missing_entry_error(transactional_id).await);
    };
    let decision = {
        let state = entry.lock().await;
        end_txn_decision(
            &state,
            (ProducerId(request.producer_id), request.producer_epoch),
            request.committed,
            (client_version.verified(), trunk_rules),
        )
    };
    match decision {
        EndTxnDecision::Prepare {
            no_partition_added, ..
        } => Ok(EndTxnValidation::Proceed {
            entry,
            no_partition_added,
        }),
        EndTxnDecision::AlreadyComplete => {
            let state = entry.lock().await;
            Ok(EndTxnValidation::AlreadyComplete(
                state.producer_id,
                state.producer_epoch,
            ))
        }
        EndTxnDecision::Refuse(code) => Err(code),
    }
}
