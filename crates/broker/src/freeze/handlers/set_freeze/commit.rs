//! The one raft append that an accepted `SetTopicFreeze` request commits.
//!
//! The consumed break-glass proposal and the registry record travel in a single
//! `submit_change` call, so an approval and the thaw it authorized reach the
//! metadata log together or not at all.

use krabka_metadata::MetadataRecord;

use super::outcome::{Accepted, Refusal};
use crate::{broker::Broker, codes, handlers::submit_failure_code};

/// Write the accepted records in one raft append.
///
/// The consumed proposal goes first, so the approval and the thaw it authorized
/// commit together.
pub(super) async fn submit(broker: &Broker, accepted: Accepted) -> Result<Accepted, Refusal> {
    let mut records = Vec::with_capacity(2);
    if let Some(proposal) = accepted.consumed_proposal.clone() {
        records.push(proposal);
    }
    records.push(MetadataRecord::V1TopicFreeze(accepted.record.clone()));

    match broker.controller.submit_change(records).await {
        Ok(_) => Ok(accepted),
        Err(error) => {
            tracing::warn!(%error, "set-topic-freeze submit failed");
            Err(Refusal {
                code: submit_failure_code(&error, codes::COORDINATOR_NOT_AVAILABLE),
                message: format!("submit failed: {error}"),
                signature_verified: accepted.signature_verified,
            })
        }
    }
}
