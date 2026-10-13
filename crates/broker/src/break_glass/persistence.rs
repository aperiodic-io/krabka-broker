//! Durable consumption before privileged actions outside the metadata log.
//!
//! Most break-glass actions put the consumed proposal and the action record in
//! one raft append. `DeleteRecords` and offset-aware unclean recovery perform
//! local work instead, so they commit the consume first and enter here. This
//! module keeps that ordering identical for both callers.

use {
    super::gate::consumed_record_matches,
    crate::broker::Broker,
    krabka_metadata::{BreakGlassAction, MetadataRecord},
    krabka_raft::RaftError,
    krabka_verified::{
        BreakGlassLocalActionDecision, BreakGlassLocalActionFacts, BreakGlassLocalSpendState,
        break_glass_local_action_decision,
    },
    std::collections::HashSet,
    uuid::Uuid,
};

fn proposal_id(record: &MetadataRecord) -> Option<Uuid> {
    match record {
        MetadataRecord::V1BreakGlassProposal(proposal) => Some(proposal.proposal_id),
        _ => None,
    }
}

fn action_decision(
    gated: bool,
    consumed: Option<&MetadataRecord>,
    action: BreakGlassAction,
    target: &str,
    already_committed: bool,
    commit_succeeded: bool,
) -> BreakGlassLocalActionDecision {
    let matches = consumed.is_some_and(|record| consumed_record_matches(record, action, target));
    let spend = if !gated {
        BreakGlassLocalSpendState::Ungated
    } else if !matches {
        BreakGlassLocalSpendState::MissingOrMismatched
    } else if already_committed {
        BreakGlassLocalSpendState::Committed
    } else {
        BreakGlassLocalSpendState::Pending
    };
    break_glass_local_action_decision(BreakGlassLocalActionFacts {
        spend,
        commit_succeeded,
    })
}

/// Commit a matching proposal consumption before a local privileged action.
///
/// A proposal enters `committed` only after `submit_change` reports success.
/// A retry can reuse that durable spend inside the same request. A failed
/// submit leaves the set unchanged, so the next row must submit again.
///
/// # Errors
///
/// Returns a rejection for a missing or mismatched consume on a gated action,
/// or the controller error when the consume did not commit.
pub(crate) async fn spend_before_local_action(
    broker: &Broker,
    committed: &mut HashSet<Uuid>,
    consumed: Option<MetadataRecord>,
    gated: bool,
    action: BreakGlassAction,
    target: &str,
) -> Result<Option<Uuid>, RaftError> {
    let id = consumed.as_ref().and_then(proposal_id);
    let already_committed = id.is_some_and(|id| committed.contains(&id));
    if action_decision(
        gated,
        consumed.as_ref(),
        action,
        target,
        already_committed,
        false,
    ) == BreakGlassLocalActionDecision::Apply
    {
        return Ok(id);
    }
    if !consumed
        .as_ref()
        .is_some_and(|record| consumed_record_matches(record, action, target))
    {
        return Err(RaftError::ChangeRejected(format!(
            "break-glass consume does not authorize {action:?} on {target}"
        )));
    }

    let id = id.expect("a matching consumed proposal record has an id");
    let record = consumed.expect("a matching consumed proposal record is present");
    match broker.controller.submit_change(vec![record.clone()]).await {
        Ok(_) => {
            let decision = action_decision(gated, Some(&record), action, target, false, true);
            if decision != BreakGlassLocalActionDecision::Apply {
                return Err(RaftError::ChangeRejected(
                    "break-glass consume committed without authorizing the local action".to_owned(),
                ));
            }
            committed.insert(id);
            Ok(Some(id))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{BreakGlassLocalActionDecision, action_decision},
        krabka_metadata::{
            BreakGlassAction, BreakGlassApproval, BreakGlassProposalRecord, DeleteTopicRecord,
            MetadataRecord,
        },
    };

    fn consumed(action: BreakGlassAction, target: &str, consumed_at_ms: i64) -> MetadataRecord {
        MetadataRecord::V1BreakGlassProposal(BreakGlassProposalRecord {
            proposal_id: uuid::Uuid::from_u128(7),
            action,
            target: target.to_owned(),
            proposer: "User:alice".to_owned(),
            reason: "incident".to_owned(),
            created_at_ms: 1,
            expires_at_ms: 100,
            approvals: vec![BreakGlassApproval {
                principal: "User:bob".to_owned(),
                approved_at_ms: 2,
                key_id: String::new(),
                signature: Vec::new(),
            }],
            consumed_at_ms,
            withdrawn: false,
        })
    }

    #[test]
    fn adapter_rejects_malformed_stale_and_failed_consumptions() {
        let action = BreakGlassAction::DeleteRecords;
        let target = "orders-3";
        for record in [
            MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: "orders".to_owned(),
            }),
            consumed(action, target, 0),
            consumed(BreakGlassAction::DeleteTopic, target, 10),
            consumed(action, "orders-4", 10),
        ] {
            assert2::check!(
                action_decision(true, Some(&record), action, target, false, true)
                    == BreakGlassLocalActionDecision::Reject
            );
        }
        let valid = consumed(action, target, 10);
        assert2::check!(
            action_decision(true, Some(&valid), action, target, false, false)
                == BreakGlassLocalActionDecision::Reject
        );
    }

    #[test]
    fn adapter_accepts_overflow_boundary_and_durable_retry() {
        let action = BreakGlassAction::DeleteRecords;
        let target = "orders-3";
        let max_timestamp = consumed(action, target, i64::MAX);
        assert2::check!(
            action_decision(true, Some(&max_timestamp), action, target, false, true)
                == BreakGlassLocalActionDecision::Apply
        );
        assert2::check!(
            action_decision(true, Some(&max_timestamp), action, target, true, false)
                == BreakGlassLocalActionDecision::Apply
        );
    }

    #[test]
    fn ungated_action_needs_no_consume() {
        assert2::check!(
            action_decision(
                false,
                None,
                BreakGlassAction::DeleteRecords,
                "orders-3",
                false,
                false,
            ) == BreakGlassLocalActionDecision::Apply
        );
    }
}
