//! The rules that turn one approval or one withdrawal into the next proposal
//! record.
//!
//! Nothing here reads or writes the metadata log. The answer is a function of
//! the stored record, this broker's policy, its operator-key trust set, and
//! the attempt, so a caller can replay it and reach the same record.

use {
    crate::{
        break_glass::{
            action_name, config::BreakGlassPolicy, handlers::Refusal,
            signing::approval_signing_bytes,
        },
        codes,
        operator_keys::OperatorKeys,
    },
    krabka_metadata::{BreakGlassApproval, BreakGlassProposalRecord},
};

/// What one caller asked to do to one proposal.
pub(crate) struct Attempt<'a> {
    /// The authenticated principal of the connection.
    pub principal: &'a str,
    /// The operator key that made `signature`, or an empty string.
    pub key_id: &'a str,
    /// The detached signature, or an empty slice.
    pub signature: &'a [u8],
    /// `true` withdraws the proposal instead of approving it.
    pub withdraw: bool,
    /// The controller's clock, in epoch milliseconds.
    pub now_ms: i64,
}

/// The proposal record that one approval or one withdrawal produces.
///
/// The result is the stored record with one approval appended, or with
/// `withdrawn` set. The caller writes it to the metadata log, where
/// [`MetadataImage::validate`](krabka_metadata::MetadataImage::validate)
/// refuses it if another approval landed first and this record does not extend
/// that list.
///
/// A withdrawal ignores `key_id` and `signature`. The proposer or any
/// configured approver may withdraw, because a withdrawal only takes authority
/// away and can never create any.
///
/// A caller that supplies a signature gets it verified even when the action
/// needs no signature. The broker never stores a signature it did not check.
/// So an `Ok` result with a non-empty `key_id` is proof that the signature
/// verified.
///
/// # Errors
///
/// Returns [`Refusal`] with:
///
/// - `POLICY_VIOLATION` (44) when the proposal is expired, withdrawn, or
///   consumed.
/// - `BREAK_GLASS_NOT_AN_APPROVER` (1008) when the principal is outside the
///   approver set.
/// - `BREAK_GLASS_DUPLICATE_APPROVER` (1007) when the principal proposed the
///   action, or already approved it.
/// - `OPERATOR_SIGNATURE_REQUIRED` (1010) when the action needs a signature and
///   the request carries none.
/// - `OPERATOR_SIGNATURE_INVALID` (1009) when the signature does not verify
///   against the trusted key set under the approving principal.
pub(crate) fn decide(
    policy: BreakGlassPolicy<'_>,
    keys: &OperatorKeys,
    stored: &BreakGlassProposalRecord,
    attempt: &Attempt<'_>,
) -> Result<BreakGlassProposalRecord, Refusal> {
    settled_state(stored)?;
    if attempt.withdraw {
        if !policy.is_approver(attempt.principal) && attempt.principal != stored.proposer {
            return Err(not_an_approver(attempt.principal));
        }
        return Ok(BreakGlassProposalRecord {
            withdrawn: true,
            ..stored.clone()
        });
    }
    if attempt.now_ms >= stored.expires_at_ms {
        return Err(Refusal::new(
            codes::POLICY_VIOLATION,
            format!(
                "break-glass proposal {} expired at {}",
                stored.proposal_id, stored.expires_at_ms
            ),
        ));
    }
    if !policy.is_approver(attempt.principal) {
        return Err(not_an_approver(attempt.principal));
    }
    if attempt.principal == stored.proposer {
        return Err(Refusal::new(
            codes::BREAK_GLASS_DUPLICATE_APPROVER,
            format!(
                "{} proposed this action and cannot also approve it",
                attempt.principal
            ),
        ));
    }
    if stored
        .approvals
        .iter()
        .any(|approval| approval.principal == attempt.principal)
    {
        return Err(Refusal::new(
            codes::BREAK_GLASS_DUPLICATE_APPROVER,
            format!("{} already approved this proposal", attempt.principal),
        ));
    }
    check_signature(policy, keys, stored, attempt)?;

    let mut approvals = stored.approvals.clone();
    approvals.push(BreakGlassApproval {
        principal: attempt.principal.to_owned(),
        approved_at_ms: attempt.now_ms,
        key_id: attempt.key_id.to_owned(),
        signature: attempt.signature.to_vec(),
    });
    Ok(BreakGlassProposalRecord {
        approvals,
        ..stored.clone()
    })
}

/// Refuse a principal that this broker does not know as an approver.
fn not_an_approver(principal: &str) -> Refusal {
    Refusal::new(
        codes::BREAK_GLASS_NOT_AN_APPROVER,
        format!("{principal} is not a break-glass approver"),
    )
}

/// Refuse a proposal that already reached the end of its lifecycle.
///
/// A withdrawal and an approval both stop here. The image validator refuses the
/// record either way, and a refusal that names the state is a better answer to
/// an operator than a rejected raft append.
fn settled_state(stored: &BreakGlassProposalRecord) -> Result<(), Refusal> {
    if stored.withdrawn {
        return Err(Refusal::new(
            codes::POLICY_VIOLATION,
            format!("break-glass proposal {} is withdrawn", stored.proposal_id),
        ));
    }
    if stored.consumed_at_ms != 0 {
        return Err(Refusal::new(
            codes::POLICY_VIOLATION,
            format!(
                "break-glass proposal {} was consumed at {}",
                stored.proposal_id, stored.consumed_at_ms
            ),
        ));
    }
    Ok(())
}

/// Check the signature that an approval carries, or the absence of one.
fn check_signature(
    policy: BreakGlassPolicy<'_>,
    keys: &OperatorKeys,
    stored: &BreakGlassProposalRecord,
    attempt: &Attempt<'_>,
) -> Result<(), Refusal> {
    let unsigned = attempt.key_id.is_empty() && attempt.signature.is_empty();
    if unsigned {
        if policy.needs_signature(stored.action) {
            return Err(Refusal::new(
                codes::OPERATOR_SIGNATURE_REQUIRED,
                format!(
                    "an approval of {} needs a detached operator signature",
                    action_name(stored.action)
                ),
            ));
        }
        return Ok(());
    }
    let message = approval_signing_bytes(stored);
    if keys.verify(
        attempt.key_id,
        attempt.principal,
        &message,
        attempt.signature,
    ) {
        Ok(())
    } else {
        Err(Refusal::new(
            codes::OPERATOR_SIGNATURE_INVALID,
            "the operator signature did not verify",
        ))
    }
}
