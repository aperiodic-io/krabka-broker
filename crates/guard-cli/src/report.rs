//! The lines this tool prints for a broker's answer, and the exit code that
//! answer becomes.
//!
//! One broker error code maps to one exit code here, so every subcommand hands
//! a runbook the same number for the same situation. A registry that the tool
//! checked locally reports what the check proved, entry by entry, and reserves
//! the signature exit code for a signature that failed rather than one it could
//! not check.

use {
    super::{
        EXIT_BAD_SIGNATURE, EXIT_MISMATCH, EXIT_NO_APPROVAL, EXIT_REFUSED,
        cli::{action_name, pattern_name},
        verify::{CheckedEntry, VerifyOutcome},
    },
    krabka_protocol::krabka::{break_glass as bg, freeze as api},
};

/// The exit code that one broker error code becomes.
///
/// Three codes get their own number because a runbook acts on them
/// differently. An action that needs an approval sends the operator for a
/// second person. A signature failure sends them to their key material. Every
/// other refusal is a refusal.
#[must_use]
pub fn exit_for_code(code: i16) -> i32 {
    use krabka_broker::codes;

    match code {
        codes::NONE => 0,
        codes::BREAK_GLASS_APPROVAL_REQUIRED => EXIT_NO_APPROVAL,
        codes::OPERATOR_SIGNATURE_INVALID | codes::OPERATOR_SIGNATURE_REQUIRED => {
            EXIT_BAD_SIGNATURE
        }
        _ => EXIT_REFUSED,
    }
}

/// Describe an error code.
///
/// `krabka-broker` has no code-to-name table, so this prints the number and
/// names the krabka-private codes that no Kafka reference lists.
#[must_use]
pub fn code_name(code: i16) -> String {
    use krabka_broker::codes;

    let note = match code {
        codes::BREAK_GLASS_APPROVAL_REQUIRED => {
            Some("no approved break-glass proposal covers this")
        }
        codes::BREAK_GLASS_DUPLICATE_APPROVER => Some("this principal already approved"),
        codes::BREAK_GLASS_NOT_AN_APPROVER => Some("this principal is not a configured approver"),
        codes::OPERATOR_SIGNATURE_INVALID => Some("the operator signature did not verify"),
        codes::OPERATOR_SIGNATURE_REQUIRED => Some("this action needs an operator signature"),
        codes::FREEZE_SCOPE_INVALID => Some("the scope is empty or reaches an internal topic"),
        codes::FREEZE_LIMIT_EXCEEDED => Some("the registry is at freeze.max_entries"),
        _ => None,
    };
    note.map_or_else(
        || format!("error {code}"),
        |note| format!("error {code} ({note})"),
    )
}

/// One error code and its message, as a line.
pub(super) fn described_error(code: i16, message: Option<&str>) -> String {
    format!(
        "{}{}",
        code_name(code),
        message.map_or_else(String::new, |m| format!(": {m}"))
    )
}

/// Print a refusal and turn it into an exit code.
pub(super) fn report_error(code: i16, message: Option<&str>) -> i32 {
    eprintln!("{}", described_error(code, message));
    exit_for_code(code)
}

/// The outcome of one freeze or thaw.
pub(super) fn report_set_freeze(response: &api::SetTopicFreezeResponse, frozen: bool) -> i32 {
    if response.error_code != 0 {
        return report_error(response.error_code, response.error_message.as_deref());
    }
    println!("{}\tok", if frozen { "frozen" } else { "thawed" });
    0
}

/// One line per registry entry.
pub(super) fn print_freeze(freeze: &api::DescribedTopicFreeze, checked: Option<&CheckedEntry>) {
    let proof = match checked.map(|entry| entry.unproved) {
        None => String::new(),
        Some(None) => format!("\tverified by {}", freeze.key_id),
        Some(Some(unproved)) => format!("\t{}", unproved.reason()),
    };
    println!(
        "{}:{}\tset by {} at {}\t{}{proof}",
        pattern_name(freeze.pattern_type),
        freeze.scope,
        freeze.set_by,
        freeze.set_at_ms,
        freeze.reason,
    );
}

/// One block per proposal.
pub(super) fn print_proposal(proposal: &bg::DescribedBreakGlassProposal) {
    let state = if proposal.withdrawn {
        "withdrawn"
    } else if proposal.consumed_at_ms > 0 {
        "consumed"
    } else {
        "open"
    };
    println!(
        "proposal {} {state}",
        uuid::Uuid::from_bytes(proposal.proposal_id.0)
    );
    println!("  action     {}", action_name(proposal.action));
    println!("  target     {}", proposal.target);
    println!("  proposer   {}", proposal.proposer);
    println!("  reason     {}", proposal.reason);
    println!("  created    {}", proposal.created_at_ms);
    println!("  expires    {}", proposal.expires_at_ms);
    for approval in &proposal.approvals {
        println!(
            "  approved   {} at {} {}",
            approval.principal,
            approval.approved_at_ms,
            approval_evidence(approval)
        );
    }
}

/// What one approval offers as evidence of who made it.
///
/// The broker never stores a signature it did not check, so a `key_id` on a
/// stored approval is already proof that the signature verified there. The
/// first bytes of the signature are printed so an auditor can line one approval
/// up against the audit event that carries the whole of it.
fn approval_evidence(approval: &bg::BreakGlassApproval) -> String {
    /// How many leading signature bytes identify one approval in a report.
    const PREFIX: usize = 8;

    if approval.key_id.is_empty() {
        return "unsigned".to_owned();
    }
    format!(
        "signed by {} ({}...)",
        approval.key_id,
        hex::encode(&approval.signature[..approval.signature.len().min(PREFIX)])
    )
}

/// What a local verification of the registry found.
///
/// A signature that failed outranks a key this machine does not hold, because
/// the first says the tool checked and the answer is wrong, and the second says
/// the tool could not check.
pub(super) fn report_verify(freezes: &[api::DescribedTopicFreeze], outcome: &VerifyOutcome) -> i32 {
    for (freeze, checked) in freezes.iter().zip(&outcome.entries) {
        print_freeze(freeze, Some(checked));
    }
    if outcome.any_signature_failed() {
        eprintln!("the registry does not verify against the operator keys on this machine");
        return EXIT_BAD_SIGNATURE;
    }
    if outcome.any_key_is_unknown() {
        eprintln!("the registry names an operator key that this machine does not hold");
        return EXIT_MISMATCH;
    }
    println!(
        "{} of {} entries are proved by an operator signature",
        outcome.proved(),
        outcome.entries.len()
    );
    0
}

#[cfg(test)]
mod tests;
