//! The fingerprint of a configured approver set.
//!
//! Every break-glass audit event carries this hash, so an auditor sees which
//! approver set a broker held when it made a decision. The hash covers the
//! members alone, and not the order an operator wrote them in.

use {
    sha2::{Digest as _, Sha256},
    std::collections::BTreeSet,
};

/// SHA-256 hex fingerprint of a configured approver set.
///
/// The approver set comes from each broker's own `broker.toml`, so two brokers
/// can legitimately disagree during a rolling config change. Every break-glass
/// audit event records this fingerprint, which makes the disagreement visible
/// after the fact.
///
/// The input is sorted and de-duplicated first, and each name is
/// length-prefixed, so the fingerprint depends on the members alone: not on the
/// order an operator wrote them in, and not on where one name ends and the next
/// begins.
#[must_use]
pub fn approver_set_fingerprint(approvers: &[String]) -> String {
    let unique: BTreeSet<&str> = approvers.iter().map(String::as_str).collect();
    let mut hasher = Sha256::new();
    for approver in unique {
        let len = u32::try_from(approver.len()).unwrap_or(u32::MAX);
        hasher.update(len.to_be_bytes());
        hasher.update(approver.as_bytes());
    }
    hex::encode(hasher.finalize())
}
