//! The four PLAIN credentials the suite authenticates as, and the principal
//! spelling every KFC-9 surface names them by.
//!
//! Three of the four are in `break_glass.approvers` and the fourth is
//! deliberately outside it, which is what lets the refusal cases tell an
//! authenticated stranger apart from an approver.

/// The PLAIN credentials the listener knows. Each authenticates as
/// `User:<name>`, which is the spelling `approvers` and `[[operator_keys]]`
/// use.
pub(super) const USERS: &[(&str, &str)] = &[
    ("alice", "alice-secret"),
    ("bob", "bob-secret"),
    ("carol", "carol-secret"),
    ("mallory", "mallory-secret"),
];

/// The proposer in every case. She is an approver too, because the broker
/// refuses a proposal from a principal outside the set.
pub(super) const ALICE: &str = "alice";
/// The first approver.
pub(super) const BOB: &str = "bob";
/// The second approver. `required_approvals` is two and a proposer may not
/// approve, so a completed loop needs three people.
pub(super) const CAROL: &str = "carol";
/// Authenticated, and outside `break_glass.approvers`.
pub(super) const MALLORY: &str = "mallory";

/// The principals that may approve. `mallory` is deliberately absent.
pub(super) const APPROVERS: &[&str] = &[ALICE, BOB, CAROL];

/// `principal` in the `KafkaPrincipal` string form every KFC-9 surface uses.
pub(super) fn principal(user: &str) -> String { format!("User:{user}") }
