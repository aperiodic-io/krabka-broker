//! The canonical bytes an operator signs, built on the operator's own machine.
//!
//! `--sign-with` takes a PKCS#8 Ed25519 key file and never sends it. This
//! module builds the signed payload here, signs it here, and hands the caller a
//! `key_id` and a detached signature. Those two fields are the only part of the
//! signature that reaches a broker. The private key never leaves the machine
//! that runs the command.
//!
//! # Why the layout is repeated rather than called
//!
//! The broker defines the same two layouts in `freeze::signing` and
//! `break_glass::signing`, and both builders are `pub(crate)` inside
//! `pub(crate)` modules. Nothing outside `krabka-broker` can call either one,
//! so this module reproduces both layouts. Two copies of a byte layout drift
//! silently, so the tests below rebuild every expected vector by hand, field by
//! field, from the layout the broker's module documents. A change on either
//! side that the other does not follow fails a test here rather than an
//! operator's thaw during an incident.
//!
//! # The freeze layout
//!
//! ```text
//! FREEZE_DOMAIN                     b"krabka-topic-freeze-v1\0"
//! cluster_id      u32 big-endian length, then the UTF-8 bytes
//! pattern_type    u8                3 literal, 4 prefixed
//! scope           u32 big-endian length, then the UTF-8 bytes
//! frozen          u8                1 freeze, 0 thaw
//! reason          u32 big-endian length, then the UTF-8 bytes
//! set_by          u32 big-endian length, then the UTF-8 bytes
//! set_at_ms       i64 big-endian
//! proposal_id     16 bytes
//! ```
//!
//! # The break-glass approval layout
//!
//! ```text
//! BREAK_GLASS_DOMAIN                b"krabka-break-glass-v1\0"
//! proposal_id     16 bytes
//! action          u8                the wire value of the action
//! target          u32 big-endian length, then the UTF-8 bytes
//! proposer        u32 big-endian length, then the UTF-8 bytes
//! created_at_ms   i64 big-endian
//! expires_at_ms   i64 big-endian
//! ```
//!
//! An approval signs the proposal that the broker holds, and not a proposal the
//! caller supplies. So `krabka-guard break-glass approve --sign-with` reads the
//! proposal back first and signs what the broker stored. The approvals list is
//! outside the signed bytes on purpose: every approver signs the same payload,
//! and a payload that grew with each approval could never verify twice.

use std::path::Path;

use krabka_audit::FileEd25519Signer;

/// Domain separator for a freeze record signature (KFC-9).
///
/// It differs from every other separator in the workspace, so a signature made
/// for one purpose never verifies as another.
pub const FREEZE_DOMAIN: &[u8] = b"krabka-topic-freeze-v1\0";

/// Domain separator for a break-glass approval signature (KFC-9).
pub const BREAK_GLASS_DOMAIN: &[u8] = b"krabka-break-glass-v1\0";

/// The freeze record fields that a signature covers.
///
/// Three of them answer a named attack. `frozen` is signed, so a signature
/// captured from a freeze cannot be replayed as the thaw. `cluster_id` is
/// signed, so a signed freeze cannot be replayed into a second cluster.
/// `set_at_ms` is signed, so the broker can refuse a timestamp outside its skew
/// window and one that is not newer than the entry it replaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreezeSigningInput<'a> {
    /// The cluster the record acts on, in the string form `DescribeCluster`
    /// gives.
    pub cluster_id: &'a str,
    /// `3` literal, `4` prefixed. Kafka's ACL pattern-type discriminant.
    pub pattern_type: i8,
    /// A literal topic name, or a topic-name prefix.
    pub scope: &'a str,
    /// `true` sets the freeze, `false` lifts it.
    pub frozen: bool,
    /// Free text the operator supplied.
    pub reason: &'a str,
    /// The principal the broker authenticates on the connection, which is the
    /// author the record carries.
    pub set_by: &'a str,
    /// Milliseconds since the Unix epoch, at the moment of signing.
    pub set_at_ms: i64,
    /// The break-glass proposal that authorized a thaw. All zero on a freeze.
    pub proposal_id: [u8; 16],
}

/// The break-glass proposal fields that an approval signature covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApprovalSigningInput<'a> {
    /// The proposal the approval names.
    pub proposal_id: [u8; 16],
    /// The wire value of the gated action.
    pub action: i8,
    /// What the transition applies to.
    pub target: &'a str,
    /// The principal that opened the proposal.
    pub proposer: &'a str,
    /// When the controller opened the proposal.
    pub created_at_ms: i64,
    /// When the proposal stops being usable.
    pub expires_at_ms: i64,
}

/// Build the canonical bytes of a freeze record.
///
/// The layout is the one this module documents, and the broker rebuilds the
/// same bytes to verify.
#[must_use]
pub fn freeze_signing_bytes(input: &FreezeSigningInput<'_>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        FREEZE_DOMAIN.len()
            + 4 * LEN_PREFIX
            + input.cluster_id.len()
            + input.scope.len()
            + input.reason.len()
            + input.set_by.len()
            + 2
            + size_of::<i64>()
            + input.proposal_id.len(),
    );
    bytes.extend_from_slice(FREEZE_DOMAIN);
    put_len_prefixed(&mut bytes, input.cluster_id.as_bytes());
    bytes.push(input.pattern_type.to_be_bytes()[0]);
    put_len_prefixed(&mut bytes, input.scope.as_bytes());
    bytes.push(u8::from(input.frozen));
    put_len_prefixed(&mut bytes, input.reason.as_bytes());
    put_len_prefixed(&mut bytes, input.set_by.as_bytes());
    bytes.extend_from_slice(&input.set_at_ms.to_be_bytes());
    bytes.extend_from_slice(&input.proposal_id);
    bytes
}

/// Build the canonical bytes that an approval of one proposal signs.
#[must_use]
pub fn approval_signing_bytes(input: &ApprovalSigningInput<'_>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        BREAK_GLASS_DOMAIN.len()
            + input.proposal_id.len()
            + 1
            + 2 * LEN_PREFIX
            + input.target.len()
            + input.proposer.len()
            + 2 * size_of::<i64>(),
    );
    bytes.extend_from_slice(BREAK_GLASS_DOMAIN);
    bytes.extend_from_slice(&input.proposal_id);
    bytes.push(input.action.to_be_bytes()[0]);
    put_len_prefixed(&mut bytes, input.target.as_bytes());
    put_len_prefixed(&mut bytes, input.proposer.as_bytes());
    bytes.extend_from_slice(&input.created_at_ms.to_be_bytes());
    bytes.extend_from_slice(&input.expires_at_ms.to_be_bytes());
    bytes
}

/// Read the PKCS#8 Ed25519 key at `path` as the key named `key_id`.
///
/// The key stays in this process. Only the signature it makes goes on the wire.
///
/// # Errors
///
/// Returns a message when the file cannot be read, and when it does not hold a
/// PKCS#8 Ed25519 key.
pub fn load_signer(path: &Path, key_id: &str) -> Result<FileEd25519Signer, String> {
    FileEd25519Signer::from_pkcs8_file(path, key_id.to_owned())
        .map_err(|error| format!("cannot sign with {}: {error}", path.display()))
}

/// Byte length of one `u32` big-endian length prefix.
const LEN_PREFIX: usize = size_of::<u32>();

/// Append `field` behind its `u32` big-endian length.
///
/// The prefixes are what keep two different records from building one byte
/// string. Without them a scope of `"a"` with a reason of `"bc"` and a scope of
/// `"ab"` with a reason of `"c"` would sign the same.
fn put_len_prefixed(bytes: &mut Vec<u8>, field: &[u8]) {
    let len = u32::try_from(field.len()).unwrap_or(u32::MAX);
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(field);
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    krabka_macros::break_glass_golden_id!(PROPOSAL_ID);

    fn freeze_input() -> FreezeSigningInput<'static> {
        FreezeSigningInput {
            cluster_id: "krabka-test",
            pattern_type: 3,
            scope: "orders",
            frozen: true,
            reason: "DR cutover",
            set_by: "User:alice",
            set_at_ms: 1_770_000_000_000,
            proposal_id: [0; 16],
        }
    }

    fn approval_input() -> ApprovalSigningInput<'static> {
        ApprovalSigningInput {
            proposal_id: PROPOSAL_ID,
            action: 6,
            target: "doomed",
            proposer: "User:alice",
            created_at_ms: 1_770_000_000_000,
            expires_at_ms: 1_770_000_180_000,
        }
    }

    /// The layout the broker's `freeze::signing` module documents, built by
    /// hand rather than by the code under test. This is the pin: the two
    /// copies of the layout cannot drift while this vector is written out
    /// field by field.
    #[test]
    fn the_freeze_bytes_match_the_documented_layout() {
        let mut expected = Vec::new();
        expected.extend_from_slice(b"krabka-topic-freeze-v1\0");
        expected.extend_from_slice(&11u32.to_be_bytes());
        expected.extend_from_slice(b"krabka-test");
        expected.push(3);
        expected.extend_from_slice(&6u32.to_be_bytes());
        expected.extend_from_slice(b"orders");
        expected.push(1);
        expected.extend_from_slice(&10u32.to_be_bytes());
        expected.extend_from_slice(b"DR cutover");
        expected.extend_from_slice(&10u32.to_be_bytes());
        expected.extend_from_slice(b"User:alice");
        expected.extend_from_slice(&1_770_000_000_000i64.to_be_bytes());
        expected.extend_from_slice(&[0u8; 16]);

        check!(freeze_signing_bytes(&freeze_input()) == expected);
        // 23 domain + 4 + 11 cluster + 1 pattern + 4 + 6 scope + 1 frozen
        // + 4 + 10 reason + 4 + 10 set_by + 8 timestamp + 16 proposal.
        check!(expected.len() == 102);
    }

    /// The layout the broker's `break_glass::signing` module documents, built
    /// by hand for the same reason.
    #[test]
    fn the_approval_bytes_match_the_documented_layout() {
        let mut expected = Vec::new();
        expected.extend_from_slice(b"krabka-break-glass-v1\0");
        expected.extend_from_slice(&PROPOSAL_ID);
        expected.push(6);
        expected.extend_from_slice(&6u32.to_be_bytes());
        expected.extend_from_slice(b"doomed");
        expected.extend_from_slice(&10u32.to_be_bytes());
        expected.extend_from_slice(b"User:alice");
        expected.extend_from_slice(&1_770_000_000_000i64.to_be_bytes());
        expected.extend_from_slice(&1_770_000_180_000i64.to_be_bytes());

        check!(approval_signing_bytes(&approval_input()) == expected);
        // 22 domain + 16 id + 1 action + 4 + 6 target + 4 + 10 proposer
        // + 8 created + 8 expires.
        check!(expected.len() == 79);
    }

    /// A signature captured from a freeze must not authorize the thaw, and a
    /// signature made for one cluster must not authorize the same record in
    /// another. Both properties come from the field being inside the signed
    /// bytes, so every signed field has to change the bytes.
    #[test]
    fn a_change_to_any_signed_freeze_field_changes_the_bytes() {
        let baseline = freeze_signing_bytes(&freeze_input());
        let cases: [(&'static str, FreezeSigningInput<'static>); 8] = [
            (
                "another cluster",
                FreezeSigningInput {
                    cluster_id: "other-cluster",
                    ..freeze_input()
                },
            ),
            (
                "a prefixed scope",
                FreezeSigningInput {
                    pattern_type: 4,
                    ..freeze_input()
                },
            ),
            (
                "another scope",
                FreezeSigningInput {
                    scope: "payments",
                    ..freeze_input()
                },
            ),
            (
                "the thaw of the same scope",
                FreezeSigningInput {
                    frozen: false,
                    ..freeze_input()
                },
            ),
            (
                "another reason",
                FreezeSigningInput {
                    reason: "tenant offboarding",
                    ..freeze_input()
                },
            ),
            (
                "another author",
                FreezeSigningInput {
                    set_by: "User:mallory",
                    ..freeze_input()
                },
            ),
            (
                "another timestamp",
                FreezeSigningInput {
                    set_at_ms: 1_770_000_000_001,
                    ..freeze_input()
                },
            ),
            (
                "another proposal",
                FreezeSigningInput {
                    proposal_id: PROPOSAL_ID,
                    ..freeze_input()
                },
            ),
        ];
        for (case, input) in cases {
            check!(freeze_signing_bytes(&input) != baseline, "{case}");
        }
    }

    #[test]
    fn a_change_to_any_signed_approval_field_changes_the_bytes() {
        let baseline = approval_signing_bytes(&approval_input());
        let cases: [(&'static str, ApprovalSigningInput<'static>); 6] = [
            (
                "another proposal",
                ApprovalSigningInput {
                    proposal_id: [0; 16],
                    ..approval_input()
                },
            ),
            (
                "another action",
                ApprovalSigningInput {
                    action: 7,
                    ..approval_input()
                },
            ),
            (
                "another target",
                ApprovalSigningInput {
                    target: "orders",
                    ..approval_input()
                },
            ),
            (
                "another proposer",
                ApprovalSigningInput {
                    proposer: "User:mallory",
                    ..approval_input()
                },
            ),
            (
                "another creation time",
                ApprovalSigningInput {
                    created_at_ms: 1_770_000_000_001,
                    ..approval_input()
                },
            ),
            (
                "another expiry",
                ApprovalSigningInput {
                    expires_at_ms: 1_770_000_180_001,
                    ..approval_input()
                },
            ),
        ];
        for (case, input) in cases {
            check!(approval_signing_bytes(&input) != baseline, "{case}");
        }
    }

    /// A length prefix in front of each text field is what stops two different
    /// field splits from producing one byte string.
    #[test]
    fn a_field_split_cannot_produce_one_byte_string() {
        let left = freeze_signing_bytes(&FreezeSigningInput {
            scope: "a",
            reason: "bc",
            ..freeze_input()
        });
        let right = freeze_signing_bytes(&FreezeSigningInput {
            scope: "ab",
            reason: "c",
            ..freeze_input()
        });
        check!(left != right);
    }

    /// A separator shared between two signature purposes lets a captured
    /// signature be presented for a purpose the signer never agreed to. The
    /// trailing `\0` is what stops one separator starting the other.
    #[test]
    fn the_two_domain_separators_differ_and_neither_starts_the_other() {
        check!(FREEZE_DOMAIN != BREAK_GLASS_DOMAIN);
        check!(!FREEZE_DOMAIN.starts_with(BREAK_GLASS_DOMAIN));
        check!(!BREAK_GLASS_DOMAIN.starts_with(FREEZE_DOMAIN));
    }
}
