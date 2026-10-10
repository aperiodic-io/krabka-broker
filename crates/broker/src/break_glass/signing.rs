//! The canonical bytes that a break-glass approval signature covers.
//!
//! An approval can carry a detached Ed25519 signature that the approver makes
//! on their own machine. The operator tool builds these bytes, signs them, and
//! sends the `key_id` and the signature. The private key never reaches a
//! broker. The broker rebuilds the same bytes from the stored proposal and
//! verifies the signature through
//! [`OperatorKeys::verify`](crate::operator_keys::OperatorKeys::verify), which
//! also binds the signature to the approving principal.
//!
//! # The layout
//!
//! ```text
//! BREAK_GLASS_DOMAIN                       22 bytes, "krabka-break-glass-v1\0"
//! proposal_id                              16 raw bytes, big-endian
//! action                                   u8, the wire value of the action
//! target_len   ‖ target                    u32 big-endian ‖ UTF-8 bytes
//! proposer_len ‖ proposer                  u32 big-endian ‖ UTF-8 bytes
//! created_at_ms                            i64 big-endian
//! expires_at_ms                            i64 big-endian
//! ```
//!
//! Every length prefix is a `u32` in big-endian order, which is the
//! length-prefixed form that
//! [`krabka_audit::signing::checkpoint_signing_bytes`] and the WORM manifest
//! bytes also use. A length prefix in front of each text field is what stops
//! two different field splits producing one byte string, so a proposer named
//! `"User:al"` on target `"ice"` cannot collide with a proposer named
//! `"User:alice"` on an empty target.
//!
//! # What the signature does not cover
//!
//! The approvals list, `consumed_at_ms`, and `withdrawn` are outside the signed
//! bytes. Every approver signs the same bytes, and the list grows as they sign,
//! so a signature over the list could never verify for the second approver. The
//! signature proves who agreed to this action on this target inside this
//! window. It does not prove what the broker then did with the agreement.
//!
//! # The domain separator
//!
//! [`BREAK_GLASS_DOMAIN`] is the break-glass separator, and it differs from
//! every other separator in this workspace.
//! [`crate::signing_domains`] holds the constants and the test that compares
//! them pairwise. A separator shared between two signature purposes lets an
//! attacker present a captured signature for a purpose the signer never agreed
//! to.

use krabka_metadata::BreakGlassProposalRecord;

use crate::{
    break_glass::action_to_wire,
    signing_domains::{BREAK_GLASS_DOMAIN, put_len_prefixed},
};

/// Build the canonical bytes that an approval of `proposal` signs.
///
/// The layout is the one this module documents. The verifier rebuilds the same
/// bytes from the stored record, so the broker never trusts a byte string that
/// a caller supplied.
pub(crate) fn approval_signing_bytes(proposal: &BreakGlassProposalRecord) -> Vec<u8> {
    let target = proposal.target.as_bytes();
    let proposer = proposal.proposer.as_bytes();
    let mut out = Vec::with_capacity(
        BREAK_GLASS_DOMAIN.len() + 16 + 1 + 4 + target.len() + 4 + proposer.len() + 8 + 8,
    );
    out.extend_from_slice(BREAK_GLASS_DOMAIN);
    out.extend_from_slice(proposal.proposal_id.as_bytes());
    out.push(u8::from_be_bytes(
        action_to_wire(proposal.action).to_be_bytes(),
    ));
    put_len_prefixed(&mut out, target);
    put_len_prefixed(&mut out, proposer);
    out.extend_from_slice(&proposal.created_at_ms.to_be_bytes());
    out.extend_from_slice(&proposal.expires_at_ms.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{BreakGlassAction, BreakGlassApproval};
    use uuid::Uuid;

    use super::*;
    use crate::break_glass::ALL_ACTIONS;

    krabka_macros::break_glass_golden_id!(PROPOSAL_ID);

    fn proposal() -> BreakGlassProposalRecord {
        BreakGlassProposalRecord {
            proposal_id: Uuid::from_bytes(PROPOSAL_ID),
            action: BreakGlassAction::DeleteTopic,
            target: "doomed".to_owned(),
            proposer: "User:alice".to_owned(),
            reason: "topic holds test data only".to_owned(),
            created_at_ms: 1_770_000_000_000,
            expires_at_ms: 1_770_000_180_000,
            approvals: Vec::new(),
            consumed_at_ms: 0,
            withdrawn: false,
        }
    }

    // The layout the module documents, built by hand rather than by the code
    // under test.
    fn expected_bytes(proposal: &BreakGlassProposalRecord, action_wire: u8) -> Vec<u8> {
        let mut expected = Vec::new();
        expected.extend_from_slice(b"krabka-break-glass-v1\0");
        expected.extend_from_slice(proposal.proposal_id.as_bytes());
        expected.push(action_wire);
        expected.extend_from_slice(&u32::try_from(proposal.target.len()).unwrap().to_be_bytes());
        expected.extend_from_slice(proposal.target.as_bytes());
        expected.extend_from_slice(
            &u32::try_from(proposal.proposer.len())
                .unwrap()
                .to_be_bytes(),
        );
        expected.extend_from_slice(proposal.proposer.as_bytes());
        expected.extend_from_slice(&proposal.created_at_ms.to_be_bytes());
        expected.extend_from_slice(&proposal.expires_at_ms.to_be_bytes());
        expected
    }

    #[test]
    fn the_signing_bytes_match_the_documented_layout() {
        let proposal = proposal();

        let bytes = approval_signing_bytes(&proposal);

        check!(bytes == expected_bytes(&proposal, 6));
        check!(bytes.starts_with(BREAK_GLASS_DOMAIN));
        // 22 domain + 16 id + 1 action + 4 + 6 target + 4 + 10 proposer + 8 + 8.
        check!(bytes.len() == 79);
    }

    #[test]
    fn every_action_puts_its_own_wire_value_in_the_signed_bytes() {
        for action in ALL_ACTIONS {
            let proposal = BreakGlassProposalRecord {
                action,
                ..proposal()
            };
            let wire = u8::try_from(action_to_wire(action)).unwrap();
            check!(
                approval_signing_bytes(&proposal) == expected_bytes(&proposal, wire),
                "{}",
                crate::break_glass::action_name(action)
            );
        }
    }

    #[test]
    fn a_change_to_any_signed_field_changes_the_bytes() {
        let baseline = approval_signing_bytes(&proposal());
        let cases: [(&'static str, BreakGlassProposalRecord); 6] = [
            (
                "another proposal id",
                BreakGlassProposalRecord {
                    proposal_id: Uuid::from_u128(1),
                    ..proposal()
                },
            ),
            (
                "another action",
                BreakGlassProposalRecord {
                    action: BreakGlassAction::DeleteRecords,
                    ..proposal()
                },
            ),
            (
                "another target",
                BreakGlassProposalRecord {
                    target: "orders".to_owned(),
                    ..proposal()
                },
            ),
            (
                "another proposer",
                BreakGlassProposalRecord {
                    proposer: "User:mallory".to_owned(),
                    ..proposal()
                },
            ),
            (
                "another creation time",
                BreakGlassProposalRecord {
                    created_at_ms: 1_770_000_000_001,
                    ..proposal()
                },
            ),
            (
                "a later expiry",
                BreakGlassProposalRecord {
                    expires_at_ms: 1_780_000_000_000,
                    ..proposal()
                },
            ),
        ];
        for (label, changed) in cases {
            check!(approval_signing_bytes(&changed) != baseline, "case {label}");
        }
    }

    #[test]
    fn the_mutable_fields_stay_outside_the_signed_bytes() {
        let baseline = approval_signing_bytes(&proposal());
        let cases = [
            (
                "one approval collected",
                BreakGlassProposalRecord {
                    approvals: vec![BreakGlassApproval {
                        principal: "User:bob".to_owned(),
                        approved_at_ms: 1_770_000_060_000,
                        key_id: "bob-yubi".to_owned(),
                        signature: vec![7; 64],
                    }],
                    ..proposal()
                },
            ),
            (
                "the approval spent",
                BreakGlassProposalRecord {
                    consumed_at_ms: 1_770_000_090_000,
                    ..proposal()
                },
            ),
            (
                "the proposal withdrawn",
                BreakGlassProposalRecord {
                    withdrawn: true,
                    ..proposal()
                },
            ),
            (
                "another reason",
                BreakGlassProposalRecord {
                    reason: "a different reason".to_owned(),
                    ..proposal()
                },
            ),
        ];
        for (label, changed) in cases {
            check!(approval_signing_bytes(&changed) == baseline, "case {label}");
        }
    }

    #[test]
    fn a_field_split_cannot_collide_with_another_one() {
        let split = BreakGlassProposalRecord {
            target: "doo".to_owned(),
            proposer: "medUser:alice".to_owned(),
            ..proposal()
        };

        check!(approval_signing_bytes(&split) != approval_signing_bytes(&proposal()));
    }

    #[test]
    fn an_empty_target_and_proposer_still_carry_their_length_prefixes() {
        let empty = BreakGlassProposalRecord {
            target: String::new(),
            proposer: String::new(),
            ..proposal()
        };

        let bytes = approval_signing_bytes(&empty);

        check!(bytes == expected_bytes(&empty, 6));
        check!(bytes.len() == 63);
    }
}
