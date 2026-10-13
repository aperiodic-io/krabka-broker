//! Break-glass proposal admission and deterministic selection.

use creusot_std::prelude::*;

/// First fail-closed reason that prevents a proposal from being spent.
#[krabka_macros::model_type]
pub enum BreakGlassAdmission {
    Withdrawn,
    Consumed,
    Expired,
    NotEnoughApprovals,
    Unsigned,
    Usable,
}

/// Whether the configured policy requires signed approvals for the action.
#[krabka_macros::model_type]
pub enum BreakGlassSignaturePolicy {
    Optional,
    Required,
}

/// Whether the proposal carries at least one approval and every approval
/// carries a key id and a signature.
#[krabka_macros::model_type]
pub enum BreakGlassApprovalSigning {
    AllSigned,
    NotAllSigned,
}

/// The independent lifecycle and approval facts of one covering proposal.
#[krabka_macros::model_type]
pub struct BreakGlassProposalFacts {
    pub withdrawn: bool,
    pub consumed: bool,
    pub expired: bool,
    /// Distinct approving principals.
    pub held_approvals: usize,
    pub required_approvals: usize,
    pub signature_policy: BreakGlassSignaturePolicy,
    pub signing: BreakGlassApprovalSigning,
}

/// Apply the break-glass lifecycle and approval checks in reporting order:
/// each variant holds exactly when its check fails and every earlier one
/// passes.
#[ensures((result == BreakGlassAdmission::Withdrawn) == facts.withdrawn)]
#[ensures((result == BreakGlassAdmission::Consumed) == (!facts.withdrawn && facts.consumed))]
#[ensures((result == BreakGlassAdmission::Expired) ==
    (!facts.withdrawn && !facts.consumed && facts.expired))]
#[ensures((result == BreakGlassAdmission::NotEnoughApprovals) ==
    (!facts.withdrawn && !facts.consumed && !facts.expired
        && facts.held_approvals@ < facts.required_approvals@))]
#[ensures((result == BreakGlassAdmission::Unsigned) ==
    (!facts.withdrawn && !facts.consumed && !facts.expired
        && facts.held_approvals@ >= facts.required_approvals@
        && facts.signature_policy == BreakGlassSignaturePolicy::Required
        && facts.signing == BreakGlassApprovalSigning::NotAllSigned))]
#[ensures((result == BreakGlassAdmission::Usable) ==
    (!facts.withdrawn && !facts.consumed && !facts.expired
        && facts.held_approvals@ >= facts.required_approvals@
        && (facts.signature_policy == BreakGlassSignaturePolicy::Optional
            || facts.signing == BreakGlassApprovalSigning::AllSigned)))]
#[must_use]
pub fn break_glass_admission(facts: BreakGlassProposalFacts) -> BreakGlassAdmission {
    if facts.withdrawn {
        BreakGlassAdmission::Withdrawn
    } else if facts.consumed {
        BreakGlassAdmission::Consumed
    } else if facts.expired {
        BreakGlassAdmission::Expired
    } else if facts.held_approvals < facts.required_approvals {
        BreakGlassAdmission::NotEnoughApprovals
    } else if let (BreakGlassSignaturePolicy::Required, BreakGlassApprovalSigning::NotAllSigned) =
        (facts.signature_policy, facts.signing)
    {
        BreakGlassAdmission::Unsigned
    } else {
        BreakGlassAdmission::Usable
    }
}

/// Lexicographic `<=` on `(expiry, UUID high bits, UUID low bits)` keys.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn lex_le(a: (i64, u64, u64), b: (i64, u64, u64)) -> bool {
    pearlite! {
        a.0@ < b.0@
            || (a.0@ == b.0@ && (a.1@ < b.1@ || (a.1@ == b.1@ && a.2@ <= b.2@)))
    }
}

/// `best` is the first index of a lexicographically minimal key among the
/// first `len` keys: no key is smaller, and every earlier key is strictly
/// greater.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn is_min_by(keys: Seq<(i64, u64, u64)>, best: Int, len: Int) -> bool {
    pearlite! {
        0 <= best && best < len && len <= keys.len()
            && (forall<j: Int> 0 <= j && j < len ==> lex_le(keys[best], keys[j]))
            && (forall<j: Int> 0 <= j && j < best ==> !lex_le(keys[j], keys[best]))
    }
}

/// Select the earliest `(expiry, UUID high bits, UUID low bits)` key; on an
/// exact duplicate key, the first index wins.
#[ensures(match result {
    None => candidates@.len() == 0,
    Some(index) => is_min_by(candidates@, index@, candidates@.len()),
})]
#[must_use]
pub fn select_break_glass_candidate(candidates: &[(i64, u64, u64)]) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut cursor = 0usize;
    #[invariant(cursor@ <= candidates@.len())]
    #[invariant(match best {
        None => cursor@ == 0,
        Some(b) => is_min_by(candidates@, b@, cursor@),
    })]
    #[variant(candidates@.len() - cursor@)]
    while cursor < candidates.len() {
        let candidate = candidates[cursor];
        best = match best {
            None => Some(cursor),
            Some(b) => {
                let current = candidates[b];
                if candidate.0 < current.0
                    || (candidate.0 == current.0 && candidate.1 < current.1)
                    || (candidate.0 == current.0
                        && candidate.1 == current.1
                        && candidate.2 < current.2)
                {
                    Some(cursor)
                } else {
                    Some(b)
                }
            }
        };
        cursor += 1;
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usable() -> BreakGlassProposalFacts {
        BreakGlassProposalFacts {
            withdrawn: false,
            consumed: false,
            expired: false,
            held_approvals: 2,
            required_approvals: 2,
            signature_policy: BreakGlassSignaturePolicy::Required,
            signing: BreakGlassApprovalSigning::AllSigned,
        }
    }

    #[test]
    fn break_glass_checks_are_ordered_and_fail_closed() {
        use BreakGlassAdmission::{
            Consumed, Expired, NotEnoughApprovals, Unsigned, Usable, Withdrawn,
        };

        let all_failing = BreakGlassProposalFacts {
            withdrawn: true,
            consumed: true,
            expired: true,
            held_approvals: 0,
            signing: BreakGlassApprovalSigning::NotAllSigned,
            ..usable()
        };
        for (facts, expected) in [
            (all_failing, Withdrawn),
            (
                BreakGlassProposalFacts {
                    withdrawn: false,
                    ..all_failing
                },
                Consumed,
            ),
            (
                BreakGlassProposalFacts {
                    withdrawn: false,
                    consumed: false,
                    ..all_failing
                },
                Expired,
            ),
            (
                BreakGlassProposalFacts {
                    held_approvals: 1,
                    ..usable()
                },
                NotEnoughApprovals,
            ),
            (
                BreakGlassProposalFacts {
                    signing: BreakGlassApprovalSigning::NotAllSigned,
                    ..usable()
                },
                Unsigned,
            ),
            (
                BreakGlassProposalFacts {
                    signature_policy: BreakGlassSignaturePolicy::Optional,
                    signing: BreakGlassApprovalSigning::NotAllSigned,
                    ..usable()
                },
                Usable,
            ),
            (usable(), Usable),
            (
                BreakGlassProposalFacts {
                    held_approvals: 3,
                    ..usable()
                },
                Usable,
            ),
        ] {
            assert2::assert!(break_glass_admission(facts) == expected);
        }
    }

    #[test]
    fn proposal_selection_uses_expiry_then_uuid_then_first_index() {
        for (candidates, expected) in [
            (&[][..], None),
            (&[(20, 0, 0), (10, 9, 9)][..], Some(1)),
            (&[(10, 4, 9), (10, 3, 99)][..], Some(1)),
            (&[(10, 3, 9), (10, 3, 8)][..], Some(1)),
            (&[(10, 3, 9), (20, 2, 8)][..], Some(0)),
            (&[(10, 3, 9), (20, 3, 2)][..], Some(0)),
            (&[(10, 3, 9), (10, 4, 2)][..], Some(0)),
            // An exact duplicate key keeps the first index.
            (&[(10, 3, 8), (10, 3, 8)][..], Some(0)),
            (&[(30, 0, 0), (10, 3, 8), (10, 3, 8)][..], Some(1)),
        ] {
            assert2::assert!(select_break_glass_candidate(candidates) == expected);
        }
    }
}
