//! Domain separators for the operator signatures that KFC-9 defines.
//!
//! A signature over one kind of message must never verify as another kind. So
//! every signing scheme in this workspace puts a distinct constant in front of
//! its canonical bytes, and the verifier rebuilds those bytes with the same
//! constant. Two schemes that share a separator let an attacker present a
//! captured signature for a purpose the signer never agreed to.
//!
//! `crates/remote-storage/src/worm/manifest.rs` states the same rule. It gives
//! the segment manifest [`krabka_remote_storage::MANIFEST_DOMAIN`] and the
//! chain preimage [`krabka_remote_storage::MANIFEST_BODY_DOMAIN`], and it
//! deliberately does not share [`krabka_audit::signing::CHECKPOINT_DOMAIN`],
//! even though both schemes sign with the same Ed25519 keys and the same
//! verifier.
//!
//! The two constants sit in one module of their own, and not beside the code
//! that builds each canonical layout, because the freeze path and the
//! break-glass path are separate modules. The tests below compare every domain
//! separator in the workspace against every other one. A test inside either
//! path would have to reach into the other path for its constant.

/// Domain separator for a `TopicFreezeRecord` signature (KFC-9).
pub(crate) const FREEZE_DOMAIN: &[u8] = b"krabka-topic-freeze-v1\0";

/// Domain separator for a break-glass approval signature (KFC-9).
pub(crate) const BREAK_GLASS_DOMAIN: &[u8] = b"krabka-break-glass-v1\0";

/// Append `field` behind its `u32` big-endian length, as both canonical
/// layouts frame a variable-length field.
///
/// A field longer than `u32::MAX` saturates. Every field is a string a
/// request frame already bounds far below that, so no reachable input
/// saturates here; the saturation keeps the function total rather than
/// panicking.
pub(crate) fn put_len_prefixed(out: &mut Vec<u8>, field: &[u8]) {
    let len = u32::try_from(field.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(field);
}

#[cfg(test)]
mod tests {
    use {
        super::{BREAK_GLASS_DOMAIN, FREEZE_DOMAIN},
        assert2::check,
        krabka_audit::signing::CHECKPOINT_DOMAIN,
        krabka_remote_storage::{MANIFEST_BODY_DOMAIN, MANIFEST_DOMAIN},
    };

    /// A signing scheme's domain separator and the name a failure reports it by.
    struct SigningDomain {
        name: &'static str,
        separator: &'static [u8],
    }

    /// Every domain separator in the workspace, with the name a failure
    /// reports. Add a row here whenever a new signing scheme lands.
    const WORKSPACE_DOMAINS: [SigningDomain; 5] = [
        SigningDomain {
            name: "FREEZE_DOMAIN",
            separator: FREEZE_DOMAIN,
        },
        SigningDomain {
            name: "BREAK_GLASS_DOMAIN",
            separator: BREAK_GLASS_DOMAIN,
        },
        SigningDomain {
            name: "CHECKPOINT_DOMAIN",
            separator: CHECKPOINT_DOMAIN,
        },
        SigningDomain {
            name: "MANIFEST_DOMAIN",
            separator: MANIFEST_DOMAIN,
        },
        SigningDomain {
            name: "MANIFEST_BODY_DOMAIN",
            separator: MANIFEST_BODY_DOMAIN,
        },
    ];

    #[test]
    fn every_workspace_domain_separator_differs_pairwise() {
        for (index, left) in WORKSPACE_DOMAINS.iter().enumerate() {
            for right in &WORKSPACE_DOMAINS[index + 1..] {
                check!(
                    left.separator != right.separator,
                    "{} and {}",
                    left.name,
                    right.name
                );
            }
        }
    }

    /// Two separators that differ are still unsafe when one starts the other,
    /// because the longer scheme can then reproduce the shorter scheme's
    /// prefix. The trailing `\0` on every constant is what rules that out, and
    /// this test is what keeps the `\0` from being dropped.
    #[test]
    fn no_workspace_domain_separator_starts_another_one() {
        for (left_index, left) in WORKSPACE_DOMAINS.iter().enumerate() {
            for (right_index, right) in WORKSPACE_DOMAINS.iter().enumerate() {
                if left_index == right_index {
                    continue;
                }
                check!(
                    !left.separator.starts_with(right.separator),
                    "{} starts with {}",
                    left.name,
                    right.name
                );
            }
        }
    }
}
