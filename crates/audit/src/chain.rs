//! Per-broker audit hash-chain primitives.
//!
//! The chain binds each record to all prior records: `new_head =
//! SHA256(prev_head ‖ seq_be ‖ value)`. The writer stamps `(seq, prev_head)`
//! on each record. The verifier recomputes the chain and checks continuity.
//! The writer and the verifier must use these functions. Never reimplement the
//! formula.

use sha2::{Digest, Sha256};

/// Chain head before the writer writes the first record.
pub const GENESIS_HEAD: [u8; 32] = [0u8; 32];

/// Canonical chain hash. `new_head = SHA256(prev ‖ seq.to_be_bytes() ‖ value)`.
#[must_use]
pub fn chain_hash(prev: &[u8; 32], seq: u64, value: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(prev);
    h.update(seq.to_be_bytes());
    h.update(value);
    h.finalize().into()
}

/// The chain position a record is stamped with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainLink {
    /// Sequence number of the record.
    pub seq: u64,
    /// Chain head as it was before the record.
    pub prev_head: [u8; 32],
}

/// Running per-broker chain state.
#[derive(Debug, Clone, krabka_macros::FieldDefaults)]
pub struct ChainState {
    next_seq: u64,
    #[default(GENESIS_HEAD)]
    head: [u8; 32],
}

impl ChainState {
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Resume a chain from a recovered position after a restart or a spool replay.
    #[must_use]
    pub fn resume(next_seq: u64, head: [u8; 32]) -> Self { Self { next_seq, head } }

    /// Advance the chain for a record that holds `value`.
    ///
    /// Returns the [`ChainLink`] to stamp on that record. `prev_head` is
    /// the head as it was *before* this record. The method then updates the
    /// head with this record.
    #[tracing::instrument(level = "debug", skip_all, fields(seq = self.next_seq, bytes = value.len()))]
    pub fn extend(&mut self, value: &[u8]) -> ChainLink {
        let seq = self.next_seq;
        let prev = self.head;
        self.head = chain_hash(&prev, seq, value);
        self.next_seq += 1;
        ChainLink {
            seq,
            prev_head: prev,
        }
    }

    #[must_use]
    pub fn head(&self) -> [u8; 32] { self.head }

    #[must_use]
    pub fn next_seq(&self) -> u64 { self.next_seq }
}

/// Encode `bytes` as lowercase hex.
#[must_use]
pub fn to_hex(bytes: &[u8]) -> String { hex::encode(bytes) }

/// Parse exactly 32 bytes of lowercase or uppercase hex.
#[must_use]
pub fn from_hex32(s: &str) -> Option<[u8; 32]> {
    let mut out = [0u8; 32];
    hex::decode_to_slice(s, &mut out).ok()?;
    Some(out)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn genesis_is_zero() {
        check!(GENESIS_HEAD == [0u8; 32]);
    }

    #[test]
    fn chain_hash_is_deterministic_and_order_sensitive() {
        let a = chain_hash(&GENESIS_HEAD, 0, b"alpha");
        let b = chain_hash(&GENESIS_HEAD, 0, b"alpha");
        check!(a == b);
        // different seq => different hash
        check!(chain_hash(&GENESIS_HEAD, 1, b"alpha") != a);
        // different value => different hash
        check!(chain_hash(&GENESIS_HEAD, 0, b"beta") != a);
        // different prev => different hash
        check!(chain_hash(&a, 0, b"alpha") != a);
    }

    #[test]
    fn chain_state_advances_and_reports_prev() {
        let mut c = ChainState::new();
        check!(c.next_seq() == 0);
        let link0 = c.extend(b"r0");
        check!(
            link0
                == ChainLink {
                    seq: 0,
                    prev_head: GENESIS_HEAD
                }
        );
        check!((c.next_seq(), c.head()) == (1, chain_hash(&GENESIS_HEAD, 0, b"r0")));

        let link1 = c.extend(b"r1");
        check!(
            link1
                == ChainLink {
                    seq: 1,
                    prev_head: chain_hash(&GENESIS_HEAD, 0, b"r0"),
                }
        );
        check!(c.head() == chain_hash(&link1.prev_head, 1, b"r1"));
    }

    #[test]
    fn resume_sets_next_seq_and_head_and_continues() {
        let head = chain_hash(&GENESIS_HEAD, 4, b"r4");
        let mut c = ChainState::resume(5, head);
        check!((c.next_seq(), c.head()) == (5, head));
        let link = c.extend(b"r5");
        check!(
            link == ChainLink {
                seq: 5,
                prev_head: head
            }
        );
        check!(c.head() == chain_hash(&head, 5, b"r5"));
    }

    #[test]
    fn hex_round_trips() {
        let h = chain_hash(&GENESIS_HEAD, 7, b"x");
        let s = to_hex(&h);
        check!(s.len() == 64);
        let upper = s.to_uppercase();
        for (name, input, expected) in [
            ("round trip", s.as_str(), Some(h)),
            ("non-hex input", "zz", None),
            ("odd-length input", "abc", None),
            ("uppercase input", upper.as_str(), Some(h)),
        ] {
            check!(from_hex32(input) == expected, "case {name}");
        }
    }
}
