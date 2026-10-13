//! The detached operator signature over a freeze record (KFC-9).
//!
//! The `set_by` name on a freeze record is the broker's word for who set it.
//! That is not good enough for the one record whose whole job is to say that a
//! privileged person did a privileged thing: anyone who can write the metadata
//! log can write any name into that field. So the record carries an Ed25519
//! signature that the operator's own machine makes before the request leaves
//! it. The broker verifies the signature and cannot make one, and the metadata
//! log keeps it, so an auditor re-verifies it later with no trust in any
//! broker.
//!
//! # The canonical bytes
//!
//! [`freeze_signing_bytes`] is the one definition of the signed payload. The
//! operator's command builds it, and the broker rebuilds it to verify. The
//! layout is:
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
//! proposal_id     16 raw bytes
//! ```
//!
//! Every length prefix is a `u32` in big-endian order, which is the shape
//! [`krabka_audit::signing::checkpoint_signing_bytes`] gives the one variable
//! field it covers. The prefixes are what keep two different records from
//! building one byte string: without them a scope of `"a"` with a reason of
//! `"bc"` and a scope of `"ab"` with a reason of `"c"` would sign the same.
//!
//! [`crate::signing_domains::FREEZE_DOMAIN`] is the separator, and it differs
//! from every other separator in the workspace. A signature made for one
//! purpose then never verifies as another.
//!
//! # What each field is covering
//!
//! Three fields are in the payload to answer a named attack.
//!
//! `frozen` is signed, so a signature captured from a freeze cannot be
//! replayed as the thaw. Without it the two records differ by one byte and one
//! signature would authorize both.
//!
//! `cluster_id` is signed, so a signed freeze cannot be replayed into a second
//! cluster.
//!
//! `set_at_ms` is signed, and [`verify_freeze_signature`] checks it two ways.
//! It must sit inside `freeze.signature_max_skew` of this broker's clock, and
//! it must be newer than the timestamp of the entry it replaces. Those two
//! checks are what kill the replay of an old signed thaw. The skew window is a
//! clock assumption, which KFC-8 exists to measure.
//!
//! # One code for every refusal
//!
//! Every failure that [`verify_freeze_signature`] reports answers with
//! `OPERATOR_SIGNATURE_INVALID` (1009). The response message says which check
//! failed and the code does not, because a code that separates them tells an
//! attacker which check they got past.

pub(crate) use self::{
    canonical_bytes::freeze_signing_bytes,
    refusal::{SignatureRefusal, WireRefusal},
    verify::{FreezeSignatureCheck, verify_freeze_signature},
};

mod canonical_bytes;
mod refusal;
mod verify;

#[cfg(test)]
mod tests;
