//! Where a manifest sits in its partition's hash chain.
//!
//! The chain-position values — the epoch, the sequence number, and the head
//! they carry — live here with `manifest_head`, which computes the head one
//! manifest body produces from the head that preceded it.

use {
    super::{
        ManifestBody,
        encoding::canonical_manifest_bytes,
        hex_bytes::{deserialize_hex32, serialize_hex},
    },
    derive_more::{Display, From, Into},
    krabka_audit::chain::{GENESIS_HEAD, chain_hash},
    serde::{Deserialize, Serialize},
    uuid::Uuid,
};

/// Position of one manifest in its partition's hash chain.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct ManifestSeq(pub u64);

/// Identifier of one unbroken run of a partition's chain.
///
/// A chain that cannot find its previous head starts a new epoch rather than
/// silently restarting the old one at genesis.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct EpochId(pub Uuid);

#[derive(derive_more::From, derive_more::Into)]
/// Head of a partition's manifest hash chain.
///
/// Serialises as a lowercase hex string.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    derive_more::Debug,
    Display,
    Serialize,
    Deserialize,
)]
#[display("{}", hex::encode(_0))]
#[debug("ChainHead({})", hex::encode(_0))]
#[serde(transparent)]
pub struct ChainHead(
    #[serde(
        serialize_with = "serialize_hex",
        deserialize_with = "deserialize_hex32"
    )]
    pub [u8; 32],
);

impl ChainHead {
    /// The head before a chain writes its first manifest.
    pub const GENESIS: Self = Self(GENESIS_HEAD);
}

/// Where this manifest sits in its partition's hash chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainStamp {
    /// Chain run this manifest belongs to.
    pub epoch_id: EpochId,
    /// Position within the run, counted from zero.
    pub seq: ManifestSeq,
    /// Chain head as it was before this manifest.
    pub prev_head: ChainHead,
}

/// Chain head after `body`.
///
/// Chains the canonical bytes of the body onto the head the body itself
/// records as its predecessor.
#[must_use]
pub fn manifest_head(body: &ManifestBody) -> ChainHead {
    ChainHead(chain_hash(
        &body.chain.prev_head.0,
        body.chain.seq.0,
        &canonical_manifest_bytes(body),
    ))
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn genesis_head_is_the_audit_genesis() {
        check!(ChainHead::GENESIS.0 == krabka_audit::chain::GENESIS_HEAD);
    }
}
