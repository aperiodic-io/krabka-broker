//! The remote tier's write-once posture and the position a copy's manifest
//! takes in its partition's WORM chain.
//!
//! Both values gate the copy, retention and delete passes, so they live apart
//! from any one of them.

use {
    krabka_remote_storage::{ChainStamp, EpochId, RemoteLogSegmentMetadata, next_chain_stamp},
    uuid::Uuid,
};

/// Whether the remote tier this partition writes to is a write-once archive.
///
/// KIP-405 remote retention deletes segments the tier still holds. A WORM
/// archive cannot honour that and must not be asked to: the eviction set is
/// empty, so the pass never reaches an RSM delete the backend would refuse
/// and the bucket policy would reject anyway.
///
/// A two-variant enum rather than a `bool`, because the retention helpers
/// already take several positional arguments and a bare flag among them is
/// exactly the transposition the style guide's newtype rule targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveMode {
    /// An ordinary tiered-storage backend. Retention may delete what it wrote.
    Mutable,
    /// A write-once archive. Every object written stays written, and every
    /// segment copy is sealed into a chained, verifiable manifest.
    WriteOnce,
}

impl ArchiveMode {
    /// The mode a broker's WORM setting implies: a
    /// [`WormConfig`](krabka_remote_storage::WormConfig) makes the tier
    /// write-once, and its absence leaves it mutable.
    pub(crate) const fn from_worm(worm: Option<&krabka_remote_storage::WormConfig>) -> Self {
        match worm {
            Some(_) => Self::WriteOnce,
            None => Self::Mutable,
        }
    }
}

/// Where a copy's manifest joins its partition's WORM chain, or that the tier
/// keeps no chain at all.
///
/// A copy into a write-once archive **must** carry a chain stamp: an unstamped
/// copy uploads every object and only then fails with
/// [`WormError::MissingChainStamp`](krabka_remote_storage::WormError::MissingChainStamp),
/// leaving orphans that nothing can ever collect. Pairing the mode and the
/// stamp in one value, instead of passing an [`ArchiveMode`] beside an
/// `Option<ChainStamp>`, makes that combination unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChainPosition {
    /// Mutable tier: the copy stamps nothing.
    Unchained,
    /// Write-once archive: the next manifest joins the chain here.
    At(ChainStamp),
    /// Write-once archive: the last receipt used `u64::MAX`, so no later
    /// manifest can be assigned a distinct sequence number.
    Exhausted,
}

impl ChainPosition {
    /// The position a partition's next copy takes, given every segment the
    /// metadata manager currently holds for it.
    ///
    /// `listed` is the copy pass's own RLMM listing, reused rather than
    /// re-fetched: seeding the chain must not cost a second list per segment.
    /// The fresh epoch id only survives when no receipt in `listed` does, so
    /// [`next_chain_stamp`] takes it as an argument and stays pure.
    pub(super) fn seed(archive: ArchiveMode, listed: &[RemoteLogSegmentMetadata]) -> Self {
        match archive {
            ArchiveMode::Mutable => Self::Unchained,
            ArchiveMode::WriteOnce => {
                next_chain_stamp(listed, EpochId(Uuid::new_v4())).map_or(Self::Exhausted, Self::At)
            }
        }
    }

    /// The archive mode this position belongs to: a stamp exists exactly when
    /// the tier is write-once.
    pub(super) const fn archive(self) -> ArchiveMode {
        match self {
            Self::Unchained => ArchiveMode::Mutable,
            Self::At(_) | Self::Exhausted => ArchiveMode::WriteOnce,
        }
    }
}
