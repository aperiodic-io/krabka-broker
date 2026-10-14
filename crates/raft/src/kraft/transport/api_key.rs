//! KIP-595 api keys used by the engine's peer sends.
//!
//! The engine tags every outbound body with one of these `api_key` values, and
//! the inbound dispatch routes on the same numbers.

pub const FETCH: i16 = 1;
pub const VOTE: i16 = 52;
pub const BEGIN_QUORUM_EPOCH: i16 = 53;
pub const END_QUORUM_EPOCH: i16 = 54;
pub const FETCH_SNAPSHOT: i16 = 59;

/// The five KIP-595 peer RPCs, keyed by their Kafka api key.
///
/// Every table that treats the peer apis alike (dispatch, authorization,
/// version negotiation) matches on this rather than restating the key list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum PeerApi {
    Fetch = FETCH,
    Vote = VOTE,
    BeginQuorumEpoch = BEGIN_QUORUM_EPOCH,
    EndQuorumEpoch = END_QUORUM_EPOCH,
    FetchSnapshot = FETCH_SNAPSHOT,
}

impl PeerApi {
    /// The peer api with Kafka api key `key`, or `None` for any other api.
    #[must_use]
    pub const fn from_api_key(key: i16) -> Option<Self> {
        Some(match key {
            FETCH => Self::Fetch,
            VOTE => Self::Vote,
            BEGIN_QUORUM_EPOCH => Self::BeginQuorumEpoch,
            END_QUORUM_EPOCH => Self::EndQuorumEpoch,
            FETCH_SNAPSHOT => Self::FetchSnapshot,
            _ => return None,
        })
    }

    /// The Kafka api key.
    #[must_use]
    pub const fn api_key(self) -> i16 { self as i16 }

    /// The version the engine encodes this api's body at, the newest it
    /// speaks.
    #[must_use]
    pub const fn version(self) -> i16 {
        use super::wire::{
            FETCH_SNAPSHOT_VERSION, FETCH_VERSION, QUORUM_EPOCH_VERSION, VOTE_VERSION,
        };
        match self {
            Self::Fetch => FETCH_VERSION,
            Self::Vote => VOTE_VERSION,
            Self::BeginQuorumEpoch | Self::EndQuorumEpoch => QUORUM_EPOCH_VERSION,
            Self::FetchSnapshot => FETCH_SNAPSHOT_VERSION,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[yare::parameterized(
        fetch = { FETCH, Some(PeerApi::Fetch) },
        vote = { VOTE, Some(PeerApi::Vote) },
        begin_quorum_epoch = { BEGIN_QUORUM_EPOCH, Some(PeerApi::BeginQuorumEpoch) },
        end_quorum_epoch = { END_QUORUM_EPOCH, Some(PeerApi::EndQuorumEpoch) },
        fetch_snapshot = { FETCH_SNAPSHOT, Some(PeerApi::FetchSnapshot) },
        non_peer_key_zero = { 0, None },
        non_peer_key_between_ranges = { 55, None },
    )]
    fn peer_api_round_trips_through_its_api_key(key: i16, expected: Option<PeerApi>) {
        let actual = PeerApi::from_api_key(key);
        assert2::assert!(actual == expected);
        if let Some(api) = expected {
            assert2::assert!(api.api_key() == key);
        }
    }
}
