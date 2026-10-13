//! Wire sentinels and epoch arithmetic for KIP-227 fetch sessions.
//!
//! A `FetchRequest` carries a session id and a session epoch, and the two
//! reserved epoch values select between opening, continuing, and closing a
//! session. This module holds those wire constants, the aliases that name the
//! two wire fields, and `next_epoch`, which computes the epoch the broker
//! expects on the request after a successful incremental fetch.

/// A KIP-227 fetch session id as carried on the wire
/// (`FetchRequest.session_id`). `0` ([`INVALID_SESSION_ID`]) means "no
/// session". Valid ids are strictly positive.
pub type FetchSessionId = i32;

/// A KIP-227 fetch session epoch as carried on the wire
/// (`FetchRequest.session_epoch`). `0` ([`INITIAL_EPOCH`]) opens a session and
/// `-1` ([`FINAL_EPOCH`]) closes one. Valid incremental epochs are strictly
/// positive.
pub type FetchSessionEpoch = i32;

/// Wire sentinel: "no session". A request with `session_id == 0` and
/// `session_epoch == -1` is a sessionless full fetch. A response with
/// `session_id == 0` tells the client that the broker allocated no session.
pub const INVALID_SESSION_ID: FetchSessionId = 0;

/// Wire sentinel: "open a new session". A request with `session_id == 0`
/// and `session_epoch == 0` asks the broker to allocate a new session.
pub const INITIAL_EPOCH: FetchSessionEpoch = 0;

/// Wire sentinel for "no session" and for "close session". On a request with
/// `session_id == 0`, `FINAL_EPOCH` means a sessionless full fetch. On a
/// request with `session_id != 0`, it means close the named session.
pub const FINAL_EPOCH: FetchSessionEpoch = -1;

/// The first `Fetch` version that names topics by id (KIP-516). A session
/// remembers whether the request that created it was at this version or
/// later, and Kafka's `FetchManager.newContext` refuses an incremental fetch
/// of the other kind with `FETCH_SESSION_TOPIC_ID_ERROR`.
pub const FIRST_TOPIC_ID_FETCH_VERSION: i16 = 13;

/// The first id the allocator gives out. Ids count up from here. `0` is
/// reserved as [`INVALID_SESSION_ID`], and negative ids never go on the wire
/// because clients reject them.
pub(super) const FIRST_SESSION_ID: FetchSessionId = 1;

/// Computes the epoch the broker expects on the next request after a
/// successful incremental fetch. The value wraps from `i32::MAX` back to `1`
/// and skips the two reserved sentinels: `0` for INITIAL and `-1` for FINAL.
#[must_use]
pub fn next_epoch(prev: FetchSessionEpoch) -> FetchSessionEpoch {
    let n = prev.wrapping_add(1);
    if n <= 0 { 1 } else { n }
}

pub(super) fn session_id_is_reserved(candidate: FetchSessionId) -> bool { candidate <= 0 }

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn next_epoch_wraps_skipping_sentinels() {
        let cases = [(0, 1), (1, 2), (i32::MAX, 1), (-1, 1)];
        for (epoch, want) in cases {
            assert!(next_epoch(epoch) == want, "epoch {epoch}");
        }
    }

    #[test]
    fn session_id_reserved_predicate_matches_wire_sentinels() {
        let cases = [(INVALID_SESSION_ID, true), (FINAL_EPOCH, true), (1, false)];
        for (session_id, want) in cases {
            assert!(
                session_id_is_reserved(session_id) == want,
                "session_id {session_id}"
            );
        }
    }
}
