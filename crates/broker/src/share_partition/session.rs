//! KIP-932 share-session cache.
//!
//! A share session tracks the incremental `ShareFetch`/`ShareAcknowledge`
//! conversation between one consumer (`(group, member)`) and the
//! share-partition leader. The `share_session_epoch` on each request drives a
//! small state machine, Kafka's `SharePartitionManager.newContext`:
//!
//! - epoch `0` opens a session, and the stored epoch becomes `1`. It replaces
//!   a live session of the same member without releasing that member's
//!   records: they stay acquired until the member acknowledges them or their
//!   locks run out.
//! - epoch `-1` (`FINAL_EPOCH`) closes the session, and the cache removes the
//!   entry.
//! - any other epoch must match the stored epoch exactly. The cache then bumps
//!   the stored epoch for the next request.
//!
//! Mismatches map to Kafka's share-session error codes:
//! `INVALID_SHARE_SESSION_EPOCH` for a stale or ahead epoch,
//! `SHARE_SESSION_NOT_FOUND` for a non-zero epoch with no live session, and
//! `SHARE_SESSION_LIMIT_REACHED` when the cache is full.
//!
//! A session keeps its partitions in order, with Kafka's
//! `CachedSharePartition.requiresUpdateInResponse` flag on each, so that an
//! incremental response leaves out a partition that has nothing new to say.
//!
//! Locking discipline: each cache operation releases the mutex before it
//! returns, and this module holds nothing across an `.await`.

use {
    crate::codes,
    std::{
        collections::{HashMap, HashSet},
        sync::Mutex,
    },
};

/// Epoch value a client sends to close its share session (Kafka's
/// `ShareRequestMetadata.FINAL_EPOCH`).
const FINAL_EPOCH: i32 = -1;
/// Epoch value a client sends to open a fresh share session.
const INITIAL_EPOCH: i32 = 0;

/// One share partition of a session: `(topic_id, partition)`.
pub(crate) type SharePartitionKey = (uuid::Uuid, i32);

/// One partition of a live session, Kafka's `CachedSharePartition`.
#[derive(Debug, Clone, Copy)]
struct CachedPartition {
    key: SharePartitionKey,
    /// The next incremental response must carry this partition: it was just
    /// added, or its last row carried an error.
    requires_update: bool,
}

/// One live share session. It holds the current epoch and the share
/// partitions that the member has in its session, in Kafka's order: an
/// initial fetch sets them in request order, an incremental fetch appends the
/// new ones, and a partition that returned records moves to the end.
#[derive(Debug)]
struct ShareSession {
    epoch: i32,
    partitions: Vec<CachedPartition>,
    connection_id: String,
}

/// What a `ShareFetch` does after the share-session update.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ShareFetchSessionUpdate {
    /// The partitions to fetch, in the session's order. A final request
    /// fetches nothing.
    pub(crate) partitions: Vec<SharePartitionKey>,
    /// Partitions whose outstanding acquisitions the member gives back
    /// because its final request closed the session.
    pub(crate) released: HashSet<SharePartitionKey>,
    pub(crate) final_request: bool,
    /// The request continued a session, so the response carries only the
    /// partitions that have something to say.
    pub(crate) incremental: bool,
}

/// One partition row of an incremental response, as
/// [`ShareSessionCache::prune_response`] judges it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResponseRow {
    pub(crate) key: SharePartitionKey,
    pub(crate) has_records: bool,
    pub(crate) has_error: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ClosedShareSession {
    pub(crate) group: String,
    pub(crate) member: String,
    pub(crate) partitions: HashSet<SharePartitionKey>,
}

/// The group and member that own a session.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SessionKey {
    group: String,
    member: String,
}

impl SessionKey {
    fn new(group: &str, member: &str) -> Self {
        Self {
            group: group.to_string(),
            member: member.to_string(),
        }
    }
}

#[derive(Debug, Default)]
struct Inner {
    sessions: HashMap<SessionKey, ShareSession>,
    connections: HashMap<String, SessionKey>,
}

/// The share partitions of one `ShareFetch`: the ones it names, in request
/// order, and the ones it forgets.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FetchPartitions<'a> {
    pub(crate) requested: &'a [SharePartitionKey],
    pub(crate) forgotten: &'a HashSet<SharePartitionKey>,
}

/// Process-wide cache of live share sessions keyed by `(group, member)`.
#[derive(Debug)]
pub(crate) struct ShareSessionCache {
    inner: Mutex<Inner>,
    max: usize,
}

impl ShareSessionCache {
    /// Create a cache that holds at most `max` concurrent sessions.
    pub(crate) fn new(max: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            max,
        }
    }

    /// Kafka's `SharePartitionManager.newContext` for a `ShareFetch`.
    ///
    /// A final request needs a live session, closes it, and fetches nothing,
    /// whatever partitions it names or forgets. An initial request refuses
    /// acknowledgements with `INVALID_REQUEST`, replaces a live session of
    /// the member without releasing its records, ignores the forgotten
    /// partitions, and answers `SHARE_SESSION_LIMIT_REACHED` when the cache
    /// is full. An incremental request adds the named partitions that the
    /// session does not hold yet and removes the forgotten ones.
    pub(crate) fn update_fetch(
        &self,
        (group, member): (&str, &str),
        connection_id: &str,
        epoch: i32,
        partitions: FetchPartitions<'_>,
        has_acknowledgements: bool,
    ) -> Result<ShareFetchSessionUpdate, i16> {
        let key = SessionKey::new(group, member);
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");

        if epoch == FINAL_EPOCH {
            let session = remove_session(&mut inner, &key).ok_or(codes::SHARE_SESSION_NOT_FOUND)?;
            return Ok(ShareFetchSessionUpdate {
                partitions: Vec::new(),
                released: session.partitions.iter().map(|p| p.key).collect(),
                final_request: true,
                incremental: false,
            });
        }

        if epoch == INITIAL_EPOCH {
            if has_acknowledgements {
                return Err(codes::INVALID_REQUEST);
            }
            remove_session(&mut inner, &key);
            if inner.sessions.len() >= self.max {
                return Err(codes::SHARE_SESSION_LIMIT_REACHED);
            }
            let mut ordered: Vec<CachedPartition> = Vec::with_capacity(partitions.requested.len());
            for requested in partitions.requested {
                if !ordered.iter().any(|p| p.key == *requested) {
                    ordered.push(CachedPartition {
                        key: *requested,
                        requires_update: false,
                    });
                }
            }
            let fetch = ordered.iter().map(|p| p.key).collect();
            inner
                .connections
                .insert(connection_id.to_string(), key.clone());
            inner.sessions.insert(key, ShareSession {
                epoch: 1,
                partitions: ordered,
                connection_id: connection_id.to_string(),
            });
            return Ok(ShareFetchSessionUpdate {
                partitions: fetch,
                released: HashSet::new(),
                final_request: false,
                incremental: false,
            });
        }

        let session = session_at_epoch(&mut inner, &key, epoch)?;
        for requested in partitions.requested {
            if !session.partitions.iter().any(|p| p.key == *requested) {
                session.partitions.push(CachedPartition {
                    key: *requested,
                    requires_update: true,
                });
            }
        }
        session
            .partitions
            .retain(|p| !partitions.forgotten.contains(&p.key));
        session.epoch = next_epoch(session.epoch);
        Ok(ShareFetchSessionUpdate {
            partitions: session.partitions.iter().map(|p| p.key).collect(),
            released: HashSet::new(),
            final_request: false,
            incremental: true,
        })
    }

    /// Kafka's `ShareSessionContext.updateAndGenerateResponseData` for an
    /// incremental response: returns, for each row, whether the response
    /// carries it, and updates the session.
    ///
    /// A row with records or an error is always carried, and so is a
    /// partition that the session flagged. An error flags the partition for
    /// the next response too, so the client learns when it clears. A
    /// partition that returned records moves to the end of the session's
    /// order. A row of a partition that the session does not hold is carried.
    pub(crate) fn prune_response(
        &self,
        group: &str,
        member: &str,
        rows: &[ResponseRow],
    ) -> Vec<bool> {
        let key = SessionKey::new(group, member);
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");
        let Some(session) = inner.sessions.get_mut(&key) else {
            return vec![true; rows.len()];
        };
        rows.iter()
            .map(|row| {
                let Some(index) = session.partitions.iter().position(|p| p.key == row.key) else {
                    return true;
                };
                let cached = &mut session.partitions[index];
                let must_respond = row.has_records || row.has_error || cached.requires_update;
                cached.requires_update = row.has_error;
                if row.has_records {
                    let moved = session.partitions.remove(index);
                    session.partitions.push(moved);
                }
                must_respond
            })
            .collect()
    }

    /// Validate a `ShareAcknowledge` epoch and advance or close its session.
    pub(crate) fn update_acknowledge(
        &self,
        group: &str,
        member: &str,
        epoch: i32,
    ) -> Result<HashSet<SharePartitionKey>, i16> {
        if epoch == INITIAL_EPOCH {
            return Err(codes::INVALID_SHARE_SESSION_EPOCH);
        }
        let key = SessionKey::new(group, member);
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");
        if epoch == FINAL_EPOCH {
            return remove_session(&mut inner, &key)
                .map(|session| session.partitions.iter().map(|p| p.key).collect())
                .ok_or(codes::SHARE_SESSION_NOT_FOUND);
        }
        let session = session_at_epoch(&mut inner, &key, epoch)?;
        session.epoch = next_epoch(session.epoch);
        Ok(HashSet::new())
    }

    /// Removes every session, as Kafka's `ShareSessionCache.removeAllSessions`.
    pub(crate) fn clear(&self) {
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");
        inner.sessions.clear();
        inner.connections.clear();
    }

    /// Remove the session owned by a closing client connection.
    pub(crate) fn disconnect(&self, connection_id: &str) -> Option<ClosedShareSession> {
        let mut inner = self.inner.lock().expect("share-session mutex poisoned");
        let key = inner.connections.remove(connection_id)?;
        let session = inner.sessions.get(&key)?;
        if session.connection_id != connection_id {
            return None;
        }
        let session = inner.sessions.remove(&key)?;
        Some(ClosedShareSession {
            group: key.group,
            member: key.member,
            partitions: session.partitions.iter().map(|p| p.key).collect(),
        })
    }
}

fn remove_session(inner: &mut Inner, key: &SessionKey) -> Option<ShareSession> {
    let session = inner.sessions.remove(key)?;
    if inner.connections.get(&session.connection_id) == Some(key) {
        inner.connections.remove(&session.connection_id);
    }
    Some(session)
}

/// Find a session and validate its epoch while the caller holds the cache lock.
fn session_at_epoch<'a>(
    inner: &'a mut Inner,
    key: &SessionKey,
    epoch: i32,
) -> Result<&'a mut ShareSession, i16> {
    let session = inner
        .sessions
        .get_mut(key)
        .ok_or(codes::SHARE_SESSION_NOT_FOUND)?;
    if session.epoch != epoch {
        return Err(codes::INVALID_SHARE_SESSION_EPOCH);
    }
    Ok(session)
}

fn next_epoch(epoch: i32) -> i32 { epoch.checked_add(1).unwrap_or(1) }

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    fn partition(id: u8, partition: i32) -> SharePartitionKey {
        (uuid::Uuid::from_bytes([id; 16]), partition)
    }

    fn set(partitions: &[SharePartitionKey]) -> HashSet<SharePartitionKey> {
        partitions.iter().copied().collect()
    }

    fn fetch(
        cache: &ShareSessionCache,
        member: &str,
        epoch: i32,
        requested: &[SharePartitionKey],
        forgotten: &[SharePartitionKey],
    ) -> Result<ShareFetchSessionUpdate, i16> {
        fetch_on(cache, member, "connection-a", epoch, requested, forgotten)
    }

    fn fetch_on(
        cache: &ShareSessionCache,
        member: &str,
        connection: &str,
        epoch: i32,
        requested: &[SharePartitionKey],
        forgotten: &[SharePartitionKey],
    ) -> Result<ShareFetchSessionUpdate, i16> {
        cache.update_fetch(
            ("g", member),
            connection,
            epoch,
            FetchPartitions {
                requested,
                forgotten: &set(forgotten),
            },
            false,
        )
    }

    #[test]
    fn open_then_incremental_merges_forgets_and_advances() {
        let cache = ShareSessionCache::new(8);
        let p0 = partition(1, 0);
        let p1 = partition(1, 1);
        let p2 = partition(2, 0);

        let opened = fetch(&cache, "m", 0, &[p1, p0], &[]).expect("open");
        assert!(opened.partitions == vec![p1, p0]);
        assert!(!opened.incremental);

        let updated = fetch(&cache, "m", 1, &[p2], &[p0]).expect("incremental");
        assert!(updated.partitions == vec![p1, p2]);
        assert!(updated.incremental);
        assert!(fetch(&cache, "m", 2, &[], &[]).is_ok());
    }

    #[test]
    fn stale_epoch_is_invalid() {
        let cache = ShareSessionCache::new(8);
        assert!(fetch(&cache, "m", 0, &[], &[]).is_ok());
        // Stored epoch is 1; sending the wrong epoch is rejected.
        assert!(fetch(&cache, "m", 5, &[], &[]) == Err(codes::INVALID_SHARE_SESSION_EPOCH));
    }

    #[test]
    fn unknown_member_non_zero_epoch_not_found() {
        let cache = ShareSessionCache::new(8);
        assert!(fetch(&cache, "ghost", 3, &[], &[]) == Err(codes::SHARE_SESSION_NOT_FOUND));
    }

    /// Kafka's `FinalContext` accepts partitions to add and to forget, and
    /// fetches none of them.
    #[test]
    fn close_removes_session_whatever_the_request_names() {
        let p = partition(1, 0);
        let other = partition(2, 0);
        for (requested, forgotten) in [
            (Vec::new(), Vec::new()),
            (vec![other], Vec::new()),
            (Vec::new(), vec![p]),
        ] {
            let cache = ShareSessionCache::new(8);
            assert!(fetch(&cache, "m", 0, &[p], &[]).is_ok());
            let closed = fetch(&cache, "m", -1, &requested, &forgotten).expect("close");
            assert!(
                closed
                    == ShareFetchSessionUpdate {
                        partitions: Vec::new(),
                        released: set(&[p]),
                        final_request: true,
                        incremental: false,
                    }
            );
            assert!(fetch(&cache, "m", 1, &[], &[]) == Err(codes::SHARE_SESSION_NOT_FOUND));
        }
    }

    #[test]
    fn close_absent_session_is_not_found() {
        let cache = ShareSessionCache::new(8);
        assert!(fetch(&cache, "never", -1, &[], &[]) == Err(codes::SHARE_SESSION_NOT_FOUND));
    }

    /// Kafka refuses an initial fetch with acknowledgements and ignores its
    /// forgotten partitions.
    #[test]
    fn initial_fetch_refuses_acknowledgements_and_ignores_forgotten_partitions() {
        let cache = ShareSessionCache::new(8);
        let p = partition(1, 0);
        let acknowledging = cache.update_fetch(
            ("g", "m"),
            "connection-a",
            0,
            FetchPartitions {
                requested: &[p],
                forgotten: &HashSet::new(),
            },
            true,
        );
        assert!(acknowledging == Err(codes::INVALID_REQUEST));
        let forgetting = fetch(&cache, "m", 0, &[p], &[p]).expect("open");
        assert!(forgetting.partitions == vec![p]);
    }

    #[test]
    fn acknowledge_zero_epoch_is_invalid_and_final_closes() {
        let cache = ShareSessionCache::new(8);
        let p = partition(1, 0);
        assert!(fetch(&cache, "m", 0, &[p], &[]).is_ok());
        assert!(cache.update_acknowledge("g", "m", 0) == Err(codes::INVALID_SHARE_SESSION_EPOCH));
        assert!(cache.update_acknowledge("g", "m", 1).is_ok());
        assert!(cache.update_acknowledge("g", "m", -1) == Ok(set(&[p])));
    }

    /// A full cache refuses a new member. A member that re-opens its own
    /// session frees its old entry first, and gives back no records.
    #[test]
    fn over_capacity_is_limit_reached_and_reopening_releases_nothing() {
        let cache = ShareSessionCache::new(1);
        let p = partition(1, 0);
        assert!(fetch(&cache, "m1", 0, &[p], &[]).is_ok());
        assert!(fetch(&cache, "m2", 0, &[], &[]) == Err(codes::SHARE_SESSION_LIMIT_REACHED));
        let reopened = fetch(&cache, "m1", 0, &[], &[]).expect("reopen");
        assert!(reopened.released.is_empty());
    }

    /// Kafka's `CachedSharePartition.maybeUpdateResponseData` over three
    /// incremental responses.
    #[test]
    fn incremental_responses_carry_only_partitions_with_news() {
        let cache = ShareSessionCache::new(8);
        let (p0, p1, p2) = (partition(1, 0), partition(1, 1), partition(1, 2));
        let row = |key, has_records, has_error| ResponseRow {
            key,
            has_records,
            has_error,
        };
        fetch(&cache, "m", 0, &[p0, p1], &[]).expect("open");
        // p2 joins at epoch 1, so its first incremental row is carried.
        let joined = fetch(&cache, "m", 1, &[p2], &[]).expect("add");
        let first = cache.prune_response("g", "m", &[
            row(p0, true, false),
            row(p1, false, true),
            row(p2, false, false),
        ]);
        // p1 had an error, so it is carried once more after it clears.
        fetch(&cache, "m", 2, &[], &[]).expect("continue");
        let second = cache.prune_response("g", "m", &[
            row(p0, false, false),
            row(p1, false, false),
            row(p2, false, false),
        ]);
        let after = fetch(&cache, "m", 3, &[], &[]).expect("continue");
        let third = cache.prune_response("g", "m", &[
            row(p0, false, false),
            row(p1, false, false),
            row(p2, false, false),
        ]);

        assert!(joined.partitions == vec![p0, p1, p2]);
        assert!(first == vec![true, true, true]);
        assert!(second == vec![false, true, false]);
        assert!(third == vec![false, false, false]);
        // p0 returned records, so it moved to the end of the order.
        assert!(after.partitions == vec![p1, p2, p0]);
    }

    #[test]
    fn disconnect_removes_only_the_connections_live_session() {
        let cache = ShareSessionCache::new(8);
        let p = partition(1, 0);
        assert!(fetch(&cache, "m", 0, &[p], &[]).is_ok());
        let closed = cache.disconnect("connection-a").expect("session");
        assert!(closed.group == "g");
        assert!(closed.member == "m");
        assert!(closed.partitions == set(&[p]));
        assert!(cache.disconnect("connection-a").is_none());
    }

    #[test]
    fn reopening_does_not_remove_another_sessions_connection_mapping() {
        let cache = ShareSessionCache::new(8);
        let old_partition = partition(1, 0);
        let current_partition = partition(2, 0);
        fetch_on(&cache, "old", "connection-a", 0, &[old_partition], &[])
            .expect("open old session");
        fetch_on(
            &cache,
            "current",
            "connection-a",
            0,
            &[current_partition],
            &[],
        )
        .expect("replace connection mapping");
        fetch_on(&cache, "old", "connection-b", 0, &[], &[])
            .expect("reopen old member on a new connection");

        let closed = cache
            .disconnect("connection-a")
            .expect("current session remains mapped");
        assert!(closed.group == "g");
        assert!(closed.member == "current");
        assert!(closed.partitions == set(&[current_partition]));
    }
}
