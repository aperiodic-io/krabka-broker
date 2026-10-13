//! The `OffsetCommit` half of the composition: the independent fence oracle
//! and the transition that drives the real
//! `GroupState::validate_offset_commit` against it.
//!
//! The oracle and the driven call sit in one file because the cross-check
//! between them is the point of this model, and a divergence between the two
//! is what a failure reports.

use {
    super::{
        MAX_OFFSET, TOPIC,
        projection::rebuild_group,
        state::{CgcState, CommittedPartitionOffset, EpochKind, committed_map, member},
    },
    crate::{
        codes::{STALE_MEMBER_EPOCH, UNKNOWN_MEMBER_ID},
        coordinator::unified::actor::CommitFence,
    },
    krabka_log::Offset,
    std::cmp::Ordering,
};

/// INDEPENDENT oracle for the `OffsetCommit` fence: the expected decision for a
/// native member that presents `epoch` for partition `part`. It reads the
/// projected model state, not the real `GroupState`, and uses an `Ordering`
/// match where the real rule uses guards. The model therefore drives the real
/// fn and asserts equality as a genuine cross-check. A fence regression
/// diverges.
///
/// Kafka's rule with KIP-1251: the member's epoch commits any partition, a
/// newer epoch is stale, and an older epoch commits only a partition the
/// member holds with an assignment epoch at or below the presented epoch.
fn oracle_commit(s: &CgcState, id: &str, part: i32, epoch: i32) -> Result<(), i16> {
    let Some(m) = member(s, id) else {
        return Err(UNKNOWN_MEMBER_ID);
    };
    let accepted = match epoch.cmp(&m.member_epoch) {
        Ordering::Equal => true,
        Ordering::Greater => false,
        Ordering::Less => {
            let held = m.assigned.contains(&part) || m.pending_revocation.contains(&part);
            held && m
                .assignment_epochs
                .iter()
                .any(|row| row.partition == part && row.epoch <= epoch)
        }
    };
    if accepted {
        Ok(())
    } else {
        Err(STALE_MEMBER_EPOCH)
    }
}

/// Drive the REAL `OffsetCommit` fence (`validate_offset_commit`) for the
/// epoch `kind` the member presents, and cross-check it against the
/// independent oracle. Only on accept this function advances the bounded
/// committed offset. The member's CURRENT epoch and its partitions'
/// assignment epochs are whatever the real reconciliation last set, so a
/// `Stale` commit after a rebalance is accepted only for a partition the
/// member held before that rebalance, and a zombie is stopped for every other
/// one. Kafka does not check partition ownership for the member's own epoch
/// (at-least-once).
pub(super) fn do_commit(last: &CgcState, id: &str, part: i32, kind: EpochKind) -> Option<CgcState> {
    let g = rebuild_group(last);
    let cur = member(last, id).map(|m| m.member_epoch);
    let epoch = match kind {
        EpochKind::Current => cur.unwrap_or(0),
        EpochKind::Stale => cur.map_or(0, |epoch| epoch - 1),
        EpochKind::Forward => cur.map_or(0, |epoch| epoch + 1),
    };
    // GroupState's existing runtime fence takes Kafka topic/partition pairs;
    // that tuple is confined to this fixed API boundary.
    let real =
        g.validate_offset_commit(id, None, epoch, CommitFence::Offset { api_version: 9 }, &[
            (TOPIC, part),
        ]);
    let oracle = oracle_commit(last, id, part, epoch);
    assert2::assert!(
        (real) == (oracle),
        "OffsetCommit fence diverges from oracle: member={id} epoch={epoch}"
    );
    if real.is_err() {
        return None; // fenced (stale/forward/unknown) — cannot touch the offset
    }
    let mut committed = committed_map(last);
    let off = committed.entry(part).or_insert(Offset(0));
    if *off >= MAX_OFFSET {
        return None;
    }
    *off += 1;
    let mut next = last.clone();
    // std map iteration is adapted immediately into named projected coordinates.
    next.committed = committed
        .iter()
        .map(|(&partition, &offset)| CommittedPartitionOffset { partition, offset })
        .collect();
    Some(next)
}
