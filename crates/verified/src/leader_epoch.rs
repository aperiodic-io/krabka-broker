//! KIP-101/320 leader-epoch divergence lookup kernel: Kafka's
//! `LeaderEpochFileCache.endOffsetFor`.
//!
//! The log crate keeps the checkpoint entries in strictly increasing epoch and
//! start-offset order. This kernel answers the `(found_epoch, end_offset)`
//! pair that an `OffsetForLeaderEpoch` row carries and that a `Fetch` row's
//! `diverging_epoch` is built from.

use {
    creusot_std::prelude::*,
    krabka_ids::{LeaderEpoch, Offset},
};

/// Kafka `LeaderEpochFileCache.UNDEFINED_EPOCH`.
const UNDEFINED_EPOCH: LeaderEpoch = LeaderEpoch(-1);
/// Kafka `LeaderEpochFileCache.UNDEFINED_EPOCH_OFFSET`.
const UNDEFINED_EPOCH_OFFSET: Offset = Offset(-1);

/// One leader epoch and the offset where it begins.
#[cfg_attr(not(creusot), derive(Debug, Clone, Copy, PartialEq, Eq, Hash))]
pub struct EpochEntry {
    pub epoch: LeaderEpoch,
    pub start_offset: Offset,
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// `h` is the position of Kafka's `epochs.higherEntry(requested)`: the least
/// recorded epoch strictly above `requested`. Over entries in increasing
/// epoch order that is the first entry above `requested`, and the entry just
/// before it, if any, is `epochs.floorEntry(requested)`.
pub fn higher_entry_at(entries: Seq<EpochEntry>, requested: Int, h: Int) -> bool {
    pearlite! {
        0 <= h && h < entries.len()
            && entries[h].epoch.0@ > requested
            && forall<j: Int> 0 <= j && j < h ==> entries[j].epoch.0@ <= requested
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Kafka's `epochs.higherEntry(requested)` is `null`: no recorded epoch is
/// above `requested`. An empty cache has none.
pub fn no_higher_entry(entries: Seq<EpochEntry>, requested: Int) -> bool {
    pearlite! {
        forall<i: Int> 0 <= i && i < entries.len() ==> entries[i].epoch.0@ <= requested
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Kafka's `LeaderEpochFileCache.endOffsetFor(requestedEpoch, logEndOffset)`
/// case table, in Kafka's order, as the relation between the inputs and the
/// answer `(found, end)`:
///
/// 1. `requested == UNDEFINED_EPOCH` → `(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)`.
/// 2. `requested` is the latest recorded epoch → `(requested, log_end)`.
/// 3. no `higherEntry` (this includes an empty cache) →
///    `(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)`.
/// 4. a `higherEntry` but no `floorEntry` (`requested` is below every
///    recorded epoch) → `(requested, higher.start_offset)`.
/// 5. both (an exact older epoch, or a gap epoch) →
///    `(floor.epoch, higher.start_offset)`.
///
/// The relation is functional: every input has exactly one answer.
pub fn kafka_end_offset_for(
    entries: Seq<EpochEntry>,
    requested: Int,
    log_end: Int,
    found: Int,
    end: Int,
) -> bool {
    pearlite! {
        if requested == -1 {
            found == -1 && end == -1
        } else if entries.len() > 0 && entries[entries.len() - 1].epoch.0@ == requested {
            found == requested && end == log_end
        } else if no_higher_entry(entries, requested) {
            found == -1 && end == -1
        } else {
            exists<h: Int> higher_entry_at(entries, requested, h)
                && end == entries[h].start_offset.0@
                && found == if h == 0 { requested } else { entries[h - 1].epoch.0@ }
        }
    }
}

/// Resolve a requested leader epoch to `(found_epoch, end_offset)`, exactly as
/// Kafka's `LeaderEpochFileCache.endOffsetFor` does (see
/// `kafka_end_offset_for` for the case table).
///
/// `(UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)` = `(-1, -1)` is the "cannot
/// place this epoch" answer: `UnifiedLog.endOffsetForEpoch` turns it into
/// `None`, which `Partition.lastOffsetForLeaderEpoch` answers with the
/// schema defaults `(-1, -1)` and `Partition.readRecords` answers with
/// `OFFSET_OUT_OF_RANGE`. Those two uses are the caller's.
///
/// Entries must be strictly increasing in both fields.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].epoch.0@ < entries@[j].epoch.0@
        && entries@[i].start_offset.0@ < entries@[j].start_offset.0@)]
#[ensures(kafka_end_offset_for(
    entries@, requested_epoch.0@, log_end_offset.0@, result.0.0@, result.1.0@))]
#[must_use]
pub fn epoch_and_offset_for_entries(
    entries: &[EpochEntry],
    requested_epoch: LeaderEpoch,
    log_end_offset: Offset,
) -> (LeaderEpoch, Offset) {
    if requested_epoch.0 == UNDEFINED_EPOCH.0 {
        return (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET);
    }

    // `i` becomes the position of Kafka's `higherEntry`, or `len` if none.
    let mut i = 0usize;
    #[invariant(i@ <= entries@.len())]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> entries@[j].epoch.0@ <= requested_epoch.0@)]
    #[variant(entries@.len() - i@)]
    while i < entries.len() && entries[i].epoch.0 <= requested_epoch.0 {
        i += 1;
    }

    if i == entries.len() {
        if i > 0 && entries[i - 1].epoch.0 == requested_epoch.0 {
            (requested_epoch, log_end_offset)
        } else {
            (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET)
        }
    } else {
        proof_assert!(higher_entry_at(entries@, requested_epoch.0@, i@));
        proof_assert!(entries@[entries@.len() - 1].epoch.0@ >= entries@[i@].epoch.0@);
        if i == 0 {
            (requested_epoch, entries[i].start_offset)
        } else {
            (entries[i - 1].epoch, entries[i].start_offset)
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, proptest::prelude::*, std::collections::BTreeMap};

    /// Kafka's own `endOffsetFor`, written against a `TreeMap`-shaped index
    /// (`higherEntry` / `floorEntry`) rather than the kernel's linear scan.
    fn tree_map_oracle(
        entries: &[EpochEntry],
        requested_epoch: LeaderEpoch,
        log_end_offset: Offset,
    ) -> (LeaderEpoch, Offset) {
        let epochs: BTreeMap<i32, EpochEntry> = entries.iter().map(|e| (e.epoch.0, *e)).collect();
        if requested_epoch == UNDEFINED_EPOCH {
            return (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET);
        }
        if epochs.last_key_value().map(|(epoch, _)| *epoch) == Some(requested_epoch.0) {
            return (requested_epoch, log_end_offset);
        }
        let higher = epochs
            .range(requested_epoch.0.saturating_add(1)..)
            .next()
            .map(|(_, entry)| *entry);
        let floor = epochs
            .range(..=requested_epoch.0)
            .next_back()
            .map(|(_, entry)| *entry);
        match (higher, floor) {
            (None, _) => (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET),
            (Some(higher), None) => (requested_epoch, higher.start_offset),
            (Some(higher), Some(floor)) => (floor.epoch, higher.start_offset),
        }
    }

    const fn entry(epoch: i32, start_offset: i64) -> EpochEntry {
        EpochEntry {
            epoch: LeaderEpoch(epoch),
            start_offset: Offset(start_offset),
        }
    }

    /// One row per `endOffsetFor` branch, plus the gap-epoch variants.
    #[test]
    fn lookup_matches_kafka_end_offset_for_rows() {
        const ENTRIES: &[EpochEntry] = &[entry(0, 0), entry(2, 50), entry(5, 100)];
        const LATE_START: &[EpochEntry] = &[entry(3, 30), entry(4, 40)];
        const UNDEFINED: (LeaderEpoch, Offset) = (UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET);

        for (name, entries, requested, log_end, expected) in [
            ("empty cache", &[][..], 0, 9, UNDEFINED),
            (
                "undefined requested epoch, empty cache",
                &[][..],
                -1,
                9,
                UNDEFINED,
            ),
            ("undefined requested epoch", ENTRIES, -1, 200, UNDEFINED),
            (
                "latest epoch answers the log end offset",
                ENTRIES,
                5,
                200,
                (LeaderEpoch(5), Offset(200)),
            ),
            (
                "requested epoch above the latest",
                ENTRIES,
                7,
                200,
                UNDEFINED,
            ),
            (
                "below the first epoch: requested epoch, first start",
                LATE_START,
                1,
                200,
                (LeaderEpoch(1), Offset(30)),
            ),
            (
                "exact older epoch: that epoch, next start",
                ENTRIES,
                2,
                200,
                (LeaderEpoch(2), Offset(100)),
            ),
            (
                "gap epoch: floor epoch, next start",
                ENTRIES,
                3,
                200,
                (LeaderEpoch(2), Offset(100)),
            ),
            (
                "gap epoch below the latest: floor epoch 2, start of 5",
                ENTRIES,
                4,
                200,
                (LeaderEpoch(2), Offset(100)),
            ),
            (
                "gap epoch just above the first: floor epoch 0, start of 2",
                ENTRIES,
                1,
                200,
                (LeaderEpoch(0), Offset(50)),
            ),
        ] {
            assert2::check!(
                epoch_and_offset_for_entries(entries, LeaderEpoch(requested), Offset(log_end))
                    == expected,
                "case {name}"
            );
        }
    }

    proptest! {
        #[test]
        fn lookup_matches_tree_map_oracle(
            epochs in proptest::collection::btree_set(0i32..100, 0..32),
            requested in -3i32..110,
            log_end in 0i64..10_000,
        ) {
            let entries = epochs
                .into_iter()
                .enumerate()
                .map(|(i, epoch)| {
                    entry(epoch, i64::try_from(i).expect("epoch set length is bounded to 32") * 10)
                })
                .collect::<Vec<_>>();
            prop_assert_eq!(
                epoch_and_offset_for_entries(
                    &entries,
                    LeaderEpoch(requested),
                    Offset(log_end),
                ),
                tree_map_oracle(&entries, LeaderEpoch(requested), Offset(log_end))
            );
        }
    }
}
