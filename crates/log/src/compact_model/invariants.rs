//! The five KIP-534 safety rules one compaction pass must obey, stated over the
//! pass's input log, its output log and the clock it ran at.
//!
//! Every rule is computed from the two logs alone. None of them calls the
//! production cores or reads bookkeeping from the pass, so a pass cannot
//! satisfy a rule by construction: the rules are the specification the pass is
//! checked against.
//!
//! A pass may only delete entries and stamp a horizon on an entry that has
//! none, so its output must be an order-preserving subsequence of its input
//! under that one permitted rewrite. [`Invariant::IdempotentStamp`] checks that
//! shape. The retention rules each name the input entries a pass must *not*
//! delete, and hold when those entries appear, in order, in the output. See
//! [`retains`]. The two checks are independent, so a pass that breaks one rule
//! is reported against that rule alone.

use {
    super::{
        DELETE_RETENTION_MS,
        state::{Entry, EntryKind},
    },
    std::collections::{HashMap, HashSet},
};

/// One named pass rule. The name is the `always` property that checks it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Invariant {
    /// A control marker leaves the log only through its delete horizon. Every
    /// input marker whose horizon has not elapsed is in the output. Markers
    /// are never deduped against one another by their shared control key.
    ControlNotDeduped,
    /// A marker never leaves the log while a data entry of its transaction is
    /// in the pass's input: every input marker with a data entry of its
    /// producer in front of it, since that producer's previous marker, is in
    /// the output, whether or not its horizon has elapsed. Kafka decides this
    /// per transaction, so a marker whose transaction's data is gone ages out
    /// even when the same producer has newer live data.
    MarkerDataPrecedence,
    /// A tombstone stays for `delete.retention.ms` and then goes. No output
    /// tombstone has an elapsed horizon, and every input tombstone that is the
    /// newest for its key and has not reached its horizon is in the output.
    TombstoneAging,
    /// The pass only deletes entries and stamps unstamped horizons with
    /// `now + delete.retention.ms`. It never changes an existing horizon and
    /// never invents or reorders an entry.
    IdempotentStamp,
    /// The set of keys whose newest value is live is unchanged. No live newest
    /// value is lost, and deleting a tombstone never resurrects a superseded
    /// value.
    NoDataLoss,
}

impl Invariant {
    pub(super) const ALL: [Self; 5] = [
        Self::ControlNotDeduped,
        Self::MarkerDataPrecedence,
        Self::TombstoneAging,
        Self::IdempotentStamp,
        Self::NoDataLoss,
    ];

    /// Whether this rule holds for a pass at `clock` that turned `input` into
    /// `output`.
    pub(super) fn holds(self, input: &[Entry], output: &[Entry], clock: i64) -> bool {
        match self {
            Self::ControlNotDeduped => retains(input, output, |_, e| {
                matches!(e.kind, EntryKind::Marker { .. }) && !e.horizon_elapsed(clock)
            }),
            Self::MarkerDataPrecedence => {
                let held = markers_behind_data(input);
                retains(input, output, |idx, _| held.contains(&idx))
            }
            Self::TombstoneAging => {
                let newest = newest_data_indices(input);
                output.iter().all(|e| {
                    !(matches!(e.kind, EntryKind::Data { value: None }) && e.horizon_elapsed(clock))
                }) && retains(input, output, |idx, e| {
                    matches!(e.kind, EntryKind::Data { value: None })
                        && newest.contains(&idx)
                        && !e.horizon_elapsed(clock)
                })
            }
            Self::IdempotentStamp => is_stamp_only_subsequence(input, output, clock),
            Self::NoDataLoss => live_keys(input) == live_keys(output),
        }
    }

    /// Every rule `input → output` breaks, in [`Invariant::ALL`] order.
    pub(super) fn violations(input: &[Entry], output: &[Entry], clock: i64) -> Vec<Self> {
        Self::ALL
            .into_iter()
            .filter(|inv| !inv.holds(input, output, clock))
            .collect()
    }
}

/// Keys whose newest data entry carries a value.
pub(super) fn live_keys(log: &[Entry]) -> HashSet<u8> {
    let mut newest: HashMap<u8, bool> = HashMap::new();
    for entry in log {
        if let (EntryKind::Data { value }, Some(k)) = (&entry.kind, entry.key) {
            newest.insert(k, value.is_some());
        }
    }
    newest
        .into_iter()
        .filter_map(|(k, live)| live.then_some(k))
        .collect()
}

/// Indices of the commit markers that have a data entry of their producer in
/// front of them, after that producer's previous marker. The model associates a
/// producer with the data under the key equal to its id.
fn markers_behind_data(log: &[Entry]) -> HashSet<usize> {
    let mut with_data: HashSet<u8> = HashSet::new();
    let mut held = HashSet::new();
    for (idx, entry) in log.iter().enumerate() {
        match entry.kind {
            EntryKind::Data { .. } => {
                with_data.extend(entry.key);
            }
            EntryKind::Marker {
                producer_id,
                commit: true,
            } => {
                if with_data.remove(&producer_id) {
                    held.insert(idx);
                }
            }
            EntryKind::Marker { commit: false, .. } => {}
        }
    }
    held
}

/// Indices of the entries that are the newest data entry for their key.
fn newest_data_indices(log: &[Entry]) -> HashSet<usize> {
    let mut newest: HashMap<u8, usize> = HashMap::new();
    for (idx, entry) in log.iter().enumerate() {
        if let (EntryKind::Data { .. }, Some(k)) = (&entry.kind, entry.key) {
            newest.insert(k, idx);
        }
    }
    newest.into_values().collect()
}

/// Whether a pass at `clock` may rewrite input entry `from` into output entry
/// `to`: same key and payload, and the horizon either carried unchanged or
/// stamped from `None` to `clock + delete.retention.ms`.
fn rewrites_to(from: &Entry, to: &Entry, clock: i64) -> bool {
    from.key == to.key
        && from.kind == to.kind
        && (from.horizon == to.horizon
            || (from.horizon.is_none()
                && to.horizon == Some(clock.saturating_add(DELETE_RETENTION_MS))))
}

/// Whether the input entries `required` selects appear, in order, in
/// `output`: each matched to a later output entry with the same key and
/// payload. The horizon is not compared, because a stamp is a lawful rewrite
/// and [`Invariant::IdempotentStamp`] owns its value.
fn retains(input: &[Entry], output: &[Entry], required: impl Fn(usize, &Entry) -> bool) -> bool {
    let mut rest = output.iter();
    input
        .iter()
        .enumerate()
        .filter(|&(idx, e)| required(idx, e))
        .all(|(_, e)| rest.any(|o| o.key == e.key && o.kind == e.kind))
}

/// Whether `output` is an order-preserving subsequence of `input` under
/// [`rewrites_to`].
///
/// `fits[i][j]` says whether `output[j..]` aligns into `input[i..]`. It is
/// filled from the back: an input entry is either skipped, deleted by the
/// pass, or matched to the next output entry, which [`rewrites_to`] must
/// allow. Greedy matching would not do: an input may hold two entries with the
/// same key and payload, only one of which the output entry's horizon fits.
fn is_stamp_only_subsequence(input: &[Entry], output: &[Entry], clock: i64) -> bool {
    let (n, m) = (input.len(), output.len());
    let mut fits = vec![vec![false; m + 1]; n + 1];
    fits[n][m] = true;
    for i in (0..n).rev() {
        for j in (0..=m).rev() {
            let matched = j < m && rewrites_to(&input[i], &output[j], clock) && fits[i + 1][j + 1];
            fits[i][j] = fits[i + 1][j] || matched;
        }
    }
    fits[0][0]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(key: u8, value: Option<u8>, horizon: Option<i64>) -> Entry {
        Entry {
            key: Some(key),
            kind: EntryKind::Data { value },
            horizon,
        }
    }

    fn marker(producer_id: u8, horizon: Option<i64>) -> Entry {
        Entry {
            key: None,
            kind: EntryKind::Marker {
                producer_id,
                commit: true,
            },
            horizon,
        }
    }

    /// Each row is a hand-written pass that breaks exactly the rules listed,
    /// or a lawful pass that breaks none. The rules are the specification, so
    /// they are pinned against scenarios rather than against the model's own
    /// pass.
    #[test]
    fn each_rule_rejects_the_pass_that_breaks_it() {
        let clock = 4;
        let stamp = Some(clock + DELETE_RETENTION_MS);
        for (name, input, output, want) in [
            (
                "lawful: superseded value dropped, tombstone stamped, marker kept",
                vec![data(0, Some(0), None), data(0, None, None), marker(1, None)],
                vec![data(0, None, stamp), marker(1, stamp)],
                vec![],
            ),
            (
                "lawful: aged tombstone and aged marker without data leave",
                vec![data(0, None, Some(clock)), marker(1, Some(clock - 1))],
                vec![],
                vec![],
            ),
            (
                "lawful: an aged marker leaves although its producer has newer live data",
                vec![marker(0, Some(clock - 1)), data(0, Some(0), None)],
                vec![data(0, Some(0), None)],
                vec![],
            ),
            (
                "legacy dedup: older marker dropped against the newer one",
                vec![
                    data(0, Some(0), None),
                    marker(0, None),
                    data(1, Some(0), None),
                    marker(1, None),
                ],
                vec![
                    data(0, Some(0), None),
                    data(1, Some(0), None),
                    marker(1, None),
                ],
                vec![
                    Invariant::ControlNotDeduped,
                    Invariant::MarkerDataPrecedence,
                ],
            ),
            (
                "aged marker dropped while data of its transaction is in front of it",
                vec![data(0, Some(0), None), marker(0, Some(clock))],
                vec![data(0, Some(0), None)],
                vec![Invariant::MarkerDataPrecedence],
            ),
            (
                "unexpired newest tombstone dropped",
                vec![data(0, None, Some(clock + 1))],
                vec![],
                vec![Invariant::TombstoneAging],
            ),
            (
                "aged tombstone kept",
                vec![data(0, None, Some(clock))],
                vec![data(0, None, Some(clock))],
                vec![Invariant::TombstoneAging],
            ),
            (
                "existing horizon re-stamped",
                vec![marker(1, Some(clock + 1))],
                vec![marker(1, stamp)],
                vec![Invariant::IdempotentStamp],
            ),
            (
                "newest live value dropped",
                vec![data(0, Some(0), None)],
                vec![],
                vec![Invariant::NoDataLoss],
            ),
            (
                "tombstone dropped ahead of the value it superseded",
                vec![data(0, Some(0), None), data(0, None, Some(clock))],
                vec![data(0, Some(0), None)],
                vec![Invariant::NoDataLoss],
            ),
        ] {
            assert2::assert!(
                Invariant::violations(&input, &output, clock) == want,
                "case {name}"
            );
        }
    }
}
