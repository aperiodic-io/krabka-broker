//! The abstract state the compaction model enumerates: a log [`Entry`], the
//! [`CompactState`] the checker fingerprints, the [`CompactAction`] alphabet,
//! the [`Cleaner`] a pass runs, and the [`CompactModel`] bounds together with
//! the derivations that a compaction pass needs.

use {
    crate::compact::{
        BatchMeta, RecordMeta, RetainDecision, TxnDataState, retain_decision, should_index_key,
    },
    std::collections::{HashMap, HashSet},
};

/// What a log entry carries downstream of the compaction decision.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum EntryKind {
    /// A data record. `value: None` is a tombstone.
    Data { value: Option<u8> },
    /// A transaction control marker, commit or abort, for `producer_id`.
    Marker { producer_id: u8, commit: bool },
}

/// One abstract log entry. `horizon` mirrors the batch's KIP-534
/// delete-horizon stamp. It is `None` until the stamp, and then
/// `Some(now + delete.retention.ms)`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct Entry {
    pub(super) key: Option<u8>,
    pub(super) kind: EntryKind,
    pub(super) horizon: Option<i64>,
}

impl Entry {
    /// Whether this entry carries a delete horizon that `clock` has reached.
    pub(super) fn horizon_elapsed(&self, clock: i64) -> bool {
        self.horizon.is_some_and(|h| clock >= h)
    }

    /// The key this entry offers the cleaner's dedup filter, if any.
    fn map_key(&self) -> Option<MapKey> {
        match self.kind {
            EntryKind::Data { .. } => self.key.map(MapKey::Data),
            EntryKind::Marker { commit, .. } => Some(MapKey::Control { commit }),
        }
    }
}

/// A key in the cleaner's key→newest-offset dedup map. Data keys and control
/// keys are different byte strings, so they never collide.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) enum MapKey {
    Data(u8),
    Control { commit: bool },
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) struct CompactState {
    pub(super) log: Vec<Entry>,
    /// Abstract wall clock in ms. Entries hold horizons as absolute stamp
    /// values, and the model compares them against this clock. The state does
    /// NOT hold latched non-vacuity witnesses. [`stateright::Model::properties`]
    /// derives them from `(log, clock, last_pass)`, which keeps the monotonic
    /// witness bools out of the fingerprint. Those would otherwise multiply
    /// the reachable state space by about 32.
    pub(super) clock: i64,
    /// The log that the transition into this state compacted, when that
    /// transition was a `Compact`, and `None` after any other transition.
    ///
    /// It is what lets the pass invariants be `always` properties over the
    /// pair `(last_pass, log)`, so that a violation comes back from the checker
    /// as a counterexample path rather than a panic inside `next_state`.
    pub(super) last_pass: Option<Vec<Entry>>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(super) enum CompactAction {
    AppendData(u8, u8),
    AppendTombstone(u8),
    AppendCommit(u8),
    Tick(i64),
    Compact,
}

/// The dedup-map filter signature: [`should_index_key`]'s.
pub(super) type IndexKeyFn = fn(Option<&[u8]>, bool) -> bool;

/// The retain-decision signature: [`retain_decision`]'s.
pub(super) type RetainFn =
    fn(RecordMeta, BatchMeta, bool, TxnDataState, i64, i64) -> RetainDecision;

/// The two decisions a cleaner makes: which records enter the dedup map, and
/// what happens to each record given the map. The model is generic over them so
/// that the same checker runs against the production cores and against the
/// deliberately-broken legacy pair in `legacy.rs`.
#[derive(Clone, Copy)]
pub(super) struct Cleaner {
    pub(super) index_key: IndexKeyFn,
    pub(super) retain: RetainFn,
}

impl Cleaner {
    /// The production cores the log cleaner's rewrite path runs.
    pub(super) const PRODUCTION: Self = Self {
        index_key: should_index_key,
        retain: retain_decision,
    };

    /// Build the key→newest-index dedup map over the entries this cleaner's
    /// filter admits. Later positions overwrite earlier ones, so the newest
    /// wins.
    pub(super) fn offset_map(self, log: &[Entry]) -> HashMap<MapKey, usize> {
        let mut map: HashMap<MapKey, usize> = HashMap::new();
        for (idx, entry) in log.iter().enumerate() {
            if let Some(map_key) = entry.map_key()
                && self.admits(map_key)
            {
                map.insert(map_key, idx);
            }
        }
        map
    }

    /// Whether this cleaner's filter lets `map_key` into the dedup map. A data
    /// record offers its own key bytes. A control record always carries the
    /// `ControlRecordType` key: version `0` and then type `COMMIT = 1` or
    /// `ABORT = 0`, each a big-endian `int16`.
    fn admits(self, map_key: MapKey) -> bool {
        match map_key {
            MapKey::Data(k) => (self.index_key)(Some(&[k]), false),
            MapKey::Control { commit } => {
                (self.index_key)(Some(&[0, 0, 0, u8::from(commit)]), true)
            }
        }
    }

    /// Whether `log[idx]` is the entry the dedup map holds for its key.
    pub(super) fn is_newest(
        log: &[Entry],
        offset_map: &HashMap<MapKey, usize>,
        idx: usize,
    ) -> bool {
        log[idx]
            .map_key()
            .is_some_and(|map_key| offset_map.get(&map_key).copied() == Some(idx))
    }

    /// The transactional-data state of every marker in `log`, by index, as the
    /// production rewrite derives it from `CleanedTransactionMetadata`: a
    /// marker is [`TxnDataState::DataFullyGone`] when the pass reads no data
    /// entry of its transaction between the previous marker of its producer
    /// and itself, and [`TxnDataState::DataSurvives`] otherwise.
    ///
    /// The pass reads an entry before it decides the entry's fate, so a data
    /// entry the pass is about to delete still holds the marker behind it for
    /// this pass.
    ///
    /// Data entries in this abstract model are anonymous, so the model
    /// associates producers with data by key: marker `pid` goes with the data
    /// entries under key `pid`. Every data entry belongs to a committed
    /// transaction, so an abort marker never has a batch of its transaction
    /// behind it.
    pub(super) fn marker_states(log: &[Entry]) -> HashMap<usize, TxnDataState> {
        let mut observed: HashSet<u8> = HashSet::new();
        let mut states = HashMap::new();
        for (idx, entry) in log.iter().enumerate() {
            match entry.kind {
                EntryKind::Data { .. } => {
                    observed.extend(entry.key);
                }
                EntryKind::Marker {
                    producer_id,
                    commit,
                } => {
                    let has_data = commit && observed.remove(&producer_id);
                    states.insert(
                        idx,
                        if has_data {
                            TxnDataState::DataSurvives
                        } else {
                            TxnDataState::DataFullyGone
                        },
                    );
                }
            }
        }
        states
    }
}

pub(super) struct CompactModel {
    /// Maximum log length the actions generator and `within_boundary` enforce.
    pub(super) max_len: usize,
    /// Maximum value `clock` may reach.
    pub(super) max_clock: i64,
    /// The cleaner every `Compact` transition runs.
    pub(super) cleaner: Cleaner,
}
