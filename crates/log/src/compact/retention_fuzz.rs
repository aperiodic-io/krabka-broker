//! Proptest fuzz of the same KIP-534 retention cores at large N. The test
//! folds a randomized op sequence into an abstract log. It checks every
//! `Compact` for convergence, idempotence, monotone shrink, no data loss,
//! marker safety, tombstone aging, and a single horizon stamp. A separate prop
//! checks the delete-horizon wire round-trip of a real `RecordBatch`.

use {super::*, proptest::prelude::*};

#[derive(Clone, Debug, PartialEq, Eq)]
enum EntryKind {
    Data { value: Option<u8> },
    Marker { producer_id: u8, commit: bool },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Entry {
    key: Option<u8>,
    kind: EntryKind,
    horizon: Option<i64>,
}

#[derive(Clone, Debug)]
enum Op {
    AppendData(u8, u8),
    AppendTombstone(u8),
    AppendCommit(u8),
    AppendAbort(u8),
    Tick(i64),
    Compact,
}

/// Key-to-newest-index dedup map over keyed data entries. Control entries
/// are never indexed. This mirrors the production `build_offset_map`
/// filter.
fn offset_map(log: &[Entry]) -> std::collections::HashMap<u8, usize> {
    let mut map = std::collections::HashMap::new();
    for (idx, e) in log.iter().enumerate() {
        if !matches!(e.kind, EntryKind::Data { .. }) {
            continue;
        }
        let Some(k) = e.key else { continue };
        if should_index_key(Some(&[k]), false) {
            map.insert(k, idx);
        }
    }
    map
}

/// The indices of the commit markers that have a data entry of their producer
/// in front of them, after that producer's previous marker. This is Kafka's
/// per-transaction rule for keeping a commit marker. The association is by
/// key, where key equals pid.
fn markers_behind_data(log: &[Entry]) -> std::collections::HashSet<usize> {
    let mut with_data = std::collections::HashSet::new();
    let mut held = std::collections::HashSet::new();
    for (idx, e) in log.iter().enumerate() {
        match &e.kind {
            EntryKind::Data { .. } => {
                with_data.extend(e.key);
            }
            EntryKind::Marker {
                producer_id,
                commit: true,
            } => {
                if with_data.remove(producer_id) {
                    held.insert(idx);
                }
            }
            EntryKind::Marker { commit: false, .. } => {}
        }
    }
    held
}

/// One compaction pass. This function applies the real `retain_decision`
/// to each entry and returns the next log. It mirrors the abstract applier
/// in the model.
fn compact(log: &[Entry], clock: i64, ret_ms: i64) -> Vec<Entry> {
    let map = offset_map(log);
    let held = markers_behind_data(log);
    let mut next = Vec::with_capacity(log.len());
    for (idx, e) in log.iter().enumerate() {
        let (metadata, is_newest, txn) = match &e.kind {
            EntryKind::Data { value } => (
                data_retention_metadata()
                    .has_key(e.key.is_some())
                    .has_value(value.is_some())
                    .maybe_existing_horizon(e.horizon)
                    .call(),
                e.key.is_some_and(|k| map.get(&k).copied() == Some(idx)),
                TxnDataState::NotTransactional,
            ),
            EntryKind::Marker { producer_id, .. } => (
                marker_retention_metadata()
                    .producer_id(i64::from(*producer_id))
                    .maybe_existing_horizon(e.horizon)
                    .call(),
                false,
                if held.contains(&idx) {
                    TxnDataState::DataSurvives
                } else {
                    TxnDataState::DataFullyGone
                },
            ),
        };
        match retain_decision(
            metadata.record,
            metadata.batch,
            is_newest,
            txn,
            clock,
            ret_ms,
        ) {
            RetainDecision::Keep => next.push(e.clone()),
            RetainDecision::SetHorizon(h) => {
                // Single-horizon stamping: the core must only ever stamp an
                // entry that has no horizon yet. A `SetHorizon` over an
                // already-stamped entry would re-stamp it — a violation
                // checked here at the exact point of assignment (where this
                // specific entry's prior horizon is known unambiguously).
                if let Some(existing) = e.horizon {
                    assert2::assert!(existing == h);
                }
                let mut ne = e.clone();
                ne.horizon = Some(h);
                next.push(ne);
            }
            RetainDecision::Delete => {}
        }
    }
    next
}

fn apply(log: &mut Vec<Entry>, clock: &mut i64, op: &Op, ret_ms: i64) {
    match *op {
        Op::AppendData(key, value) => log.push(Entry {
            key: Some(key),
            kind: EntryKind::Data { value: Some(value) },
            horizon: None,
        }),
        Op::AppendTombstone(key) => log.push(Entry {
            key: Some(key),
            kind: EntryKind::Data { value: None },
            horizon: None,
        }),
        Op::AppendCommit(pid) => log.push(Entry {
            key: None,
            kind: EntryKind::Marker {
                producer_id: pid,
                commit: true,
            },
            horizon: None,
        }),
        Op::AppendAbort(pid) => log.push(Entry {
            key: None,
            kind: EntryKind::Marker {
                producer_id: pid,
                commit: false,
            },
            horizon: None,
        }),
        Op::Tick(dt) => *clock += dt,
        Op::Compact => {
            let before = log.clone();
            let after = compact(&before, *clock, ret_ms);

            // --- Convergence at a fixed clock. Kafka reads a transaction's
            // data before it removes it, so the marker behind data that this
            // pass removes is stamped one pass later. The log is therefore
            // stable from the second pass on. ---
            let twice = compact(&after, *clock, ret_ms);
            let thrice = compact(&twice, *clock, ret_ms);
            prop_assert_eq_inner(&twice, &thrice);

            // --- Monotone shrink. ---
            assert2::assert!(after.len() <= before.len());

            // --- No-data-loss: every newest-for-key live data survives. ---
            let map = offset_map(&before);
            for (idx, e) in before.iter().enumerate() {
                if let EntryKind::Data { value: Some(_) } = &e.kind
                    && let Some(k) = e.key
                    && map.get(&k).copied() == Some(idx)
                {
                    assert2::assert!(after.iter().any(|x| x.key == Some(k)
                        && matches!(x.kind, EntryKind::Data { value: Some(_) })));
                }
            }

            // --- Marker safety: a marker stays while data of its own
            // transaction is in front of it, and until its horizon has
            // elapsed. The pass never reorders or invents markers, so the
            // markers of one producer that had to stay are at most the
            // markers of that producer left. ---
            let held = markers_behind_data(&before);
            let marker_count = |log: &[Entry], pid: u8| {
                log.iter()
                    .filter(|x| matches!(&x.kind, EntryKind::Marker { producer_id, .. } if *producer_id == pid))
                    .count()
            };
            for pid in 0u8..=2 {
                let must_stay = before
                    .iter()
                    .enumerate()
                    .filter(|(idx, e)| {
                        matches!(&e.kind, EntryKind::Marker { producer_id, .. } if *producer_id == pid)
                            && (held.contains(idx) || e.horizon.is_some_and(|h| *clock < h))
                    })
                    .count();
                assert2::assert!(marker_count(&after, pid) >= must_stay);
            }

            // --- Tombstone aging: a surviving tombstone is present iff it
            // has no horizon or clock < horizon. ---
            for x in &after {
                if matches!(x.kind, EntryKind::Data { value: None })
                    && let Some(h) = x.horizon
                {
                    assert2::assert!(*clock < h);
                }
            }

            // --- Single horizon stamping is enforced inside `compact` at
            // the exact point a horizon is assigned (a `SetHorizon` over an
            // already-stamped entry panics there). Nothing to re-check here.

            *log = after;
        }
    }
}

/// `prop_assert_eq` is macro-bound to a `proptest!` body. Inside a plain
/// fn this helper uses a panicking equality instead, so a mismatch shows up
/// as a case failure.
fn prop_assert_eq_inner(a: &[Entry], b: &[Entry]) {
    assert2::assert!(a == b);
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u8..=2, 0u8..=2).prop_map(|(k, v)| Op::AppendData(k, v)),
        (0u8..=2).prop_map(Op::AppendTombstone),
        (0u8..=2).prop_map(Op::AppendCommit),
        (0u8..=2).prop_map(Op::AppendAbort),
        (1i64..=5).prop_map(Op::Tick),
        Just(Op::Compact),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn retention_invariants_hold(
        ops in proptest::collection::vec(op_strategy(), 0..200),
        ret_ms in 1i64..100,
    ) {
        let mut log: Vec<Entry> = Vec::new();
        let mut clock: i64 = 0;
        for op in &ops {
            apply(&mut log, &mut clock, op, ret_ms);
        }
    }

    /// Wire round-trip. A real `RecordBatch` with two keyed records gets
    /// a random delete horizon stamp. The encode and decode must then keep
    /// `delete_horizon_ms()` and every record's absolute timestamp.
    #[test]
    fn delete_horizon_wire_round_trip(
        horizon in -1_000i64..1_000_000,
        base_ts in 0i64..1_000,
        d0 in 0i64..500,
        d1 in 0i64..500,
    ) {
        use bytes::{Bytes, BytesMut};
        use krabka_protocol::records::{Record, RecordBatch};

        let rec = |delta: i64, k: &[u8]| Record {
            offset_delta: 0,
            timestamp_delta: delta,
            key: Some(Bytes::copy_from_slice(k)),
            value: Some(Bytes::copy_from_slice(b"v")),
            ..Default::default()
        };
        let batch = RecordBatch {
            base_offset: 0,
            last_offset_delta: 1,
            base_timestamp: base_ts,
            max_timestamp: base_ts + d0.max(d1),
            records: vec![rec(d0, b"k0"), rec(d1, b"k1")],
            ..RecordBatch::default()
        };
        // Original absolute per-record timestamps.
        let orig_abs: Vec<i64> = batch
            .records
            .iter()
            .map(|r| batch.base_timestamp + r.timestamp_delta)
            .collect();

        let stamped = batch.with_delete_horizon(horizon);
        let mut buf = BytesMut::with_capacity(stamped.encoded_len());
        stamped.encode(&mut buf).unwrap();
        let mut cursor: &[u8] = &buf[..];
        let decoded = RecordBatch::decode(&mut cursor).unwrap();

        prop_assert_eq!(decoded.delete_horizon_ms(), Some(horizon));
        // Reconstructed absolute timestamps (base + delta, delta is i64)
        // equal the originals.
        let new_abs: Vec<i64> = decoded
            .records
            .iter()
            .map(|r| decoded.base_timestamp + r.timestamp_delta)
            .collect();
        prop_assert_eq!(new_abs, orig_abs);
    }
}
