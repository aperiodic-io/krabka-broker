//! The RED witness: the legacy control-dedup cleaner, and the tests that show
//! the checker's pass rules reject it.
//!
//! A model that no configuration can break is not evidence, so the broken
//! cleaner and the runs that expose it stay together in one file. Both runs go
//! through the model's real [`compact_pass`], with only the [`Cleaner`]
//! swapped.

use {
    super::{
        invariants::Invariant,
        pass::compact_pass,
        state::{Cleaner, CompactAction, CompactModel, Entry, EntryKind},
    },
    crate::compact::{BatchMeta, RecordMeta, RetainDecision, TxnDataState, retain_decision},
    stateright::{Checker, Model},
    std::collections::BTreeSet,
};

/// The OLD dedup-map filter: every keyed record is indexed, control records
/// included. Two commit markers carry the same `ControlRecordType` key bytes,
/// so the newer one supersedes the older in the map.
fn legacy_should_index_key(key: Option<&[u8]>, _is_control_batch: bool) -> bool { key.is_some() }

/// The OLD retain decision. A control record is treated as keyed data: it is
/// kept only while it is the newest record for its control key, and an older
/// marker is deleted as a superseded duplicate. That is the control-batch
/// data-loss bug the KIP-534 fix removes. The data path is the fixed core's.
fn legacy_retain(
    rec: RecordMeta,
    batch: BatchMeta,
    is_newest_for_key: bool,
    txn: TxnDataState,
    now_ms: i64,
    delete_retention_ms: i64,
) -> RetainDecision {
    if batch.is_control {
        return if is_newest_for_key {
            RetainDecision::Keep
        } else {
            RetainDecision::Delete
        };
    }
    retain_decision(
        rec,
        batch,
        is_newest_for_key,
        txn,
        now_ms,
        delete_retention_ms,
    )
}

const LEGACY: Cleaner = Cleaner {
    index_key: legacy_should_index_key,
    retain: legacy_retain,
};

fn data(key: u8) -> Entry {
    Entry {
        key: Some(key),
        kind: EntryKind::Data { value: Some(0) },
        horizon: None,
    }
}

fn commit(producer_id: u8) -> Entry {
    Entry {
        key: None,
        kind: EntryKind::Marker {
            producer_id,
            commit: true,
        },
        horizon: None,
    }
}

/// The recorded counterexample, run through the real pass under each cleaner.
///
/// Two committed transactions, pid 0 and pid 1, whose data both survives.
/// The production cleaner keeps the log unchanged. The legacy cleaner dedups
/// the two markers by their shared control key and deletes pid 0's, while
/// pid 0's data is still live.
#[test]
fn legacy_cleaner_breaks_the_marker_rules_on_the_recorded_counterexample() {
    let input = vec![data(0), commit(0), data(1), commit(1)];
    for (name, cleaner, want_output, want_violations) in [
        ("production", Cleaner::PRODUCTION, input.clone(), vec![]),
        ("legacy", LEGACY, vec![data(0), data(1), commit(1)], vec![
            Invariant::ControlNotDeduped,
            Invariant::MarkerDataPrecedence,
        ]),
    ] {
        let output = compact_pass(&input, 0, cleaner);
        assert2::assert!(output == want_output, "cleaner {name}");
        assert2::assert!(
            Invariant::violations(&input, &output, 0) == want_violations,
            "cleaner {name}"
        );
    }
}

/// The checker, run over the legacy cleaner, finds exactly the two marker
/// rules broken, and the recorded action path is one of its counterexamples.
#[test]
fn checker_rejects_the_legacy_cleaner() {
    let checker = CompactModel {
        max_len: 4,
        max_clock: 4,
        cleaner: LEGACY,
    }
    .checker()
    .spawn_bfs()
    .join();
    let broken: BTreeSet<&str> = checker
        .discoveries()
        .into_keys()
        .filter(|name| {
            [
                "control_not_deduped",
                "marker_data_precedence",
                "tombstone_aging",
                "idempotent_stamp",
                "no_data_loss",
            ]
            .contains(name)
        })
        .collect();
    assert2::assert!(broken == BTreeSet::from(["control_not_deduped", "marker_data_precedence"]));
    for property in ["control_not_deduped", "marker_data_precedence"] {
        checker.assert_discovery(property, vec![
            CompactAction::AppendData(0, 0),
            CompactAction::AppendCommit(0),
            CompactAction::AppendData(1, 0),
            CompactAction::AppendCommit(1),
            CompactAction::Compact,
        ]);
    }
}
