use {super::*, crate::restore::RestoreContentExclusions, proptest::prelude::*};

mod abort_union;
mod byte_quorum;
mod consume_trace;
mod list_offsets;
mod quota_time;
mod refill;
mod tiered_timestamp;
mod typed_timestamp;

fn restore_exclusions(key: bool) -> RestoreExclusions {
    RestoreExclusions {
        producer: false,
        offset: false,
        content: RestoreContentExclusions { key, header: false },
    }
}

/// Full-width available/debt/burst inputs and two whole-token charges.
fn quota_charge_cases() -> impl Strategy<Value = ((u64, u64), u64, u64, u64)> {
    (
        any::<u64>(),
        any::<u64>(),
        any::<u64>(),
        any::<bool>(),
        any::<u64>(),
        any::<u64>(),
    )
        .prop_map(|(available, debt, burst, owes, refill, requested)| {
            let balance = if owes {
                (0, debt)
            } else {
                (available.min(burst), 0)
            };
            (balance, burst, refill, requested)
        })
}

/// Original nonnegative reload coordinates, including saturation at `i64::MAX`.
fn replay_range_cases() -> impl Strategy<Value = (ProducerReloadRange, i64)> {
    (
        0i64..=i64::MAX,
        0i64..=i64::MAX,
        0i64..=i64::MAX,
        0i64..=i64::MAX,
    )
        .prop_map(|(start, span, local, tail)| {
            let cut = start.saturating_add(span);
            (
                ProducerReloadRange {
                    log_start: start,
                    local_start: local.min(cut),
                    log_end: cut.saturating_add(tail),
                },
                cut,
            )
        })
}

/// The same complete debt, burst and fractional-credit input domain for ledger tests.
fn token_balance_cases() -> impl Strategy<Value = ((u64, u64, u64), u64)> {
    (
        any::<u64>(),
        prop_oneof![0_u64..100_000_000, any::<u64>()],
        prop_oneof![0_u64..100_000_000, any::<u64>()],
        any::<bool>(),
        0_u64..1_000_000_000,
    )
        .prop_map(|(available, debt, burst, owes, fraction)| {
            let initial = if owes {
                (0, debt, fraction)
            } else {
                (available.min(burst), 0, fraction)
            };
            (initial, burst)
        })
}

fn token_acl(operation: AclOperationKind, allow: bool, flags: u16) -> TokenDescriptionAcl {
    let bit = |index| flags & (1_u16 << index) != 0_u16;
    TokenDescriptionAcl {
        resource: AclResourceFacts {
            resource_type: if bit(0) {
                AclResourceTypeMatch::Same
            } else {
                AclResourceTypeMatch::Different
            },
            exact_name: bit(1),
            wildcard_name: bit(2),
            name_has_prefix: bit(3),
        },
        pattern: if bit(4) {
            AclPatternKind::Literal
        } else {
            AclPatternKind::Prefixed
        },
        operation,
        allow,
        principal: (bit(5), bit(6)),
        host: (bit(7), bit(8), bit(9)),
    }
}

fn recovered_window_row(base: i64, delta: i32, last_sequence: i32) -> ProducerSnapshotEntryFacts {
    ProducerSnapshotEntryFacts {
        producer_id: 42,
        producer_epoch: 7,
        last_sequence,
        last_offset: base + i64::from(delta),
        offset_delta: delta,
        coordinator_epoch: -1,
        current_txn_first_offset: -1,
    }
}

type TimestampRecords = (Vec<u32>, Vec<i64>, Vec<(usize, usize)>);

fn timestamp_records(
    records: &std::collections::BTreeMap<u32, i64>,
    step: usize,
    span: usize,
) -> TimestampRecords {
    (
        records.keys().copied().collect(),
        records.values().copied().collect(),
        (0..records.len())
            .step_by(step)
            .map(|i| (i, (i + span).min(records.len() - 1)))
            .collect(),
    )
}

fn prefix_maxima(window: SparseTimestampWindow<'_>) -> Vec<TimeEntry> {
    let (offsets, times, rows) = window;
    rows.iter()
        .map(|&(indexed, through)| {
            TimeEntry::new(*times[..=through].iter().max().unwrap(), offsets[indexed])
        })
        .collect()
}

struct IndexedTimestampRecords {
    offsets: Vec<u32>,
    timestamps: Vec<i64>,
    entries: Vec<TimeEntry>,
}

fn indexed_timestamp_records(
    records: &std::collections::BTreeMap<u32, i64>,
    step: usize,
) -> IndexedTimestampRecords {
    let (offsets, times, rows) = timestamp_records(records, step, 0);
    let entries = prefix_maxima((&offsets, &times, &rows));
    IndexedTimestampRecords {
        offsets,
        timestamps: times,
        entries,
    }
}

fn oracle_sequence(value: i64) -> i32 { i32::try_from(value.rem_euclid(1_i64 << 31)).unwrap() }

fn oracle_retry_matches(row: &ProducerSnapshotEntryFacts, request: (i16, i32, i32, bool)) -> bool {
    request.0 == row.producer_epoch
        && request.1 == oracle_sequence(i64::from(row.last_sequence) - i64::from(row.offset_delta))
        && row.last_sequence == oracle_sequence(i64::from(request.1) + i64::from(request.2))
}

fn epoch_rows(count: usize, epoch: i16, sequence: i32) -> Vec<ProducerSnapshotEntryFacts> {
    (0..count)
        .map(|i| {
            let mut row = recovered_window_row(4 * i64::try_from(i).unwrap(), 2, sequence);
            row.producer_epoch = epoch;
            row
        })
        .collect()
}

fn maximum_epoch_row(epoch: i16) -> ProducerSnapshotEntryFacts {
    let mut row = recovered_window_row(i64::MAX - 1, 0, i32::MAX);
    row.producer_epoch = epoch;
    row
}

fn constant_time_window(offsets: &[u32]) -> SparseTimestampWindow<'_> {
    (offsets, &[100; 3], &[(0, 0), (1, 1), (2, 2)])
}

type ScheduledFixture<'a> = (&'a [(i64, i32)], &'a [i64]);

const VALID_SCHEDULES: [ScheduledFixture<'_>; 2] = [
    (&[(0, 1), (2, 1), (4, 1)], &[0, 100, 0]),
    (&[(0, 1), (4, 1)], &[10, 20]),
];

mod charge_refund_agrees_with_a_signed_ledger_and_detects_storage_loss;

mod replayed_window_keeps_first_alias_and_pads_the_last_batch_at_slot_four;

mod snapshot_retry_covers_wraparound_exhaustion_and_invalid_rows;

mod sparse_timestamp_composition_matches_full_record_oracle;

mod checked_wal_copy_boundaries;

mod trim_composition_boundaries_expose_inherited_frontier_requirement;

mod delivery_replication_and_restore_composition_boundaries;

mod restored_state_composition_boundaries;

mod restore_retry;

mod append;

mod stable_fetch;

mod loss_replay;

mod scheduled_prefix;

mod wal_placement;

mod epoch_replay;

mod offset_seek;

mod constructed_tiered_timestamp;

mod validated_time_scan;

mod trim_witnesses;

mod time_range;

mod stable_time_range;

mod covered_retention;

mod remote_breach;

mod remote_delete;

mod schema_walk;

mod schema_produce;

mod reconfiguration;

mod control_truncation;

mod truncated_producer;

mod snapshot_tail;

mod completed_producer;

mod completed_eviction;

mod epoch_handoff;

mod epoch_delayed;

mod rotation_marker;

mod marker_admission;

mod token_session;

mod oauth_completion;

mod jwks_publication;

mod controller_session;

fn oracle_next_producer_decision(
    last: Option<&ProducerSnapshotEntryFacts>,
    request: (i16, i32, i32, bool),
    end: i64,
) -> ProducerDecision {
    match last {
        Some(last) if request.0 < last.producer_epoch => ProducerDecision::Fenced,
        Some(last) if request.0 > last.producer_epoch => {
            if request.1 == 0 {
                ProducerDecision::Append
            } else {
                ProducerDecision::OutOfOrder
            }
        }
        Some(last) => {
            if request.1 == oracle_sequence(i64::from(last.last_sequence) + 1) {
                ProducerDecision::Append
            } else {
                ProducerDecision::OutOfOrder
            }
        }
        None if request.3 && end == 0 && request.1 != 0 => ProducerDecision::OutOfOrder,
        None => ProducerDecision::Append,
    }
}
