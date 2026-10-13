use {
    super::{ProducerDecision, ProducerSnapshotEntryFacts, completed_batches_preserve_first_retry},
    crate::transaction::{
        InitProducerIdIdentityDecision, init_producer_id_identity_decision, next_producer_identity,
    },
    creusot_std::prelude::*,
};

type EpochHandoff = (
    i16,
    InitProducerIdIdentityDecision,
    Vec<usize>,
    ProducerDecision,
);

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Ordered same-producer rows and an admitted handoff at the current epoch.
pub fn handoff_input_valid(
    end: Int,
    hwm: Int,
    epoch: Int,
    rows: Seq<ProducerSnapshotEntryFacts>,
    first: ProducerSnapshotEntryFacts,
) -> bool {
    pearlite! {
        0 <= end && 0 <= hwm && 0 <= epoch && epoch < i16::MAX@ - 1
            && rows.len() <= 5
            && crate::producer_snapshot::snapshot_entry_valid_model(end, first) && first.last_offset@ >= 0 && first.producer_epoch@ == epoch
            && (forall<i: Int> 0 <= i && i < rows.len() ==> crate::producer_snapshot::snapshot_entry_valid_model(end, rows[i]) && rows[i].last_offset@ >= 0 && rows[i].producer_id == first.producer_id && rows[i].producer_epoch@ == epoch)
            && (forall<i: Int, j: Int> 0 <= i && i < j && j < rows.len() ==> rows[i].last_offset@ < rows[j].last_offset@)
    }
}

/// A verified same-PID completion advances the epoch. Once data at that epoch
/// completes, the old identity remains an `InitProducerId` retry while every
/// old-epoch data retry is fenced, even if its sequences match a retained alias.
/// Batch facts initially carry the old epoch; the transition supplies the new
/// identity. Actual batch projection, marker publication and PID routing remain
/// host obligations. PID rotation and recovery identities are separate paths.
#[requires(handoff_input_valid(end@, hwm@, epoch@, rows@, first))]
#[ensures(result.0@ == epoch@ + 1 && result.0@ < i16::MAX@)]
#[ensures(result.1 == InitProducerIdIdentityDecision::Retry)]
#[ensures(result.2@.len() == 1 && result.2@[0]@ == rows@.len())]
#[ensures(result.3 == ProducerDecision::Fenced)]
pub(super) fn epoch_handoff_distinguishes_identity_and_data_retry(
    end: i64,
    hwm: i64,
    epoch: i16,
    rows: &[ProducerSnapshotEntryFacts],
    mut first: ProducerSnapshotEntryFacts,
    request: (i32, i32),
) -> EpochHandoff {
    let (pid, next_epoch) =
        next_producer_identity(true, false, first.producer_id, epoch, None).unwrap();
    let identity =
        init_producer_id_identity_decision(pid, next_epoch, epoch, -1, first.producer_id, epoch);
    first.producer_epoch = next_epoch;
    let (_, selected, decision, _) = completed_batches_preserve_first_retry(
        end,
        hwm,
        Some(epoch),
        rows,
        first,
        (epoch, request.0, request.1),
    );
    (next_epoch, identity, selected, decision)
}
