#[cfg(creusot)]
use super::spec::{copy_admitted, reference_valid};
use {
    super::{WalCopyBatch, WalCopyObservation},
    crate::{
        composition::covering_copy_preserves_logical_fetch,
        wal::{WalFetchAdmission, wal_fetch_admission},
    },
    creusot_std::prelude::*,
};

/// Derive a vote from authenticated epoch admission and a durably completed
/// byte-identical copy, including a logical floor inside the first batch.
/// These observed bytes and the completion flag must faithfully describe the
/// fsynced/checkpointed replica. This does not establish actual I/O completion.
#[requires(reference_valid(reference.0@, reference.1@, reference.2@))]
#[ensures((result != None) ==
    copy_admitted(voters@, claimed, local, leader_epoch, reference.0@, *observation))]
#[ensures(match result {
    None => true,
    Some(end) => reference.1@ <= end@ && end@ <= reference.2@
        && end@ == if observation.3@.len() == 0 { reference.1@ }
            else { reference.0@[observation.3@.len() - 1].0@
                + reference.0@[observation.3@.len() - 1].1@ + 1 }
        && observation.2 && observation.0 == Some(claimed)
        && (observation.1@ < 0 || observation.1 == leader_epoch)
        && observation.3@.len() <= reference.0@.len()
        && forall<i: Int> 0 <= i && i < observation.3@.len()
            ==> observation.3@[i].0 == reference.0@[i].0 && observation.3@[i].1 == reference.0@[i].1 && observation.3@[i].2@ == reference.0@[i].2@,
})]
pub(super) fn authenticated_copy_vote(
    voters: &[u64],
    claimed: u64,
    local: u64,
    leader_epoch: i32,
    reference: (&[WalCopyBatch], i64, i64), // source, logical floor, end
    observation: &WalCopyObservation,
) -> Option<i64> {
    if !observation.2
        || !matches!(
            wal_fetch_admission(
                observation.0,
                claimed,
                local,
                voters,
                observation.1,
                leader_epoch,
            ),
            WalFetchAdmission::Serve
        )
        || observation.3.len() > reference.0.len()
    {
        return None;
    }
    let count = observation.3.len();
    let end = if count == 0 {
        reference.1
    } else {
        reference.0[count - 1].0 + i64::from(reference.0[count - 1].1) + 1
    };
    covering_copy_preserves_logical_fetch(
        &reference.0[..count],
        &observation.3,
        (reference.1, reference.1),
        end,
        0,
        u64::MAX,
        reference.1,
    )?;
    Some(end)
}
