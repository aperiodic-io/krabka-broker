//! Diskless WAL admission decisions.

use creusot_std::prelude::*;

/// Result of authorizing and epoch-fencing one diskless WAL Fetch.
#[krabka_macros::model_type]
pub enum WalFetchAdmission {
    Denied,
    FencedLeaderEpoch,
    UnknownLeaderEpoch,
    Serve,
}

mod batch_equal;
#[cfg(creusot)]
pub use batch_equal::wal_batch_layout;
pub use batch_equal::{
    exact_wal_batch_range, wal_batch_equal, wal_checkpoint_range_valid, wal_covering_batch_range,
};

mod select_wal_voters;
pub use select_wal_voters::{
    select_wal_voter_index, select_wal_voters, wal_fetch_admission, wal_voter_set_valid,
};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// Every selected WAL voter has a distinct node ID and rack ID.
pub fn placement_identities_distinct(selected: Seq<(u64, u64)>) -> bool {
    pearlite! { forall<i: Int, j: Int> 0 <= i && i < j && j < selected.len()
    ==> selected[i].0 != selected[j].0 && selected[i].1 != selected[j].1 }
}

#[cfg(test)]
mod tests;
