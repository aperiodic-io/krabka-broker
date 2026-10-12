//! Exhaustive stateright model of a three-site stretch cluster with a
//! data-bearing witness. The design is
//! [KFC-2](../../../docs/KFCs/KFC-2-witness-broker-stretch-cluster.md).
//!
//! A witness replicates the partition, it counts toward `min.insync.replicas`,
//! and it votes in `KRaft`. It serves no client, and it never takes partition
//! leadership. One replica for each of three sites, with
//! `min.insync.replicas=2`, then survives the loss of any single site with
//! `acks=all` intact. This model checks that claim over every reachable state
//! of a bounded cluster.
//!
//! # The model drives the real code
//!
//! The model adds no election logic of its own.
//!
//! - [`stretch_replicas`](crate::site_placement::stretch_replicas) builds the
//!   replica list, so the model inherits the real site spread and the real
//!   leadership pinning.
//! - [`crate::leader_election::failover_one`] takes every controller failover decision.
//! - [`select_new_leader_for_partition`](crate::leader_election::select_new_leader_for_partition)
//!   takes every KIP-460 preferred election.
//! - The durability arithmetic comes from the Creusot-proved kernels in
//!   [`krabka_verified::stretch`].
//!
//! Two behaviours have no pure production core to call, so the model states
//! them itself and names the code they mirror.
//!
//! - `SiteUp` and `SiteHeal` fold in the `isr_maintenance` catch-up. A replica
//!   that runs again and that reaches the leader rejoins the in-sync replica
//!   set, because an idle partition has nothing to catch up on.
//! - `ProduceAcksAll` mirrors the two `acks=all` gates of
//!   `handlers::produce`. An in-sync replica set under `min.insync.replicas`
//!   gives `NOT_ENOUGH_REPLICAS`. An in-sync replica that the leader cannot
//!   reach holds back the high watermark, and the wait times out with
//!   `REQUEST_TIMED_OUT`.
//!
//! # What the model checks
//!
//! Five `always` properties:
//!
//! 1. `leader_never_witness` — no reachable state has a witness leader.
//! 2. `single_site_loss_keeps_acks_all_writable` — with at most one site down
//!    or isolated, with the controller converged, and with every reachable
//!    replica caught up, an `acks=all` produce commits. This is the headline
//!    claim of the feature.
//! 3. `one_leader_per_epoch` — a leader change always carries a strictly
//!    greater leader epoch, so two leaders never share one epoch.
//! 4. `minority_never_commits` — a set of sites that holds fewer than a voter
//!    majority never commits a write.
//! 5. `preferred_site_keeps_leadership` — a preferred election puts the leader
//!    in the preferred site whenever that site holds an alive, in-sync,
//!    non-witness replica.
//!
//! Five `sometimes` properties keep a vacuous pass out of reach. The important
//! one is `witness_stays_in_isr_after_failover`: the witness is what keeps
//! `acks=all` writable after a site loss, so an election that skipped it must
//! still leave it in the in-sync replica set.
//!
//! # The controller quorum is not modelled
//!
//! The model lets the controller decide and commit in every state, including
//! states where `KRaft` holds no metadata quorum. This over-approximates the
//! reachable set. An `always` safety property that holds over a superset holds
//! over the real set too, so every result here is the stronger form of the
//! claim: it needs no assumption about metadata availability at all. The extra
//! states matter, because they are the states where the witness is the only
//! surviving in-sync member. Those are the states the witness rule exists for,
//! and a quorum gate would hide them.
//!
//! # Why the model stays at one replica per site
//!
//! A `replication.factor` above the site count puts two replicas in one site,
//! and `minority_never_commits` then reports a write that commits inside that
//! one site. That report is not an artefact of the missing quorum gate: the
//! in-sync replica set can shrink to those two while a real `KRaft` quorum
//! holds, because one site down plus one lagging replica is enough and the
//! quorum lives in the other two sites. The write is then acknowledged with
//! every copy in one site, and the loss of that site loses it while the
//! survivors still hold the majority that elects a leader which never saw it.
//!
//! Such a configuration is therefore rejected before a broker starts, and not
//! merely left out of the model.
//! [`krabka_verified::stretch::min_insync_is_site_loss_safe`] requires that no
//! single site hold `min.insync.replicas` replicas, which for four replicas
//! over three sites leaves no safe value at all. The model checks the profile
//! that validation admits, which is one replica per site.
//!
//! # RED witness
//!
//! [`legacy_elect`](self::red_witness::legacy_elect) reproduces the pre-witness
//! controller, which took the first alive in-sync member with no witness
//! filter. A `#[should_panic]` test proves that `leader_never_witness` fires
//! against it. A second `#[should_panic]` test drops `min.insync.replicas` to 1
//! and proves that `minority_never_commits` fires. A model that cannot fail is
//! not evidence.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! `within_boundary` + `target_state_count` fence each run. You MUST run these
//! models under the host memory watchdog while you tune the bounds.
//!
//! # Module layout
//!
//! This file is the module root. It holds the shared `block_on` bridge and the
//! green-path test. Each child holds one concern: [`config`] the cluster shape,
//! [`state`] the model state and actions, [`topology`] the site reachability
//! queries, [`isr`] the in-sync-replica bookkeeping, [`metadata_view`] the
//! projection onto the real `krabka_metadata` types, [`produce`] the `acks=all`
//! write, [`election`] the controller decisions, [`properties`] the stateright
//! [`Model`](stateright::Model) implementation, [`runner`] the checker bounds,
//! and [`red_witness`] the deliberately broken controller.

// Each child is declared with an explicit `#[path]`. The declaration then names
// the same file whether or not this root is itself reached through a `#[path]`.
#[path = "stretch_cluster_model/config.rs"]
mod config;
#[path = "stretch_cluster_model/election.rs"]
mod election;
#[path = "stretch_cluster_model/isr.rs"]
mod isr;
#[path = "stretch_cluster_model/metadata_view.rs"]
mod metadata_view;
#[path = "stretch_cluster_model/produce.rs"]
mod produce;
#[path = "stretch_cluster_model/properties.rs"]
mod properties;
#[path = "stretch_cluster_model/red_witness.rs"]
mod red_witness;
#[path = "stretch_cluster_model/runner.rs"]
mod runner;
#[path = "stretch_cluster_model/state.rs"]
mod state;
#[path = "stretch_cluster_model/topology.rs"]
mod topology;

use self::{
    config::StretchModel,
    election::production_elect,
    runner::{PINNED_UNIQUE_STATES_THREE_SITES, run},
};

/// Run `future` to completion on a per-thread current-thread tokio runtime.
///
/// [`select_new_leader_for_partition`](crate::leader_election::select_new_leader_for_partition)
/// is `async`, and the stateright BFS runs on plain threads with no runtime.
/// Each thread builds one runtime and reuses it for every transition.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    thread_local! {
        static RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread tokio runtime");
    }
    RUNTIME.with(|runtime| runtime.block_on(future))
}

#[test]
fn stretch_three_sites() {
    // The supported shape: one broker per site, RF=3, min.insync.replicas=2.
    run(
        StretchModel::three_sites(2, production_elect),
        "stretch_three_sites",
        PINNED_UNIQUE_STATES_THREE_SITES,
    );
}
