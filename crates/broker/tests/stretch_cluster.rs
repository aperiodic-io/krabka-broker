//! Three-site stretch-cluster durability (KFC-2).
//!
//! The deployment under test is the one the witness role exists for: two data
//! sites that serve clients, one witness site that only replicates and votes,
//! one replica of every partition in each site, and `min.insync.replicas=2`.
//! The claim is that such a cluster survives the loss of **any single site**
//! with `acks=all` intact — including the loss of a data site, where the two
//! survivors are one data replica and the witness.
//!
//! # What each test stops
//!
//! A site is lost by stopping its broker. That covers the site-loss half of the
//! claim: a site that is gone cannot serve, cannot vote, and cannot replicate.
//!
//! # Leadership
//!
//! `replicas[0]` is the preferred leader in Kafka, so site-aware placement puts
//! a broker of the preferred site first. When that site is lost, leadership
//! must move to the **other data site** — never to the witness, which serves no
//! client. Every failover in this file is watched for that, not merely checked
//! at the end: the poll asserts that the witness is not the leader on *every*
//! observation it makes while it waits.
//!
//! # Partitions, and what this harness can and cannot cut
//!
//! The second half of the file leaves every broker running and takes away the
//! network instead, with `support::relay`'s [`SiteLink`] in front of a site's
//! controller and client listeners. A stopped site and an unreachable site are
//! different failures, and only the second one leaves a *live* site that could
//! misbehave — acknowledge a write it can no longer replicate, or elect a
//! second leader.
//!
//! The cut is **one-way**, and the tests are written to that. A relay sits in
//! front of a site's *listeners*, so cutting it stops the rest of the cluster
//! from reaching that site; the site's own outbound dials still land, because
//! they go to the relays of the other two sites. Making the cut two-way is not
//! reachable here:
//!
//! * Every node dials a given peer's controller listener at the *same*
//!   address, because the address comes from the committed `VotersRecord` in
//!   the metadata log, not from each node's own configuration. One relay per
//!   site is therefore the finest controller-plane cut available; a relay per
//!   ordered pair collapses onto it as soon as the voter set commits.
//! * Inter-broker data traffic — replication fetches, and the broker heartbeat
//!   that drives partition failover — resolves the target's single advertised
//!   endpoint out of the metadata image. Cutting a site's *outbound* data path
//!   means cutting the relay in front of whatever it dials, which is the same
//!   relay the surviving sites use to reach each other.
//!
//! So the partition test here asserts the half the harness can produce
//! honestly: what an isolated **leader** must stop doing once its replicas can
//! no longer reach it, and that healing puts the cluster back. Two things stay
//! out of reach, and are named here rather than faked with a weaker assertion:
//!
//! * **A live minority that must refuse while a live majority serves.** It
//!   needs a two-way cut, for the reason above. With a one-way cut the isolated
//!   site keeps its outbound reach, so it goes on heartbeating the controller
//!   and is never fenced, and the failover that would let the majority side
//!   take over never fires.
//! * **Isolating a site that happens to hold controller leadership.** Such a
//!   node keeps pushing to peers that can no longer dial it: it stays the
//!   controller, declares the sites it cannot hear from dead, and rewrites the
//!   metadata they depend on. Which node holds controller leadership is not
//!   steerable from a test, so any case that cuts a site which might be the
//!   controller would assert one outcome and hit another about a third of the
//!   time. The leader-site test below is written to hold either way.
//!
//! # Bounded waits
//!
//! Every wait is bounded with `tokio::time::timeout`. CI kills a test at 600s
//! and reports TIMEOUT with no cause; a bounded wait names the step that hung.

use std::{future::Future, time::Duration};

use support::cluster_lock;

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `stretch_cluster/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "stretch_cluster/cluster.rs"]
mod cluster;
#[path = "stretch_cluster/linked.rs"]
mod linked;
#[path = "stretch_cluster/produce.rs"]
mod produce;
#[path = "stretch_cluster/profile.rs"]
mod profile;
#[path = "stretch_cluster/site_loss.rs"]
mod site_loss;
#[path = "stretch_cluster/unreachable_site.rs"]
mod unreachable_site;
#[path = "stretch_cluster/view.rs"]
mod view;

/// The preferred leader site. Placement puts `replicas[0]` here.
const SITE_A: &str = "site-a";
/// The second data site: the failover target when `site-a` is lost.
const SITE_B: &str = "site-b";
/// The witness site. It replicates and votes; it never leads and serves no
/// client.
const SITE_C: &str = "site-c";
/// `SITES[i]` is the rack of broker `i + 1`.
const SITES: [&str; 3] = [SITE_A, SITE_B, SITE_C];

/// Cluster index of each site's broker. Node ids are the index plus one.
const NODE_A: usize = 0;
const NODE_B: usize = 1;
const NODE_C: usize = 2;

/// The witness's node id.
const WITNESS: u64 = 3;

const TOPIC: &str = "stretch-topic";
const N_RECORDS: i32 = 4;

/// Generous enough that a loaded runner does not fail a healthy cluster, short
/// enough that a broken one reports in seconds rather than at CI's kill.
const STEP_TIMEOUT: Duration = Duration::from_secs(45);

async fn within<F: Future>(what: &str, future: F) -> F::Output {
    support::poll::within(what, STEP_TIMEOUT, future).await
}
