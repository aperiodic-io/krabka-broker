//! KIP-853 controller auto-join.
//!
//! A broker started in [`crate::BootstrapMode::Join`] with
//! `auto_join = true` is NOT yet a member of the controller raft group: its
//! Raft log is empty and it waits as an observer. This module
//! drives the joiner side of the dance — it discovers the leader via the
//! configured `bootstrap_servers` and sends the **Kafka `AddRaftVoter` wire
//! RPC** (`api_key` 80) carrying its own voter identity. The leader-side
//! handler (`crate::handlers::add_raft_voter`) waits for the observer to catch
//! up and appends the authoritative `VotersRecord`. Once the joiner sees its
//! exact node and directory identity in the committed voter set it stops.
//!
//! The joiner advertises its **real bound** controller endpoint (not the
//! configured `controller_listen_addr`, which may carry port 0 for an
//! OS-assigned port) so the leader's `add_learner` can dial it back.
//!
//! This is purely a client-side driver: it does NOT touch the reconfiguration
//! Raft state directly. All lockstep safety lives in the leader's single-owner
//! Raft engine.
//!
//! The two background tasks the module exposes — [`run`] for the join itself
//! and [`run_voter_updates`] for the endpoint advertisement — live in
//! `join_loop` and `voter_updates`. The pieces they share sit in `request`
//! (identity and request construction), `rpc` (the one-shot wire calls) and
//! `outcome` (reply classification).

use std::sync::Arc;

use krabka_units::Time;

mod join_loop;
mod outcome;
mod request;
mod rpc;
#[cfg(test)]
mod test_support;
mod voter_updates;

pub(crate) use self::{join_loop::run, voter_updates::run_voter_updates};
use crate::{metadata_source::MetadataSource, network::client::InterBrokerClient};

/// Everything the auto-join driver needs, pulled out of `BrokerConfig` +
/// `Broker` so the loop can be spawned *before* the full `Broker` Arc exists.
/// A `Join` broker's `Broker::start` blocks waiting for a leader, and that
/// leader only appears once this loop has driven the leader-side `add_learner`
/// + promotion — so the two must run concurrently.
#[derive(Clone)]
pub(crate) struct AutoJoinParams {
    pub auto_join: bool,
    pub retry_backoff: Time,
    pub voter_request_timeout: Time,
    pub node_id: krabka_raft::NodeId,
    pub directory_id: uuid::Uuid,
    pub cluster_id: Option<uuid::Uuid>,
    pub bootstrap_servers: Vec<String>,
    /// This node's own controller endpoint as the rest of the cluster is
    /// configured to reach it, from its `controller.quorum.voters` entry.
    ///
    /// The voter RPCs publish this rather than the address the controller
    /// socket is bound to. A controller that binds `0.0.0.0` has no routable
    /// address of its own. Without this entry it publishes the machine's host
    /// name, as Kafka does for a wildcard controller listener.
    pub advertised_controller: Option<String>,
    /// Protocol of the bootstrap server's controller listener.
    pub listener_protocol: krabka_security::ListenerProtocol,
    pub inter_broker_server_name: String,
    pub controller: Arc<dyn MetadataSource>,
    pub inter_broker_client: Arc<InterBrokerClient>,
}
