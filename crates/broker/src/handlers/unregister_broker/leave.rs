//! The partition changes that take a broker out of every ISR before its
//! registration is dropped.
//!
//! Kafka's `ReplicationControlManager.handleBrokerUnregistered` runs
//! `generateLeaderAndIsrUpdates` with the broker to remove, then appends the
//! `UnregisterBrokerRecord`, all in one record list. Every ISR that names the
//! broker loses it, and every partition it leads elects another acceptable
//! replica. That is what the dead-broker failover scan computes, so this
//! module asks it about the broker.
//!
//! Only the active controller may call this. Its image is the one that the
//! partition records are built from, and any other node's image can trail the
//! controller's, so the handler forwards the request to the active controller
//! and refuses it anywhere else.

use krabka_metadata::{MetadataImage, MetadataRecord, NodeId};

use crate::{
    broker::Broker, heartbeat::controller_state::replicated_registrations,
    leader_election::compute_failover_changes,
};

/// The partition changes that make `node_id` leave every ISR and every
/// leadership another replica can take, as `image` holds them, on the active
/// controller.
///
/// Who may take over is Kafka's `ClusterControlManager.isActive`: a registered
/// broker that is neither fenced nor in controlled shutdown. The controller
/// answers that from its liveness registry, which also knows the brokers that
/// stopped heartbeating. The registry of this controller term is the one
/// `AlterPartition` reads, so a request served right after a failover does not
/// read the registry that an earlier term left.
///
/// A partition that no other replica can lead is left alone, as it is when the
/// broker is fenced. The liveness sweep hands it to the offset-aware recovery
/// once the registry declares the broker dead.
pub(super) async fn leave_isrs(
    broker: &Broker,
    image: &MetadataImage,
    node_id: NodeId,
) -> Vec<MetadataRecord> {
    broker
        .liveness
        .seed_term(
            broker.controller.current_controller_epoch(),
            replicated_registrations(image),
        )
        .await;
    compute_failover_changes(image, node_id, &broker.liveness, &broker.metrics)
        .await
        .changes
}
