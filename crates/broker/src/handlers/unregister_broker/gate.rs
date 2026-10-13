//! KFC-9: the break-glass two-person rule over a broker unregistration.
//!
//! Dropping a broker takes its endpoints out of every `Metadata` response, so
//! it is gated. This module builds the record list one unregistration appends
//! -- the consumed proposal ahead of the unregister record, so a single raft
//! append carries both -- spells the target that a proposal must name, and
//! reads back the proposal that list spends.

use {
    crate::break_glass::gate::{
        self, BreakGlassAction, BreakGlassConfig, BreakGlassDenial, MetadataImage, MetadataRecord,
    },
    krabka_metadata::{NodeId, UnregisterBrokerRecord},
};

/// The records one unregistration appends.
///
/// The consumed break-glass proposal goes first, and the unregister record
/// follows it, so one raft append carries both. That single append is what
/// stops an approval from being spent twice across a crash: a broker that
/// committed the transition has committed the consume with it.
///
/// The unregister record carries `broker_epoch`, the epoch of the registration
/// that it removes, as Kafka's `ReplicationControlManager.unregisterBroker`
/// writes it. The image refuses an unregistration at any other epoch.
///
/// A broker whose `[break_glass]` names no approver gates nothing, and the
/// answer is then the unregister record alone.
///
/// # Errors
///
/// Returns the [`BreakGlassDenial`] when no approved proposal covers this
/// broker id. The caller answers `POLICY_VIOLATION (44)` with its text.
pub(super) fn unregister_records(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    node_id: NodeId,
    broker_epoch: i64,
    now_ms: i64,
) -> Result<Vec<MetadataRecord>, BreakGlassDenial> {
    let record = MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
        node_id,
        broker_epoch,
    });
    gate::records_with_consume(
        image,
        config,
        BreakGlassAction::UnregisterBroker,
        &broker_target(node_id),
        now_ms,
        record,
    )
}

/// `records` with the partition changes in `leaves` placed ahead of the
/// unregister record and behind any consumed proposal.
///
/// This is the order of Kafka's `ReplicationControlManager.handleBrokerUnregistered`:
/// the broker leaves every ISR, and only then does its registration go. The
/// consume stays first, so the approval commits with the transition it
/// authorized.
pub(super) fn with_leaves(
    mut records: Vec<MetadataRecord>,
    leaves: Vec<MetadataRecord>,
) -> Vec<MetadataRecord> {
    let unregister_at = records
        .iter()
        .position(|record| matches!(record, MetadataRecord::V1UnregisterBroker(_)))
        .unwrap_or(records.len());
    records.splice(unregister_at..unregister_at, leaves);
    records
}

/// The break-glass target of one broker: its id in decimal, as an operator
/// spells it on `krabka-guard break-glass propose --target`.
///
/// `UnregisterBroker` names no partition, so the gate takes this target
/// exactly and no wider proposal covers it.
pub(super) fn broker_target(node_id: NodeId) -> String { node_id.0.to_string() }
