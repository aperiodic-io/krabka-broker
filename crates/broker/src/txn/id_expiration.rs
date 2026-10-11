//! KIP-98: background sweep that expires idle and terminal transactional ids.
//!
//! [`Broker::start`](crate::Broker::start) spawns this task on every broker.
//! On each tick it refreshes the coordinator's leader-partition view from the
//! live metadata image, then asks
//! [`TxnCoordinator::expire_transactional_ids`] to tombstone every
//! locally-coordinated transactional id whose last transition is older than
//! `transactional.id.expiration.ms`. Without it `__transaction_state` keeps one
//! live entry per transactional id ever used.
//!
//! **KIP-939 invariant:** this task never expires a prepared two-phase-commit
//! transaction. The skip lives in
//! [`crate::txn::coordinator::expiry::should_expire_transactional_id`], which
//! refuses every `Prepare*` state, so this task cannot break the property.
//!
//! Every broker runs the loop, as Kafka's
//! `transaction.remove.expired.transaction.cleanup.interval.ms` sweep does, but
//! each one acts only on the transactional ids it coordinates. The tombstone is
//! idempotent -- a second one for an id already gone is a no-op on replay -- so
//! a duplicate or late sweep on a moved partition is safe.

use std::sync::Arc;

use krabka_units::{Time, convert::TimeExt as _};
use tracing::{debug, info};

use crate::{
    metadata_source::MetadataSource,
    txn::{coordinator::TxnCoordinator, util::now_millis},
};

super::util::reaper_task! {
    /// Entry point of the spawned task. It returns when `shutdown` is cancelled.
    ///
    /// The cadence is
    /// [`crate::config::BrokerConfig::txn_id_expiration_cleanup_interval`], which
    /// mirrors Kafka's
    /// `transaction.remove.expired.transaction.cleanup.interval.ms` and defaults to
    /// one hour. The broker spawns this task only when that interval is non-zero.
    /// `expiration` is
    /// [`crate::config::BrokerConfig::txn_id_expiration`], Kafka's
    /// `transactional.id.expiration.ms`.
    run(coord, controller, interval, expiration: Time; shutdown) => sweep_once(&coord, &*controller, expiration);
    "transactional-id expiry sweep shutting down"
}

/// Runs one sweep: refresh the leader-partition view, then expire.
///
/// This is the tick [`run`] drives, and the seam the expiry tests in
/// [`crate::txn::coordinator::expiry`] drive instead of waiting on a timer.
pub(in crate::txn) async fn sweep_once(
    coord: &Arc<TxnCoordinator>,
    controller: &dyn MetadataSource,
    expiration: Time,
) {
    let image = controller.current_image();
    // The sweep is a background task, so it waits for the loads it starts.
    coord
        .refresh_leader_partitions(&image)
        .await
        .finished()
        .await;
    let now_ms = now_millis();
    let expired = coord
        .expire_transactional_ids(now_ms, expiration.millis_i64())
        .await;
    if expired.is_empty() {
        debug!("txn id expiry: no transactional ids to expire");
    } else {
        info!(
            count = expired.len(),
            "txn id expiry: tombstoned expired transactional ids"
        );
    }
}
