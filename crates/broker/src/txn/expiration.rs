//! KIP-98 / KIP-939: background reaper that aborts timed-out transactions.
//!
//! [`Broker::start`] spawns this task on every broker. On each tick it
//! refreshes the coordinator's leader-partition view from the live metadata
//! image, then asks [`TxnCoordinator::sweep_expired`] to abort every
//! locally-coordinated, `Ongoing`, non-2PC transaction whose timeout has
//! elapsed.
//!
//! **KIP-939 invariant:** this task *never* reaps a two-phase-commit
//! transaction, which is persisted with the
//! [`crate::txn::two_pc::NO_TIMEOUT_MS`] sentinel. Its external transaction
//! manager owns the commit or abort decision. The skip lives in
//! [`crate::txn::two_pc::should_abort_idle_txn`], the exhaustively
//! model-checked decision core, so this task can never break the property.
//!
//! Every broker runs the loop, as Kafka's
//! `transaction.abort.timed.out.transaction.cleanup.interval.ms` sweep does,
//! but each one acts only on the transactions it coordinates.
//! `__transaction_state` persistence and the producer-epoch fence on
//! completion make a duplicate or late sweep on a moved partition a safe
//! no-op.

use {
    crate::{
        metadata_source::MetadataSource,
        txn::{coordinator::TxnCoordinator, util::now_millis, version::resolve_txn_version},
    },
    std::sync::Arc,
    tracing::{debug, info},
};

super::util::reaper_task! {
    /// Entry point of the spawned task. It returns when `shutdown` is cancelled.
    ///
    /// The cadence is
    /// [`crate::config::BrokerConfig::txn_abort_cleanup_interval`], which mirrors
    /// Kafka's `transaction.abort.timed.out.transaction.cleanup.interval.ms` and
    /// defaults to 10s. The broker spawns this task only when that interval is
    /// non-zero.
    run(coord, controller, interval; shutdown) => sweep_once(&coord, &*controller);
    "txn idle-transaction reaper shutting down"
}

/// Runs one sweep. It resolves `transaction.version`, refreshes the
/// leader-partition view, then aborts any expired transactions.
async fn sweep_once(coord: &Arc<TxnCoordinator>, controller: &dyn MetadataSource) {
    let image = controller.current_image();
    let txnv = resolve_txn_version(&image);
    // The sweep is a background task, so it waits for the loads it starts.
    coord
        .refresh_leader_partitions(&image)
        .await
        .finished()
        .await;
    let now_ms = now_millis();
    let aborted = coord.sweep_expired(now_ms, txnv).await;
    if aborted.is_empty() {
        debug!("txn reaper: no timed-out transactions");
    } else {
        info!(
            count = aborted.len(),
            "txn reaper: aborted timed-out transactions"
        );
    }
}
