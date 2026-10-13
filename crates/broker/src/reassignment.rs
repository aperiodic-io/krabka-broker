//! KIP-455 reassignment-completion background task.
//!
//! This task runs on the controller leader and watches the metadata image.
//! When Kafka's completion predicate holds for a reassignment, the task moves
//! atomically to the target replica set. Every one of its `adding_replicas`
//! must be in the ISR, and a replication factor decrease must not shrink the
//! ISR. When the current leader is in `removing_replicas`, the task first hands
//! leadership to a target replica that is in the ISR.
//!
//! The pure per-partition decision that the task applies lives in
//! [`self::policy`].

use {
    crate::heartbeat::controller_state::ControllerLivenessState,
    async_trait::async_trait,
    krabka_metadata::{MetadataImage, MetadataRecord},
    std::sync::Arc,
    tokio::sync::watch,
    tokio_util::sync::CancellationToken,
    tracing::{debug, info, warn},
};

mod policy;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

/// `reassign_one` itself is used only through `compute_reassignment_progress`
/// in a normal build. The re-export exists for `reassignment_model`, the
/// `stateright` model checker, which drives the decision directly.
#[cfg(test)]
pub(crate) use self::policy::reassign_one;
pub(crate) use self::policy::{
    CompletedReassignment, compute_reassignment_progress, maybe_complete_reassignment,
    remap_directories,
};

/// Minimal trait for the controller surface that this task needs. It lets a
/// unit test inject a mock without a real raft cluster.
#[async_trait]
pub(crate) trait ReassignmentController: Send + Sync {
    fn is_leader(&self) -> bool;
    fn current_image(&self) -> Arc<MetadataImage>;
    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>>;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String>;
}

/// Background task entry point. Image-apply events drive it.
pub(crate) async fn run(
    controller: Arc<dyn ReassignmentController>,
    liveness: Arc<ControllerLivenessState>,
    shutdown: CancellationToken,
) {
    let mut watcher = controller.watch_image();
    loop {
        tokio::select! {
            result = watcher.changed() => {
                if result.is_err() {
                    // Channel closed — controller dropped.
                    break;
                }
            },
            () = shutdown.cancelled() => {
                info!("reassignment task shutting down");
                return;
            }
        }
        if !controller.is_leader() {
            debug!("reassignment tick skipped: not controller leader");
            continue;
        }
        let image = controller.current_image();
        let updates = compute_reassignment_progress(&image, &liveness).await;
        if !updates.is_empty() {
            info!(
                count = updates.len(),
                "reassignment: submitting completion updates"
            );
            if let Err(e) = controller.submit_change(updates).await {
                warn!(error = %e, "reassignment: submit failed");
            }
        }
    }
}

#[cfg(test)]
#[path = "reassignment_model.rs"]
mod reassignment_model;
