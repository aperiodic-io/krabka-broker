//! Bridges the blocking [`RemoteLogMetadataManager`] mutation SPI onto the
//! Tokio blocking pool, so no metadata write runs on a runtime worker thread.

use {
    crate::{blocking::spawn_blocking, time_util::now_ms},
    krabka_remote_storage::RemoteLogMetadataManager,
    std::sync::Arc,
};

/// Run one blocking [`RemoteLogMetadataManager`] mutation on the blocking
/// pool. The topic-backed manager's synchronous SPI methods bridge to a
/// Tokio runtime with `block_on`, which panics on a runtime worker thread.
/// `spawn_blocking` gives them a thread that is allowed to block. For the
/// in-memory manager the closure is a cheap no-op there.
/// This mirrors the `spawn_blocking` wrapping that this module already uses
/// for the blocking
/// [`RemoteStorageManager`](krabka_remote_storage::RemoteStorageManager) SPI.
pub(super) async fn rlmm_mutate<F>(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    op: F,
) -> Result<(), krabka_remote_storage::RemoteStorageError>
where
    F: FnOnce(
            &dyn RemoteLogMetadataManager,
        ) -> Result<(), krabka_remote_storage::RemoteStorageError>
        + Send
        + 'static,
{
    let rlmm = Arc::clone(rlmm);
    match spawn_blocking(move || op(rlmm.as_ref())).await {
        Ok(res) => res,
        Err(e) => Err(krabka_remote_storage::RemoteStorageError::Backend(format!(
            "RLMM mutation task panicked: {e}"
        ))),
    }
}

/// Publish one lifecycle transition with a fresh event timestamp.
pub(super) async fn update_segment(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    id: krabka_remote_storage::RemoteLogSegmentId,
    custom_metadata: Option<krabka_remote_storage::CustomMetadata>,
    state: krabka_remote_storage::RemoteLogSegmentState,
    broker_id: i32,
) -> Result<(), krabka_remote_storage::RemoteStorageError> {
    let update = krabka_remote_storage::RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: id,
        event_timestamp_ms: now_ms(),
        custom_metadata,
        state,
        broker_id,
    };
    rlmm_mutate(rlmm, move |manager| {
        manager.update_remote_log_segment_metadata(update)
    })
    .await
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::remote_log_manager::test_support as fixtures,
        std::sync::atomic::{AtomicBool, Ordering},
    };

    #[tokio::test]
    async fn rlmm_mutate_runs_op_and_propagates_error() {
        let rlmm = fixtures::in_memory_metadata();
        let called = Arc::new(AtomicBool::new(false));
        let called_clone = Arc::clone(&called);

        let res = rlmm_mutate(&rlmm, move |_| {
            called_clone.store(true, Ordering::SeqCst);
            Err(krabka_remote_storage::RemoteStorageError::Backend(
                "injected error".into(),
            ))
        })
        .await;

        assert2::check!(called.load(Ordering::SeqCst));
        assert2::check!(res.is_err());
    }
}
