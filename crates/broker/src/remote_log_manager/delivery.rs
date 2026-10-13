//! Activation evidence for scheduled remote retention. Retention timestamps
//! can fall back to file modification time, so they cannot authorize removing
//! a scheduled batch. Read its stored header and apply the fetch deadline rule.

use {
    crate::{blocking::spawn_blocking, remote_reader::segment_activation_frontier},
    krabka_ids::Offset,
    krabka_remote_storage::{RemoteLogSegmentMetadata, RemoteStorageManager},
    krabka_units::Time,
    std::sync::Arc,
};

/// A failed or incomplete scan keeps the segment and everything behind it.
/// Only candidates selected by retention are scanned; immediate topics never
/// reach this function. Header range reads bound memory independently of the
/// size or compression of the segment's record bodies.
pub(super) async fn first_pending_remote_segment(
    candidates: &[RemoteLogSegmentMetadata],
    rsm: &Arc<dyn RemoteStorageManager>,
    uncertainty: Time,
    now_ms: i64,
) -> Option<uuid::Uuid> {
    for metadata in candidates {
        let rsm = Arc::clone(rsm);
        let candidate = metadata.clone();
        let result = spawn_blocking(move || {
            segment_activation_frontier(
                rsm.as_ref(),
                &candidate,
                Offset(candidate.start_offset()),
                uncertainty,
                now_ms,
                None,
            )
        })
        .await;
        if !matches!(result, Ok(Ok(frontier)) if frontier.0 > metadata.end_offset()) {
            tracing::debug!(segment = %metadata.remote_log_segment_id().id,
                "remote retention holds a pending or unreadable scheduled segment");
            return Some(metadata.remote_log_segment_id().id);
        }
    }
    None
}
