//! The broadcast-side filter that decides which live record reaches one
//! subscription's stream.
//!
//! A record passes only when its partition is currently assigned and its
//! offset is at or after that partition's cursor. A partition added mid-stream
//! is marked `via_inject`, so its passing records are re-routed into the
//! subscription's FIFO instead of being emitted here, which keeps them behind
//! the backlog that [`AssignmentHandle::add`] already queued.
//!
//! [`AssignmentHandle::add`]: crate::log::AssignmentHandle::add

use {
    super::SubscriptionState,
    crate::log::{MetadataEventRecord, MetadataEventStream},
    futures_util::{StreamExt, stream::unfold},
    std::sync::Arc,
    tokio::sync::broadcast,
};

/// What [`filtered_broadcast`] does with a received live record.
enum Forward {
    /// Emit directly on the broadcast stream.
    Emit,
    /// Re-route through the `inject` FIFO for a partition added mid-stream.
    Inject,
    /// Not assigned, or below the cursor. Discard the record.
    Drop,
}

/// Forward a broadcast record only when its partition is currently
/// assigned and its offset is at/after the recorded live cursor for
/// that partition.
///
/// For a partition added mid-stream, which is the `via_inject` case, this
/// function re-routes a passing record into the `inject` FIFO instead of
/// emitting it here. The record then sits behind that partition's
/// already-injected backlog rather than races it through `stream::select`.
pub(super) fn filtered_broadcast(
    rx: broadcast::Receiver<MetadataEventRecord>,
    state: Arc<SubscriptionState>,
) -> MetadataEventStream {
    unfold((rx, state), |(mut rx, state)| async move {
        loop {
            match rx.recv().await {
                Ok(record) => {
                    let action = {
                        let assigned = state.assigned.lock().expect("assigned mutex poisoned");
                        match assigned.get(&record.partition) {
                            Some(cur) if record.offset >= cur.next => {
                                if cur.via_inject {
                                    Forward::Inject
                                } else {
                                    Forward::Emit
                                }
                            }
                            _ => Forward::Drop,
                        }
                    };
                    match action {
                        Forward::Emit => return Some((record, (rx, state))),
                        Forward::Inject => {
                            // Queue behind backlog; if the receiver is gone
                            // the stream is being dropped anyway.
                            let _ = state.inject.send(record);
                        }
                        Forward::Drop => {}
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // The in-memory snapshot already supplied earlier
                    // records; a lag only happens when the consumer
                    // pump fell behind a single-process write burst
                    // that overflowed the broadcast capacity (1024).
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    })
    .boxed()
}
