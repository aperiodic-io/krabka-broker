//! The cloneable `AuditLog` handle that broker code calls to record events.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arc_swap::ArcSwapOption;
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};

use crate::{
    event::AuditEvent,
    sink::AuditError,
    spool::{PendingLosses, Spool},
};

/// Availability policy for privileged audit records.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuditMode {
    /// Keep serving when audit storage is unavailable and mark the loss later.
    #[default]
    FailOpen,
    /// Refuse a privileged action unless its write-ahead record is durable.
    FailClosed,
}

pub(super) struct AuditMessage {
    pub(super) event: AuditEvent,
    pub(super) acknowledgement: Option<oneshot::Sender<Result<(), AuditError>>>,
}

/// Receiver half of an [`AuditLog`].
///
/// Tests can receive plain events; [`super::AuditWriter`] consumes the full
/// message so it can acknowledge durable fail-closed writes.
pub struct AuditReceiver {
    receiver: mpsc::Receiver<AuditMessage>,
    pending_losses: Arc<PendingLosses>,
}

impl AuditReceiver {
    pub async fn recv(&mut self) -> Option<AuditEvent> {
        self.receiver.recv().await.map(|message| message.event)
    }

    /// Receive an event without waiting.
    ///
    /// # Errors
    /// Returns `Empty` when no event is ready and `Disconnected` when every
    /// sender has closed.
    pub fn try_recv(&mut self) -> Result<AuditEvent, mpsc::error::TryRecvError> {
        self.receiver.try_recv().map(|message| message.event)
    }

    pub(super) async fn recv_message(&mut self) -> Option<AuditMessage> {
        self.receiver.recv().await
    }

    pub(super) fn pending_losses(&self) -> Arc<PendingLosses> {
        Arc::clone(&self.pending_losses)
    }
}

/// Cloneable, cheap handle that broker code calls to record events.
#[derive(Debug)]
pub struct AuditLog {
    tx: ArcSwapOption<mpsc::Sender<AuditMessage>>,
    mode: AuditMode,
    dropped: AtomicU64,
    pending_losses: Arc<PendingLosses>,
}

impl AuditLog {
    /// Create a fail-open log.
    #[must_use]
    pub fn new(capacity: usize) -> (Arc<Self>, AuditReceiver) {
        Self::new_with_mode(capacity, AuditMode::FailOpen)
    }

    /// Create an enabled log with an explicit privileged-record policy.
    #[must_use]
    pub fn new_with_mode(capacity: usize, mode: AuditMode) -> (Arc<Self>, AuditReceiver) {
        Self::new_with_losses(capacity, mode, PendingLosses::memory())
    }

    /// Create an enabled log backed by the spool's durable loss counter.
    #[must_use]
    pub fn new_with_mode_and_spool(
        capacity: usize,
        mode: AuditMode,
        spool: &Spool,
    ) -> (Arc<Self>, AuditReceiver) {
        Self::new_with_losses(capacity, mode, spool.pending_losses())
    }

    fn new_with_losses(
        capacity: usize,
        mode: AuditMode,
        pending_losses: Arc<PendingLosses>,
    ) -> (Arc<Self>, AuditReceiver) {
        let (tx, receiver) = mpsc::channel(capacity);
        (
            Arc::new(Self {
                tx: ArcSwapOption::new(Some(Arc::new(tx))),
                mode,
                dropped: AtomicU64::new(0),
                pending_losses: Arc::clone(&pending_losses),
            }),
            AuditReceiver {
                receiver,
                pending_losses,
            },
        )
    }

    /// A no-op log for a disabled audit subsystem.
    #[must_use]
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            tx: ArcSwapOption::new(None),
            mode: AuditMode::FailOpen,
            dropped: AtomicU64::new(0),
            pending_losses: PendingLosses::memory(),
        })
    }

    /// Record an ordinary fail-open event without blocking.
    pub fn emit(&self, event: AuditEvent) {
        let Some(tx) = self.tx.load_full() else {
            return;
        };
        if tx
            .try_send(AuditMessage {
                event,
                acknowledgement: None,
            })
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            self.pending_losses.add(1);
            tracing::warn!("audit event dropped (queue full or writer stopped)");
        }
    }

    /// Durably record a privileged write-ahead event in fail-closed mode.
    ///
    /// Fail-open mode retains the non-blocking behavior of [`Self::emit`].
    /// Fail-closed mode refuses immediately under queue backpressure, then
    /// waits until the writer confirms that the sink or spool accepted the
    /// record durably.
    ///
    /// # Errors
    /// Returns an unavailable error if the queue is full, the writer stopped,
    /// or neither the sink nor spool can durably store the record.
    pub async fn emit_required(&self, event: AuditEvent) -> Result<(), AuditError> {
        if self.mode == AuditMode::FailOpen {
            self.emit(event);
            return Ok(());
        }
        let Some(tx) = self.tx.load_full() else {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return Err(AuditError::Unavailable(
                "audit writer is not running".into(),
            ));
        };
        let (acknowledgement, result) = oneshot::channel();
        tx.try_send(AuditMessage {
            event,
            acknowledgement: Some(acknowledgement),
        })
        .map_err(|error| {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            AuditError::Unavailable(match error {
                mpsc::error::TrySendError::Full(_) => "audit queue is full".into(),
                mpsc::error::TrySendError::Closed(_) => "audit writer is not running".into(),
            })
        })?;
        result.await.map_err(|_| {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            AuditError::Unavailable("audit writer stopped before durable acknowledgement".into())
        })?
    }

    /// Whether privileged actions require durable write-ahead audit records.
    #[must_use]
    pub fn mode(&self) -> AuditMode {
        self.mode
    }

    /// Close the event stream for every clone of this handle.
    pub fn close(&self) {
        self.tx.store(None);
    }

    /// Count events lost or refused because audit processing was unavailable.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::log::test_support::life;

    #[tokio::test]
    async fn close_ends_stream_for_every_log_clone_after_queued_events() {
        let (log, mut rx) = AuditLog::new(16);
        let clone = Arc::clone(&log);
        log.emit(life(crate::NodeId(1)));

        clone.close();
        log.emit(life(crate::NodeId(2)));

        check!(rx.recv().await == Some(life(crate::NodeId(1))));
        check!(rx.recv().await.is_none());
        check!(log.dropped() == 0);
    }

    #[test]
    fn disabled_log_drops_without_panicking() {
        let log = AuditLog::disabled();
        log.emit(life(crate::NodeId(1)));
        check!(log.dropped() == 0);
    }

    #[tokio::test]
    async fn full_queue_increments_dropped_and_pending_loss() {
        let (log, rx) = AuditLog::new(1);
        for i in 0..10 {
            log.emit(life(crate::NodeId(i)));
        }
        check!(log.dropped() == 9);
        check!(rx.pending_losses.count() == 9);
    }

    #[test]
    fn full_queue_does_not_persist_losses_on_the_emit_thread() {
        let dir = tempfile::tempdir().unwrap();
        let spool = Spool::open(dir.path(), krabka_units::prelude::bytes(0)).unwrap();
        let (log, _rx) = AuditLog::new_with_mode_and_spool(1, AuditMode::FailOpen, &spool);
        crate::log::test_support::emit_lifecycle(&log, &[crate::NodeId(1), crate::NodeId(2)]);

        let state = std::fs::read(dir.path().join("audit.losses")).unwrap();
        let count = &state[crate::spool::FILE_HEADER_LEN + 8..];
        check!(u64::from_be_bytes(count.try_into().unwrap()) == 0);
        check!(log.dropped() == 1);
    }

    #[tokio::test]
    async fn fail_closed_refuses_queue_backpressure() {
        let (log, _rx) = AuditLog::new_with_mode(1, AuditMode::FailClosed);
        log.emit(life(crate::NodeId(0)));
        let error = log.emit_required(life(crate::NodeId(1))).await.unwrap_err();
        check!(error.to_string().contains("queue is full"));
    }

    #[test]
    fn mode_returns_configured_mode() {
        let (closed, _rx) = AuditLog::new_with_mode(1, AuditMode::FailClosed);
        check!(closed.mode() == AuditMode::FailClosed);
        let (open, _rx) = AuditLog::new_with_mode(1, AuditMode::FailOpen);
        check!(open.mode() == AuditMode::FailOpen);
    }
}
