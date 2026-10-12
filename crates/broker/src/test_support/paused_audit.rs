//! A real fail-closed audit writer whose durable acknowledgment a test controls.

use std::sync::Arc;

use krabka_audit::{
    AuditError, AuditLog, AuditMode, AuditRecord, AuditSink, AuditStats, AuditWriter,
    AuditWriterParams, ChainState, MemorySink,
};
use krabka_units::prelude::hours;
use tokio::{sync::Notify, task::JoinHandle};

use crate::broker::Broker;

/// The first required write announces admission and waits for the test.
#[derive(Debug, Default)]
pub(crate) struct PausedAudit {
    entered: Notify,
    release: Notify,
    records: MemorySink,
}

#[async_trait::async_trait]
impl AuditSink for PausedAudit {
    async fn write(&self, record: AuditRecord, durable: bool) -> Result<(), AuditError> {
        if durable {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.records.write(record, durable).await
    }
}

impl PausedAudit {
    pub(crate) fn start() -> (Arc<AuditLog>, Arc<Self>, JoinHandle<()>) {
        let sink = Arc::new(Self::default());
        let (log, receiver) = AuditLog::new_with_mode(8, AuditMode::FailClosed);
        let params = AuditWriterParams {
            product: Broker::audit_product(),
            chain: ChainState::new(),
            sink: sink.clone(),
            timer: Arc::new(qubit_clock::StdTimer::new()),
            stats: Arc::new(AuditStats::new()),
            checkpoint_every: hours(1),
            replay_every: hours(1),
            signer: None,
            spool: None,
            checkpoint_every_n: 0,
        };
        let writer = tokio::spawn(AuditWriter::new(receiver, params).run());
        (log, sink, writer)
    }

    /// Intervene after the real writer starts the durable write, before its acknowledgment.
    pub(crate) async fn release_after<T>(&self, change: impl FnOnce() -> T) -> T {
        self.entered.notified().await;
        let result = change();
        self.release.notify_one();
        result
    }

    pub(crate) fn records(&self) -> Vec<AuditRecord> {
        self.records.records()
    }
}
