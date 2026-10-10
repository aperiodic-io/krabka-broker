//! Fixtures shared by the unit tests of the `log` module tree.
//!
//! The module holds the `ProductInfo` and `AuditEvent` builders, the
//! `AuditWriterParams` factories that wire a writer to a manually advanced
//! clock, the failure-injecting sink, and the polling helper that replaces a
//! real-time sleep in a test.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::SeqCst},
};

use krabka_units::prelude::{ByteSize, Time, hours, mebibytes, millis};
use qubit_clock::{ManualMonotonicClock, MonotonicClock as _, Timer};

use super::{AuditLog, AuditReceiver, AuditWriter, AuditWriterParams};
use crate::{
    event::{AuditEvent, LifecycleKind},
    ocsf::ProductInfo,
    sink::{AuditRecord, AuditSink, MemorySink},
    spool::Spool,
    stats::AuditStats,
};

pub fn product() -> ProductInfo {
    ProductInfo {
        vendor_name: "Krabka".into(),
        name: "krabka-broker".into(),
        version: "0".into(),
    }
}

/// Lifecycle fixture for a node, with its identity also used as the timestamp.
pub fn life(node: crate::NodeId) -> AuditEvent {
    let n = i64::try_from(node.0).expect("fixture node identity fits the audit record");
    AuditEvent::Lifecycle {
        kind: LifecycleKind::BrokerStarted,
        node_id: n,
        time_ms: n,
    }
}

/// Emit a lifecycle sequence in caller-specified node order.
pub fn emit_lifecycle(log: &AuditLog, nodes: &[crate::NodeId]) {
    for &node in nodes {
        log.emit(life(node));
    }
}

pub fn header(rec: &AuditRecord, key: &str) -> Option<String> {
    rec.headers
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
}

pub fn spawn_writer(
    receiver: AuditReceiver,
    params: AuditWriterParams,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(AuditWriter::new(receiver, params).run())
}

/// Close the only event sender, then wait for the writer's final drain.
pub async fn finish_writer(log: Arc<AuditLog>, handle: tokio::task::JoinHandle<()>) {
    drop(log);
    handle.await.unwrap();
}

/// Emit the supplied lifecycle sequence and drain it through the writer's shutdown.
pub async fn finish_lifecycle_writer(
    log: Arc<AuditLog>,
    handle: tokio::task::JoinHandle<()>,
    nodes: &[crate::NodeId],
) {
    emit_lifecycle(&log, nodes);
    finish_writer(log, handle).await;
}

pub fn failed_sink_stats() -> (Arc<FailableSink>, Arc<AuditStats>) {
    let sink = Arc::new(FailableSink::default());
    sink.set_fail(true);
    (sink, Arc::new(AuditStats::new()))
}

pub fn healthy_sink_stats() -> (Arc<FailableSink>, Arc<AuditStats>) {
    (
        Arc::new(FailableSink::default()),
        Arc::new(AuditStats::new()),
    )
}

pub fn roomy_spool() -> (tempfile::TempDir, Spool) {
    let directory = tempfile::tempdir().unwrap();
    let spool = Spool::open(directory.path(), ROOMY_CAP).unwrap();
    (directory, spool)
}

pub fn record_sequences(records: &[AuditRecord]) -> Vec<String> {
    records
        .iter()
        .filter(|record| record.class != crate::AuditEventClass::Checkpoint)
        .map(|record| header(record, "seq").unwrap())
        .collect()
}

pub(crate) use crate::test_support::shared_signer as test_signer;

#[derive(Debug, krabka_macros::FieldDefaults)]
pub struct FailableSink {
    #[default(AtomicBool::new(false))]
    fail: AtomicBool,
    #[default(AtomicBool::new(false))]
    indeterminate: AtomicBool,
    #[default(AtomicI64::new(-1))]
    indeterminate_after: AtomicI64,
    /// -1 = unlimited; >= 0 = writes remaining before budget error.
    #[default(AtomicI64::new(-1))]
    allow: AtomicI64,
    #[default(AtomicU64::new(0))]
    durable_requests: AtomicU64,
    pub inner: MemorySink,
}

impl FailableSink {
    pub fn set_fail(&self, v: bool) {
        self.fail.store(v, SeqCst);
    }

    pub fn set_indeterminate(&self, value: bool) {
        self.indeterminate.store(value, SeqCst);
    }

    pub fn set_indeterminate_after(&self, successful_writes: i64) {
        self.indeterminate_after.store(successful_writes, SeqCst);
    }

    pub fn allow_n(&self, n: i64) {
        self.fail.store(false, SeqCst);
        self.allow.store(n, SeqCst);
    }

    pub fn allow_unlimited(&self) {
        self.allow.store(-1, SeqCst);
        self.fail.store(false, SeqCst);
    }

    pub fn durable_requests(&self) -> u64 {
        self.durable_requests.load(SeqCst)
    }
}

#[async_trait::async_trait]
impl AuditSink for FailableSink {
    async fn write(
        &self,
        record: AuditRecord,
        durable: bool,
    ) -> Result<(), crate::sink::AuditError> {
        if durable {
            self.durable_requests.fetch_add(1, SeqCst);
        }
        let indeterminate_after = self.indeterminate_after.load(SeqCst);
        if self.indeterminate.load(SeqCst) || indeterminate_after == 0 {
            return Err(crate::sink::AuditError::Indeterminate("forced".into()));
        }
        if indeterminate_after > 0 {
            self.indeterminate_after.fetch_sub(1, SeqCst);
        }
        if self.fail.load(SeqCst) {
            return Err(crate::sink::AuditError::Sink("forced".into()));
        }
        let allow = self.allow.load(SeqCst);
        if allow >= 0 {
            if allow == 0 {
                return Err(crate::sink::AuditError::Sink("budget exhausted".into()));
            }
            self.allow.fetch_sub(1, SeqCst);
        }
        self.inner.write(record, durable).await
    }
}

/// Replay ticker cadence for the test params. Tests advance the manual clock
/// by this amount to fire the replay ticker exactly once.
pub const REPLAY_EVERY: Time = millis(20);

/// A cadence that no test reaches. No test advances the manual clock that
/// far, so the ticker this cadence drives stays dormant.
pub const DORMANT: Time = hours(1);

/// A spool cap that is large enough that no test reaches it by accident.
pub const ROOMY_CAP: ByteSize = mebibytes(1);

pub fn params(sink: Arc<dyn AuditSink>, spool: Spool, stats: Arc<AuditStats>) -> AuditWriterParams {
    AuditWriterParams {
        sink,
        product: product(),
        signer: None,
        checkpoint_every_n: 0,
        checkpoint_every: DORMANT,
        chain: crate::chain::ChainState::new(),
        spool: Some(spool),
        stats,
        replay_every: REPLAY_EVERY,
        timer: dormant_timer(),
    }
}

/// A count of accepted audit events.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
pub struct AuditEventCount(pub u64);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CheckpointFrequency {
    #[default]
    Disabled,
    Every(AuditEventCount),
}

/// Keep time-based work dormant while a test controls count-based checkpoints.
#[derive(krabka_macros::FieldDefaults)]
pub struct CheckpointSetup {
    #[default(Arc::new(AuditStats::new()))]
    pub stats: Arc<AuditStats>,
    pub signer: Option<Arc<crate::FileEd25519Signer>>,
    pub frequency: CheckpointFrequency,
}

pub fn quiet_params(
    sink: Arc<dyn AuditSink>,
    spool: Spool,
    setup: CheckpointSetup,
) -> AuditWriterParams {
    let CheckpointSetup {
        stats,
        signer,
        frequency,
    } = setup;
    let mut params = params(sink, spool, stats);
    params.signer = signer;
    params.checkpoint_every_n = match frequency {
        CheckpointFrequency::Disabled => 0,
        CheckpointFrequency::Every(count) => count.0,
    };
    params.replay_every = DORMANT;
    params
}

/// A timer whose clock nothing holds and therefore nothing advances.
///
/// The checkpoint and replay tickers it drives only fire when the manual
/// clock behind them moves, and the caller keeps no handle on that clock, so
/// tests that do not exercise the tickers stay quiet and deterministic.
pub fn dormant_timer() -> Arc<dyn Timer> {
    ManualMonotonicClock::new_shared().new_timer()
}

/// Like [`params`], but also returns the [`ManualMonotonicClock`] the writer's
/// timer was made from.
///
/// The clock backs the checkpoint and replay tickers. A test can fire them
/// deterministically with `clock.advance(replay_every)` instead of a sleep in
/// real time.
pub fn params_with_clock(
    sink: Arc<dyn AuditSink>,
    spool: Spool,
    stats: Arc<AuditStats>,
) -> (AuditWriterParams, Arc<ManualMonotonicClock>) {
    let clock = ManualMonotonicClock::new_shared();
    let mut p = params(sink, spool, stats);
    p.timer = clock.new_timer();
    (p, clock)
}

/// Polls `cond` on every executor turn until it holds.
///
/// The function yields, so the spawned writer task can make progress. It
/// replaces the fixed `sleep` calls that waited for the writer to drain the
/// channel. It returns at the instant the observable condition is true,
/// which is deterministic. The large iteration cap is only a hang guard.
pub async fn await_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..1_000_000 {
        if cond() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held: {what}");
}
