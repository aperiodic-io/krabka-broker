//! The gate that lets a produce append on its own task while the partition's
//! writer task is idle.
//!
//! Every log mutation goes through the writer task, in the order its channel
//! received them. A produce that took that path paid two cross-task wakeups
//! for an append that takes microseconds: one to start the writer, one to
//! resume the request with its answer. On a busy machine those wakeups cost
//! more CPU than the append and most of its latency.
//!
//! The gate keeps the writer's order and drops the hops when nothing is
//! waiting. The writer holds [`WriterGate::busy`] while it handles a message,
//! and raises [`WriterGate::holding`] before every poll of its channel, so a
//! message it has taken but not yet started still counts as taken. A produce
//! may append on its own task only when it gets `busy` without waiting, the
//! writer holds no message, and the channel is empty: then every message sent
//! before it has been handled in full, and every message sent after it waits
//! for `busy` and lands after it, as it would have through the channel.

use {
    super::{Partition, WriterMessage},
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        task::{Context, Poll},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::sync::{MutexGuard, Notify, mpsc},
};

/// The writer's deadline value when it has none armed.
const NO_DEADLINE: u64 = u64::MAX;

/// See the module documentation.
#[derive(Debug)]
pub(crate) struct WriterGate {
    /// Whether appends may bypass the writer at all. A diskless partition
    /// assigns offsets through a sequencer and acknowledges through its WAL,
    /// so it always takes the channel.
    inline: bool,
    /// Held by whoever is mutating the log: the writer handling a message,
    /// or a produce appending on its own task.
    busy: tokio::sync::Mutex<()>,
    /// Raised by the writer before it polls its channel, and lowered only
    /// when the poll finds the channel empty.
    holding: AtomicBool,
    /// The epoch millisecond of the writer's next log-maintenance deadline,
    /// or [`NO_DEADLINE`].
    armed_deadline_ms: AtomicU64,
    /// Wakes the writer to recompute its maintenance deadline after an
    /// append it did not see brought the deadline forward.
    nudge: Notify,
}

impl WriterGate {
    /// A gate that lets produces append inline when `inline` is set.
    pub(crate) fn shared(inline: bool) -> Arc<Self> {
        Arc::new(Self {
            inline,
            busy: tokio::sync::Mutex::new(()),
            holding: AtomicBool::new(false),
            armed_deadline_ms: AtomicU64::new(NO_DEADLINE),
            nudge: Notify::new(),
        })
    }

    /// The writer's guard over one message.
    pub(crate) async fn writer_busy(&self) -> MutexGuard<'_, ()> { self.busy.lock().await }

    /// The guard under which `partition` may take a produce append on the
    /// calling task, or `None` when the produce has to go through the
    /// writer's channel.
    pub(crate) fn try_inline<'a>(&'a self, partition: &Partition) -> Option<MutexGuard<'a, ()>> {
        if !self.inline {
            return None;
        }
        let busy = self.busy.try_lock().ok()?;
        let idle = !self.holding.load(Ordering::SeqCst)
            && partition.writer_tx.capacity() == partition.writer_tx.max_capacity();
        idle.then_some(busy)
    }

    /// Receives the writer's next message, keeping [`Self::holding`] raised
    /// from before the channel is looked at until a message is handled.
    pub(crate) fn poll_recv(
        &self,
        rx: &mut mpsc::Receiver<WriterMessage>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<WriterMessage>> {
        self.holding.store(true, Ordering::SeqCst);
        let polled = rx.poll_recv(cx);
        if polled.is_pending() {
            self.holding.store(false, Ordering::SeqCst);
        }
        polled
    }

    /// Records the maintenance deadline the writer is about to sleep toward.
    pub(crate) fn arm(&self, delay: Option<Duration>) {
        let deadline = delay.map_or(NO_DEADLINE, |delay| deadline_ms(SystemTime::now(), delay));
        self.armed_deadline_ms.store(deadline, Ordering::SeqCst);
    }

    /// Waits for a [`Self::nudge_if_earlier`].
    pub(crate) async fn nudged(&self) { self.nudge.notified().await }

    /// Wakes the writer when an inline append left the log needing
    /// maintenance sooner than the writer is armed for.
    pub(crate) fn nudge_if_earlier(&self, delay: Option<Duration>) {
        let Some(delay) = delay else {
            return;
        };
        let deadline = deadline_ms(SystemTime::now(), delay);
        if deadline < self.armed_deadline_ms.load(Ordering::SeqCst) {
            self.armed_deadline_ms.store(deadline, Ordering::SeqCst);
            self.nudge.notify_one();
        }
    }
}

/// The epoch millisecond `delay` after `now`.
fn deadline_ms(now: SystemTime, delay: Duration) -> u64 {
    let at = now
        .checked_add(delay)
        .unwrap_or(now)
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(at.as_millis()).unwrap_or(NO_DEADLINE - 1)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            log_dir_status::LogDirRegistry,
            partition::{ProduceData, ProduceJob, test_support::test_partition},
            partition_writer::produce_inline,
            producer_state::ProducerState,
            test_support::partition_records_batch,
        },
        assert2::{assert, check},
        futures_util::FutureExt as _,
        tempfile::TempDir,
        tokio::sync::oneshot,
    };

    /// A partition whose gate admits inline appends, with no writer task
    /// running: the test holds the writer's end of the channel itself.
    fn gated(inline: bool) -> (Partition, mpsc::Receiver<WriterMessage>, TempDir) {
        let (base, dir) = test_partition(Arc::new(Notify::new()));
        let (writer_tx, rx) = mpsc::channel(8);
        let partition = Partition {
            writer_tx,
            writer_gate: WriterGate::shared(inline),
            ..base
        };
        (partition, rx, dir)
    }

    /// The writer polls its channel once, as its loop does.
    fn writer_polls(
        gate: &WriterGate,
        rx: &mut mpsc::Receiver<WriterMessage>,
    ) -> Poll<Option<WriterMessage>> {
        let waker = futures_util::task::noop_waker();
        gate.poll_recv(rx, &mut Context::from_waker(&waker))
    }

    #[tokio::test]
    async fn only_an_idle_writer_admits_an_inline_append() {
        let (partition, mut rx, _dir) = gated(true);
        let gate = &partition.writer_gate;
        check!(gate.try_inline(&partition).is_some(), "an idle writer");

        // Queued, and then taken but not yet handled: either way the message
        // was sent first, so the append must follow it through the channel.
        let (ack, _answer) = oneshot::channel();
        partition
            .writer_tx
            .try_send(WriterMessage::Barrier { ack })
            .expect("room in the channel");
        check!(gate.try_inline(&partition).is_none(), "a queued message");
        assert!(writer_polls(gate, &mut rx).is_ready());
        check!(gate.try_inline(&partition).is_none(), "a message in hand");

        let busy = gate.writer_busy().await;
        check!(
            gate.try_inline(&partition).is_none(),
            "a message in progress"
        );
        drop(busy);

        // The writer's next poll finds nothing and lowers its flag.
        assert!(writer_polls(gate, &mut rx).is_pending());
        check!(gate.try_inline(&partition).is_some(), "idle again");
    }

    #[tokio::test]
    async fn a_gate_without_inline_appends_keeps_them_all_on_the_channel() {
        let (partition, _rx, _dir) = gated(false);
        check!(partition.writer_gate.try_inline(&partition).is_none());
    }

    #[tokio::test]
    async fn an_inline_produce_appends_and_answers_before_it_returns() {
        let (partition, _rx, _dir) = gated(true);
        let (ack, answer) = oneshot::channel();
        let job = ProduceJob {
            data: ProduceData::Owned(partition_records_batch().count(3).call()),
            ack,
            producer_check: None,
        };

        let appended = produce_inline(
            &partition,
            job,
            &LogDirRegistry::default(),
            &ProducerState::new(),
        )
        .await;

        assert!(appended.is_ok());
        let answered = answer.now_or_never().expect("answered inline");
        check!(answered.expect("ack sent").expect("appended").base_offset.0 == 0);
        check!(partition.log_end_offset().0 == 3);
    }

    #[test]
    fn a_nudge_fires_only_for_an_earlier_deadline() {
        let gate = WriterGate::shared(true);
        gate.arm(Some(Duration::from_secs(60)));
        gate.nudge_if_earlier(None);
        gate.nudge_if_earlier(Some(Duration::from_secs(600)));
        check!(gate.nudged().now_or_never().is_none());
        gate.nudge_if_earlier(Some(Duration::ZERO));
        check!(gate.nudged().now_or_never().is_some());
    }
}
