//! Deliver-at-time visibility: the broker side of KFC-1.
//!
//! A topic set to [`DeliveryPolicy::Scheduled`](krabka_log::DeliveryPolicy)
//! treats a record's own timestamp as the time the record becomes visible to a
//! consumer. [`krabka_log::Log`] owns the rule and derives one offset from it,
//! the **delivery watermark**: the first offset that is not visible yet. This
//! module is what the broker builds on top of that offset.
//!
//! # Correctness comes from the pull path, not from the timer
//!
//! The fetch handler recomputes the watermark with
//! [`Log::advance_delivery_watermark`](krabka_log::Log::advance_delivery_watermark)
//! while it already holds the partition's log mutex, and caps the fetch at what
//! that call returns. A fetch therefore reads a watermark derived from the
//! clock reading of that same fetch, and it can never serve a batch early or
//! hold back a batch whose activation time has passed.
//!
//! [`scheduler`] exists only for **liveness**. A consumer parked in a long poll
//! at the watermark has no fetch in flight to recompute anything, so something
//! has to wake it when the next batch comes due. That is the scheduler's whole
//! job. If the scheduler task died outright, every fetch would still return the
//! correct records; a parked consumer would simply wait until its long poll
//! expired and it polled again. **A dead scheduler makes delivery late. It
//! never makes delivery early, and it never makes delivery wrong.**
//!
//! # Never early, late by a bounded amount
//!
//! A batch activates once `max_timestamp + delivery_clock_uncertainty` is at or
//! before the broker's clock reading. Call the reading `c` and the declared
//! bound `e`: true time lies in `[c - e, c + e]`, so `c >= activation + e`
//! proves true time has reached the activation instant. Delivery is therefore
//! never early. It is late by at most `2e` plus one scheduler tick, because the
//! broker waits out the full bound against its own clock and true time can
//! already sit a further `e` ahead of that reading.
//!
//! The activation-lateness histogram in [`metrics`] measures exactly that
//! price. A rising tail means the declared bound is not honest, or that the
//! scheduler does not get enough CPU.
//!
//! # Nothing here is durable
//!
//! The schedule is the replicated records themselves. Each delivery time
//! arrives in the log, replicates to the ISR, and survives a crash exactly as
//! the record data does, so no timer store and no checkpoint is needed.
//!
//! The delivery watermark is derived state in the same sense the high watermark
//! is derived. A leader that starts, or a follower that takes over, recomputes
//! it from the log and the clock, and a recomputation cannot disagree with the
//! log it comes from. That is why a restart and a leader change need no special
//! case here, and why the scheduler's heap is rebuilt from a sweep rather than
//! read back from disk.
//!
//! # What lives here
//!
//! - [`config`] holds the scheduler's tunables and the injected clock and
//!   timer.
//! - [`handles`] holds the per-partition state: the lock-free watermark mirror,
//!   the long-poll wake, and the slot the scheduler installs its poke into.
//! - [`waker`] is that poke. A produce that lands a batch due before the
//!   instant the scheduler sleeps on re-arms the task through it.
//! - [`scheduler`] is the one broker-wide task that holds a min-heap of
//!   activation deadlines and advances the partitions that come due.
//! - [`metrics`] is the metric seam, so a unit test needs no live registry.
//!
//! # Cost on an ordinary topic
//!
//! A partition whose topic delivers immediately never enters the heap and never
//! reports a metric series. [`DeliveryHandles::publish`] still keeps its mirror
//! equal to the log end offset, which costs one uncontended mutex and no I/O,
//! because `Log::advance_delivery_watermark` answers such a topic before it
//! reads a single batch header.

pub(crate) mod config;
pub(crate) mod handles;
pub(crate) mod metrics;
pub(crate) mod scheduler;
pub(crate) mod waker;

#[cfg(test)]
pub(crate) mod test_support;

pub(crate) use self::{handles::DeliveryHandles, waker::DeliveryWaker};
use krabka_ids::Offset;

/// One scheduled partition's delivery state, as a recompute left it.
///
/// [`DeliveryHandles::publish`] builds this from the
/// [`DeliveryAdvance`](krabka_log::DeliveryAdvance) the log returns, plus the
/// log end offset that the same lock acquisition read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartitionDelivery {
    /// First offset that is not visible yet. A fetch caps its limit offset at
    /// this value.
    pub(crate) watermark: Offset,

    /// Records that are durable but not visible yet: the log end offset minus
    /// [`Self::watermark`].
    pub(crate) pending: i64,

    /// Epoch-millisecond instant at which the first waiting batch becomes
    /// visible, or `None` when nothing waits. The clock-uncertainty bound is
    /// already included, so a caller compares it against its own clock and adds
    /// nothing.
    pub(crate) next_deadline_ms: Option<i64>,
}
