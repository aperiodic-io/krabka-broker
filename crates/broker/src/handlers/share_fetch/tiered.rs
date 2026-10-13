//! KIP-405 under KIP-932: the share-fetch read of offsets that only the remote
//! tier holds.
//!
//! Kafka's `DelayedShareFetch` reads the local log first. When the fetch offset
//! of a share partition is below the local log start, the read comes back with
//! `delayedRemoteStorageFetch`, and the request reads the partition through
//! `RemoteLogManager.asyncRead`. `ShareFetchUtils.processFetchResponse` then
//! acquires inside the records that the remote read returned, as it does for
//! a local read. The acquire step here makes the same choice at the first
//! offset that it can hand out.
//!
//! Before it acquires, an acquire pass takes three kinds of batch out of the
//! window: control batches, the data of aborted transactions under
//! `read_committed`, and KFC-1 batches that are not due yet. The local-log
//! scans that find them cannot read the tier, so this module finds them in the
//! batches that the remote read returned.

use {
    super::records::{AbortedRanges, batch_spans},
    crate::{
        error::BrokerError, metrics::BrokerMetrics, partition::Partition,
        remote_reader::RemoteReader,
    },
    bytes::Bytes,
    krabka_log::Offset,
    krabka_remote_storage::{RemoteStorageError, TopicIdPartition},
    krabka_units::convert::TimeExt as _,
};

/// The remote tier of one tiered share partition.
#[derive(Clone)]
pub(super) struct TieredSource<'a> {
    pub(super) reader: &'a RemoteReader,
    pub(super) metrics: &'a BrokerMetrics,
    pub(super) tp: TopicIdPartition,
    /// The group's isolation level: whether the data of aborted transactions
    /// must stay out of the response.
    pub(super) read_committed: bool,
}

/// What one remote read gives an acquire step.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct TieredRead {
    /// Whole batches, as the remote segment stores them.
    pub(super) bytes: Bytes,
    /// The offsets of the read that a share consumer must never get.
    pub(super) unreadable: Vec<(Offset, Offset)>,
    /// The offsets of the read that KFC-1 scheduled delivery holds back.
    pub(super) not_due: Vec<(Offset, Offset)>,
}

impl TieredSource<'_> {
    /// Reads the batches at `from` from the remote tier, at most `max_bytes`
    /// of them past the batch that holds `from`, and finds the offsets of the
    /// read that the acquire step must not hand out.
    ///
    /// It returns `None` when no finished remote segment holds `from` yet.
    ///
    /// # Errors
    ///
    /// A failed remote read is [`BrokerError::Io`], which the partition row
    /// answers with `UNKNOWN_SERVER_ERROR`, as Kafka answers a
    /// `RemoteStorageException`. The client keeps its place and retries.
    pub(super) async fn read(
        &self,
        partition: &Partition,
        from: Offset,
        max_bytes: i32,
    ) -> Result<Option<TieredRead>, BrokerError> {
        // A remote read with no cap reads the whole segment, so the smallest
        // budget is one byte, which still returns the first batch whole.
        let budget = usize::try_from(max_bytes.max(1)).unwrap_or(usize::MAX);
        let read = self
            .reader
            .read_partition(partition, &self.tp, from, budget, self.metrics)
            .await
            .map_err(|error| tier_failure(&error))?;
        let Some(bytes) = read else {
            return Ok(None);
        };
        let spans = batch_spans(&bytes)?;
        let Some(last) = spans.last().map(|span| span.last) else {
            return Ok(None);
        };
        let mut aborted = AbortedRanges::default();
        if self.read_committed {
            let remote = self
                .reader
                .aborted_in_partition(partition, &self.tp, from, Offset(last))
                .await
                .map_err(|error| tier_failure(&error))?;
            for txn in remote {
                aborted.add(txn.producer_id, txn.start_offset, txn.last_offset);
            }
            // An abort marker can sit in a local segment past the tier.
            let local = partition
                .log
                .lock()
                .expect("log mutex poisoned")
                .aborted_in_range(from, Offset(last.saturating_add(1)));
            for txn in local {
                aborted.add(txn.producer_id.get(), txn.start_offset.0, txn.last_offset.0);
            }
        }
        let (policy, uncertainty_ms) = {
            let config = partition
                .log
                .lock()
                .expect("log mutex poisoned")
                .config_snapshot();
            (
                config.delivery_policy,
                config.delivery_clock_uncertainty.millis_i64_trunc(),
            )
        };
        let now_ms = partition.delivery.now_ms();
        // The first batch can start below `from`, and the offsets before
        // `from` are not this read's to change.
        let range = |base: i64, last: i64| (Offset(base).max(from), Offset(last));
        let unreadable = spans
            .iter()
            .filter(|span| {
                aborted.excludes(span.attributes, span.producer_id, (span.base, span.last))
            })
            .map(|span| range(span.base, span.last))
            .collect();
        let not_due = spans
            .iter()
            .filter(|span| {
                !krabka_log::batch_is_deliverable(
                    policy,
                    uncertainty_ms,
                    span.max_timestamp,
                    now_ms,
                )
            })
            .map(|span| range(span.base, span.last))
            .collect();
        Ok(Some(TieredRead {
            bytes,
            unreadable,
            not_due,
        }))
    }
}

/// The partition error of a remote read that the tier failed.
fn tier_failure(error: &RemoteStorageError) -> BrokerError {
    BrokerError::Io(std::io::Error::other(format!(
        "remote tier read failed: {error}"
    )))
}
