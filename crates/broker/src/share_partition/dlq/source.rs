//! The source records of a dead-letter range, for a group that copies them:
//! Kafka's `ShareGroupDLQRecordFetcher`.
//!
//! The share partition sits on the leader of the source partition, so the read
//! is a local one, or a read of the remote tier for an offset that only the
//! tier holds (KIP-405), as Kafka's `LogReader.readAsync` reads it. It is best
//! effort. A record that cannot be read, or that cannot fit in the dead-letter
//! topic, is not copied, and its dead-letter record has headers alone; the
//! write is never held back for it.

use std::{collections::BTreeMap, sync::Arc};

use bytes::Buf as _;
use krabka_compression::RecordDecompressionPolicy;
use krabka_log::Offset;
use krabka_protocol::records::RecordBatch;
use krabka_remote_storage::TopicIdPartition;
use krabka_units::{ByteSize, convert::ByteSizeExt as _, mebibytes};

use super::record::SourceRecord;
use crate::{metrics::BrokerMetrics, partition::Partition, remote_reader::RemoteReader};

/// The most bytes of one log read. Most dead-letter ranges are one record, so
/// a larger read would be wasted. Kafka's `DLQ_MAX_FETCH_BYTES`.
const MAX_FETCH: ByteSize = mebibytes(1);

/// What a read of the source records found.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Fetched {
    pub(super) records: BTreeMap<i64, SourceRecord>,
    /// The last offset that the read reached: every offset of the range up to
    /// it is either in `records` or is not to be copied. An offset past it was
    /// not looked at, and a later round reads it with a fresh budget. Kafka's
    /// `FetchResult.lastResolvedOffset`.
    pub(super) last_resolved: i64,
}

/// Collects the records of one range from the batches of a read.
struct Collector {
    range: (i64, i64),
    /// The most bytes of records to collect, and the size limit of one: the
    /// `max.message.bytes` of the dead-letter topic.
    budget: usize,
    collected: usize,
    records: BTreeMap<i64, SourceRecord>,
    last_resolved: i64,
    /// A record did not fit beside those collected: the rest of the range is
    /// left for a later round.
    deferred: bool,
}

impl Collector {
    fn new(range: (i64, i64), budget: usize) -> Self {
        Self {
            range,
            budget,
            collected: 0,
            records: BTreeMap::new(),
            last_resolved: range.0 - 1,
            deferred: false,
        }
    }

    fn stopped(&self) -> bool {
        self.deferred || self.collected >= self.budget || self.last_resolved >= self.range.1
    }

    /// Takes the records of `batch` that the range still wants.
    fn take(&mut self, batch: &RecordBatch) {
        if batch.attributes.is_control_batch() {
            return;
        }
        for record in &batch.records {
            let offset = batch.base_offset + i64::from(record.offset_delta);
            if offset <= self.last_resolved {
                continue;
            }
            if offset > self.range.1 {
                return;
            }
            let size = record.encoded_len();
            if size > self.budget {
                // Too big for the topic on its own, now or in any later read.
                self.last_resolved = offset;
                continue;
            }
            if self.collected + size > self.budget {
                self.deferred = true;
                return;
            }
            self.records.insert(
                offset,
                SourceRecord {
                    key: record.key.clone(),
                    value: record.value.clone(),
                },
            );
            self.collected += size;
            self.last_resolved = offset;
        }
    }

    /// Gives up on the rest of the range: a read that failed or made no
    /// progress will not do better with a fresh budget, so the remainder
    /// takes headers alone.
    fn give_up(mut self) -> Fetched {
        self.last_resolved = self.range.1;
        self.finish()
    }

    fn finish(self) -> Fetched {
        Fetched {
            records: self.records,
            last_resolved: self.last_resolved,
        }
    }
}

/// The remote tier of a source partition, which serves the offsets below its
/// local log start (KIP-405).
pub(super) struct SourceTier<'a> {
    pub(super) reader: &'a RemoteReader,
    pub(super) metrics: &'a BrokerMetrics,
    pub(super) tp: TopicIdPartition,
}

/// Reads the raw batches of `[first, last]` from the log of `partition`, off
/// the reactor thread, or from the remote tier when only the tier holds
/// `first`. A remote read can return batches past `last`, and the collector
/// leaves them out.
async fn read_raw(
    partition: &Arc<Partition>,
    tier: Option<&SourceTier<'_>>,
    first: i64,
    last: i64,
) -> Option<bytes::Bytes> {
    if let Some(tier) = tier.filter(|_| RemoteReader::serves(partition, Offset(first))) {
        let max_bytes = usize::try_from(MAX_FETCH.bytes_u64()).unwrap_or(usize::MAX);
        return match tier
            .reader
            .read_partition(partition, &tier.tp, Offset(first), max_bytes, tier.metrics)
            .await
        {
            Ok(read) => read,
            Err(error) => {
                tracing::warn!(%error, first, last, "dead-letter source remote read failed");
                None
            }
        };
    }
    let log = partition.log.clone();
    let read = crate::blocking::spawn_blocking(move || {
        let log = log.lock().expect("log mutex poisoned");
        log.read_raw(Offset(first), Offset(last.saturating_add(1)), MAX_FETCH)
    })
    .await;
    match read {
        Ok(Ok(raw)) if raw.total > 0 => Some(raw.bytes),
        Ok(Ok(_)) => None,
        Ok(Err(error)) => {
            tracing::warn!(%error, first, last, "dead-letter source read failed");
            None
        }
        Err(error) => {
            tracing::warn!(%error, first, last, "dead-letter source read task failed");
            None
        }
    }
}

/// Reads the records of `[first, last]` from the local `partition`, or from
/// its remote `tier` where only the tier holds them, at most `budget` bytes of
/// them, decompressing under `policy`.
///
/// It reads on until the range is covered, the budget is used, or a read finds
/// nothing new. A partition that this broker does not hold, a failed read and
/// a batch that does not decode all give up on the rest of the range.
pub(super) async fn fetch(
    partition: Option<&Arc<Partition>>,
    tier: Option<&SourceTier<'_>>,
    (first, last): (i64, i64),
    budget: usize,
    policy: RecordDecompressionPolicy,
) -> Fetched {
    let mut collector = Collector::new((first, last), budget);
    let Some(partition) = partition else {
        return collector.give_up();
    };
    while !collector.stopped() {
        let before = collector.last_resolved;
        let Some(mut raw) = read_raw(partition, tier, before + 1, last).await else {
            return collector.give_up();
        };
        while raw.has_remaining() && !collector.stopped() {
            match RecordBatch::decode_with_policy(&mut raw, policy) {
                Ok(batch) => collector.take(&batch),
                Err(error) => {
                    tracing::warn!(%error, first, last, "dead-letter source batch did not decode");
                    return collector.give_up();
                }
            }
        }
        if collector.last_resolved == before && !collector.stopped() {
            return collector.give_up();
        }
    }
    collector.finish()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;

    use super::*;
    use crate::share_partition::manager::test_support::open_data_partition;

    fn records(pairs: &[(i64, &'static [u8])]) -> BTreeMap<i64, SourceRecord> {
        pairs
            .iter()
            .map(|(offset, value)| {
                (
                    *offset,
                    SourceRecord {
                        key: None,
                        value: Some(Bytes::from_static(value)),
                    },
                )
            })
            .collect()
    }

    /// A partition holding three batches: offsets 0-1, 2-3 and 4.
    async fn partition() -> (tempfile::TempDir, Arc<Partition>) {
        let dir = tempfile::tempdir().expect("temp dir");
        let registry = crate::partition_registry::PartitionRegistry::new();
        open_data_partition(
            &registry,
            dir.path(),
            crate::share_partition::manager::test_support::DataPartitionSetup {
                batches: vec![
                    crate::share_partition::manager::test_support::TimedValues {
                        timestamp: crate::test_support::UnixMillis(1_000),
                        values: &[b"a", b"b"],
                    },
                    crate::share_partition::manager::test_support::TimedValues {
                        timestamp: crate::test_support::UnixMillis(2_000),
                        values: &[b"c", b"d"],
                    },
                    crate::share_partition::manager::test_support::TimedValues {
                        timestamp: crate::test_support::UnixMillis(3_000),
                        values: &[b"e"],
                    },
                ],
                high_watermark: Offset(5),
                ..Default::default()
            },
        )
        .await;
        let part = registry
            .get("t", krabka_ids::PartitionIndex(0))
            .expect("partition registered");
        (dir, part)
    }

    /// The records of the range, across batches, and none outside it, and the
    /// whole range resolved.
    #[tokio::test]
    async fn the_range_is_read_across_batches() {
        let (_dir, part) = partition().await;

        let fetched = fetch(
            Some(&part),
            None,
            (1, 3),
            1 << 20,
            RecordDecompressionPolicy::default(),
        )
        .await;

        assert!(
            fetched
                == Fetched {
                    records: records(&[(1, b"b"), (2, b"c"), (3, b"d")]),
                    last_resolved: 3,
                }
        );
    }

    /// Kafka's `deferredRemainder`: a record that does not fit beside those
    /// collected stops the read, and the offsets from it on are left for a
    /// round with a fresh budget. A record that is too big on its own is
    /// skipped for good.
    #[tokio::test]
    async fn a_budget_that_is_used_up_leaves_the_rest_for_a_later_round() {
        let (_dir, part) = partition().await;
        // One record of these is a few bytes of value and overhead.
        let one = RecordBatch {
            records: vec![krabka_protocol::records::Record {
                value: Some(Bytes::from_static(b"a")),
                ..Default::default()
            }],
            ..Default::default()
        }
        .records[0]
            .encoded_len();

        // Room for two records: the third is left for the next round.
        let two = fetch(
            Some(&part),
            None,
            (0, 4),
            one * 2 + 1,
            RecordDecompressionPolicy::default(),
        )
        .await;
        // Room for none: each record is too big on its own.
        let none = fetch(
            Some(&part),
            None,
            (0, 4),
            one - 1,
            RecordDecompressionPolicy::default(),
        )
        .await;

        assert!(
            (two, none)
                == (
                    Fetched {
                        records: records(&[(0, b"a"), (1, b"b")]),
                        last_resolved: 1,
                    },
                    Fetched {
                        records: BTreeMap::new(),
                        last_resolved: 4,
                    },
                )
        );
    }

    /// A partition that this broker does not hold, or an offset that the log
    /// no longer has, gives up on the range: headers alone.
    #[tokio::test]
    async fn an_unreadable_range_gives_up_on_the_remainder() {
        let (_dir, part) = partition().await;

        let missing = fetch(
            None,
            None,
            (0, 2),
            1 << 20,
            RecordDecompressionPolicy::default(),
        )
        .await;
        let past_the_end = fetch(
            Some(&part),
            None,
            (9, 10),
            1 << 20,
            RecordDecompressionPolicy::default(),
        )
        .await;

        assert!(
            (missing, past_the_end)
                == (
                    Fetched {
                        records: BTreeMap::new(),
                        last_resolved: 2,
                    },
                    Fetched {
                        records: BTreeMap::new(),
                        last_resolved: 10,
                    },
                )
        );
    }
}
