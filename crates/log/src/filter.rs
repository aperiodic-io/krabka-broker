//! Record-level filtering of a [`RecordBatch`] that keeps Kafka's
//! compacted-log offset shape.
//!
//! A caller that drops records from a batch must not renumber the survivors.
//! Kafka's cleaner keeps `base_offset` and every surviving record's
//! `offset_delta`, so an absolute offset never moves and a dropped record
//! leaves a gap in the offset sequence. Consumers depend on that: an offset a
//! client already holds must still name the same record after the rewrite, and
//! the offset index maps offsets to file positions on the same assumption.
//! Only `last_offset_delta` changes, to the highest delta that survives.
//!
//! [`filter_batch`] holds that arithmetic in one place. Log compaction drops
//! records under the KIP-534 retain rules, and a point-in-time restore drops
//! records under an operator's rule, but both owe the log the same offset
//! shape.

use krabka_protocol::records::{Record, RecordBatch};

/// What [`filter_batch`] made of a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilteredBatch {
    /// No record was dropped, so the batch needs no rewrite.
    ///
    /// A caller that still holds the batch's original bytes can copy them
    /// verbatim. That path skips the re-encode and keeps the original CRC,
    /// which is the strongest evidence that the rewrite did not damage the
    /// record data.
    Unchanged,
    /// At least one record was dropped. Carries the rewritten batch.
    Filtered(RecordBatch),
    /// The batch had records and every one of them was dropped.
    ///
    /// The caller decides what an emptied batch becomes. Compaction re-emits a
    /// bare header when the batch is the last one of an active producer, so
    /// the producer's sequence state survives; a restore may drop the batch
    /// outright. That choice is policy, so this module does not make it.
    Empty,
}

/// Drop the records of `batch` for which `keep` returns `false`.
///
/// `keep` runs exactly once per record, in batch order, so a caller may use it
/// to collect per-record facts as a side effect.
///
/// The result keeps Kafka's compacted-log shape: `base_offset` never moves,
/// each surviving record keeps its `offset_delta`, and `last_offset_delta`
/// becomes the highest surviving delta. Every other header field carries over
/// unchanged. That includes `max_timestamp`, which stays put even when the
/// record that carried the newest timestamp is dropped, because the time index
/// and the retention check read the header and must not see the batch move
/// backwards in time.
///
/// A batch that holds no records is [`FilteredBatch::Unchanged`], not
/// [`FilteredBatch::Empty`]: nothing was dropped from it. A bare header that an
/// earlier rewrite left behind therefore survives this one intact.
#[must_use]
pub fn filter_batch<F>(batch: &RecordBatch, mut keep: F) -> FilteredBatch
where
    F: FnMut(&Record) -> bool,
{
    // Borrow the survivors on the first pass. The all-survive case is the
    // common one for a restore, and it must not pay for a clone of the records
    // it is about to discard.
    let mut kept: Vec<&Record> = Vec::with_capacity(batch.records.len());
    for record in &batch.records {
        if keep(record) {
            kept.push(record);
        }
    }

    if kept.len() == batch.records.len() {
        return FilteredBatch::Unchanged;
    }
    let Some(last_offset_delta) = kept.iter().map(|record| record.offset_delta).max() else {
        return FilteredBatch::Empty;
    };

    FilteredBatch::Filtered(RecordBatch {
        base_offset: batch.base_offset,
        partition_leader_epoch: batch.partition_leader_epoch,
        attributes: batch.attributes,
        last_offset_delta,
        base_timestamp: batch.base_timestamp,
        max_timestamp: batch.max_timestamp,
        producer_id: batch.producer_id,
        producer_epoch: batch.producer_epoch,
        base_sequence: batch.base_sequence,
        records: kept.into_iter().cloned().collect(),
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        bytes::{Bytes, BytesMut},
        krabka_protocol::records::{Attributes, RecordHeader},
    };

    const BASE_OFFSET: i64 = 1_000;
    const BASE_TIMESTAMP: i64 = 1_700_000_000_000;

    fn record(offset_delta: i32) -> Record {
        Record {
            attributes: 0,
            timestamp_delta: i64::from(offset_delta) * 10,
            offset_delta,
            key: Some(Bytes::from(format!("k{offset_delta}"))),
            value: Some(Bytes::from(format!("v{offset_delta}"))),
            headers: vec![RecordHeader {
                key: "trace".to_owned(),
                value: Some(Bytes::from_static(b"h")),
            }],
        }
    }

    // Every header field holds a distinctive value, so a whole-struct
    // comparison catches any field the filter fails to carry over.
    fn batch(offset_deltas: &[i32]) -> RecordBatch {
        RecordBatch {
            base_offset: BASE_OFFSET,
            partition_leader_epoch: 7,
            attributes: Attributes::default().with_transactional(true),
            last_offset_delta: offset_deltas.iter().copied().max().unwrap_or(0),
            base_timestamp: BASE_TIMESTAMP,
            max_timestamp: BASE_TIMESTAMP + 30,
            producer_id: 4_242,
            producer_epoch: 3,
            base_sequence: 11,
            records: offset_deltas.iter().copied().map(record).collect(),
        }
    }

    // The batch `filter_batch` must produce, with `last_offset_delta` stated
    // outright rather than derived by the rule under test.
    fn expected(input: &[i32], survivors: &[i32], last_offset_delta: i32) -> RecordBatch {
        RecordBatch {
            last_offset_delta,
            records: survivors.iter().copied().map(record).collect(),
            ..batch(input)
        }
    }

    enum Outcome {
        Unchanged,
        Empty,
        // The surviving offset deltas, then the recomputed `last_offset_delta`.
        Filtered(&'static [i32], i32),
    }

    struct Case {
        name: &'static str,
        offset_deltas: &'static [i32],
        dropped: &'static [i32],
        outcome: Outcome,
    }

    #[test]
    fn filter_batch_keeps_the_compacted_log_shape() {
        let cases = [
            Case {
                name: "every record survives",
                offset_deltas: &[0, 1, 2, 3],
                dropped: &[],
                outcome: Outcome::Unchanged,
            },
            Case {
                name: "a middle record is dropped",
                offset_deltas: &[0, 1, 2, 3],
                dropped: &[1],
                outcome: Outcome::Filtered(&[0, 2, 3], 3),
            },
            Case {
                name: "the first record is dropped",
                offset_deltas: &[0, 1, 2, 3],
                dropped: &[0],
                outcome: Outcome::Filtered(&[1, 2, 3], 3),
            },
            Case {
                name: "the last record is dropped",
                offset_deltas: &[0, 1, 2, 3],
                dropped: &[3],
                outcome: Outcome::Filtered(&[0, 1, 2], 2),
            },
            Case {
                name: "the first and last records are dropped",
                offset_deltas: &[0, 1, 2, 3],
                dropped: &[0, 3],
                outcome: Outcome::Filtered(&[1, 2], 2),
            },
            Case {
                name: "every record is dropped",
                offset_deltas: &[0, 1, 2, 3],
                dropped: &[0, 1, 2, 3],
                outcome: Outcome::Empty,
            },
            Case {
                name: "a single-record batch survives",
                offset_deltas: &[0],
                dropped: &[],
                outcome: Outcome::Unchanged,
            },
            Case {
                name: "a single-record batch is emptied",
                offset_deltas: &[0],
                dropped: &[0],
                outcome: Outcome::Empty,
            },
            Case {
                name: "an already-compacted batch survives",
                offset_deltas: &[0, 3, 7],
                dropped: &[],
                outcome: Outcome::Unchanged,
            },
            Case {
                name: "an already-compacted batch loses its first record",
                offset_deltas: &[0, 3, 7],
                dropped: &[0],
                outcome: Outcome::Filtered(&[3, 7], 7),
            },
            Case {
                name: "an already-compacted batch loses its middle record",
                offset_deltas: &[0, 3, 7],
                dropped: &[3],
                outcome: Outcome::Filtered(&[0, 7], 7),
            },
            Case {
                name: "an already-compacted batch loses its last record",
                offset_deltas: &[0, 3, 7],
                dropped: &[7],
                outcome: Outcome::Filtered(&[0, 3], 3),
            },
            Case {
                name: "a batch without records has nothing to drop",
                offset_deltas: &[],
                dropped: &[],
                outcome: Outcome::Unchanged,
            },
        ];

        for case in cases {
            let input = batch(case.offset_deltas);
            let dropped = case.dropped;
            let got = filter_batch(&input, |record| !dropped.contains(&record.offset_delta));
            let want = match case.outcome {
                Outcome::Unchanged => FilteredBatch::Unchanged,
                Outcome::Empty => FilteredBatch::Empty,
                Outcome::Filtered(survivors, last_offset_delta) => FilteredBatch::Filtered(
                    expected(case.offset_deltas, survivors, last_offset_delta),
                ),
            };
            check!(got == want, "case: {}", case.name);
        }
    }

    #[test]
    fn dropping_the_first_record_leaves_absolute_offsets_alone() {
        let input = batch(&[0, 1, 2]);
        let original: Vec<i64> = input
            .records
            .iter()
            .map(|record| input.base_offset + i64::from(record.offset_delta))
            .collect();

        let got = filter_batch(&input, |record| record.offset_delta != 0);

        let FilteredBatch::Filtered(out) = got else {
            panic!("dropping a record must produce a filtered batch");
        };
        check!(out.base_offset == BASE_OFFSET);
        let survivors: Vec<i64> = out
            .records
            .iter()
            .map(|record| out.base_offset + i64::from(record.offset_delta))
            .collect();
        check!(survivors == original[1..].to_vec());
    }

    #[test]
    fn keep_sees_every_record_once_in_batch_order() {
        let input = batch(&[0, 3, 7]);
        let mut seen = Vec::new();

        let got = filter_batch(&input, |record| {
            seen.push(record.offset_delta);
            true
        });

        check!(seen == [0, 3, 7]);
        check!(got == FilteredBatch::Unchanged);
    }

    #[test]
    fn filtered_batches_round_trip_through_the_wire() {
        let offset_deltas: &[i32] = &[0, 1, 2, 3];
        let cases: [(&[i32], &[i32], i32); 3] = [
            (&[1], &[0, 2, 3], 3),
            (&[3], &[0, 1, 2], 2),
            (&[0], &[1, 2, 3], 3),
        ];

        for (dropped, survivors, last_offset_delta) in cases {
            let input = batch(offset_deltas);
            let got = filter_batch(&input, |record| !dropped.contains(&record.offset_delta));

            let FilteredBatch::Filtered(out) = got else {
                panic!("dropping {dropped:?} must produce a filtered batch");
            };
            let mut buf = BytesMut::with_capacity(out.encoded_len());
            out.encode(&mut buf).unwrap();
            let mut cursor: &[u8] = &buf[..];
            let decoded = RecordBatch::decode(&mut cursor).unwrap();

            assert!(cursor.is_empty());
            check!(decoded == expected(offset_deltas, survivors, last_offset_delta));
        }
    }

    proptest::proptest! {
        // Arbitrary batch shapes and arbitrary keep decisions: the surviving
        // records keep their absolute offsets, and `last_offset_delta` follows
        // the survivors and nothing else.
        #[test]
        fn survivors_keep_their_absolute_offsets(
            offset_deltas in proptest::collection::hash_set(0i32..64, 1..12),
            keep_mask in proptest::collection::vec(proptest::bool::ANY, 12),
        ) {
            let mut offset_deltas: Vec<i32> = offset_deltas.into_iter().collect();
            offset_deltas.sort_unstable();
            let input = batch(&offset_deltas);

            let mut next = 0usize;
            let got = filter_batch(&input, |_| {
                let keep = keep_mask[next];
                next += 1;
                keep
            });

            let survivors: Vec<i32> = offset_deltas
                .iter()
                .copied()
                .zip(&keep_mask)
                .filter(|(_, keep)| **keep)
                .map(|(offset_delta, _)| offset_delta)
                .collect();
            match got {
                FilteredBatch::Unchanged => proptest::prop_assert_eq!(&survivors, &offset_deltas),
                FilteredBatch::Empty => proptest::prop_assert!(survivors.is_empty()),
                FilteredBatch::Filtered(out) => {
                    proptest::prop_assert_eq!(out.base_offset, input.base_offset);
                    let kept: Vec<i32> =
                        out.records.iter().map(|record| record.offset_delta).collect();
                    proptest::prop_assert_eq!(&kept, &survivors);
                    proptest::prop_assert_eq!(
                        out.last_offset_delta,
                        survivors.iter().copied().max().unwrap_or_default()
                    );
                }
            }
        }
    }
}
