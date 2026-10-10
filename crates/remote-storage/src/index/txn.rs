//! The Kafka transaction-index on-disk layout and the range-overlap test the
//! fetch path applies to it.
//!
//! An entry records one aborted transaction's offset span and the producer
//! that wrote it, so a fetch can report the aborted transactions that
//! intersect the offsets it returns.

use krabka_verified::remote_txn::{RemoteTxnOverlapDecision, remote_txn_overlap_decision};
use zerocopy::{
    BigEndian, FromBytes, Immutable, KnownLayout, Unaligned,
    byteorder::{I16, I64},
};

use super::{LogOffset, corrupt_index};
use crate::error::RemoteStorageError;

/// The only aborted-transaction record version Kafka writes or accepts.
const SUPPORTED_VERSION: i16 = 0;

/// 34 bytes per entry: a big-endian i16 `version` (always 0), then
/// `producer_id` i64 BE, `start_offset` i64 BE, `last_offset` i64 BE, and
/// `last_stable_offset` i64 BE. It mirrors
/// `krabka_log::txn_index::AbortedTxnRaw`, so the remote-tier copy of a
/// `.txnindex` file decodes through the same byte layout that wrote the
/// local index.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct AbortedTxnIndexEntry {
    /// Record version. Always 0; a reader refuses anything else.
    pub version: I16<BigEndian>,
    /// Producer that wrote, and then aborted, the transaction.
    pub producer_id: I64<BigEndian>,
    /// First offset the aborted transaction wrote.
    pub start_offset: I64<BigEndian>,
    /// Last offset the aborted transaction wrote.
    pub last_offset: I64<BigEndian>,
    /// The last stable offset at the moment this transaction aborted.
    pub last_stable_offset: I64<BigEndian>,
}

/// Byte length of one serialized aborted-transaction index entry.
const TXN_INDEX_ENTRY_LEN: usize = std::mem::size_of::<AbortedTxnIndexEntry>();

const _: [(); 34] = [(); TXN_INDEX_ENTRY_LEN];

/// Borrows Kafka's transaction-index format as a zero-copy
/// `&[AbortedTxnIndexEntry]`, at 34 bytes per entry: a big-endian i16
/// version (always 0), then `producer_id`, `start_offset`, `last_offset`,
/// and `last_stable_offset`, each a big-endian i64. The result borrows from
/// `bytes`.
///
/// # Errors
///
/// Returns [`RemoteStorageError::Io`] when the object store returned bytes
/// that do not form an entry array, or when an entry's version prefix is
/// not the one supported version.
pub fn parse_txn_index(bytes: &[u8]) -> Result<&[AbortedTxnIndexEntry], RemoteStorageError> {
    if !bytes.len().is_multiple_of(TXN_INDEX_ENTRY_LEN) {
        return Err(corrupt_index("transaction"));
    }
    let entries = <[AbortedTxnIndexEntry]>::ref_from_bytes(bytes)
        .map_err(|_| corrupt_index("transaction"))?;
    if entries
        .iter()
        .any(|entry| entry.version.get() != SUPPORTED_VERSION)
    {
        return Err(corrupt_index("transaction"));
    }
    if entries.iter().any(|entry| {
        matches!(
            remote_txn_overlap_decision(
                entry.start_offset.get(),
                entry.last_offset.get(),
                entry.start_offset.get(),
                entry.last_offset.get(),
            ),
            RemoteTxnOverlapDecision::Invalid
        )
    }) {
        return Err(corrupt_index("transaction"));
    }
    Ok(entries)
}

/// Reports whether an aborted-transaction entry overlaps the inclusive offset
/// range `[from_offset, to_offset]`. It mirrors the overlap test in
/// `TxnIndex::aborted_in_range` against an inclusive range: the entry's
/// `[start, last]` intersects `[from, to]` if and only if
/// `start <= to && last >= from`.
#[must_use]
pub fn txn_overlaps(
    entry: &AbortedTxnIndexEntry,
    from_offset: LogOffset,
    to_offset: LogOffset,
) -> bool {
    matches!(
        remote_txn_overlap_decision(
            entry.start_offset.get(),
            entry.last_offset.get(),
            from_offset,
            to_offset,
        ),
        RemoteTxnOverlapDecision::Overlap
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_ids::{Offset, ProducerId};

    use super::*;

    #[derive(Clone, Copy)]
    struct TxnIndexVersion(i16);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct TxnIndexEntrySetup {
        #[default(TxnIndexVersion(0))]
        version: TxnIndexVersion,
        #[default(ProducerId(1000))]
        producer_id: ProducerId,
        #[default(Offset(0))]
        start: Offset,
        #[default(Offset(4))]
        last: Offset,
        #[default(Offset(5))]
        lso: Offset,
    }

    fn entry_bytes(setup: TxnIndexEntrySetup) -> Vec<u8> {
        let TxnIndexEntrySetup {
            version,
            producer_id,
            start,
            last,
            lso,
        } = setup;
        let mut buf = Vec::new();
        buf.extend_from_slice(&version.0.to_be_bytes());
        buf.extend_from_slice(&producer_id.0.to_be_bytes());
        buf.extend_from_slice(&start.0.to_be_bytes());
        buf.extend_from_slice(&last.0.to_be_bytes());
        buf.extend_from_slice(&lso.0.to_be_bytes());
        buf
    }

    #[test]
    fn parse_txn_index_round_trips_known_entries() {
        // Mirror TxnIndex::append: 2B version, 8B producer_id, 8B
        // start_offset, 8B last_offset, 8B last_stable_offset, all BE.
        let mut buf = Vec::new();
        for (start, last, pid) in [(0_i64, 4_i64, 1000_i64), (10, 14, 2000)] {
            buf.extend(entry_bytes(TxnIndexEntrySetup {
                producer_id: ProducerId(pid),
                start: Offset(start),
                last: Offset(last),
                lso: Offset(last + 1),
                ..Default::default()
            }));
        }
        let entries = parse_txn_index(&buf).expect("valid txn index");
        let decoded: Vec<(i64, i64, i64, i64)> = entries
            .iter()
            .map(|e| {
                (
                    e.start_offset.get(),
                    e.last_offset.get(),
                    e.producer_id.get(),
                    e.last_stable_offset.get(),
                )
            })
            .collect();
        assert!(decoded == vec![(0, 4, 1000, 5), (10, 14, 2000, 15)]);
    }

    #[test]
    fn parse_txn_index_rejects_trailing_partial_bytes() {
        let mut buf = entry_bytes(TxnIndexEntrySetup::default());
        // 5 trailing bytes that don't complete a 34-byte entry.
        buf.extend_from_slice(&[0xAA; 5]);
        assert!(parse_txn_index(&buf).is_err(), "partial entry is corrupt");
    }

    #[test]
    fn parse_txn_index_empty_is_empty() {
        assert!(parse_txn_index(&[]).expect("empty is valid").is_empty());
    }

    #[test]
    fn parse_txn_index_rejects_an_unsupported_version() {
        let buf = entry_bytes(TxnIndexEntrySetup {
            version: TxnIndexVersion(1),
            ..Default::default()
        });
        assert!(
            parse_txn_index(&buf).is_err(),
            "an unsupported version prefix is corrupt"
        );
    }

    #[test]
    fn txn_overlaps_boundaries() {
        let e = AbortedTxnIndexEntry {
            version: I16::new(0),
            producer_id: I64::new(1),
            start_offset: I64::new(10),
            last_offset: I64::new(14),
            last_stable_offset: I64::new(15),
        };
        let cases = [
            // Range fully before the entry → excluded.
            (0, 9, false),
            // Range touching the entry's first offset → included.
            (0, 10, true),
            // Range fully inside the entry → included.
            (11, 13, true),
            // Range touching the entry's last offset → included.
            (14, 100, true),
            // Range fully after the entry → excluded.
            (15, 100, false),
            // Range fully covering the entry → included.
            (0, 100, true),
        ];
        for (start, end, want) in cases {
            assert!(
                txn_overlaps(&e, start, end) == want,
                "range [{start},{end}]"
            );
        }
    }

    #[test]
    fn txn_overlaps_rejects_inverted_query_and_parser_rejects_inverted_entry() {
        let valid = AbortedTxnIndexEntry {
            version: I16::new(0),
            producer_id: I64::new(1),
            start_offset: I64::new(10),
            last_offset: I64::new(14),
            last_stable_offset: I64::new(15),
        };
        let inverted = AbortedTxnIndexEntry {
            version: I16::new(0),
            producer_id: I64::new(1),
            start_offset: I64::new(14),
            last_offset: I64::new(10),
            last_stable_offset: I64::new(11),
        };

        assert!(!txn_overlaps(&valid, 14, 10));
        assert!(!txn_overlaps(&inverted, 0, 100));

        let bytes = entry_bytes(TxnIndexEntrySetup {
            producer_id: ProducerId(1),
            start: Offset(14),
            last: Offset(10),
            lso: Offset(11),
            ..Default::default()
        });
        assert!(parse_txn_index(&bytes).is_err());
    }
}
