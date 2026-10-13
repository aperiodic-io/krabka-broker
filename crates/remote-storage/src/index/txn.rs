//! The Kafka transaction-index on-disk layout and the range-overlap test the
//! fetch path applies to it.
//!
//! An entry records one aborted transaction's offset span and the producer
//! that wrote it, so a fetch can report the aborted transactions that
//! intersect the offsets it returns.

use {
    super::{LogOffset, corrupt_index},
    crate::error::RemoteStorageError,
    krabka_verified::remote_txn::{RemoteTxnOverlapDecision, remote_txn_overlap_decision},
    zerocopy::{
        BigEndian, FromBytes, Immutable, KnownLayout, Unaligned,
        byteorder::{I16, I64},
    },
};

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
    use {
        super::*,
        assert2::assert,
        krabka_ids::{Offset, ProducerId},
    };

    #[derive(derive_more::From, derive_more::Into, Clone, Copy)]
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

    #[bon::builder]
    fn entry_bytes(
        #[builder(into, default = TxnIndexVersion (0))] version: TxnIndexVersion,
        #[builder(into, default = ProducerId (1000))] producer_id: ProducerId,
        #[builder(into, default = Offset (0))] start: Offset,
        #[builder(into, default = Offset (4))] last: Offset,
        #[builder(into, default = Offset (5))] lso: Offset,
    ) -> Vec<u8> {
        let setup = TxnIndexEntrySetup {
            version,
            producer_id,
            start,
            last,
            lso,
        };
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
        /// The decoded fields of one aborted-transaction entry.
        #[derive(Debug, PartialEq, Eq)]
        struct DecodedTxn {
            start_offset: i64,
            last_offset: i64,
            producer_id: i64,
            last_stable_offset: i64,
        }
        // Mirror TxnIndex::append: 2B version, 8B producer_id, 8B
        // start_offset, 8B last_offset, 8B last_stable_offset, all BE.
        let mut buf = Vec::new();
        for (start, last, pid) in [(0_i64, 4_i64, 1000_i64), (10, 14, 2000)] {
            buf.extend(
                entry_bytes()
                    .producer_id(pid)
                    .start(start)
                    .last(last)
                    .lso(last + 1)
                    .call(),
            );
        }
        let entries = parse_txn_index(&buf).expect("valid txn index");
        let decoded: Vec<DecodedTxn> = entries
            .iter()
            .map(|e| DecodedTxn {
                start_offset: e.start_offset.get(),
                last_offset: e.last_offset.get(),
                producer_id: e.producer_id.get(),
                last_stable_offset: e.last_stable_offset.get(),
            })
            .collect();
        assert!(
            decoded
                == vec![
                    DecodedTxn {
                        start_offset: 0,
                        last_offset: 4,
                        producer_id: 1000,
                        last_stable_offset: 5,
                    },
                    DecodedTxn {
                        start_offset: 10,
                        last_offset: 14,
                        producer_id: 2000,
                        last_stable_offset: 15,
                    },
                ]
        );
    }

    #[test]
    fn parse_txn_index_rejects_trailing_partial_bytes() {
        let mut buf = entry_bytes().call();
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
        let buf = entry_bytes().version(1).call();
        assert!(
            parse_txn_index(&buf).is_err(),
            "an unsupported version prefix is corrupt"
        );
    }

    fn sample_aborted_txn_entry(start: i64, last: i64, lso: i64) -> AbortedTxnIndexEntry {
        AbortedTxnIndexEntry {
            version: I16::new(0),
            producer_id: I64::new(1),
            start_offset: I64::new(start),
            last_offset: I64::new(last),
            last_stable_offset: I64::new(lso),
        }
    }

    #[test]
    fn txn_overlaps_boundaries() {
        let e = sample_aborted_txn_entry(10, 14, 15);
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
        let valid = sample_aborted_txn_entry(10, 14, 15);
        let inverted = sample_aborted_txn_entry(14, 10, 11);

        assert!(!txn_overlaps(&valid, 14, 10));
        assert!(!txn_overlaps(&inverted, 0, 100));

        let bytes = entry_bytes()
            .producer_id(1)
            .start(14)
            .last(10)
            .lso(11)
            .call();
        assert!(parse_txn_index(&bytes).is_err());
    }
}
