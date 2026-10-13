//! The `ListOffsets` timestamp sentinels, their response placeholders, and the
//! protocol version each sentinel needs.
//!
//! Kafka encodes what a partition row asks for in the request timestamp, so
//! every sentinel here is a wire value that must keep its exact number. A
//! client that names a sentinel its request version does not carry receives
//! `UNSUPPORTED_VERSION`, which is what [`timestamp_supported`] decides.

/// Request timestamp sentinel (-2): resolve the earliest available offset.
/// Kafka's `ListOffsetsRequest.EARLIEST_TIMESTAMP`.
#[cfg(test)]
pub(super) const EARLIEST_TIMESTAMP: i64 = -2;

/// Request timestamp sentinel (-1): resolve the log-end (next) offset.
/// Kafka's `ListOffsetsRequest.LATEST_TIMESTAMP`.
#[cfg(test)]
pub(super) const LATEST_TIMESTAMP: i64 = -1;

/// Request timestamp sentinel (-3, KIP-734): resolve the offset of the record
/// with the highest timestamp. Kafka's `ListOffsetsRequest.MAX_TIMESTAMP`.
#[cfg(test)]
pub(super) const MAX_TIMESTAMP: i64 = -3;

/// Request timestamp sentinel (-4, KIP-405): resolve the earliest offset still
/// in local storage. Kafka's `ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP`.
#[cfg(test)]
pub(super) const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;

/// Request timestamp sentinel (-5, KIP-1005): resolve the highest offset in a
/// finished remote segment.
#[cfg(test)]
pub(super) const LATEST_TIERED_TIMESTAMP: i64 = -5;

/// Request timestamp sentinel (-6, KIP-1023): resolve the first offset that
/// has not been uploaded to the remote tier.
#[cfg(test)]
pub(super) const EARLIEST_PENDING_UPLOAD_TIMESTAMP: i64 = -6;

/// Response placeholder (-1) meaning "no record timestamp matched/echoed".
/// Kafka's `ListOffsetsResponse.UNKNOWN_TIMESTAMP`.
pub(super) const UNKNOWN_TIMESTAMP: i64 = -1;

/// Response placeholder (-1) meaning "no offset was resolved".
/// Kafka's `ListOffsetsResponse.UNKNOWN_OFFSET`.
pub(super) const UNKNOWN_OFFSET: i64 = -1;

/// Response placeholder (-1) meaning "no leader epoch is being reported".
/// Kafka's `ListOffsetsResponse.UNKNOWN_EPOCH`.
pub(super) const UNKNOWN_EPOCH: i32 = -1;

pub(super) fn timestamp_kind(timestamp: i64, version: i16) -> krabka_verified::ListOffsetsKind {
    krabka_verified::list_offsets_kind(timestamp, version)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn sentinel_constants_match_kafka_wire_values() {
        let cases = [
            ("EARLIEST_TIMESTAMP", EARLIEST_TIMESTAMP, -2),
            ("LATEST_TIMESTAMP", LATEST_TIMESTAMP, -1),
            ("MAX_TIMESTAMP", MAX_TIMESTAMP, -3),
            ("EARLIEST_LOCAL_TIMESTAMP", EARLIEST_LOCAL_TIMESTAMP, -4),
            ("LATEST_TIERED_TIMESTAMP", LATEST_TIERED_TIMESTAMP, -5),
            (
                "EARLIEST_PENDING_UPLOAD_TIMESTAMP",
                EARLIEST_PENDING_UPLOAD_TIMESTAMP,
                -6,
            ),
        ];
        for (name, sentinel, want) in cases {
            assert!(sentinel == want, "{name}");
        }
    }

    #[test]
    fn tiered_sentinels_require_their_kafka_versions() {
        let cases = [
            (EARLIEST_TIMESTAMP, 0, true),
            (LATEST_TIMESTAMP, 0, true),
            (MAX_TIMESTAMP, 6, false),
            (MAX_TIMESTAMP, 7, true),
            (EARLIEST_LOCAL_TIMESTAMP, 7, false),
            (EARLIEST_LOCAL_TIMESTAMP, 8, true),
            (LATEST_TIERED_TIMESTAMP, 8, false),
            (LATEST_TIERED_TIMESTAMP, 9, true),
            (EARLIEST_PENDING_UPLOAD_TIMESTAMP, 10, false),
            (EARLIEST_PENDING_UPLOAD_TIMESTAMP, 11, true),
            (-7, 11, false),
            (0, 1, true),
        ];
        for (timestamp, version, expected) in cases {
            assert!(
                (timestamp_kind(timestamp, version)
                    != krabka_verified::ListOffsetsKind::Unsupported)
                    == expected
            );
        }
    }
}
