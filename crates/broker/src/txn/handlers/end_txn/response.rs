//! The `EndTxnResponse` shapes. One carries the successful completion
//! identity, the other the wire sentinels that stand in for it when the handler
//! answers with an error code.

use {
    crate::{codes, txn::util::producer_fenced_wire_code},
    krabka_protocol::owned::end_txn_response::EndTxnResponse,
};

/// Kafka wire sentinel: "no producer id" (`RecordBatch.NO_PRODUCER_ID`).
/// Returned on `EndTxn` error responses, where the identity is meaningless.
const NO_PRODUCER_ID: i64 = -1;

/// Kafka wire sentinel: "no producer epoch" (`RecordBatch.NO_PRODUCER_EPOCH`).
const NO_PRODUCER_EPOCH: i16 = -1;

pub(super) fn err_response(version: i16, error_code: i16) -> EndTxnResponse {
    // On the error path the producer_id/epoch fields are not meaningful;
    // leave them at the "no producer" wire sentinels.
    response(
        producer_fenced_wire_code(version, error_code),
        NO_PRODUCER_ID,
        NO_PRODUCER_EPOCH,
    )
}

/// A successful `EndTxn` response. `producer_id` and `producer_epoch` are the
/// post-completion identity. The epoch bumps for a `TV_2` client, that is
/// `EndTxn` v5, or rolls to a new `producer_id` on epoch exhaustion; see
/// [`next_producer_identity`](super::producer_identity::next_producer_identity). They
/// are only on the wire at v5 (KIP-890). A lower version never bumps the
/// epoch, since its producer could not learn the new one.
pub(super) fn ok_response(producer_id: i64, producer_epoch: i16) -> EndTxnResponse {
    response(codes::NONE, producer_id, producer_epoch)
}

fn response(error_code: i16, producer_id: i64, producer_epoch: i16) -> EndTxnResponse {
    EndTxnResponse {
        throttle_time_ms: 0,
        error_code,
        producer_id,
        producer_epoch,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::txn::util::producer_fenced_cases, assert2::assert,
        krabka_protocol::UnknownTaggedFields,
    };

    #[test]
    fn producer_fenced_is_invalid_producer_epoch_below_version_2() {
        // (version, error code, expected code)
        let cases = producer_fenced_cases(0);
        for (version, error_code, expected) in cases {
            let expected = EndTxnResponse {
                throttle_time_ms: 0,
                error_code: expected,
                producer_id: -1,
                producer_epoch: -1,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            };
            assert!(err_response(version, error_code) == expected, "v{version}");
        }
    }

    #[test]
    fn ok_response_carries_the_producer_identity() {
        let expected = EndTxnResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            producer_id: 42,
            producer_epoch: 7,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(ok_response(42, 7) == expected);
    }
}
