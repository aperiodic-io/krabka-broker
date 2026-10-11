//! The KFC-7 schema-validation gate, which checks every record of a prepared
//! batch against the registry after the leadership gate and before append.

use std::sync::Arc;

use krabka_compression::RecordDecompressionPolicy;
use krabka_protocol::{
    owned::produce_response::BatchIndexAndErrorMessage, records::RecordBatchBorrowed,
};
use krabka_verified::{
    SchemaBatchAdmission, SchemaFieldAction, SchemaFieldRole, schema_batch_admission,
    schema_field_action,
};

use super::prepare::{PreparedBatch, PreparedSource};
use crate::{
    metrics::BrokerMetrics,
    schema_validation::{RejectReason, Role, SchemaGate, SchemaValidator},
};

/// The KIP-467 `error_message` a schema rejection carries.
///
/// The per-record `record_errors` say which records failed and why; this is
/// the partition-level line a client shows when it does not read them, and it
/// is what a pre-v8 client would have seen had the field existed for it.
pub(super) const SCHEMA_REJECTION_MESSAGE: &str = "one or more records failed schema validation";

/// The largest number of per-record errors one rejected batch reports.
///
/// A batch can hold thousands of records and a producer that framed none of
/// them would otherwise make the broker build a response larger than the
/// request. The first few name the problem; the producer does not need the
/// rest to act.
const MAX_RECORD_ERRORS: usize = 8;

/// Check every validated field of every record in `prepared` against the
/// registry.
///
/// `Ok(())` admits the batch. `Err(record_errors)` rejects it whole, which is
/// what the batch's own CRC requires: the broker appends the producer's exact
/// bytes, so it cannot drop one record without re-encoding the batch. The
/// returned rows name the offending records.
///
/// # The second decode
///
/// The verbatim path materializes no records — it walks them to check their
/// structure and throws each one away — so there is no key or value here to
/// look at. This decodes the batch again, for inspection only, and then
/// discards the decoded view and leaves `prepared` untouched. The log still
/// holds exactly what the producer wrote. The cost is a second CRC pass and,
/// on a compressed batch, a second decompression, paid only on a topic that
/// asked for validation.
pub(super) async fn validate_batch_schemas(
    prepared: &PreparedBatch,
    gate: SchemaGate,
    validator: Option<&Arc<SchemaValidator>>,
    topic_name: &str,
    policy: RecordDecompressionPolicy,
    metrics: &BrokerMetrics,
) -> Result<(), Vec<BatchIndexAndErrorMessage>> {
    let Some(validator) = validator else {
        // The topic asked for validation and this broker has no registry to
        // ask. Admitting the record would make the topic's setting a lie, so
        // this fails closed, and it fails the same way for every record in the
        // batch rather than naming one.
        let reason = RejectReason::RegistryUnavailable(
            "no [schema_registry] section is configured on this broker".to_owned(),
        );
        return Err(vec![schema_rejection(metrics, topic_name, 0, &reason)]);
    };

    let check = SchemaCheck {
        validator,
        gate,
        topic_name,
        metrics,
    };
    let mut errors = Vec::new();
    let mut walk_complete = true;
    let mut applicable = 0_u64;
    let mut admitted = 0_u64;
    match &prepared.source {
        PreparedSource::Owned(batch) => {
            for (index, record) in batch.records.iter().enumerate() {
                let tally = check
                    .record(
                        index,
                        record.key.as_deref(),
                        record.value.as_deref(),
                        &mut errors,
                    )
                    .await;
                applicable += tally.applicable;
                admitted += tally.admitted;
                if errors.len() >= MAX_RECORD_ERRORS {
                    walk_complete = false;
                    break;
                }
            }
        }
        PreparedSource::Verbatim(bytes) => {
            let mut cursor: &[u8] = bytes;
            // `prepare_batch` already proved this decodes; a failure here is
            // not reachable through it, and treating it as "cannot validate"
            // is the safe reading if it ever became reachable.
            let Ok(batch) = RecordBatchBorrowed::decode_borrow_with_policy(&mut cursor, policy)
            else {
                let reason = RejectReason::Unframed("batch did not decode".to_owned());
                return Err(vec![schema_rejection(metrics, topic_name, 0, &reason)]);
            };
            for (index, record) in batch.iter().enumerate() {
                // `prepare_batch`'s `validate_records` walk already parsed
                // every record, so this is not reachable through it. It fails
                // closed anyway: this is a different walk from that one, and if
                // the two ever disagree, a validated topic must not admit a
                // record the broker could not read. Breaking without a row
                // would leave `errors` empty and admit the batch.
                let Ok(record) = record else {
                    let reason = RejectReason::Unframed("record did not decode".to_owned());
                    errors.push(schema_rejection(
                        metrics,
                        topic_name,
                        i32::try_from(index).unwrap_or(i32::MAX),
                        &reason,
                    ));
                    walk_complete = false;
                    break;
                };
                let tally = check
                    .record(index, record.key, record.value, &mut errors)
                    .await;
                applicable += tally.applicable;
                admitted += tally.admitted;
                if errors.len() >= MAX_RECORD_ERRORS {
                    walk_complete = false;
                    break;
                }
            }
        }
    }

    match schema_batch_admission(walk_complete, applicable, admitted) {
        SchemaBatchAdmission::Admit => Ok(()),
        SchemaBatchAdmission::Reject => Err(errors),
    }
}

/// Every schema refusal records its reason and reports the same wire row.
fn schema_rejection(
    metrics: &BrokerMetrics,
    topic: &str,
    batch_index: i32,
    reason: &RejectReason,
) -> BatchIndexAndErrorMessage {
    metrics.record_schema_validation_rejection(topic, reason.label());
    BatchIndexAndErrorMessage {
        batch_index,
        batch_index_error_message: Some(reason.to_string()),
        ..Default::default()
    }
}

/// Everything the per-record check needs that does not change between
/// records, held together so that the check takes one argument for its
/// context and one for the record.
#[derive(Clone, Copy)]
struct SchemaCheck<'a> {
    validator: &'a Arc<SchemaValidator>,
    gate: SchemaGate,
    topic_name: &'a str,
    metrics: &'a BrokerMetrics,
}

#[derive(Clone, Copy, Default)]
struct SchemaTally {
    applicable: u64,
    admitted: u64,
}

impl SchemaCheck<'_> {
    /// Check one record's key and value, appending a row for each that failed.
    ///
    /// A null field is skipped rather than rejected. A null key is ordinary,
    /// and a null value is a tombstone, which a compacted topic needs —
    /// rejecting one would make schema validation and compaction mutually
    /// exclusive.
    async fn record(
        self,
        index: usize,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        errors: &mut Vec<BatchIndexAndErrorMessage>,
    ) -> SchemaTally {
        let batch_index = i32::try_from(index).unwrap_or(i32::MAX);
        let mut tally = SchemaTally::default();
        for (role, field) in [(SchemaFieldRole::Key, key), (SchemaFieldRole::Value, value)] {
            let role =
                match schema_field_action(self.gate.key, self.gate.value, role, field.is_some()) {
                    SchemaFieldAction::Skip => continue,
                    SchemaFieldAction::CheckKey => Role::Key,
                    SchemaFieldAction::CheckValue => Role::Value,
                };
            let Some(field) = field else { continue };
            tally.applicable += 1;
            match self
                .validator
                .check(self.topic_name, role, self.gate.mode, field, self.metrics)
                .await
            {
                Ok(()) => tally.admitted += 1,
                Err(reason) => {
                    errors.push(schema_rejection(
                        self.metrics,
                        self.topic_name,
                        batch_index,
                        &reason,
                    ));
                }
            }
        }
        tally
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::Bytes;
    use krabka_protocol::records::{Record, RecordBatch};
    use krabka_units::secs;

    use super::*;
    use crate::{
        handlers::produce::test_support::encode_batch,
        schema_validation::{SchemaGate, ValidationMode},
    };

    /// A record the broker cannot decode must not be admitted by a validated
    /// topic.
    ///
    /// This is defence in depth, not a live hole: `prepare_batch` runs
    /// `validate_records` over every record before this function is reached,
    /// so a batch that gets here has already had each record parsed. The two
    /// walks are different code, though, and the batch-level decode a few
    /// lines above this one already fails closed for exactly that reason.
    /// Before the fix, the per-record arm broke out of the loop without
    /// recording anything, so `errors` stayed empty and the batch was
    /// *admitted* — the one outcome a validation feature must never produce.
    #[tokio::test]
    async fn a_record_that_does_not_decode_is_rejected_not_admitted() {
        let mut batch = RecordBatch {
            last_offset_delta: 0,
            producer_id: -1,
            ..RecordBatch::default()
        };
        // A null value is a tombstone, which the checker skips, so this record
        // passes without a registry call. That matters: if it recorded an
        // error of its own, `errors` would be non-empty and the test would
        // pass whether or not the decode failure was handled.
        batch.records.push(Record {
            value: None,
            ..Default::default()
        });
        let whole = encode_batch(&batch);
        // Claim one more record than the bytes carry. The v2 header is intact
        // and self-consistent, so the batch-level decode succeeds; the walk
        // then yields the real record and fails on the phantom one. The record
        // count is the i32 at offset 57 of the 61-byte header.
        let mut bytes = whole.to_vec();
        bytes[57..61].copy_from_slice(&2i32.to_be_bytes());
        // The batch-level decode verifies the CRC, so it has to be restored
        // over the edited count or this never reaches the per-record walk.
        // The CRC covers the header from offset 21 and then the record bytes.
        let crc = crc32c::crc32c_append(crc32c::crc32c(&bytes[21..61]), &bytes[61..]);
        bytes[17..21].copy_from_slice(&crc.to_be_bytes());
        let bytes = Bytes::from(bytes);

        let prepared = PreparedBatch {
            attributes: batch.attributes,
            last_offset_delta: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            keyless_records: Vec::new(),
            invalid_timestamp_records: Vec::new(),
            source: PreparedSource::Verbatim(bytes),
        };
        // No request reaches the registry: the record fails to decode before
        // any field is checked, so the address is never dialled.
        let validator = Arc::new(
            SchemaValidator::new(
                "http://127.0.0.1:1".to_owned(),
                false,
                100,
                secs(60),
                secs(5),
            )
            .expect("validator"),
        );
        let gate = SchemaGate {
            key: false,
            value: true,
            mode: ValidationMode::Id,
        };
        let metrics = BrokerMetrics::new();

        let got = validate_batch_schemas(
            &prepared,
            gate,
            Some(&validator),
            "orders",
            RecordDecompressionPolicy::default(),
            &metrics,
        )
        .await;

        assert!(let Err(errors) = &got);
        check!(errors.len() == 1);
        // Index 1, the record that did not decode — not 0, which would mean the
        // batch-level arm above had fired instead and this test proved nothing.
        check!(errors[0].batch_index == 1);
        check!(
            errors[0]
                .batch_index_error_message
                .as_deref()
                .is_some_and(|m| m.contains("record did not decode")),
            "{:?}",
            errors[0].batch_index_error_message
        );
    }
}
