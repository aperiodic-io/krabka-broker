//! Body write plan for a `FetchResponse`: the encoder that splits the response
//! body into the inline metadata runs and the records segments between them.
//!
//! The records bytes never enter the buffer this module fills. Every records
//! field becomes its own `FetchWriteOp::Records` op, so the caller can resolve
//! it to a shared `Bytes` view or to a file region instead of a copy.

use {
    crate::coordinator::unified::persistence::MAX_STRING_BYTES,
    bytes::{Bytes, BytesMut},
    krabka_protocol::{
        Encode, ProtocolError,
        owned::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
        primitives::{
            array::{put_array_len, put_nullable_array_len},
            fixed::{put_i16, put_i32, put_i64},
            string_bytes::{put_compact_string, put_string},
            uuid::put_uuid,
        },
        records::RecordsPayload,
        tagged_fields::{WriteTaggedFields, encode_to_bytes},
    },
};

/// One ordered segment of a `FetchResponse` body write plan.
#[derive(Debug, Clone)]
pub(super) enum FetchWriteOp {
    Inline(Bytes),
    Records(RecordsPayload),
}

impl FetchWriteOp {
    #[must_use]
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Inline(bytes) => bytes.len(),
            Self::Records(payload) => payload.payload_len(),
        }
    }
}

pub(super) fn fetch_response_write_plan(
    response: &FetchResponse,
    version: i16,
) -> Result<Vec<FetchWriteOp>, ProtocolError> {
    assert2::assert!(
        version >= 4,
        "write plan is only valid for the canonical Fetch codec (v4+)"
    );

    let flex = version >= 12;
    let mut ops = Vec::new();
    let mut buf = BytesMut::with_capacity(128);

    if version >= 1 {
        put_i32(&mut buf, response.throttle_time_ms);
    }
    if version >= 7 {
        put_i16(&mut buf, response.error_code);
        put_i32(&mut buf, response.session_id);
    }

    put_array_len(&mut buf, response.responses.len(), flex);
    for topic in &response.responses {
        encode_fetch_topic(&mut buf, &mut ops, topic, version, flex)?;
    }

    if flex {
        let mut tagged = WriteTaggedFields::new();
        if !krabka_protocol::codegen_helpers::is_default(&response.node_endpoints) {
            let node_endpoints = &response.node_endpoints;
            let payload = encode_to_bytes(
                krabka_protocol::primitives::array::array_len_prefix_len(
                    node_endpoints.len(),
                    flex,
                ) + node_endpoints
                    .iter()
                    .map(|endpoint| endpoint.encoded_len(version))
                    .sum::<usize>(),
                |bytes| {
                    put_array_len(bytes, node_endpoints.len(), flex);
                    for endpoint in node_endpoints {
                        endpoint.encode(bytes, version)?;
                    }
                    Ok(())
                },
            );
            tagged.add(0, payload);
        }
        tagged.write(&mut buf, &response.unknown_tagged_fields);
    }

    flush_fetch_inline(&mut buf, &mut ops);
    Ok(ops)
}

fn encode_fetch_topic(
    buf: &mut BytesMut,
    ops: &mut Vec<FetchWriteOp>,
    topic: &FetchableTopicResponse,
    version: i16,
    flex: bool,
) -> Result<(), ProtocolError> {
    if (0..=12).contains(&version) {
        if flex {
            put_compact_string(buf, &topic.topic);
        } else if topic.topic.len() > MAX_STRING_BYTES {
            // A v12 request carries its topic as a compact string of any length
            // and the fetch session keeps the name, so a later v7 to v11 request
            // on that session would make `put_string` panic. Kafka's reader
            // refuses such a name on the wire.
            return Err(ProtocolError::InvalidValue(
                "STRING longer than 32767 bytes",
            ));
        } else {
            put_string(buf, &topic.topic);
        }
    }
    if version >= 13 {
        put_uuid(buf, topic.topic_id);
    }

    put_array_len(buf, topic.partitions.len(), flex);
    for partition in &topic.partitions {
        encode_fetch_partition(buf, ops, partition, version, flex)?;
    }
    if flex {
        WriteTaggedFields::new().write(buf, &topic.unknown_tagged_fields);
    }
    Ok(())
}

fn encode_fetch_partition(
    buf: &mut BytesMut,
    ops: &mut Vec<FetchWriteOp>,
    partition: &PartitionData,
    version: i16,
    flex: bool,
) -> Result<(), ProtocolError> {
    put_i32(buf, partition.partition_index);
    put_i16(buf, partition.error_code);
    put_i64(buf, partition.high_watermark);
    if version >= 4 {
        put_i64(buf, partition.last_stable_offset);
    }
    if version >= 5 {
        put_i64(buf, partition.log_start_offset);
    }
    if version >= 4 {
        put_nullable_array_len(
            buf,
            partition.aborted_transactions.as_ref().map(Vec::len),
            flex,
        );
        if let Some(aborted_transactions) = &partition.aborted_transactions {
            for transaction in aborted_transactions {
                transaction.encode(buf, version)?;
            }
        }
    }
    if version >= 11 {
        put_i32(buf, partition.preferred_read_replica);
    }

    encode_records_prefix(buf, ops, partition.records.as_ref(), flex)?;

    if flex {
        let mut tagged = WriteTaggedFields::new();
        if !krabka_protocol::codegen_helpers::is_default(&partition.diverging_epoch) {
            tagged.add(
                0,
                encode_to_bytes(partition.diverging_epoch.encoded_len(version), |bytes| {
                    partition.diverging_epoch.encode(bytes, version)?;
                    Ok(())
                }),
            );
        }
        if !krabka_protocol::codegen_helpers::is_default(&partition.current_leader) {
            tagged.add(
                1,
                encode_to_bytes(partition.current_leader.encoded_len(version), |bytes| {
                    partition.current_leader.encode(bytes, version)?;
                    Ok(())
                }),
            );
        }
        if !krabka_protocol::codegen_helpers::is_default(&partition.snapshot_id) {
            tagged.add(
                2,
                encode_to_bytes(partition.snapshot_id.encoded_len(version), |bytes| {
                    partition.snapshot_id.encode(bytes, version)?;
                    Ok(())
                }),
            );
        }
        tagged.write(buf, &partition.unknown_tagged_fields);
    }

    Ok(())
}

/// The record set a partition actually puts on the wire, or `None` when the
/// partition has nothing to serve.
///
/// The broker spells "nothing to serve" two ways -- an absent record set and a
/// zero-byte one -- and neither may reach a client as a *null* record set.
/// Kafka's `FetchResponse` schema types the field as nullable and then never
/// uses that: its broker always writes `MemoryRecords.EMPTY`, so no null has
/// ever reached a client and clients are written to what Kafka does rather
/// than to what the schema permits. sarama decodes the field with
/// `getSubset(int(recordsSize))`, which rejects a negative length outright
/// (`invalid byteslice length`) and tears down the whole connection -- one
/// empty partition in a multi-partition fetch takes the fetches for its
/// siblings with it. librdkafka and the Java client happen to tolerate the
/// null, which is why only a third client found this.
///
/// Two encoders reach the Fetch wire and each has to honour that at its own
/// boundary, so this is the one place that decides which record sets are
/// nothing:
///
/// * [`encode_records_prefix`] below, the canonical v4+ write plan, which
///   answers `None` with a zero-length prefix *and no `Records` op at all*.
///   It cannot normalise `None` into an empty payload up front instead: an
///   empty payload would still become a `FetchWriteOp::Records`, and the
///   caller's resolver would turn that into a pointless zero-length inline
///   segment -- or, for a file-backed payload, a zero-length `sendfile`
///   region -- in every otherwise-empty partition of a fetch response.
/// * `handlers::fetch::serve_empty_rather_than_null_records`, which rewrites
///   the response *struct* before the codegen'd `kafka_3_6_2` encoder sees it,
///   because that encoder lives in a pinned sibling crate, is faithful to the
///   nullable schema, and takes no such hook.
///
/// A third path that encodes a `FetchResponse` owes the wire the same
/// guarantee, and `network::fetch_writer::tests` holds the cross-path
/// equivalence test that says so.
pub(crate) fn records_to_serve(records: Option<&RecordsPayload>) -> Option<&RecordsPayload> {
    records.filter(|payload| payload.payload_len() > 0)
}

fn encode_records_prefix(
    buf: &mut BytesMut,
    ops: &mut Vec<FetchWriteOp>,
    records: Option<&RecordsPayload>,
    flex: bool,
) -> Result<(), ProtocolError> {
    let Some(payload) = records_to_serve(records) else {
        if flex {
            krabka_protocol::primitives::varint::put_uvarint(buf, 1);
        } else {
            put_i32(buf, 0);
        }
        return Ok(());
    };

    let payload_len = payload.payload_len();
    if flex {
        let prefixed_len = u32::try_from(payload_len + 1)
            .map_err(|_| ProtocolError::InvalidValue("records too large for compact len"))?;
        krabka_protocol::primitives::varint::put_uvarint(buf, prefixed_len);
    } else {
        let records_len = i32::try_from(payload_len)
            .map_err(|_| ProtocolError::InvalidValue("records too large for i32 len"))?;
        put_i32(buf, records_len);
    }
    flush_fetch_inline(buf, ops);
    ops.push(FetchWriteOp::Records(payload.clone()));
    Ok(())
}

fn flush_fetch_inline(buf: &mut BytesMut, ops: &mut Vec<FetchWriteOp>) {
    if buf.is_empty() {
        return;
    }
    ops.push(FetchWriteOp::Inline(buf.split().freeze()));
}
