//! Unit tests for the framed plan the module root builds: the golden
//! comparison against the copying `encode_response` path, the frame-length
//! accounting of the leading prefix.

use krabka_protocol::Encode;

use super::{
    test_support::{inline_bytes, sample_response},
    *,
};
use crate::handlers::fetch::encode_fetch_response;

fn empty_plan_body(
    response: &krabka_protocol::owned::fetch_response::FetchResponse,
    version: i16,
    context: &str,
) -> BytesMut {
    let mut body = BytesMut::new();
    for op in fetch_response_write_plan(response, version).unwrap() {
        match op {
            FetchWriteOp::Inline(bytes) => body.extend_from_slice(&bytes),
            FetchWriteOp::Records(_) => panic!("{context}"),
        }
    }
    body
}

/// The broker-level golden test: the full framed bytes that
/// `build_fetch_plan` produces, which are the length prefix, the
/// correlation header, and the body, must equal the bytes from the
/// `encode_response(encode_fetch_response(..))` path, for both
/// non-flexible and flexible versions.
#[test]
fn build_fetch_plan_matches_legacy_encode_path() {
    for version in [4i16, 7, 11, 12, 13, 16, 18] {
        let resp = sample_response(version);
        let correlation_id = 0x1234_5678;
        let body_flexible = version >= 12;

        // New path: assemble the plan bytes.
        let ops = build_fetch_plan(
            &resp,
            version,
            correlation_id,
            body_flexible,
            resolve_records_inline,
        )
        .unwrap();
        let mut new_bytes = BytesMut::new();
        for op in &ops {
            new_bytes.extend_from_slice(inline_bytes(op));
        }

        // Old path: encode the body, then the response header, then frame.
        let mut body = BytesMut::new();
        resp.encode(&mut body, version).unwrap();
        let header_v1 = response_header_v1(ApiKey::Fetch as i16, body_flexible);
        let header_len = response_header_len(ApiKey::Fetch as i16, body_flexible);
        let frame_body_len = header_len + body.len();
        let mut old_bytes = BytesMut::new();
        old_bytes.put_u32(u32::try_from(frame_body_len).unwrap());
        old_bytes.put_i32(correlation_id);
        if header_v1 {
            old_bytes.put_u8(0);
        }
        old_bytes.extend_from_slice(&body);

        assert2::assert!(
            (&new_bytes[..]) == (&old_bytes[..]),
            "plan != legacy encode at version {version}"
        );
    }
}

#[test]
fn plan_total_len_matches_frame_prefix() {
    // The 4-byte frame prefix the writer emits must equal the actual bytes
    // following it (header + body). Off-by-one here corrupts every frame.
    for version in [4i16, 12, 18] {
        let resp = sample_response(version);
        let ops =
            build_fetch_plan(&resp, version, 1, version >= 12, resolve_records_inline).unwrap();
        // First op is [u32 len][header]; the declared length must equal the
        // sum of the remaining bytes of op0 (the header) + all later ops.
        let head = inline_bytes(&ops[0]);
        let declared = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
        let header_after_len = head.len() - 4;
        let tail_len: usize = ops[1..].iter().map(WriteOp::body_len).sum();
        assert2::assert!((declared) == (header_after_len + tail_len));
    }
}

/// The whole body a partition with nothing to serve puts on the wire, at the
/// non-flexible and the flexible shape.
///
/// The records field is `0` and `0x01` -- an empty record set -- and never
/// `-1` or `0x00`, the null forms. Kafka types the field as nullable and then
/// never uses that: its broker writes `MemoryRecords.EMPTY`. A client is
/// entitled to be written to what Kafka does rather than to what the schema
/// permits, and sarama is: it hands the length straight to
/// `getSubset(int(recordsSize))`, which rejects a negative length as
/// `invalid byteslice length` and drops the connection, taking the fetches for
/// every other partition in the same response with it.
#[test]
fn a_partition_with_nothing_to_serve_encodes_an_empty_record_set() {
    let partition_header: &[u8] = &[
        0x00, 0x00, 0x00, 0x00, // partition_index
        0x00, 0x00, // error_code
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // high_watermark
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, // last_stable_offset
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // log_start_offset
    ];
    let v7 = [
        &[
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms
            0x00, 0x00, // error_code
            0x00, 0x00, 0x00, 0x07, // session_id
            0x00, 0x00, 0x00, 0x01, // responses len
            0x00, 0x01, b't', // topic
            0x00, 0x00, 0x00, 0x01, // partitions len
        ],
        partition_header,
        &[
            0xff, 0xff, 0xff, 0xff, // aborted_transactions: null
            0x00, 0x00, 0x00, 0x00, // records: empty, not null
        ],
    ]
    .concat();
    let v12 = [
        &[
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms
            0x00, 0x00, // error_code
            0x00, 0x00, 0x00, 0x07, // session_id
            0x02, // responses len
            0x02, b't', // topic
            0x02, // partitions len
        ],
        partition_header,
        &[
            0x00, // aborted_transactions: null
            0xff, 0xff, 0xff, 0xff, // preferred_read_replica
            0x01, // records: empty, not null
            0x00, // partition tagged fields
            0x00, // topic tagged fields
            0x00, // response tagged fields
        ],
    ]
    .concat();

    for (version, expected) in [(7i16, &v7[..]), (12i16, &v12[..])] {
        let response = test_support::one_partition_response(version, None);
        let body = empty_plan_body(
            &response,
            version,
            "a partition with nothing to serve emits no records op",
        );
        assert2::assert!((&body[..]) == (expected), "wrong body at version {version}");
    }
}

/// A partition holding a zero-byte record set and a partition holding none are
/// the same partition to a client, so they must be the same bytes. Only the
/// broker's own two spellings of "nothing to serve" differ, and the wire is
/// not where that difference belongs.
#[test]
fn an_empty_record_set_and_an_absent_one_encode_alike() {
    for version in [4i16, 7, 11, 12, 13, 16, 18] {
        let mut bodies = Vec::new();
        for records in [None, Some(RecordsPayload::Raw(Bytes::new()))] {
            let response = test_support::one_partition_response(version, records);
            let body = empty_plan_body(
                &response,
                version,
                "an empty record set emits no records op",
            );
            bodies.push(body);
        }
        assert2::assert!(
            (bodies[0]) == (bodies[1]),
            "they differ at version {version}"
        );
    }
}

/// The seam guard. Two encoders reach the Fetch wire -- the v4+ write plan
/// here, and `handlers::fetch::encode_fetch_response`, which serves the
/// legacy v0-3 codec through a codegen'd encoder in a pinned sibling crate
/// that cannot be hooked. Neither may put a null record set on the wire, and
/// the only way to keep two encoders honest is to make them answer the same
/// question with the same bytes.
///
/// So: for every shape of "nothing to serve" the broker produces, and for a
/// partition that does serve a batch, the body the plan emits must equal the
/// body the struct encoder emits. A third encoder added later has this test
/// to answer to as soon as it is compared here; a change to either of the two
/// that exist fails it immediately.
#[test]
fn both_fetch_encoders_agree_on_the_body_bytes() {
    for version in [4i16, 7, 11, 12, 13, 16, 18] {
        for records in [
            None,
            Some(RecordsPayload::Raw(Bytes::new())),
            Some(RecordsPayload::Raw(test_support::raw_batch(0))),
        ] {
            let response = test_support::one_partition_response(version, records.clone());

            let mut plan_body = BytesMut::new();
            for op in fetch_response_write_plan(&response, version).unwrap() {
                match op {
                    FetchWriteOp::Inline(b) => plan_body.extend_from_slice(&b),
                    FetchWriteOp::Records(payload) => {
                        let mut buf = BytesMut::new();
                        payload.encode_to(&mut buf).unwrap();
                        plan_body.extend_from_slice(&buf);
                    }
                }
            }

            let struct_body = encode_fetch_response(response, version).unwrap();

            assert2::assert!(
                (&plan_body[..]) == (&struct_body[..]),
                "the two fetch encoders disagree at version {version} for {records:?}"
            );
        }
    }
}

/// The reason the write plan does not simply normalise `None` into an empty
/// payload and let the ordinary records op carry it: an empty payload would
/// still reach the caller's resolver, which would answer it with a
/// zero-length segment -- a wasted `iovec` entry on the vectored path and a
/// zero-length `sendfile` region on the zero-copy one -- once per otherwise
/// empty partition. A fetch response full of idle partitions is the common
/// case, so the plan must carry no empty ops at all.
#[test]
fn a_plan_for_idle_partitions_carries_no_empty_ops() {
    for version in [4i16, 12, 18] {
        for records in [None, Some(RecordsPayload::Raw(Bytes::new()))] {
            let response = test_support::one_partition_response(version, records);
            let ops =
                build_fetch_plan(&response, version, 1, version >= 12, resolve_records_inline)
                    .unwrap();
            assert2::assert!(
                ops.iter().all(|op| op.body_len() > 0),
                "the plan carries an empty op at version {version}"
            );
        }
    }
}

/// A topic name that only a compact string can carry is refused below v12 as a
/// protocol error, where `put_string` would panic the connection task.
#[test]
fn a_topic_name_over_the_string_limit_is_refused_by_the_int16_encodings() {
    use crate::coordinator::unified::persistence::MAX_STRING_BYTES;

    for (version, length, expected_ok) in [
        (7i16, MAX_STRING_BYTES, true),
        (7, MAX_STRING_BYTES + 1, false),
        (11, MAX_STRING_BYTES + 1, false),
        // Flexible versions carry a varint length and write any size.
        (12, MAX_STRING_BYTES + 1, true),
    ] {
        let mut response = test_support::one_partition_response(version, None);
        response.responses[0].topic = "t".repeat(length);
        assert2::assert!(
            fetch_response_write_plan(&response, version).is_ok() == expected_ok,
            "version {version}, {length} bytes"
        );
    }
}
