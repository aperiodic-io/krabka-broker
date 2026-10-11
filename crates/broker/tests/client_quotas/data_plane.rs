//! Wire drivers for the requests the client-quota tests throttle: `Produce`
//! and a consumer `Fetch`, each on its own authenticated SASL/PLAIN
//! connection, plus an `AddOffsetsToTxn` driver that reuses one such
//! connection.

use std::net::SocketAddr;

use bytes::BytesMut;
use krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        add_offsets_to_txn_request::AddOffsetsToTxnRequest,
        add_offsets_to_txn_response::AddOffsetsToTxnResponse, fetch_response::FetchResponse,
        produce_response::ProduceResponse,
    },
};
use tokio::net::TcpStream;

use crate::{
    CLIENT_ID,
    kafka_wire::{self, ExchangeSetup as WireExchangeSetup, PlainAuthenticationSetup},
    support::{
        topics::consumer_fetch_request,
        wire::{CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding},
    },
};

/// The `AddOffsetsToTxn` version the driver below speaks: the newest the
/// broker advertises, and flexible (v3+), so the response header carries the
/// empty tagged-fields byte the leading-throttle patch has to skip over.
const ADD_OFFSETS_TO_TXN_VERSION: i16 = 4;

/// Drives one `AddOffsetsToTxn` request for a transactional id this broker
/// does not coordinate, on an already-authenticated `stream`, and returns the
/// whole decoded response.
///
/// `AddOffsetsToTxn` is picked because it is one of the few APIs that both
/// reaches `apply_request_quota` -- its dispatch entry carries
/// `RequestQuotaPolicy::ApplyFallbackAccounting` -- and puts `ThrottleTimeMs`
/// first in its response, so the request-quota delay is reported by patching
/// that leading int32 into the already-encoded body rather than by a handler
/// filling the field in. The request is expected to fail: the point is the
/// framing around the error, not the error.
///
/// The caller supplies `corr_id` so several requests can share one connection;
/// reusing the connection keeps the SASL handshake out of the quota bucket.
pub async fn drive_add_offsets_to_txn(
    stream: &mut TcpStream,
    corr_id: i32,
) -> AddOffsetsToTxnResponse {
    let req = AddOffsetsToTxnRequest {
        transactional_id: "krabka-quota-test-no-such-txn".to_string(),
        group_id: "krabka-quota-test-group".to_string(),
        ..Default::default()
    };

    let mut body = BytesMut::new();
    req.encode(&mut body, ADD_OFFSETS_TO_TXN_VERSION)
        .expect("encode AddOffsetsToTxn");
    let resp_bytes = kafka_wire::round_trip(
        stream,
        WireExchangeSetup {
            api_key: WireApiKey(25),
            version: WireApiVersion(ADD_OFFSETS_TO_TXN_VERSION),
            correlation: WireCorrelationId(corr_id),
            client_id: CLIENT_ID,
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &body,
    )
    .await
    .expect("AddOffsetsToTxn round-trip");
    let mut cur: &[u8] = &resp_bytes;
    AddOffsetsToTxnResponse::decode(&mut cur, ADD_OFFSETS_TO_TXN_VERSION)
        .expect("decode AddOffsetsToTxnResponse")
}

/// Drives a `Produce` request over an already-authenticated SASL stream.
///
/// Returns the full `ProduceResponse`.
pub async fn drive_produce_sasl(
    addr: SocketAddr,
    records: kafka_wire::ZeroFilledRecordsSetup,
    authentication: &PlainAuthenticationSetup<'_>,
) -> ProduceResponse {
    kafka_wire::produce_sasl_with_ids(
        addr,
        kafka_wire::SaslProduceSetup {
            authentication: authentication.clone(),
            produce_client_id: CLIENT_ID,
            records,
        },
    )
    .await
}

/// Drives a consumer `Fetch` request with `replica_id=-1` over SASL.
///
/// Returns the full `FetchResponse`.
pub async fn drive_fetch_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &[u8],
    topic: &str,
) -> FetchResponse {
    let version: i16 = 12; // flexible, supports throttle_time_ms

    let req = consumer_fetch_request(topic);

    let mut stream = kafka_wire::sasl_plain_authenticate(
        addr,
        &PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            user,
            password: (pass).to_vec(),
        },
    )
    .await
    .expect("SASL authenticate for Fetch");
    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("encode Fetch");
    let resp_bytes = kafka_wire::round_trip(
        &mut stream,
        WireExchangeSetup {
            api_key: WireApiKey(1),
            version: WireApiVersion(version),
            client_id: CLIENT_ID,
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &body,
    )
    .await
    .expect("Fetch round-trip");
    let mut cur: &[u8] = &resp_bytes;
    FetchResponse::decode(&mut cur, version).expect("decode FetchResponse")
}
