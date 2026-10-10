//! Live-broker tests for the `PushTelemetry` handler.
//!
//! These drive `handle` against a running broker so the instance lookup, the
//! push checks, the decompression step and the OTLP decode are all exercised
//! together, which is the only way to observe the response the handler
//! returns for a rejected, valid, malformed or empty push.

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{owned::push_telemetry_response, primitives::uuid::Uuid as ProtoUuid};
use opentelemetry_proto::tonic::metrics::v1::number_data_point;
use uuid::Uuid;

use super::*;
use crate::{
    broker::Broker,
    client_metrics::{
        config::INTERVAL_MS_DEFAULT,
        manager::{
            ClientAttributes, SubscriptionDecision, compute_subscription,
            subscription_id as compute_subscription_id,
        },
    },
    handlers::push_telemetry::test_support::{gauge_metric, metrics_data},
    test_support::{KafkaErrorCode, peer},
};

crate::test_support::codec_helpers!(
    PushTelemetryRequest,
    PushTelemetryResponse,
    version = push_telemetry_response::MAX_VERSION
);

const GZIP: i8 = 1;
const NONE: i8 = 0;

#[derive(Clone, Copy, Default)]
struct SubscriptionId(i32);

#[derive(Clone, Copy)]
struct TelemetryCompressionCode(i8);

#[derive(Clone, Copy, Default)]
enum TelemetryCompression {
    #[default]
    Gzip,
    Uncompressed,
    Malformed(TelemetryCompressionCode),
}

impl TelemetryCompression {
    fn wire_code(self) -> i8 {
        match self {
            Self::Gzip => GZIP,
            Self::Uncompressed => NONE,
            Self::Malformed(code) => code.0,
        }
    }
}

#[derive(krabka_macros::FieldDefaults)]
struct PushSetup {
    #[default(Uuid::from_u128(0x1234))]
    instance: Uuid,
    subscription: SubscriptionId,
    compression: TelemetryCompression,
    metrics: Bytes,
}

fn otlp() -> Vec<u8> {
    metrics_data(vec![gauge_metric(
        "cpu.utilization",
        number_data_point::Value::AsDouble(0.75),
    )])
    .encode_to_vec()
}

fn gzip(raw: &[u8]) -> Bytes {
    krabka_compression::compress(CompressionType::Gzip, raw).expect("compress telemetry payload")
}

fn response(error_code: KafkaErrorCode) -> PushTelemetryResponse {
    PushTelemetryResponse {
        error_code: error_code.0,
        ..Default::default()
    }
}

struct Client<'a> {
    broker: &'a Broker,
    ctx: TelemetryContext<'a>,
}

impl Client<'_> {
    fn attrs(&self, instance: Uuid) -> ClientAttributes {
        ClientAttributes {
            connection_id: self.ctx.connection_id.to_string(),
            client_instance_id: instance,
            client_id: self.ctx.client_id.to_string(),
            software_name: self.ctx.software_name.to_string(),
            software_version: self.ctx.software_version.to_string(),
            source_address: self.ctx.source_address(),
            source_port: self.ctx.peer.port(),
        }
    }

    fn get(&self, instance: Uuid) -> SubscriptionId {
        let image = self.broker.controller.current_image();
        let SubscriptionDecision::Assign(assignment) = self
            .broker
            .client_metrics
            .manager
            .get_subscription(&image, &self.attrs(instance))
        else {
            panic!("fresh client must receive a subscription");
        };
        SubscriptionId(assignment.subscription_id)
    }

    /// The subscription id that the current subscriptions give `instance`,
    /// on any broker.
    fn current_subscription_id(&self, instance: Uuid) -> SubscriptionId {
        let image = self.broker.controller.current_image();
        SubscriptionId(compute_subscription_id(
            &compute_subscription(&image, &self.attrs(instance), INTERVAL_MS_DEFAULT),
            instance,
        ))
    }

    fn push(&self, setup: PushSetup) -> PushTelemetryResponse {
        let PushSetup {
            instance,
            subscription,
            compression,
            metrics,
        } = setup;
        let req = PushTelemetryRequest {
            client_instance_id: ProtoUuid(*instance.as_bytes()),
            subscription_id: subscription.0,
            terminating: false,
            compression_type: compression.wire_code(),
            metrics,
            ..Default::default()
        };
        decode_response(
            &handle(
                self.broker,
                push_telemetry_response::MAX_VERSION,
                7,
                &encode_request(&req),
                &self.ctx,
            )
            .expect("handle"),
        )
    }
}

/// One push after a `GetTelemetrySubscriptions`, with `telemetry.max.bytes`
/// at 1024. Kafka's `CompressionType.forId` takes 0 to 4 only. A payload that
/// decompresses past `telemetry.max.bytes` is `INVALID_RECORD` on 4.3.1, which
/// stops the Java client's telemetry, and the retriable `TELEMETRY_TOO_LARGE`
/// on trunk (KAFKA-21076), which `unstable.api.versions.enable` selects
/// (#693, #1246).
#[tokio::test]
async fn push_after_get_follows_kafkas_payload_checks() {
    for unstable in [UnstableApiVersions::Disabled, UnstableApiVersions::Enabled] {
        push_after_get_checks(unstable).await;
    }
}

async fn push_after_get_checks(unstable: UnstableApiVersions) {
    let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.client_metrics_telemetry_max = krabka_units::kibibytes(1);
        cfg.features.unstable_api_versions = unstable;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let peer = peer();
    let client = Client {
        broker: &broker,
        ctx: TelemetryContext {
            connection_id: "connection-a",
            client_id: "client-a",
            peer: &peer,
            software_name: "test-client",
            software_version: "1.0.0",
        },
    };
    let bomb = gzip(&vec![0; 64 * 1024]);
    assert!(bomb.len() <= 1024);
    let decompressed_too_large = match unstable {
        UnstableApiVersions::Disabled => codes::INVALID_RECORD,
        UnstableApiVersions::Enabled => codes::TELEMETRY_TOO_LARGE,
    };
    let rows = [
        (
            "gzip OTLP",
            TelemetryCompression::Gzip,
            gzip(&otlp()),
            KafkaErrorCode(codes::NONE),
        ),
        (
            "uncompressed OTLP",
            TelemetryCompression::Uncompressed,
            Bytes::from(otlp()),
            KafkaErrorCode(codes::NONE),
        ),
        (
            "empty payload",
            TelemetryCompression::Gzip,
            Bytes::new(),
            KafkaErrorCode(codes::NONE),
        ),
        (
            "id 9 masks to gzip",
            TelemetryCompression::Malformed(TelemetryCompressionCode(9)),
            gzip(&otlp()),
            KafkaErrorCode(codes::UNSUPPORTED_COMPRESSION_TYPE),
        ),
        (
            "id 12 masks to zstd",
            TelemetryCompression::Malformed(TelemetryCompressionCode(12)),
            gzip(&otlp()),
            KafkaErrorCode(codes::UNSUPPORTED_COMPRESSION_TYPE),
        ),
        (
            "negative id",
            TelemetryCompression::Malformed(TelemetryCompressionCode(-1)),
            gzip(&otlp()),
            KafkaErrorCode(codes::UNSUPPORTED_COMPRESSION_TYPE),
        ),
        (
            "compressed payload over telemetry.max.bytes",
            TelemetryCompression::Uncompressed,
            Bytes::from(vec![0; 1025]),
            KafkaErrorCode(codes::TELEMETRY_TOO_LARGE),
        ),
        (
            "decompressed payload over telemetry.max.bytes",
            TelemetryCompression::Gzip,
            bomb,
            KafkaErrorCode(decompressed_too_large),
        ),
        (
            "payload that is not OTLP",
            TelemetryCompression::Gzip,
            gzip(b"not-otlp"),
            KafkaErrorCode(codes::INVALID_RECORD),
        ),
        (
            "payload that is not gzip",
            TelemetryCompression::Gzip,
            Bytes::from_static(b"not-gzip"),
            KafkaErrorCode(codes::INVALID_RECORD),
        ),
    ];
    for (row, (name, compression, payload, expected)) in (100u128..).zip(rows) {
        let instance = Uuid::from_u128(row);
        let subscription_id = client.get(instance);
        assert!(
            client.push(PushSetup {
                instance,
                subscription: subscription_id,
                compression,
                metrics: payload
            }) == response(expected),
            "row {name} with {unstable:?}"
        );
    }
    broker_handle.shutdown().await;
}

/// A push for an instance this broker does not hold: Kafka builds the
/// instance from the current subscriptions and checks the push, and answers
/// `INVALID_REQUEST` only for `Uuid.RESERVED` (#673).
#[tokio::test]
async fn push_without_a_get_builds_the_instance() {
    broker_fixture!(
        (broker_handle, _dir, broker),
        crate::test_support::start_broker_with(|_cfg| {})
    );
    let peer = peer();
    let client = Client {
        broker: &broker,
        ctx: TelemetryContext {
            connection_id: "connection-a",
            client_id: "client-a",
            peer: &peer,
            software_name: "test-client",
            software_version: "1.0.0",
        },
    };
    let known = Uuid::from_u128(0x1234);
    let other = Uuid::from_u128(0x5678);
    let rows = [
        (
            "the current subscription id",
            known,
            client.current_subscription_id(known),
            KafkaErrorCode(codes::NONE),
        ),
        (
            "another subscription id",
            other,
            SubscriptionId(client.current_subscription_id(other).0 ^ 1),
            KafkaErrorCode(codes::UNKNOWN_SUBSCRIPTION_ID),
        ),
        (
            "the zero id",
            Uuid::nil(),
            SubscriptionId(0),
            KafkaErrorCode(codes::INVALID_REQUEST),
        ),
        (
            "Uuid.ONE_UUID",
            Uuid::from_u128(1),
            SubscriptionId(0),
            KafkaErrorCode(codes::INVALID_REQUEST),
        ),
    ];
    for (name, instance, subscription_id, expected) in rows {
        assert!(
            client.push(PushSetup {
                instance,
                subscription: subscription_id,
                metrics: gzip(&otlp()),
                ..Default::default()
            }) == response(expected),
            "row {name}"
        );
    }
    broker_handle.shutdown().await;
}
