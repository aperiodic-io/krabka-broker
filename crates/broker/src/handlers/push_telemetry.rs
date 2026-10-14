//! `PushTelemetry` (`api_key=72`, KIP-714).
//!
//! The handler runs Kafka's push checks against the client's instance, which
//! it builds from the current subscriptions when this broker holds none. It
//! then decompresses and decodes the OTLP payload, and fans it out to the
//! Prometheus and OTLP sinks.

use {
    crate::{
        broker::Broker,
        handlers::{ApiVersion, CorrelationId, DispatchResult, encode_response},
    },
    krabka_compression::{CompressionError, CompressionType},
    krabka_protocol::owned::{
        push_telemetry_request::PushTelemetryRequest,
        push_telemetry_response::PushTelemetryResponse,
    },
    opentelemetry_proto::tonic::metrics::v1::MetricsData,
    prost::Message as _,
    uuid::Uuid,
};

mod prometheus;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {
    self::prometheus::flatten_for_prometheus,
    crate::{
        api_catalog::UnstableApiVersions,
        client_metrics::manager::{PushCheck, PushDecision},
        codes,
        handlers::context::TelemetryContext,
    },
};

/// Kafka's `CompressionType.forId`: the ids 0 to 4 and nothing else. The
/// record-batch attribute decode masks the id to three bits, which would take
/// 9 for gzip.
fn compression_for_id(id: i8) -> Option<CompressionType> {
    match id {
        0 => Some(CompressionType::None),
        1 => Some(CompressionType::Gzip),
        2 => Some(CompressionType::Snappy),
        3 => Some(CompressionType::Lz4),
        4 => Some(CompressionType::Zstd),
        _ => None,
    }
}

crate::handlers::raw_request_handler!(
    PushTelemetryRequest,
    req,
    broker,
    version,
    _correlation_id,
    req_bytes,
    ctx,
    TelemetryContext,
    {
        let instance = Uuid::from_bytes(req.client_instance_id.0);
        let codec = compression_for_id(req.compression_type);
        let manager = &broker.client_metrics.manager;

        let attrs = ctx.client_attributes(instance);
        let unstable = broker.config.features.unstable_api_versions;
        let image = broker.controller.current_image();
        let decision = manager.authorize_push(&image, &attrs, PushCheck {
            subscription_id: req.subscription_id,
            terminating: req.terminating,
            compression_supported: codec.is_some(),
            payload_len: req.metrics.len(),
            unstable,
        });

        // Kafka answers every error with `throttle_time_ms` 0; only the request
        // quota raises it.
        let error_code = match decision {
            PushDecision::Reject { error_code } => error_code,
            PushDecision::Accept if req.metrics.is_empty() => codes::NONE,
            PushDecision::Accept => {
                let ct = codec.expect("authorize_push accepts only a supported codec");
                // Kafka's exporter decompresses up to `telemetry.max.bytes`. A
                // larger payload throws `TelemetryTooLargeException`, which 4.3.1
                // answers with INVALID_RECORD like any other export failure, and
                // which makes the Java client stop pushing telemetry. Trunk
                // (KAFKA-21076) answers the retriable TELEMETRY_TOO_LARGE instead.
                match krabka_compression::decompress(ct, &req.metrics, manager.telemetry_max()) {
                    Ok(raw) => match MetricsData::decode(&raw[..]) {
                        Ok(md) => {
                            let instance_str = instance.to_string();
                            let points = flatten_for_prometheus(&md, &instance_str, ctx.client_id);
                            broker.client_metrics.prometheus.ingest(&points);
                            broker.client_metrics.otlp.forward(md, &instance_str);
                            codes::NONE
                        }
                        Err(e) => {
                            tracing::debug!(error = %e, "client-metrics OTLP decode failed");
                            codes::INVALID_RECORD
                        }
                    },
                    Err(CompressionError::TooLarge { limit })
                        if unstable == UnstableApiVersions::Enabled =>
                    {
                        tracing::debug!(
                            limit,
                            "client-metrics payload decompresses past the limit"
                        );
                        codes::TELEMETRY_TOO_LARGE
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "client-metrics decompress failed");
                        codes::INVALID_RECORD
                    }
                }
            }
        };

        let resp = PushTelemetryResponse {
            error_code,
            ..Default::default()
        };
        encode_response(&resp, version)
    }
);
