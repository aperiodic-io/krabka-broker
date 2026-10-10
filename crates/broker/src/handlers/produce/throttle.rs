//! The KIP-13 producer byte-rate and KIP-124 request-percentage accounting,
//! and the throttled response encode that closes a `Produce` request.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Encode,
    owned::produce_response::{ProduceResponse, TopicProduceResponse},
};

use super::node_endpoints::produce_node_endpoints;
use crate::{broker::Broker, error::BrokerError};

/// First `Produce` response version that carries the KIP-951 `CurrentLeader`
/// hint and its `NodeEndpoints` companion. Both are tagged fields at v10+.
const KIP_951_PRODUCE_VERSION: i16 = 10;

pub(super) fn finish_produce_response(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    request_quota_start: Option<std::time::Instant>,
    topic_results: Vec<TopicProduceResponse>,
    version: i16,
) -> Result<Bytes, BrokerError> {
    // Kafka's `KafkaApis.handleProduceRequest` charges `producer_byte_rate`
    // once per request, with `request.sizeInBytes`: the header and the body,
    // not only the record payloads.
    let data_delay = crate::quota::consume_producer_quota(
        image,
        &broker.quota_buckets,
        &context.principal.name,
        context.client_id,
        context.request_size,
    );
    // Kafka's `KafkaApis.handleProduceRequest` charges no request quota for
    // `acks = 0`, which `request_quota_start` holds as `None`. The byte-rate
    // quota above still applies.
    let request_delay = request_quota_start.map_or_else(crate::quota::QuotaDelay::zero, |start| {
        context.charge_request_quota(broker, image, start)
    });
    // KIP-219: the connection is muted for the larger of the two delays.
    // Resolving it through the metric records the throttle phase and the quota
    // that caused it, and hands back the delay the response reports and the
    // mute below honors.
    let delay = broker.metrics.record_applied_throttle(
        super::PRODUCE_API_KEY,
        &[
            (crate::metrics::QuotaType::Produce, data_delay).into(),
            (crate::metrics::QuotaType::Request, request_delay).into(),
        ],
    );
    // KIP-951: the `CurrentLeader` hints the partition rows carry are node ids,
    // and the producer can only act on one whose address it knows. Both halves
    // of the KIP encode at v10+, so a client that cannot read the hint is also
    // not offered the endpoints.
    let node_endpoints = if version >= KIP_951_PRODUCE_VERSION {
        produce_node_endpoints(
            image,
            context.connection_listener_name,
            &broker.config.inter_broker_listener_name,
            &topic_results,
        )
    } else {
        Vec::new()
    };
    let response = ProduceResponse {
        responses: topic_results,
        throttle_time_ms: crate::quota::throttle_time_ms(delay),
        node_endpoints,
        ..Default::default()
    };
    // KIP-219: report the window in the response and hand it to the connection
    // loop, which mutes the connection once these bytes are written. Sleeping
    // here would hold the response back past the client's request timeout.
    context.record_throttle(delay);
    let mut encoded = BytesMut::new();
    if (0..3).contains(&version) {
        let legacy: krabka_protocol::kafka_3_6_2::owned::produce_response::ProduceResponse =
            response.into();
        encoded.reserve(legacy.encoded_len(version));
        legacy.encode(&mut encoded, version)?;
    } else {
        encoded.reserve(response.encoded_len(version));
        response.encode(&mut encoded, version)?;
    }
    Ok(encoded.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::secs;

    #[test]
    fn consume_producer_quota_tuple_match_overage_throttles() {
        use krabka_units::{Time, convert::TimeExt};

        let img = crate::quota::test_support::image_with_quota(
            crate::quota::test_support::QuotaRecordSetup {
                entity: vec![("user", Some("alice")), ("client-id", Some("app-x"))],
                value: crate::quota::test_support::QuotaValue(1024.0),
                ..Default::default()
            },
        );
        // A one-second window, so 4096 bytes at 1024 B/s is 3072 over the
        // burst rather than inside the default 11-second one.
        let buckets = crate::quota::QuotaBuckets::with_window(secs(1));
        // Tuple match → 3072 bytes overage at 1024 B/s → throttle > 0.
        let delay_match =
            crate::quota::consume_producer_quota(&img, &buckets, "alice", Some("app-x"), 4096);
        assert!(
            delay_match.delay > <Time as TimeExt>::ZERO,
            "tuple quota match should throttle on overage; got {delay_match:?}"
        );
        // No tuple match for client_id="other"; no (user=alice)-only quota exists.
        let buckets2 = crate::quota::QuotaBuckets::with_window(secs(1));
        let delay_other =
            crate::quota::consume_producer_quota(&img, &buckets2, "alice", Some("other"), 4096);
        assert!(
            delay_other.delay == <Time as TimeExt>::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }
}
