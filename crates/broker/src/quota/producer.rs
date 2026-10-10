//! KIP-13 `producer_byte_rate` enforcement.

use krabka_metadata::MetadataImage;
use krabka_units::{Time, convert::TimeExt as _};

use super::{QuotaConsumption, buckets::QuotaBuckets, consume_configured_quota};

/// Charges `bytes` to the `producer_byte_rate` bucket of the quota entity
/// that `(principal, client_id)` resolves to.
///
/// Kafka keeps one bandwidth sensor per quota entity
/// (`ClientQuotaManager.getOrCreateQuotaSensors`), so every topic a producer
/// writes draws on the same bucket. The sensor's quota is a double, so the
/// bucket runs at the configured rate, fractional part included.
#[must_use]
pub fn consume_producer_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: Option<&str>,
    bytes: u64,
) -> super::QuotaDelay {
    consume_configured_quota(
        QuotaConsumption {
            image,
            buckets,
            principal,
            client_id,
            quota_key: "producer_byte_rate",
            amount: bytes,
            max_debt_wait: None,
        },
        |rate| rate,
        // Kafka's `ClientQuotaManager.throttleTime` does not bound a
        // byte-rate throttle.
        |overage, rate, _| Time::from_secs_f64(overage / rate),
    )
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{Time, convert::TimeExt, millis, secs};

    use super::consume_producer_quota;
    use crate::quota::QuotaBuckets;

    crate::quota::test_support::image_builder!(img_with_quota, "producer_byte_rate");

    /// One bucket per quota entity (#748): two charges share it, so the
    /// second one, which fits the rate on its own, is throttled by the first.
    #[test]
    fn producer_quota_keeps_one_bucket_per_entity() {
        let img = img_with_quota(
            vec![("user", Some("alice")), ("client-id", Some("app"))],
            128.0,
        );
        // A one-second window keeps the burst at the configured rate, so
        // these amounts are about the overage and not about the window.
        let buckets = QuotaBuckets::with_window(secs(1));

        let first = consume_producer_quota(&img, &buckets, "alice", Some("app"), 1024);
        let second = consume_producer_quota(&img, &buckets, "alice", Some("app"), 64);

        check!(first > <Time as TimeExt>::ZERO);
        check!(second > <Time as TimeExt>::ZERO);
        check!(buckets.len() == 1);
    }

    /// Kafka records the whole request on the entity's `Rate` before it checks
    /// the quota (`ClientQuotaManager.recordAndGetThrottleTimeMs`), so the
    /// overage of a throttled request stays charged: the next request on the
    /// entity, from any connection, is throttled for the debt of both (#1212).
    /// Before, the bucket kept only what it held, the refill of the mute paid
    /// the overage back, and a producer sent about twice its quota.
    #[test]
    fn the_overage_of_a_throttled_request_stays_charged() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1_000.0);
        let buckets = QuotaBuckets::with_window(secs(1));

        // 500 bytes over the one-second burst, then 1000 more on the same
        // entity from another client: the debt is 1500 bytes.
        let first = consume_producer_quota(&img, &buckets, "alice", Some("app"), 1_500);
        let second = consume_producer_quota(&img, &buckets, "alice", Some("other-app"), 1_000);

        check!(first > millis(490) && first <= millis(500), "{first:?}");
        check!(
            second > millis(1_490) && second <= millis(1_500),
            "{second:?}"
        );
    }

    #[test]
    fn producer_quota_uses_client_id_entity_precedence() {
        let img = img_with_quota(
            vec![("user", Some("alice")), ("client-id", Some("app"))],
            128.0,
        );
        let buckets = QuotaBuckets::new();

        let matching = consume_producer_quota(&img, &buckets, "alice", Some("app"), 4096);
        let other_client = consume_producer_quota(&img, &buckets, "alice", Some("other"), 4096);

        assert!(matching > <Time as TimeExt>::ZERO);
        assert!(other_client == <Time as TimeExt>::ZERO);
    }

    #[test]
    fn producer_quota_delay_reflects_exact_overage_at_configured_rate() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1_000.0);
        let buckets = QuotaBuckets::with_window(secs(1));

        let delay = consume_producer_quota(&img, &buckets, "alice", Some("app"), 1_250);

        assert!(delay == millis(250));
    }

    /// Kafka leaves a byte-rate throttle unbounded (#709): a producer far
    /// over its rate is told to back off for the whole debt, past the ten
    /// seconds the broker once capped it at.
    #[test]
    fn producer_quota_delay_is_not_capped() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1024.0);
        // The default 11-second window gives an 11 KiB burst; 20 KiB past it
        // is 20 seconds of debt at 1 KiB/s.
        let buckets = QuotaBuckets::new();

        let delay = consume_producer_quota(&img, &buckets, "alice", Some("app"), 1024 * (11 + 20));

        assert!(delay > secs(19) && delay <= secs(20), "{delay:?}");
    }

    /// Kafka enforces `producer_byte_rate` as a double, so a fractional rate
    /// throttles at that rate: neither unbounded nor rounded to a whole byte
    /// per second.
    #[test]
    fn a_fractional_producer_byte_rate_throttles() {
        // `(producer_byte_rate, request bytes, expected throttle)`. The
        // one-second window gives the bucket a burst of exactly its rate, and
        // the throttle is the shortfall over the rate.
        crate::quota::test_support::check_fractional_bandwidth(
            "producer_byte_rate",
            |image, buckets, bytes| {
                consume_producer_quota(image, buckets, "alice", Some("app"), bytes).delay
            },
        );
    }
}
