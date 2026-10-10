//! KIP-124 `request_percentage` helper.
//!
//! The request quota throttles on the server-side time a request takes, as a
//! percentage of one request-handler thread. The per-connection dispatch loop
//! calls this helper for most APIs. The `Produce` and `Fetch` handlers call it
//! inline, so that the broker can combine the request throttle with the
//! byte-rate data throttle, with `max`, into one `throttle_time_ms` and one
//! channel mute (KIP-219).

use krabka_metadata::MetadataImage;
use krabka_units::{Time, convert::TimeExt};

use super::{QuotaConsumption, buckets::QuotaBuckets, consume_configured_quota};

/// Consumes `elapsed_micros` of request-handler time from the
/// `request_percentage` bucket for `(principal, client_id)`.
///
/// It returns the throttle delay to apply before the broker sends the
/// response. The delay is a zero extent when no quota is configured, when the
/// rate is not positive, or when there was no overage. `maximum_delay`, one
/// `quota.window.size.seconds` in Kafka, caps the returned delay.
///
/// `request_percentage` is a percentage of one thread-second, so `100.0` gives
/// a budget of 1 000 000 µs per second. The bucket therefore meters in
/// microseconds, the same unit as `elapsed_micros`.
#[must_use]
pub fn consume_request_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: Option<&str>,
    elapsed_micros: u64,
    maximum_delay: Time,
) -> super::QuotaDelay {
    consume_configured_quota(
        QuotaConsumption {
            image,
            buckets,
            principal,
            client_id,
            quota_key: "request_percentage",
            amount: elapsed_micros,
            max_debt_wait: Some(maximum_delay),
        },
        request_percentage_token_rate,
        |overage_micros, _, rate_micros_per_sec| {
            Time::from_secs_f64(overage_micros / rate_micros_per_sec)
                // Kafka's `ClientRequestQuotaManager` bounds this throttle at
                // one quota window (`boundedThrottleTime`), the only client
                // quota it bounds.
                .min(maximum_delay)
        },
    )
}

/// The token rate of a `request_percentage` bucket: microseconds of handler
/// time per second, so `100.0` is 1 000 000.
pub(super) fn request_percentage_token_rate(rate_pct: f64) -> f64 {
    rate_pct * 10_000.0
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{millis, secs};

    use super::*;

    crate::quota::test_support::image_builder!(img_with_quota, "request_percentage");

    fn check_request_delay(image: &MetadataImage, elapsed: u64, cap: Time, expected: Time) {
        let buckets = QuotaBuckets::new();
        assert!(
            consume_request_quota(image, &buckets, "alice", Some(""), elapsed, cap) == expected
        );
    }

    #[test]
    fn zero_elapsed_returns_zero_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 100.0);
        check_request_delay(&img, 0, secs(1), <Time as TimeExt>::ZERO);
    }

    #[test]
    fn no_quota_returns_zero_delay() {
        let img = MetadataImage::new(uuid::Uuid::nil());
        check_request_delay(&img, 5000, secs(1), <Time as TimeExt>::ZERO);
    }

    #[test]
    fn under_budget_returns_zero_delay() {
        // rate=100% ⇒ 1_000_000 µs/sec budget; 5_000 µs is well under one
        // second of capacity → no overage.
        let img = img_with_quota(vec![("user", Some("alice"))], 100.0);
        check_request_delay(&img, 5000, secs(1), <Time as TimeExt>::ZERO);
    }

    #[test]
    fn overage_returns_capped_delay() {
        // rate=0.001% ⇒ 10 µs/sec budget; 1_000_000 µs of work is a colossal
        // overage → multi-day delay → capped at 1s.
        let img = img_with_quota(vec![("user", Some("alice"))], 0.001);
        check_request_delay(&img, 1_000_000, secs(1), secs(1));
    }

    #[test]
    fn overage_uses_configured_maximum_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 0.001);
        check_request_delay(&img, 1_000_000, millis(25), millis(25));
    }

    /// Kafka's quota window forgets what a client recorded more than
    /// `quota.window.num * quota.window.size.seconds` ago, so a client that
    /// stops overrunning is not throttled for the size of the overrun. The
    /// throttle is capped at `maximum_delay`, so debt past what `maximum_delay`
    /// repays only kept the cap in force after the overload had ended.
    #[test]
    fn an_overload_is_forgotten_once_the_maximum_delay_has_repaid_it() {
        let img = img_with_quota(vec![("user", Some("alice"))], 100.0);
        let buckets = QuotaBuckets::with_window(secs(1));
        let bound = millis(20);

        // 1000 seconds of handler time in one request: the throttle is capped.
        let during = consume_request_quota(&img, &buckets, "alice", Some(""), 1_000_000_000, bound);
        std::thread::sleep(std::time::Duration::from_millis(100));
        let after = consume_request_quota(&img, &buckets, "alice", Some(""), 1_000, bound);

        assert!(during == bound, "{during:?}");
        assert!(after == <Time as TimeExt>::ZERO, "{after:?}");
    }

    /// The overage of one request stays charged for the next (#1212), as it
    /// does for a byte rate: Kafka records the handler time before it checks
    /// the request quota, so a client that keeps overrunning it is throttled
    /// for everything it has overrun, not for its last request alone.
    #[test]
    fn overage_stays_charged_for_the_next_request() {
        // rate=100% gives a 1_000_000 us/sec budget and a one-second window a
        // one-second burst: 1_500_000 us is 500_000 over, and 500_000 more
        // leaves 1_000_000 of debt.
        let img = img_with_quota(vec![("user", Some("alice"))], 100.0);
        let buckets = QuotaBuckets::with_window(secs(1));

        let first = consume_request_quota(&img, &buckets, "alice", Some(""), 1_500_000, secs(10));
        let second = consume_request_quota(&img, &buckets, "alice", Some(""), 500_000, secs(10));

        assert!(first > millis(490) && first <= millis(500), "{first:?}");
        assert!(second > millis(990) && second <= secs(1), "{second:?}");
    }

    /// Kafka's `DefaultQuotaCallback` gives a request with a null client id the
    /// quota of the first level in `(user, <default>)`, `user`, ... that is
    /// configured, and only a user level throttles. With a `(user, <default>)`
    /// quota beside the user quota, a null client id is not throttled, and an
    /// empty one skips the pair level and pays the user quota (#1241).
    #[test]
    fn a_null_client_id_is_shadowed_by_a_user_default_client_quota() {
        let img = crate::quota::test_support::image_with_quotas(
            [
                vec![("user", Some("alice"))],
                vec![("user", Some("alice")), ("client-id", None)],
            ]
            .map(|entity| {
                crate::quota::test_support::quota_record(
                    crate::quota::test_support::QuotaRecordSetup {
                        entity,
                        key: "request_percentage",
                        value: crate::quota::test_support::QuotaValue(0.001),
                    },
                )
            })
            .to_vec(),
        );

        let [null_client, empty_client] = [None, Some("")].map(|client_id| {
            consume_request_quota(
                &img,
                &QuotaBuckets::new(),
                "alice",
                client_id,
                1_000_000,
                secs(1),
            )
        });

        assert!(null_client == <Time as TimeExt>::ZERO);
        assert!(empty_client == secs(1));
    }

    #[test]
    fn overage_returns_scaled_uncapped_delay() {
        // rate=100% gives a 1_000_000 us/sec budget. A one-second window
        // starts the bucket with one second of burst, so 1_500_000 us leaves
        // a 500_000 us overage.
        let img = img_with_quota(vec![("user", Some("alice"))], 100.0);
        let buckets = QuotaBuckets::with_window(secs(1));

        let delay = consume_request_quota(&img, &buckets, "alice", Some(""), 1_500_000, secs(1));

        assert!(delay == millis(500));
    }
}
