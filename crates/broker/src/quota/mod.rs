//! KIP-13 + KIP-124 + KIP-257 client quotas.

use {
    krabka_metadata::MetadataImage,
    krabka_units::{
        ByteRate, Time,
        convert::{ByteRateExt as _, TimeExt},
    },
    num_traits::cast::{NumCast, ToPrimitive as _},
};

mod buckets;
mod controller_mutation;
mod expiry;
mod ip_names;
mod lookup;
mod producer;
mod request;
mod throttle_slot;

pub use {
    buckets::QuotaBuckets,
    controller_mutation::consume_controller_mutation_quota,
    ip_names::{IpNames, parse_ip_literal},
    lookup::{lookup_ip_quota, lookup_ip_quota_with_key, lookup_quota, lookup_quota_with_key},
    producer::consume_producer_quota,
    request::consume_request_quota,
};
pub(crate) use {
    controller_mutation::{ControllerMutationQuota, QuotaRequest},
    lookup::entity_field,
    throttle_slot::ThrottleSlot,
};

mod refresh;
use crate::throttle::MICROS_PER_TOKEN;
pub(crate) use expiry::run as run_expiry;
pub use refresh::run;

/// Result of consuming a client quota, carrying the delay and the resolved
/// entity identity (`user` and `client_id`) that the match was charged to (#418).
#[derive(Debug, Clone, PartialEq, derive_more::Deref)]
pub struct QuotaDelay {
    #[deref]
    pub delay: Time,
    pub user: Option<String>,
    pub client_id: Option<String>,
}

impl QuotaDelay {
    /// No throttle, charged to nobody.
    #[must_use]
    pub fn zero() -> Self { Self::new(<Time as TimeExt>::ZERO, None, None) }

    /// A throttle of `delay`, charged to the principal and client id whose
    /// quota produced it (KIP-599 labels the applied throttle by entity).
    #[must_use]
    pub fn new(delay: Time, user: Option<String>, client_id: Option<String>) -> Self {
        Self {
            delay,
            user,
            client_id,
        }
    }
}

impl PartialEq<Time> for QuotaDelay {
    fn eq(&self, other: &Time) -> bool { self.delay == *other }
}

impl PartialEq<QuotaDelay> for Time {
    fn eq(&self, other: &QuotaDelay) -> bool { *self == other.delay }
}

impl PartialOrd<Time> for QuotaDelay {
    fn partial_cmp(&self, other: &Time) -> Option<std::cmp::Ordering> {
        self.delay.partial_cmp(other)
    }
}

impl PartialOrd<QuotaDelay> for Time {
    fn partial_cmp(&self, other: &QuotaDelay) -> Option<std::cmp::Ordering> {
        self.partial_cmp(&other.delay)
    }
}

#[derive(Clone, Copy)]
struct QuotaConsumption<'a> {
    image: &'a MetadataImage,
    buckets: &'a QuotaBuckets,
    principal: &'a str,
    client_id: Option<&'a str>,
    quota_key: &'a str,
    amount: u64,
    /// The most debt to keep, as the wait it takes the refill to repay it, for
    /// a quota whose throttle Kafka bounds, and `None` for one it does not.
    max_debt_wait: Option<Time>,
}

/// Charges `request.amount` tokens to the bucket of the quota entity the
/// request resolves to, and returns the throttle for the debt the charge
/// leaves.
///
/// Kafka records the whole value on the entity's `Rate` sensor and checks the
/// quota after (`ClientQuotaManager.recordAndGetThrottleTimeMs` calls
/// `quotaSensor().record(value, timeMs, true)`), so a request over the quota
/// is charged in full and the next request on the entity, on any connection,
/// still pays for it. The bucket does the same: [`TokenBucket::record`] takes
/// the charge and reports the debt (#1212).
///
/// `token_rate` turns the configured rate into the bucket's tokens per second,
/// fractional or not. `delay_for_overage` gets the tokens of debt, part token
/// included, the configured rate, and the token rate.
///
/// [`TokenBucket::record`]: crate::throttle::TokenBucket::record
fn consume_configured_quota(
    request: QuotaConsumption<'_>,
    token_rate: impl FnOnce(f64) -> f64,
    delay_for_overage: impl FnOnce(f64, f64, f64) -> Time,
) -> QuotaDelay {
    if request.amount == 0 {
        return QuotaDelay::zero();
    }
    let Some((entity_key, rate)) = lookup::lookup_quota_with_key(
        request.image,
        request.principal,
        request.client_id,
        request.quota_key,
    ) else {
        return QuotaDelay::zero();
    };
    if !rate.is_finite() || rate <= 0.0 {
        return QuotaDelay::zero();
    }
    let token_rate = token_rate(rate);
    let user = entity_field(&entity_key, "user");
    let client_id = entity_field(&entity_key, "client-id");

    let bucket = request
        .buckets
        .get_or_create(request.quota_key, &entity_key, token_rate);
    // Kafka holds the quota as a double, so the bucket keeps a part token in
    // its debt and the throttle is exact under a fractional rate. A quota
    // whose throttle is bounded keeps no more debt than the bound repays:
    // Kafka forgets a sample after its quota window, so debt past the bound
    // would throttle a client that has stopped overrunning for minutes.
    let debt = match request.max_debt_wait {
        Some(wait) => bucket.record_bounded(request.amount, wait),
        None => bucket.record(request.amount),
    };
    let Some(overage) = debt_tokens(debt) else {
        return QuotaDelay::zero();
    };
    // Kafka bounds only the request quota's throttle (`ClientRequestQuotaManager`
    // takes `boundedThrottleTime`), so the bound, where there is one, is the
    // caller's.
    let delay = delay_for_overage(overage, rate, token_rate);
    QuotaDelay::new(delay, user, client_id)
}

/// The debt a bucket reports, in tokens with its fractional part, or `None`
/// when the bucket owes nothing.
pub(crate) fn debt_tokens(debt_micros: u64) -> Option<f64> {
    (debt_micros > 0).then(|| u64_to_f64(debt_micros) / u64_to_f64(MICROS_PER_TOKEN))
}

/// A quota delay as Kafka's `throttle_time_ms` wire field: the delay in
/// milliseconds, rounded to the nearest.
///
/// Kafka's `QuotaUtils.throttleTime` and `ControllerMutationQuotaManager
/// .throttleTimeMs` both return `Math.round(...)` of the millisecond value, so
/// a 1.6 ms delay is reported as `2` and a 0.6 ms one as `1` (#1241). A delay
/// beyond `i32::MAX` milliseconds saturates.
#[must_use]
pub(crate) fn throttle_time_ms(delay: Time) -> i32 {
    i32::try_from(delay.millis_i64()).unwrap_or(i32::MAX)
}

/// A raw quota rate as the [`TokenBucket`](crate::throttle::TokenBucket)'s
/// [`ByteRate`], fractional part included.
///
/// The bucket is byte-dimensioned, but Kafka drives `request_percentage` and
/// `controller_mutation_rate` through the same token arithmetic, and those are
/// not byte throughputs. Their raw magnitudes therefore cross into the
/// bucket's dimension here, in one place, instead of at each call site.
pub(crate) fn bucket_rate(raw: f64) -> ByteRate { ByteRate::from_bytes_per_sec_f64(raw) }

/// A configured rate as a whole token count, truncated toward zero.
///
/// Negative and non-finite rates are not throughputs, so they collapse to `0`,
/// the bucket's "no limit configured" sentinel. Anything past `u64::MAX`
/// saturates.
pub(crate) fn positive_f64_to_u64(value: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    value.trunc().to_u64().unwrap_or(u64::MAX)
}

/// A token count widened for the overage-over-rate division.
///
/// The conversion is exact below 2^53, which covers every quota magnitude
/// Kafka can express. `NumCast` never fails for `u64` into `f64`. The fallback
/// keeps the quota path total instead of panicking on a value that cannot
/// occur.
pub(crate) fn u64_to_f64(value: u64) -> f64 { NumCast::from(value).unwrap_or(f64::INFINITY) }

#[cfg(test)]
pub(crate) mod test_support {
    use {
        krabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity},
        krabka_units::{Time, convert::TimeExt, secs},
    };

    /// A named input builder for one quota kind's rate tests.
    macro_rules! image_builder {
        ($name:ident, $key:literal) => {
            fn $name(
                entity: Vec<(&str, Option<&str>)>,
                rate: f64,
            ) -> krabka_metadata::MetadataImage {
                crate::quota::test_support::image_with_quota(
                    crate::quota::test_support::QuotaRecordSetup {
                        entity,
                        key: $key,
                        value: crate::quota::test_support::QuotaValue(rate),
                    },
                )
            }
        };
    }
    use crate::throttle::TokenBucket;
    pub(crate) use image_builder;

    /// Independent expected throttles for producer and consumer quotas.
    pub(crate) fn fractional_bandwidth_cases() -> [(f64, u64, Time); 7] {
        [
            (1024.0, 1024, <Time as TimeExt>::ZERO),
            (1024.0, 2048, secs(1)),
            (0.5, 1, secs(1)),
            (0.5, 100, secs(199)),
            (0.25, 1, secs(3)),
            (1.5, 1, <Time as TimeExt>::ZERO),
            (1.5, 3, secs(1)),
        ]
    }

    /// Apply the independent fractional-rate table to one byte-charging API.
    pub(crate) fn check_fractional_bandwidth(
        key: &str,
        consume: impl Fn(&MetadataImage, &super::QuotaBuckets, u64) -> Time,
    ) {
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (rate, bytes, delay) in fractional_bandwidth_cases() {
            let image = image_with_quota(QuotaRecordSetup {
                key,
                value: QuotaValue(rate),
                ..Default::default()
            });
            let buckets = super::QuotaBuckets::with_window(secs(1));
            actual.push((rate.to_string(), bytes, consume(&image, &buckets, bytes)));
            expected.push((rate.to_string(), bytes, delay));
        }
        assert2::assert!(actual == expected);
    }

    #[derive(Clone, Copy)]
    pub(crate) struct QuotaValue(pub f64);

    #[derive(krabka_macros::FieldDefaults)]
    pub(crate) struct QuotaRecordSetup<'a> {
        #[default(vec![("user", Some("alice"))])]
        pub entity: Vec<(&'a str, Option<&'a str>)>,
        #[default("producer_byte_rate")]
        pub key: &'a str,
        #[default(QuotaValue(1_000.0))]
        pub value: QuotaValue,
    }

    pub(crate) type QuotaEntitySpec<'a> = Vec<(&'a str, Option<&'a str>)>;

    #[derive(krabka_macros::FieldDefaults)]
    pub(crate) struct UniformQuotaImageSetup<'a> {
        pub entities: Vec<QuotaEntitySpec<'a>>,
        #[default("producer_byte_rate")]
        pub key: &'a str,
        #[default(QuotaValue(64.0))]
        pub value: QuotaValue,
    }

    /// Configure every entity at one probe rate, retaining the input order.
    pub(crate) fn uniform_quota_image(setup: UniformQuotaImageSetup<'_>) -> MetadataImage {
        image_with_quotas(
            setup
                .entities
                .into_iter()
                .map(|entity| {
                    quota_record(QuotaRecordSetup {
                        entity,
                        key: setup.key,
                        value: setup.value,
                    })
                })
                .collect(),
        )
    }

    pub(crate) fn producer_quota_image(entities: Vec<QuotaEntitySpec<'_>>) -> MetadataImage {
        uniform_quota_image(UniformQuotaImageSetup {
            entities,
            ..Default::default()
        })
    }

    pub(crate) fn app1_producer_quota() -> ClientQuotaRecord {
        quota_record(QuotaRecordSetup {
            entity: vec![("client-id", Some("app1"))],
            value: QuotaValue(200.0),
            ..Default::default()
        })
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    pub(crate) struct UserBucketSetup<'a> {
        #[default("producer_byte_rate")]
        pub key: &'a str,
        #[default(QuotaValue(0.0))]
        pub rate: QuotaValue,
    }

    pub(crate) fn alice_bucket(
        setup: UserBucketSetup<'_>,
    ) -> (
        std::sync::Arc<super::QuotaBuckets>,
        std::sync::Arc<TokenBucket>,
    ) {
        let buckets = std::sync::Arc::new(super::QuotaBuckets::new());
        let key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
        let bucket = buckets.get_or_create(setup.key, &key, setup.rate.0);
        (buckets, bucket)
    }

    /// The 5,000-byte pair used to compare lookup precedence with bucket refresh.
    pub(crate) fn alice_app1_producer_quota() -> ClientQuotaRecord {
        quota_record(QuotaRecordSetup {
            entity: vec![("user", Some("alice")), ("client-id", Some("app1"))],
            value: QuotaValue(5_000.0),
            ..Default::default()
        })
    }

    pub(crate) fn image_with_quota(setup: QuotaRecordSetup<'_>) -> MetadataImage {
        image_with_quotas(vec![quota_record(setup)])
    }

    pub(crate) fn image_with_quotas(records: Vec<ClientQuotaRecord>) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for record in records {
            image.apply(&MetadataRecord::V1ClientQuota(record));
        }
        image
    }

    pub(crate) fn quota_record(setup: QuotaRecordSetup<'_>) -> ClientQuotaRecord {
        let QuotaRecordSetup { entity, key, value } = setup;
        ClientQuotaRecord {
            entity: entity
                .into_iter()
                .map(|(entity_type, entity_name)| QuotaEntity {
                    entity_type: entity_type.into(),
                    entity_name: entity_name.map(Into::into),
                })
                .collect(),
            config_key: key.into(),
            config_value: Some(value.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            test_support::{QuotaRecordSetup, QuotaValue, image_with_quota},
            *,
        },
        crate::throttle::MICROS_PER_TOKEN,
        assert2::{assert, check},
        krabka_units::secs,
        std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    /// `throttle_time_ms` rounds to the nearest millisecond, half up, as
    /// Kafka's `Math.round` does: 0.4 ms reports `0`, 0.6 ms and 1.6 ms report
    /// `1` and `2` (#1241).
    #[test]
    fn throttle_time_ms_rounds_to_the_nearest_millisecond() {
        let cases = [
            (krabka_units::micros(0), 0),
            (krabka_units::micros(400), 0),
            (krabka_units::micros(500), 1),
            (krabka_units::micros(600), 1),
            (krabka_units::micros(999), 1),
            (krabka_units::millis(1), 1),
            (krabka_units::micros(1_400), 1),
            (krabka_units::micros(1_500), 2),
            (krabka_units::micros(1_600), 2),
            (krabka_units::micros(1_999), 2),
            (secs(1), 1_000),
        ];
        for (delay, want) in cases {
            check!(
                throttle_time_ms(delay) == want,
                "{delay:?} should report {want}ms"
            );
        }
    }

    #[test]
    fn throttle_time_ms_saturates_past_i32_milliseconds() {
        assert!(throttle_time_ms(krabka_units::days(36_500)) == i32::MAX);
    }

    #[test]
    fn quota_rate_conversion_is_checked_and_saturating() {
        assert!(positive_f64_to_u64(-1.0) == 0);
        assert!(positive_f64_to_u64(f64::NAN) == 0);
        assert!(positive_f64_to_u64(10.9) == 10);
        assert!(positive_f64_to_u64(f64::MAX) == u64::MAX);
        assert!(u64_to_f64(u64::MAX).is_finite());
    }

    /// The producer path once carried its own copy of this widening. The two
    /// agreed on every input, and this test pins that they still would. It
    /// compares bit patterns instead of using `==`, so the comparison is
    /// exact.
    #[test]
    fn widening_agrees_with_the_former_producer_copy() {
        for value in [0_u64, 1, 1024, 1 << 52, (1_u64 << 53) - 1, u64::MAX] {
            let former: f64 = value.to_string().parse().unwrap_or(f64::INFINITY);
            check!(u64_to_f64(value).to_bits() == former.to_bits());
        }
    }

    /// The overage keeps the part token a bucket's debt holds, and a bucket
    /// that owes nothing has none.
    #[test]
    fn debt_tokens_is_the_exact_debt() {
        let m = MICROS_PER_TOKEN;
        let cases = [
            (0, None),
            (1, Some(0.000_001)),
            (m / 2, Some(0.5)),
            (m, Some(1.0)),
            (99 * m + m / 2, Some(99.5)),
        ];
        for (debt_micros, want) in cases {
            check!(
                debt_tokens(debt_micros) == want,
                "{debt_micros} micro-tokens of debt"
            );
        }
    }

    #[test]
    fn consume_configured_quota_returns_zero_without_mutating_bucket_for_zero_amount() {
        let image = image_with_quota(QuotaRecordSetup {
            key: "request_percentage",
            value: QuotaValue(100.0),
            ..Default::default()
        });
        let buckets = QuotaBuckets::new();
        let initial_rate_called = Arc::new(AtomicBool::new(false));
        let delay_for_overage_called = Arc::new(AtomicBool::new(false));

        let delay = consume_configured_quota(
            QuotaConsumption {
                image: &image,
                buckets: &buckets,
                principal: "alice",
                client_id: Some(""),
                quota_key: "request_percentage",
                amount: 0,
                max_debt_wait: None,
            },
            {
                let called = Arc::clone(&initial_rate_called);
                move |_| {
                    called.store(true, Ordering::Relaxed);
                    100.0
                }
            },
            {
                let called = Arc::clone(&delay_for_overage_called);
                move |_, _, _| {
                    called.store(true, Ordering::Relaxed);
                    secs(1)
                }
            },
        );

        check!(delay == <Time as TimeExt>::ZERO);
        check!(buckets.is_empty());
        check!(!initial_rate_called.load(Ordering::Relaxed));
        assert!(!delay_for_overage_called.load(Ordering::Relaxed));
    }

    #[test]
    fn consume_configured_quota_ignores_non_positive_rates() {
        for rate in [-1.0, 0.0] {
            let image = image_with_quota(QuotaRecordSetup {
                value: QuotaValue(rate),
                ..Default::default()
            });
            let buckets = QuotaBuckets::new();
            let initial_rate_called = Arc::new(AtomicBool::new(false));

            let delay = consume_configured_quota(
                QuotaConsumption {
                    image: &image,
                    buckets: &buckets,
                    principal: "alice",
                    client_id: Some(""),
                    quota_key: "producer_byte_rate",
                    amount: 1,
                    max_debt_wait: None,
                },
                {
                    let called = Arc::clone(&initial_rate_called);
                    move |_| {
                        called.store(true, Ordering::Relaxed);
                        1.0
                    }
                },
                |_, _, _| secs(1),
            );

            check!(delay == <Time as TimeExt>::ZERO);
            check!(buckets.is_empty());
            assert!(!initial_rate_called.load(Ordering::Relaxed));
        }
    }

    #[test]
    fn consume_configured_quota_leaves_the_overage_delay_uncapped() {
        let image = image_with_quota(QuotaRecordSetup {
            value: QuotaValue(1.0),
            ..Default::default()
        });
        // A one-second window: at 1 B/s the burst is one byte, so 10 bytes
        // leaves the 9-byte overage the closure below checks.
        let buckets = QuotaBuckets::with_window(secs(1));

        let delay = consume_configured_quota(
            QuotaConsumption {
                image: &image,
                buckets: &buckets,
                principal: "alice",
                client_id: Some(""),
                quota_key: "producer_byte_rate",
                amount: 10,
                max_debt_wait: None,
            },
            |rate| rate,
            |overage, rate, token_rate| {
                check!((overage, rate, token_rate) == (9.0, 1.0, 1.0));
                secs(10)
            },
        );

        check!(delay == secs(10));
        assert!(buckets.len() == 1);
    }
}
