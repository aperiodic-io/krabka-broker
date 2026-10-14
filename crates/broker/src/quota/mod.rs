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

    /// One borrowed entity component of a quota fixture, in wire field order.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub(crate) struct QuotaEntityInput<'a> {
        pub(crate) entity_type: &'a str,
        pub(crate) entity_name: Option<&'a str>,
    }

    impl<'a> QuotaEntityInput<'a> {
        pub(crate) const fn user(name: Option<&'a str>) -> Self {
            Self {
                entity_type: "user",
                entity_name: name,
            }
        }

        pub(crate) const fn client_id(name: Option<&'a str>) -> Self {
            Self {
                entity_type: "client-id",
                entity_name: name,
            }
        }

        pub(crate) const fn ip(name: Option<&'a str>) -> Self {
            Self {
                entity_type: "ip",
                entity_name: name,
            }
        }
    }

    /// A named input builder for one quota kind's rate tests.
    pub(crate) fn image_with_rate(
        entity: Vec<QuotaEntityInput<'_>>,
        key: &str,
        rate: f64,
    ) -> MetadataImage {
        image_with_quota()
            .entity(entity)
            .key(key)
            .value(rate)
            .call()
    }
    use crate::throttle::TokenBucket;

    /// A quota rate, the bytes charged against it and the throttle that
    /// charge must earn.
    pub(crate) struct FractionalBandwidthCase {
        pub(crate) rate: f64,
        pub(crate) bytes: u64,
        pub(crate) delay: Time,
    }

    /// Independent expected throttles for producer and consumer quotas.
    pub(crate) fn fractional_bandwidth_cases() -> [FractionalBandwidthCase; 7] {
        let case = |rate, bytes, delay| FractionalBandwidthCase { rate, bytes, delay };
        [
            case(1024.0, 1024, <Time as TimeExt>::ZERO),
            case(1024.0, 2048, secs(1)),
            case(0.5, 1, secs(1)),
            case(0.5, 100, secs(199)),
            case(0.25, 1, secs(3)),
            case(1.5, 1, <Time as TimeExt>::ZERO),
            case(1.5, 3, secs(1)),
        ]
    }

    /// Apply the independent fractional-rate table to one byte-charging API.
    pub(crate) fn check_fractional_bandwidth(
        key: &str,
        consume: impl Fn(&MetadataImage, &super::QuotaBuckets, u64) -> Time,
    ) {
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for FractionalBandwidthCase { rate, bytes, delay } in fractional_bandwidth_cases() {
            let image = image_with_quota().key(key).value(rate).call();
            let buckets = super::QuotaBuckets::with_window(secs(1));
            actual.push((rate.to_string(), bytes, consume(&image, &buckets, bytes)));
            expected.push((rate.to_string(), bytes, delay));
        }
        assert2::assert!(actual == expected);
    }

    #[derive(derive_more::From, derive_more::Into, Clone, Copy)]
    pub(crate) struct QuotaValue(pub f64);

    #[derive(krabka_macros::FieldDefaults)]
    pub(crate) struct QuotaRecordSetup<'a> {
        #[default(vec![QuotaEntityInput::user(Some("alice"))])]
        pub entity: Vec<QuotaEntityInput<'a>>,
        #[default("producer_byte_rate")]
        pub key: &'a str,
        #[default(QuotaValue(1_000.0))]
        pub value: QuotaValue,
    }

    fn image_from_quota_setup(setup: QuotaRecordSetup<'_>) -> MetadataImage {
        image_with_quotas(vec![
            quota_record()
                .entity(setup.entity)
                .key(setup.key)
                .value(setup.value)
                .call(),
        ])
    }

    pub(crate) fn shared_image_from_quota_setup(
        setup: QuotaRecordSetup<'_>,
    ) -> std::sync::Arc<MetadataImage> {
        std::sync::Arc::new(image_from_quota_setup(setup))
    }

    /// A bucket table and the one bucket that `alice_bucket` created in it.
    pub(crate) struct AliceBucket {
        pub(crate) buckets: std::sync::Arc<super::QuotaBuckets>,
        pub(crate) bucket: std::sync::Arc<TokenBucket>,
    }

    fn record_from_quota_setup(setup: QuotaRecordSetup<'_>) -> ClientQuotaRecord {
        let QuotaRecordSetup { entity, key, value } = setup;
        ClientQuotaRecord {
            entity: entity
                .into_iter()
                .map(|entry| QuotaEntity {
                    entity_type: entry.entity_type.into(),
                    entity_name: entry.entity_name.map(Into::into),
                })
                .collect(),
            config_key: key.into(),
            config_value: Some(value.0),
        }
    }

    krabka_macros::fixture_constructors! { quota: pub(crate) image_with_quota -> MetadataImage = image_from_quota_setup; }
    krabka_macros::fixture_constructors! { quota: pub(crate) quota_record -> ClientQuotaRecord = record_from_quota_setup; }

    pub(crate) type QuotaEntitySpec<'a> = Vec<QuotaEntityInput<'a>>;

    #[derive(krabka_macros::FieldDefaults)]
    pub(crate) struct UniformQuotaImageSetup<'a> {
        pub entities: Vec<QuotaEntitySpec<'a>>,
        #[default("producer_byte_rate")]
        pub key: &'a str,
        #[default(QuotaValue(64.0))]
        pub value: QuotaValue,
    }

    /// Configure every entity at one probe rate, retaining the input order.
    #[bon::builder]
    pub(crate) fn uniform_quota_image(
        #[builder(default)] entities: Vec<QuotaEntitySpec<'_>>,
        #[builder(default = "producer_byte_rate")] key: &'_ str,
        #[builder(into, default = QuotaValue (64.0))] value: QuotaValue,
    ) -> MetadataImage {
        let setup = UniformQuotaImageSetup {
            entities,
            key,
            value,
        };
        image_with_quotas(
            setup
                .entities
                .into_iter()
                .map(|entity| {
                    quota_record()
                        .entity(entity)
                        .key(setup.key)
                        .value(setup.value)
                        .call()
                })
                .collect(),
        )
    }

    pub(crate) fn producer_quota_image(entities: Vec<QuotaEntitySpec<'_>>) -> MetadataImage {
        uniform_quota_image().entities(entities).call()
    }

    pub(crate) fn app1_producer_quota() -> ClientQuotaRecord {
        quota_record()
            .entity(vec![QuotaEntityInput::client_id(Some("app1"))])
            .value(200.0)
            .call()
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    pub(crate) struct UserBucketSetup<'a> {
        #[default("producer_byte_rate")]
        pub key: &'a str,
        #[default(QuotaValue(0.0))]
        pub rate: QuotaValue,
    }

    #[bon::builder]
    pub(crate) fn alice_bucket(
        #[builder(default = "producer_byte_rate")] key: &'_ str,
        #[builder(into, default = QuotaValue (0.0))] rate: QuotaValue,
    ) -> AliceBucket {
        let setup = UserBucketSetup { key, rate };
        let buckets = std::sync::Arc::new(super::QuotaBuckets::new());
        let key: krabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
        let bucket = buckets.get_or_create(setup.key, &key, setup.rate.0);
        AliceBucket { buckets, bucket }
    }

    /// The 5,000-byte pair used to compare lookup precedence with bucket refresh.
    pub(crate) fn alice_app1_producer_quota() -> ClientQuotaRecord {
        quota_record()
            .entity(vec![
                QuotaEntityInput::user(Some("alice")),
                QuotaEntityInput::client_id(Some("app1")),
            ])
            .value(5_000.0)
            .call()
    }

    pub(crate) fn image_with_quotas(records: Vec<ClientQuotaRecord>) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for record in records {
            image.apply(&MetadataRecord::V1ClientQuota(record));
        }
        image
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{test_support::image_with_quota, *},
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
    #[yare::parameterized(
        zero = { 0_u64 },
        one = { 1 },
        kibibyte = { 1024 },
        exact_integer_boundary = { 1 << 52 },
        largest_consecutive_integer = { (1_u64 << 53) - 1 },
        maximum = { u64::MAX },
    )]
    fn widening_agrees_with_the_former_producer_copy(value: u64) {
        let former: f64 = value.to_string().parse().unwrap_or(f64::INFINITY);
        check!(u64_to_f64(value).to_bits() == former.to_bits());
    }

    /// The overage keeps the part token a bucket's debt holds, and a bucket
    /// that owes nothing has none.
    #[yare::parameterized(
        no_debt = { 0, None },
        one_micro_token = { 1, Some(0.000_001) },
        half_token = { MICROS_PER_TOKEN / 2, Some(0.5) },
        one_token = { MICROS_PER_TOKEN, Some(1.0) },
        ninety_nine_and_half_tokens = {
            99 * MICROS_PER_TOKEN + MICROS_PER_TOKEN / 2,
            Some(99.5),
        },
    )]
    fn debt_tokens_is_the_exact_debt(debt_micros: u64, want: Option<f64>) {
        check!(
            debt_tokens(debt_micros) == want,
            "{debt_micros} micro-tokens of debt"
        );
    }

    #[test]
    fn consume_configured_quota_returns_zero_without_mutating_bucket_for_zero_amount() {
        let image = image_with_quota()
            .key("request_percentage")
            .value(100.0)
            .call();
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

    #[yare::parameterized(
        negative_rate = { -1.0 },
        zero_rate = { 0.0 },
    )]
    fn consume_configured_quota_ignores_non_positive_rates(rate: f64) {
        let image = image_with_quota().value(rate).call();
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
        assert2::assert!(!initial_rate_called.load(Ordering::Relaxed));
    }

    #[test]
    fn consume_configured_quota_leaves_the_overage_delay_uncapped() {
        let image = image_with_quota().value(1.0).call();
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
