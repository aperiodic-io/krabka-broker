//! KIP-599 `controller_mutation_rate`: the token bucket that the
//! `CreateTopics`, `CreatePartitions` and `DeleteTopics` handlers charge.
//!
//! Kafka charges each topic on its own, after the topic has passed every other
//! check, with the number of partitions it creates or deletes
//! (`ReplicationControlManager.createTopic`, `deleteTopic` and
//! `createPartitions` call `applyPartitionChangeQuota`). A handler therefore
//! opens one [`ControllerMutationQuota`] per request and calls
//! [`ControllerMutationQuota::record`] once per topic that reaches that point.
//!
//! The bucket holds `window x rate` tokens. Kafka sizes it as
//! `controller.quota.window.num x controller.quota.window.size.seconds x rate`
//! (`TokenBucket`, `QuotaFactory`), so `window` is that product: 11 s by
//! default.

use std::sync::{Arc, Mutex};

use krabka_metadata::MetadataImage;
use krabka_units::{Time, convert::TimeExt, secs};

use super::buckets::{ControllerMutationBucket, QuotaBuckets};

/// A strict quota refused one mutation: Kafka's
/// `ThrottlingQuotaExceededException`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ThrottlingQuotaExceeded;

/// The bucket one request charges and the entity it belongs to.
struct Limit {
    bucket: Arc<Mutex<ControllerMutationBucket>>,
    rate: f64,
    window_secs: f64,
    user: Option<String>,
    client_id: Option<String>,
}

/// The KIP-599 controller-mutation quota of one request.
///
/// A strict quota (`CreateTopics` v6+, `CreatePartitions` v3+, `DeleteTopics`
/// v5+) refuses a mutation when the bucket is already negative, and records
/// nothing for it. A mutation that finds the bucket at zero or above is
/// recorded even when it drives the bucket below zero. A permissive quota
/// records every mutation and reports the time the bucket needs to refill as
/// the throttle.
pub(crate) struct ControllerMutationQuota {
    limit: Option<Limit>,
    strict: bool,
    delay: Time,
}

/// The inputs that pick the bucket of one request.
pub(crate) struct QuotaRequest<'a> {
    pub(crate) image: &'a MetadataImage,
    pub(crate) buckets: &'a QuotaBuckets,
    pub(crate) principal: &'a str,
    pub(crate) client_id: Option<&'a str>,
    /// `controller.quota.window.num x controller.quota.window.size.seconds`.
    pub(crate) window: Time,
    /// Whether the request version refuses a mutation over the quota.
    pub(crate) strict: bool,
}

impl ControllerMutationQuota {
    /// Opens the quota of one request. A principal with no
    /// `controller_mutation_rate`, or with a rate that is not positive, is
    /// never throttled.
    pub(crate) fn new(request: &QuotaRequest<'_>) -> Self {
        let limit = super::lookup::lookup_quota_with_key(
            request.image,
            request.principal,
            request.client_id,
            "controller_mutation_rate",
        )
        .and_then(|(entity_key, rate)| {
            let window_secs = request.window.secs_f64();
            let usable =
                rate.is_finite() && rate > 0.0 && window_secs.is_finite() && window_secs > 0.0;
            usable.then(|| Limit {
                bucket: request
                    .buckets
                    .controller_mutation_bucket(&entity_key, rate, window_secs),
                rate,
                window_secs,
                user: super::entity_field(&entity_key, "user"),
                client_id: super::entity_field(&entity_key, "client-id"),
            })
        });
        Self {
            limit,
            strict: request.strict,
            delay: <Time as TimeExt>::ZERO,
        }
    }

    /// Charges `mutations` partitions, as Kafka's
    /// `ControllerMutationQuota.record` does.
    ///
    /// # Errors
    ///
    /// [`ThrottlingQuotaExceeded`] when the quota is strict and the bucket is
    /// already negative. Nothing is recorded then.
    pub(crate) fn record(&mut self, mutations: u64) -> Result<(), ThrottlingQuotaExceeded> {
        let Some(limit) = &self.limit else {
            return Ok(());
        };
        let mut bucket = limit
            .bucket
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = std::time::Instant::now();
        // The time since the last charge refills at the rate the bucket had
        // then, and a change of rate or window keeps that balance, capped at
        // the new capacity: Kafka's `TokenBucket` keeps its `tokens` when the
        // quota changes, so a bucket in debt stays in debt (#1241).
        let refilled = (bucket.tokens
            + now.duration_since(bucket.updated_at).as_secs_f64() * bucket.rate)
            .min(bucket.rate * bucket.window_secs);
        bucket.tokens = refilled.min(limit.rate * limit.window_secs);
        bucket.rate = limit.rate;
        bucket.window_secs = limit.window_secs;
        bucket.updated_at = now;

        let refill = |tokens: f64| Time::from_secs_f64((-tokens / limit.rate).max(0.0));
        if self.strict && bucket.tokens < 0.0 {
            let delay = refill(bucket.tokens);
            drop(bucket);
            self.delay = self.delay.max(delay);
            return Err(ThrottlingQuotaExceeded);
        }
        bucket.tokens -= super::u64_to_f64(mutations);
        if !self.strict && bucket.tokens < 0.0 {
            let delay = refill(bucket.tokens);
            drop(bucket);
            self.delay = self.delay.max(delay);
        }
        Ok(())
    }

    /// The throttle the response reports: the time the bucket needs to
    /// refill after the last refusal (strict) or overdraft (permissive).
    #[must_use]
    pub(crate) fn delay(&self) -> Time {
        self.delay
    }

    /// The throttle together with the quota entity it belongs to.
    #[must_use]
    pub(crate) fn quota_delay(&self) -> super::QuotaDelay {
        let (user, client_id) = self.limit.as_ref().map_or((None, None), |limit| {
            (limit.user.clone(), limit.client_id.clone())
        });
        super::QuotaDelay::new(self.delay, user, client_id)
    }
}

/// Consume `mutations` from the permissive `controller_mutation_rate` bucket
/// for `(principal, client_id)`, with a one-second window. This function
/// returns the throttle delay to apply before the handler sends the response.
/// The delay is zero if no quota is configured or if there is no overage. Like
/// Kafka's `ControllerMutationQuotaManager.throttleTimeMs`, it is not bounded.
#[must_use]
pub fn consume_controller_mutation_quota(
    image: &MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: Option<&str>,
    mutations: u64,
) -> super::QuotaDelay {
    let mut quota = ControllerMutationQuota::new(&QuotaRequest {
        image,
        buckets,
        principal,
        client_id,
        window: secs(1),
        strict: false,
    });
    // A permissive quota records every mutation, so it never refuses one.
    let _ = quota.record(mutations);
    quota.quota_delay()
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{millis, secs};

    use super::*;

    crate::quota::test_support::image_builder!(img_with_quota, "controller_mutation_rate");

    #[test]
    fn zero_mutations_returns_zero_delay() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", Some(""), 0);
        let expected =
            crate::quota::QuotaDelay::new(<Time as TimeExt>::ZERO, Some("alice".into()), None);
        assert!(delay == expected);
    }

    #[test]
    fn under_rate_returns_zero_delay() {
        // rate=10/sec, burst capacity=10 (one second of capacity).
        // 5 mutations consumed → bucket has 5 left → no overage.
        let img = img_with_quota(vec![("user", Some("alice"))], 10.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", Some(""), 5);
        assert!(delay.delay == <Time as TimeExt>::ZERO);
    }

    /// Kafka's `ControllerMutationQuotaManager.throttleTimeMs` is the full
    /// time to refill the bucket, with no bound (#709). At 1/sec with a
    /// one-mutation burst, 61 mutations leave 60 seconds of debt.
    #[test]
    fn overage_delay_is_not_capped() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();
        let delay = consume_controller_mutation_quota(&img, &buckets, "alice", Some(""), 61);
        check!(
            delay.delay > secs(59) && delay.delay <= secs(60),
            "{delay:?}"
        );
    }

    /// A change of rate keeps the balance of the bucket, as Kafka's
    /// `TokenBucket` keeps its tokens when the quota changes: a client that
    /// ran up 60 seconds of debt at 1 mutation per second is still in debt
    /// after its quota goes to 2, and owes it at the new rate (#1241). Before,
    /// the change refilled the bucket.
    #[test]
    fn a_rate_change_keeps_the_debt_of_the_bucket() {
        let buckets = QuotaBuckets::new();
        let before = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let after = img_with_quota(vec![("user", Some("alice"))], 2.0);

        // A one-mutation bucket and 61 mutations: 60 in debt.
        let _ = consume_controller_mutation_quota(&before, &buckets, "alice", Some(""), 61);
        let delay = consume_controller_mutation_quota(&after, &buckets, "alice", Some(""), 1);

        check!(
            delay.delay > secs(30) && delay.delay <= millis(30_500),
            "{delay:?}"
        );
    }

    /// A balance the bucket holds above the new capacity is cut down to it.
    #[test]
    fn a_smaller_capacity_caps_the_kept_balance() {
        let buckets = QuotaBuckets::new();
        let before = img_with_quota(vec![("user", Some("alice"))], 10.0);
        let after = img_with_quota(vec![("user", Some("alice"))], 1.0);

        // Ten tokens held, then a capacity of one: nine mutations are 8 over.
        let _ = consume_controller_mutation_quota(&before, &buckets, "alice", Some(""), 0);
        let delay = consume_controller_mutation_quota(&after, &buckets, "alice", Some(""), 9);

        check!(delay.delay > secs(7) && delay.delay <= secs(8), "{delay:?}");
    }

    /// Kafka's examples for `controller_mutation_rate = 10` with the default
    /// 11 s window (a 110-token bucket): each list is the per-topic charges of
    /// one or more strict requests on a fresh bucket, and the expected
    /// outcome of each charge.
    #[test]
    fn strict_quota_refuses_only_once_the_bucket_is_negative() {
        type Outcome = Result<(), ThrottlingQuotaExceeded>;
        const OK: Outcome = Ok(());
        const REFUSED: Outcome = Err(ThrottlingQuotaExceeded);
        /// A label, the per-topic charges of each request, and their outcomes.
        type Case = (
            &'static str,
            &'static [&'static [u64]],
            &'static [&'static [Outcome]],
        );
        let cases: [Case; 4] = [
            (
                "50 partitions, then two topics of 5",
                &[&[50], &[5, 5]],
                &[&[OK], &[OK, OK]],
            ),
            (
                "60, 60 and 1 in one request",
                &[&[60, 60, 1]],
                &[&[OK, OK, REFUSED]],
            ),
            (
                "a request that ends below zero refuses the next one",
                &[&[120], &[1]],
                &[&[OK], &[REFUSED]],
            ),
            (
                "a refused topic records nothing",
                &[&[111, 5, 5]],
                &[&[OK, REFUSED, REFUSED]],
            ),
        ];

        let img = img_with_quota(vec![("user", Some("alice"))], 10.0);
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (label, requests, outcomes) in cases {
            let buckets = QuotaBuckets::new();
            let got: Vec<Vec<Outcome>> = requests
                .iter()
                .map(|charges| {
                    let mut quota = ControllerMutationQuota::new(&QuotaRequest {
                        image: &img,
                        buckets: &buckets,
                        principal: "alice",
                        client_id: Some(""),
                        window: secs(11),
                        strict: true,
                    });
                    charges.iter().map(|charge| quota.record(*charge)).collect()
                })
                .collect();
            actual.push((label, got));
            expected.push((
                label,
                outcomes
                    .iter()
                    .map(|request| request.to_vec())
                    .collect::<Vec<_>>(),
            ));
        }
        assert!(actual == expected);
    }

    /// A strict request that is refused reports the time the bucket needs to
    /// climb back to zero. One that is accepted reports no throttle, even when
    /// it leaves the bucket below zero.
    #[test]
    fn strict_quota_reports_the_throttle_of_a_refusal_only() {
        let img = img_with_quota(vec![("user", Some("alice"))], 10.0);
        let buckets = QuotaBuckets::new();
        let open = || {
            ControllerMutationQuota::new(&QuotaRequest {
                image: &img,
                buckets: &buckets,
                principal: "alice",
                client_id: Some(""),
                window: secs(11),
                strict: true,
            })
        };

        let mut first = open();
        let accepted = first.record(130);
        let mut second = open();
        let refused = second.record(1);

        assert!(accepted == Ok(()));
        assert!(first.delay() == <Time as TimeExt>::ZERO);
        assert!(refused == Err(ThrottlingQuotaExceeded));
        assert!(second.delay() > millis(1_900));
        assert!(second.delay() <= secs(2));
    }

    /// A strict caller in debt is rejected with the whole refill time, which
    /// no bound shortens (#709).
    #[test]
    fn strict_rejection_reports_the_uncapped_refill_time() {
        let img = img_with_quota(vec![("user", Some("alice"))], 1.0);
        let buckets = QuotaBuckets::new();
        let mut quota = ControllerMutationQuota::new(&QuotaRequest {
            image: &img,
            buckets: &buckets,
            principal: "alice",
            client_id: Some(""),
            window: secs(1),
            strict: true,
        });

        let outcomes = [61, 1].map(|charge| quota.record(charge));

        assert!(outcomes == [Ok(()), Err(ThrottlingQuotaExceeded)]);
        check!(
            quota.delay() > secs(59) && quota.delay() <= secs(60),
            "{:?}",
            quota.delay()
        );
    }

    #[test]
    fn fractional_strict_quota_rejects_the_operation_after_debt() {
        let img = img_with_quota(vec![("user", Some("alice"))], 0.015);
        let buckets = QuotaBuckets::new();
        let mut quota = ControllerMutationQuota::new(&QuotaRequest {
            image: &img,
            buckets: &buckets,
            principal: "alice",
            client_id: Some(""),
            window: secs(2_000),
            strict: true,
        });

        let outcomes = [10, 10, 20, 1].map(|charge| quota.record(charge));

        assert!(outcomes == [Ok(()), Ok(()), Ok(()), Err(ThrottlingQuotaExceeded)]);
    }

    /// A `(user, client-id)` quota charges only the client it names.
    #[test]
    fn a_user_and_client_quota_matches_its_client_only() {
        use krabka_metadata::{ClientQuotaRecord, MetadataRecord, QuotaEntity};
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                },
                QuotaEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("app-x".into()),
                },
            ],
            config_key: "controller_mutation_rate".into(),
            config_value: Some(1.0),
        }));
        let throttled = ["app-x", "other"].map(|client_id| {
            let buckets = QuotaBuckets::new();
            consume_controller_mutation_quota(&img, &buckets, "alice", Some(client_id), 10).delay
                > <Time as TimeExt>::ZERO
        });

        assert!(throttled == [true, false]);
    }

    /// A principal with no `controller_mutation_rate` is never throttled.
    #[test]
    fn no_quota_never_refuses() {
        let img = img_with_quota(vec![("user", Some("bob"))], 1.0);
        let buckets = QuotaBuckets::new();
        let mut quota = ControllerMutationQuota::new(&QuotaRequest {
            image: &img,
            buckets: &buckets,
            principal: "alice",
            client_id: Some(""),
            window: secs(11),
            strict: true,
        });

        let outcomes = [1_000, 1_000].map(|charge| quota.record(charge));

        assert!((outcomes, quota.delay()) == ([Ok(()), Ok(())], <Time as TimeExt>::ZERO));
    }
}
