//! Per-broker cache of `TokenBucket`s, one per (`quota_key`, `entity_key`) pair.

use {
    super::IpNames,
    crate::{config::DEFAULT_QUOTA_WINDOW, throttle::TokenBucket},
    dashmap::DashMap,
    krabka_metadata::EntityKey,
    krabka_units::Time,
    std::{
        sync::{Arc, Mutex},
        time::Instant,
    },
};

/// Stored entry beside each live quota bucket. It holds no client identity:
/// the bucket belongs to its entity key, and every client that resolves to the
/// key shares it, so the first one to create it must not stand for the rest
/// (#1213).
#[derive(Debug)]
pub struct BucketEntry {
    pub bucket: Arc<TokenBucket>,
    pub last_accessed: Mutex<Instant>,
}

#[derive(Debug)]
pub struct QuotaBuckets {
    /// Keyed by (`quota_key`, canonical entity key). There is one bucket for
    /// each (`quota_type`, entity) pair, allocated lazily on the first
    /// lookup.
    buckets: DashMap<(String, EntityKey), Arc<BucketEntry>>,
    controller_mutations: DashMap<EntityKey, Arc<Mutex<ControllerMutationBucket>>>,
    quota_window: Time,
    /// The addresses the image's `ip` quota entity names stand for.
    ip_names: IpNames,
}

impl Default for QuotaBuckets {
    fn default() -> Self { Self::new() }
}

#[derive(Debug)]
pub(super) struct ControllerMutationBucket {
    pub(super) rate: f64,
    pub(super) window_secs: f64,
    pub(super) tokens: f64,
    pub(super) updated_at: Instant,
}

impl QuotaBuckets {
    /// Buckets sized by the default quota window. The broker builds its own
    /// with [`Self::with_window`] from `[runtime] quota_window`; this is for
    /// callers that have no config to read, which is the in-process tests.
    #[must_use]
    pub fn new() -> Self { Self::with_window(DEFAULT_QUOTA_WINDOW) }

    /// Buckets whose byte-rate burst is `rate * quota_window`, which is the
    /// window Kafka averages a client's rate over before it throttles.
    #[must_use]
    pub fn with_window(quota_window: Time) -> Self {
        Self {
            buckets: DashMap::new(),
            controller_mutations: DashMap::new(),
            quota_window,
            ip_names: IpNames::default(),
        }
    }

    #[must_use]
    pub fn quota_window(&self) -> Time { self.quota_window }

    /// The addresses the image's `ip` quota entity names stand for, which the
    /// quota refresh task keeps current.
    #[must_use]
    pub fn ip_names(&self) -> &IpNames { &self.ip_names }

    /// Returns the bucket for `(quota_key, entity_key)`, and creates it
    /// lazily if it does not exist. A new bucket starts at `initial_rate`
    /// tokens per second, fractional part included.
    ///
    /// # Panics
    ///
    /// Panics if a bucket's last-accessed timestamp was poisoned by a panic
    /// while it was held. Nothing under that lock can panic, so a poisoned
    /// one means the process is already unwinding.
    #[must_use]
    pub fn get_or_create(
        &self,
        quota_key: &str,
        entity_key: &EntityKey,
        initial_rate: f64,
    ) -> Arc<TokenBucket> {
        let key = (quota_key.to_string(), entity_key.clone());
        if let Some(entry) = self.buckets.get(&key) {
            *entry.last_accessed.lock().unwrap() = Instant::now();
            return entry.bucket.clone();
        }
        let b = Arc::new(TokenBucket::new());
        let new_rate = super::bucket_rate(initial_rate);
        let burst = (new_rate * self.quota_window).into();
        b.set_byte_rate_with_burst(new_rate, burst);
        let entry = self.buckets.entry(key).or_insert_with(|| {
            Arc::new(BucketEntry {
                bucket: b.clone(),
                last_accessed: Mutex::new(Instant::now()),
            })
        });
        *entry.last_accessed.lock().unwrap() = Instant::now();
        entry.bucket.clone()
    }

    /// Iterates over every (`quota_key`, `entity_key`, bucket entry) triple.
    /// The refresh task uses it to push new rates after an image change.
    pub fn iter(&self) -> impl Iterator<Item = ((String, EntityKey), Arc<BucketEntry>)> + '_ {
        self.buckets
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
    }

    /// Expire buckets unused for more than `max_age` (Kafka uses 1 hour).
    ///
    /// Returns the `(quota_key, user, client_id)` of every bucket it dropped,
    /// so the caller can unregister the metric series they carried. The user
    /// and the client id are the ones of the bucket's entity key, which are
    /// the labels a throttle charged to it published.
    ///
    /// # Panics
    ///
    /// Panics if a bucket's last-accessed timestamp was poisoned by a panic
    /// while it was held.
    #[must_use]
    pub fn expire_inactive(
        &self,
        max_age: std::time::Duration,
    ) -> Vec<(String, Option<String>, Option<String>)> {
        let now = Instant::now();
        let mut expired = Vec::new();
        self.buckets.retain(|(quota_key, entity_key), entry| {
            let last = *entry.last_accessed.lock().unwrap();
            if now.duration_since(last) > max_age {
                let field = |name: &str| {
                    entity_key
                        .iter()
                        .find(|(entity_type, _)| entity_type == name)
                        .and_then(|(_, value)| value.clone())
                };
                expired.push((quota_key.clone(), field("user"), field("client-id")));
                false
            } else {
                true
            }
        });
        expired
    }

    pub(super) fn controller_mutation_bucket(
        &self,
        entity_key: &EntityKey,
        rate: f64,
        window_secs: f64,
    ) -> Arc<Mutex<ControllerMutationBucket>> {
        self.controller_mutations
            .entry(entity_key.clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(ControllerMutationBucket {
                    rate,
                    window_secs,
                    tokens: rate * window_secs,
                    updated_at: Instant::now(),
                }))
            })
            .clone()
    }

    #[cfg(test)]
    #[must_use]
    pub fn len(&self) -> usize { self.buckets.len() }

    #[cfg(test)]
    #[must_use]
    pub fn is_empty(&self) -> bool { self.buckets.is_empty() }
}

#[cfg(test)]
mod tests {
    use {
        super::{super::bucket_rate, *},
        assert2::{assert, check},
    };

    fn key(user: &str) -> EntityKey { vec![("user".into(), Some(user.into()))] }

    /// KIP-13 measures a client's rate over `quota.window.num` windows of
    /// `quota.window.size.seconds`, so a client may spend a whole window's
    /// worth of bytes before it is throttled. The configured window is what
    /// sizes that burst, and `[runtime] quota_window` is what configures it
    /// (#397, #418).
    #[test]
    fn the_configured_window_sizes_a_new_bucket_s_burst() {
        for (window, rate, spend_without_throttle) in [
            (krabka_units::secs(1), 1_024.0, 1_024_u64),
            (krabka_units::secs(11), 1_024.0, 11_264),
        ] {
            let buckets = QuotaBuckets::with_window(window);
            let bucket = buckets.get_or_create("producer_byte_rate", &key("alice"), rate);

            check!(bucket.try_consume(spend_without_throttle) == spend_without_throttle);
            check!(
                bucket.try_consume(1) == 0,
                "{window:?} burst was not exhausted"
            );
        }
    }

    #[test]
    fn get_or_create_returns_new_bucket_first_time() {
        let buckets = QuotaBuckets::new();
        let b = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024.0);
        assert!(b.byte_rate() == bucket_rate(1024.0));
        assert!(buckets.len() == 1);
    }

    #[test]
    fn get_or_create_returns_existing_bucket_second_time() {
        let buckets = QuotaBuckets::new();
        let b1 = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024.0);
        let b2 = buckets.get_or_create("producer_byte_rate", &key("alice"), 4096.0);
        // Same Arc — initial_rate on second call is ignored.
        check!(Arc::ptr_eq(&b1, &b2));
        check!(b1.byte_rate() == bucket_rate(1024.0));
        check!(buckets.len() == 1);
    }

    #[test]
    fn different_quota_keys_get_different_buckets() {
        let buckets = QuotaBuckets::new();
        let _ = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024.0);
        let _ = buckets.get_or_create("consumer_byte_rate", &key("alice"), 2048.0);
        assert!(buckets.len() == 2);
    }

    #[test]
    fn different_entities_get_different_buckets() {
        let buckets = QuotaBuckets::new();
        let _ = buckets.get_or_create("producer_byte_rate", &key("alice"), 1024.0);
        let _ = buckets.get_or_create("producer_byte_rate", &key("bob"), 2048.0);
        assert!(buckets.len() == 2);
    }
}
