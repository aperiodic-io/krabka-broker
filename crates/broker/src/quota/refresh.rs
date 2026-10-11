//! Background task that subscribes to `MetadataImage` changes and pushes new
//! quota rates to the `QuotaBuckets` cache.
//!
//! The subscription itself is [`watch_image_loop`], shared with
//! `throttle::refresh`; only the per-image work differs.

use std::{future::Future, net::IpAddr, sync::Arc};

use krabka_metadata::{EntityKey, MetadataImage};
use krabka_units::convert::ByteRateExt as _;
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::{IpNames, buckets::QuotaBuckets, ip_names::system_lookup};
use crate::metadata_source::watch_image_loop;

pub async fn run(
    images: watch::Receiver<Arc<MetadataImage>>,
    buckets: Arc<QuotaBuckets>,
    shutdown: CancellationToken,
) {
    run_with(images, buckets, shutdown, system_lookup).await;
}

/// [`run`] with the resolver that looks a host name up.
async fn run_with<Lookup, Found>(
    images: watch::Receiver<Arc<MetadataImage>>,
    buckets: Arc<QuotaBuckets>,
    shutdown: CancellationToken,
    lookup: Lookup,
) where
    Lookup: Fn(String) -> Found,
    Found: Future<Output = Option<IpAddr>>,
{
    let latest = images.clone();
    let image_arrived = Notify::new();
    let apply_images = watch_image_loop(images, "quota refresh", shutdown, |image| {
        // Kafka resolves an `ip` entity's name when it applies the record
        // (#1214). Literals resolve here, and the lookup of a host name runs
        // in `resolve_hosts`, off this loop, so a slow resolver never holds
        // back a rate change.
        buckets.ip_names().update(image);
        refresh_buckets(image, &buckets);
        image_arrived.notify_one();
    });
    tokio::select! {
        () = apply_images => {}
        () = resolve_hosts(&latest, &buckets, &image_arrived, &lookup) => {}
    }
}

/// Looks up the host names of the latest image that have no address yet, and
/// applies each address to the buckets. It is the only place a lookup starts,
/// so two lookups of one name never run at once, and an image that arrives
/// during one waits behind it instead of starting another.
///
/// A name whose lookup failed is looked up again when its wait is over,
/// whether or not another image arrives: Kafka resolves the name once, when it
/// applies the record, so a transient resolver failure must not leave the
/// address's quota unenforced until the next metadata change. It stops once
/// the name's entity is gone from the image, and sleeps until the next image
/// when no name is waiting.
async fn resolve_hosts<Lookup, Found>(
    images: &watch::Receiver<Arc<MetadataImage>>,
    buckets: &QuotaBuckets,
    image_arrived: &Notify,
    lookup: &Lookup,
) where
    Lookup: Fn(String) -> Found,
    Found: Future<Output = Option<IpAddr>>,
{
    let names = buckets.ip_names();
    loop {
        let image = Arc::clone(&images.borrow());
        let due = names.claim(names.update(&image), Instant::now());
        if !due.is_empty() {
            names.resolve(&due, lookup).await;
            let image = Arc::clone(&images.borrow());
            names.update(&image);
            refresh_buckets(&image, buckets);
        }
        match names.next_retry() {
            Some(at) => {
                let _ = tokio::time::timeout_at(at, image_arrived.notified()).await;
            }
            None => image_arrived.notified().await,
        }
    }
}

/// The token rate a bucket for `quota_key` runs at, in the unit its consumer
/// meters: the same conversion the consumer used when it created the bucket.
///
/// `request_percentage` meters microseconds of handler time per second. Every
/// other quota meters its own unit, and keeps its fractional part: Kafka
/// holds each quota as a double, and the bucket enforces a fractional rate as
/// configured.
fn token_rate(quota_key: &str, rate: f64) -> f64 {
    match quota_key {
        "request_percentage" => super::request::request_percentage_token_rate(rate),
        _ => rate,
    }
}

fn refresh_buckets(image: &MetadataImage, buckets: &QuotaBuckets) {
    let window = buckets.quota_window();
    for ((quota_key, entity_key), entry) in buckets.iter() {
        let new_rate = configured_rate(image, buckets.ip_names(), &quota_key, &entity_key)
            .map_or(0.0, |rate| token_rate(&quota_key, rate));

        let new_rate = super::bucket_rate(new_rate);
        // Compared as the bucket stores it, so a rate finer than the bucket
        // resolves does not reset the bucket on every image.
        if !entry.bucket.runs_at_byte_rate(new_rate) {
            debug!(
                quota_key,
                ?entity_key,
                new_rate = new_rate.bytes_per_sec_f64(),
                "quota refresh: rate update"
            );
            let burst = (new_rate * window).into();
            entry.bucket.set_byte_rate_with_burst(new_rate, burst);
        }
    }
}

/// The rate the image configures for one bucket, resolved from the bucket's
/// own entity key.
///
/// A bucket is shared by every client that resolves to its key, so it is
/// re-rated from the key and not from whichever client created it (#1213):
/// Kafka re-rates each sensor from its own metric tags.
///
/// An accept-path bucket is keyed by `[("ip", Some(peer))]`, so it is looked
/// up with the `ip` precedence. The user and client-id lookup would find
/// nothing for it and would remove the `connection_creation_rate` limit at the
/// next image change.
///
/// The rate comes back in the unit the image configures it in; [`token_rate`]
/// converts it to the bucket's token unit.
fn configured_rate(
    image: &MetadataImage,
    ip_names: &IpNames,
    quota_key: &str,
    entity_key: &EntityKey,
) -> Option<f64> {
    if let [(entity_type, Some(peer))] = entity_key.as_slice()
        && entity_type == "ip"
    {
        let peer_ip = peer.parse().ok()?;
        return super::lookup::lookup_ip_quota_with_key(image, ip_names, peer_ip, quota_key)
            .map(|(_, rate)| rate);
    }
    super::lookup::lookup_bucket_rate(image, entity_key, quota_key)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use assert2::assert;
    use krabka_metadata::EntityKey;

    use super::{super::bucket_rate, *};
    use crate::{
        quota::test_support::{
            QuotaRecordSetup, QuotaValue, UserBucketSetup, alice_app1_producer_quota, alice_bucket,
            app1_producer_quota, image_with_quota as quota_image,
        },
        throttle::TokenBucket,
    };

    fn img_with_quota(setup: QuotaRecordSetup<'_>) -> Arc<MetadataImage> {
        Arc::new(quota_image(setup))
    }

    /// A refresh keeps each bucket at the rate its consumer created it with
    /// (#692). A `request_percentage` of 50 is 500 000 microseconds of handler
    /// time per second, not 50, and a fractional one does not round down to the
    /// unlimited rate 0.
    #[test]
    fn refresh_keeps_the_rate_unit_each_quota_meters() {
        for (quota_key, value, created_at, expected) in [
            ("request_percentage", 50.0, 500_000.0, 500_000.0),
            ("request_percentage", 0.0001, 1.0, 1.0),
            ("request_percentage", 0.000_05, 0.5, 0.5),
            ("connection_creation_rate", 0.5, 0.5, 0.5),
            ("producer_byte_rate", 2048.0, 2048.0, 2048.0),
        ] {
            let buckets = Arc::new(QuotaBuckets::new());
            let key: EntityKey = vec![("user".into(), Some("alice".into()))];
            let bucket = buckets.get_or_create(quota_key, &key, created_at);

            refresh_buckets(
                &img_with_quota(QuotaRecordSetup {
                    key: quota_key,
                    value: QuotaValue(value),
                    ..Default::default()
                }),
                &buckets,
            );

            assert2::check!(
                bucket.byte_rate() == bucket_rate(expected),
                "{quota_key}={value}"
            );
        }
    }

    #[test]
    fn refresh_updates_existing_bucket_rate() {
        let (buckets, b) = alice_bucket(UserBucketSetup::default());
        assert!(b.byte_rate() == bucket_rate(0.0));

        let img = img_with_quota(QuotaRecordSetup {
            value: QuotaValue(2048.0),
            ..Default::default()
        });
        refresh_buckets(&img, &buckets);
        assert!(b.byte_rate() == bucket_rate(2048.0));
    }

    #[test]
    fn refresh_zeroes_bucket_when_quota_removed_from_image() {
        let (buckets, b) = alice_bucket(UserBucketSetup {
            rate: QuotaValue(1024.0),
            ..Default::default()
        });
        assert!(b.byte_rate() == bucket_rate(1024.0));

        let empty = Arc::new(MetadataImage::new(uuid::Uuid::nil()));
        refresh_buckets(&empty, &buckets);
        assert!(b.byte_rate() == bucket_rate(0.0));
    }

    /// An image change keeps the `connection_creation_rate` of an `ip` bucket.
    /// The rate comes from the exact `ip` entity, or else from the default
    /// `ip` entity, as on the accept path.
    #[test]
    fn refresh_keeps_the_ip_connection_creation_rate() {
        let cases: [(&str, Option<&str>, f64, f64); 4] = [
            ("the exact ip entity", Some("127.0.0.1"), 3.0, 3.0),
            ("the default ip entity", None, 3.0, 3.0),
            ("another ip entity", Some("10.0.0.1"), 3.0, 0.0),
            ("a rate under one per second", Some("127.0.0.1"), 0.5, 0.5),
        ];
        for (case, entity_name, rate, expected) in cases {
            let buckets = Arc::new(QuotaBuckets::new());
            let key: EntityKey = vec![("ip".into(), Some("127.0.0.1".into()))];
            let b = buckets.get_or_create("connection_creation_rate", &key, 1.0);

            let img = img_with_quota(QuotaRecordSetup {
                entity: vec![("ip", entity_name)],
                key: "connection_creation_rate",
                value: QuotaValue(rate),
            });
            refresh_buckets(&img, &buckets);
            assert!(b.byte_rate() == bucket_rate(expected), "{case}");
        }
    }

    /// An accept-path bucket is keyed by the peer's canonical address, and a
    /// refresh keeps the rate of an `ip` entity that spells that address
    /// another way (#1214).
    #[test]
    fn refresh_keeps_the_rate_an_alias_ip_entity_configures() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("ip".into(), Some("::1".into()))];
        let b = buckets.get_or_create("connection_creation_rate", &key, 1.0);
        let img = img_with_quota(QuotaRecordSetup {
            entity: vec![("ip", Some("0:0:0:0:0:0:0:1"))],
            key: "connection_creation_rate",
            value: QuotaValue(3.0),
        });

        let _ = buckets.ip_names().update(&img);
        refresh_buckets(&img, &buckets);

        assert!(b.byte_rate() == bucket_rate(3.0));
    }

    /// The refresh task resolves an `ip` entity that names a host, and applies
    /// its rate to the bucket of the address the host resolved to (#1214).
    #[tokio::test]
    async fn run_applies_a_host_name_entity_to_its_address() {
        let peer = tokio::net::lookup_host("localhost:0")
            .await
            .expect("localhost resolves")
            .next()
            .expect("localhost has an address")
            .ip()
            .to_canonical();
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("ip".into(), Some(peer.to_string()))];
        let b = buckets.get_or_create("connection_creation_rate", &key, 1.0);
        let img = img_with_quota(QuotaRecordSetup {
            entity: vec![("ip", Some("localhost"))],
            key: "connection_creation_rate",
            value: QuotaValue(3.0),
        });
        let (_tx, rx) = watch::channel(img);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(rx, Arc::clone(&buckets), shutdown.clone()));

        let mut rate = b.byte_rate();
        for _ in 0..200 {
            rate = b.byte_rate();
            if rate == bucket_rate(3.0) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        shutdown.cancel();
        task.await.expect("the refresh task stops on cancellation");

        assert!(rate == bucket_rate(3.0));
    }

    /// A resolver a test scripts: it fails its first `failures` lookups and then
    /// answers `address`, takes `delay` to answer either way, and counts the
    /// lookups and the most that ran at once.
    struct Resolver {
        failures: usize,
        address: IpAddr,
        delay: Duration,
        calls: AtomicUsize,
        running: AtomicUsize,
        most_running: AtomicUsize,
    }

    impl Resolver {
        fn new(failures: usize, delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                failures,
                address: IpAddr::from([10, 0, 0, 9]),
                delay,
                calls: AtomicUsize::new(0),
                running: AtomicUsize::new(0),
                most_running: AtomicUsize::new(0),
            })
        }

        async fn lookup(&self, _host: String) -> Option<IpAddr> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            self.most_running.fetch_max(running, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            self.running.fetch_sub(1, Ordering::SeqCst);
            (call >= self.failures).then_some(self.address)
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    /// The refresh task running against `resolver`, with a bucket of the
    /// address `resolver` answers, made at the rate 1.
    struct Running {
        images: watch::Sender<Arc<MetadataImage>>,
        bucket: Arc<TokenBucket>,
        shutdown: CancellationToken,
        task: tokio::task::JoinHandle<()>,
    }

    impl Running {
        fn start(image: Arc<MetadataImage>, resolver: &Arc<Resolver>) -> Self {
            let buckets = Arc::new(QuotaBuckets::new());
            let key: EntityKey = vec![("ip".into(), Some(resolver.address.to_string()))];
            let bucket = buckets.get_or_create("connection_creation_rate", &key, 1.0);
            let (images, rx) = watch::channel(image);
            let shutdown = CancellationToken::new();
            let resolver = Arc::clone(resolver);
            let task = tokio::spawn(run_with(rx, buckets, shutdown.clone(), move |host| {
                let resolver = Arc::clone(&resolver);
                async move { resolver.lookup(host).await }
            }));
            Self {
                images,
                bucket,
                shutdown,
                task,
            }
        }

        async fn stop(self) {
            self.shutdown.cancel();
            self.task
                .await
                .expect("the refresh task stops on cancellation");
        }
    }

    fn db_image() -> Arc<MetadataImage> {
        img_with_quota(QuotaRecordSetup {
            entity: vec![("ip", Some("db"))],
            key: "connection_creation_rate",
            value: QuotaValue(3.0),
        })
    }

    fn other_image() -> Arc<MetadataImage> {
        img_with_quota(QuotaRecordSetup {
            value: QuotaValue(1.0),
            ..Default::default()
        })
    }

    /// A host name whose lookup failed is looked up again when its wait is
    /// over, with no image to prompt it: one lookup at the start, then after
    /// 1 s and after 2 s more (the wait doubles), and none once it answers.
    /// Before, the lookup ran only when an image arrived, so a transient
    /// resolver failure on a quiet cluster left the address unenforced.
    #[tokio::test(start_paused = true)]
    async fn run_looks_a_failed_host_up_again_without_another_image() {
        let resolver = Resolver::new(2, Duration::ZERO);
        let running = Running::start(db_image(), &resolver);

        let mut seen = Vec::new();
        // (how long to let the clock run, then what to expect)
        for step in [500, 1_000, 2_000, 600_000] {
            tokio::time::sleep(Duration::from_millis(step)).await;
            seen.push((resolver.calls(), running.bucket.byte_rate()));
        }
        running.stop().await;

        assert2::check!(
            seen == vec![
                (1, bucket_rate(0.0)),
                (2, bucket_rate(0.0)),
                (3, bucket_rate(3.0)),
                (3, bucket_rate(3.0)),
            ]
        );
    }

    /// Images that arrive while a lookup runs, or while its name waits out a
    /// failure, start no lookup of that name: one runs at a time, and the
    /// retry comes at the end of the wait and not before.
    #[tokio::test(start_paused = true)]
    async fn images_do_not_start_a_lookup_of_a_host_that_is_running_or_waiting() {
        let resolver = Resolver::new(usize::MAX, Duration::from_secs(3));
        let running = Running::start(db_image(), &resolver);

        // While the first lookup runs, ten more images arrive.
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            running
                .images
                .send(db_image())
                .expect("the task is running");
        }
        let mut calls = Vec::new();
        // The lookup ends at 3 s and the retry is due at 4 s.
        for step in [2_200, 300, 1_000] {
            tokio::time::sleep(Duration::from_millis(step)).await;
            running
                .images
                .send(db_image())
                .expect("the task is running");
            calls.push(resolver.calls());
        }
        running.stop().await;

        assert2::check!(
            (calls, resolver.most_running.load(Ordering::SeqCst)) == (vec![1, 1, 2], 1)
        );
    }

    /// A failed host whose entity has left the image is not looked up again,
    /// however long the task waits, and a record that puts it back is looked up
    /// at once.
    #[tokio::test(start_paused = true)]
    async fn a_failed_host_whose_entity_is_gone_is_not_retried() {
        let resolver = Resolver::new(usize::MAX, Duration::ZERO);
        let running = Running::start(db_image(), &resolver);
        tokio::time::sleep(Duration::from_millis(500)).await;
        let first = resolver.calls();

        running
            .images
            .send(other_image())
            .expect("the task is running");
        tokio::time::sleep(Duration::from_secs(3_600)).await;
        let while_gone = resolver.calls();

        running
            .images
            .send(db_image())
            .expect("the task is running");
        tokio::time::sleep(Duration::from_millis(1)).await;
        let put_back = resolver.calls();
        running.stop().await;

        assert2::check!((first, while_gone, put_back) == (1, 1, 2));
    }

    /// An image change sets a byte-rate bucket to the configured rate,
    /// fractional part included: Kafka holds the quota as a double, and
    /// neither rounds it to a whole byte per second nor to no limit.
    #[test]
    fn refresh_keeps_a_fractional_byte_rate() {
        let cases: [(&str, f64, f64); 6] = [
            ("consumer_byte_rate", 2048.0, 2048.0),
            ("consumer_byte_rate", 1.5, 1.5),
            ("consumer_byte_rate", 0.5, 0.5),
            ("producer_byte_rate", 2048.0, 2048.0),
            ("producer_byte_rate", 1.5, 1.5),
            ("producer_byte_rate", 0.5, 0.5),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (quota_key, rate, want) in cases {
            let (buckets, b) = alice_bucket(UserBucketSetup {
                key: quota_key,
                rate: QuotaValue(1024.0),
            });

            let img = img_with_quota(QuotaRecordSetup {
                key: quota_key,
                value: QuotaValue(rate),
                ..Default::default()
            });
            refresh_buckets(&img, &buckets);
            actual.push((quota_key, rate.to_string(), b.byte_rate()));
            expected.push((quota_key, rate.to_string(), bucket_rate(want)));
        }
        assert!(actual == expected);
    }

    /// A bucket is shared by every client that resolves to its key, so a
    /// refresh rates it from that key and not from the client that created it
    /// (#1213). Adding a quota to a more specific or an overlapping level
    /// changes what the creating client resolves to, and used to change the
    /// rate every other client of the bucket draws on.
    #[test]
    fn refresh_rates_a_shared_bucket_by_its_own_key() {
        use crate::quota::test_support::{image_with_quotas, quota_record};

        let user: EntityKey = vec![("user".into(), Some("alice".into()))];
        let client: EntityKey = vec![("client-id".into(), Some("app1".into()))];
        let pair: EntityKey = vec![
            ("client-id".into(), Some("app1".into())),
            ("user".into(), Some("alice".into())),
        ];
        let alice = || quota_record(QuotaRecordSetup::default());
        let app1 = || app1_producer_quota();
        let alice_app1 = || alice_app1_producer_quota();
        // (label, bucket key, quotas the bucket was made under, quotas after
        // the alter, the rate the bucket keeps)
        let cases = [
            (
                "the user bucket, after the creating client got a pair quota",
                user.clone(),
                vec![alice()],
                vec![alice(), alice_app1()],
                1_000.0,
            ),
            (
                "the client bucket, after the creating client's user got a quota",
                client,
                vec![app1()],
                vec![app1(), alice()],
                200.0,
            ),
            (
                "the pair bucket, after its user got a quota",
                pair,
                vec![alice_app1()],
                vec![alice_app1(), alice()],
                5_000.0,
            ),
            (
                "the user bucket, after a client quota appeared",
                user,
                vec![alice()],
                vec![alice(), app1()],
                1_000.0,
            ),
        ];
        for (label, key, before, after, expected) in cases {
            let buckets = QuotaBuckets::new();
            let bucket = buckets.get_or_create(
                "producer_byte_rate",
                &key,
                configured_rate(
                    &image_with_quotas(before),
                    buckets.ip_names(),
                    "producer_byte_rate",
                    &key,
                )
                .expect("the bucket was made under a quota"),
            );

            refresh_buckets(&image_with_quotas(after), &buckets);

            assert2::check!(bucket.byte_rate() == bucket_rate(expected), "{label}");
        }
    }

    /// Re-applying a rate finer than the bucket stores does not reset the
    /// bucket: a reset refills it to its burst, which would hand a drained
    /// client a fresh window on every image change.
    #[test]
    fn refresh_does_not_refill_a_bucket_whose_rate_is_unchanged() {
        let rate = 0.123_456_7;
        let buckets = Arc::new(QuotaBuckets::with_window(krabka_units::secs(10)));
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, rate);
        let drained = b.try_consume(5);

        refresh_buckets(
            &img_with_quota(QuotaRecordSetup {
                value: QuotaValue(rate),
                ..Default::default()
            }),
            &buckets,
        );

        assert!((drained, b.try_consume(1)) == (1, 0));
    }
}
