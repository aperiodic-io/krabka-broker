//! Which `ip` quota entity names stand for which peer address.
//!
//! Kafka resolves the name of an `ip` entity to an `InetAddress` when it
//! applies the quota record (`ClientQuotaMetadataManager.handleIpQuota` calls
//! `InetAddress.getByName`), and enforces the quota on every connection whose
//! peer address equals the result: `SocketServer` keeps `connectionRatePerIp`
//! keyed by `InetAddress` and reads it with `socket.getInetAddress`. So
//! `localhost`, `127.1`, `0:0:0:0:0:0:0:1` and an upper-case IPv6 spelling all
//! enforce, and an IPv4-mapped peer is an `Inet4Address` (#1214).
//!
//! `AlterClientQuotas` stores the name as the operator wrote it, and
//! `DescribeClientQuotas` reports it back that way, so the image keeps
//! `localhost` and not the address it resolved to. This module is the
//! resolution Kafka does at apply time: an IP literal in any spelling
//! resolves on the spot. A host name resolves off the request path, when the
//! quota refresh task sees it and no lookup of it is running. A lookup is
//! bounded by a timeout, and a name that fails is held back for a wait that
//! grows with each failure, so a resolver that is down is not asked again on
//! every image. The refresh task asks again when the wait is over
//! ([`IpNames::next_retry`]) and does not need another image to do it: Kafka
//! resolves once, so a name that failed must not stay unenforced until the
//! next metadata change.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    future::Future,
    net::IpAddr,
    sync::{Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use krabka_metadata::MetadataImage;
use tokio::time::Instant;

/// The address an IP literal names, with an IPv4-mapped IPv6 address as the
/// IPv4 address it maps to, as Java's `InetAddress.getByName` returns it.
/// `None` for anything that is not a literal, such as a host name.
///
/// A bracketed IPv6 literal such as `[::1]` is a literal, as it is to Java,
/// and so is an IPv4 address of one to three parts, such as `127.1`, which
/// Java reads the way `inet_aton` does. Reading them here and not through the
/// resolver keeps the answer the same on every platform: Windows' resolver
/// refuses `127.1`.
#[must_use]
pub fn parse_ip_literal(name: &str) -> Option<IpAddr> {
    let bare = name
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .unwrap_or(name);
    bare.parse::<IpAddr>()
        .ok()
        .or_else(|| short_ipv4(bare).map(IpAddr::V4))
        .map(|addr| addr.to_canonical())
}

/// An IPv4 address of one to four decimal parts, as Java's
/// `IPAddressUtil.textToNumericFormatV4` reads it: the last part fills the
/// bytes that the parts before it leave, so `127.1` is `127.0.0.1` and a lone
/// `2130706433` is too.
fn short_ipv4(text: &str) -> Option<std::net::Ipv4Addr> {
    let parts = text
        .split('.')
        .map(|part| {
            (!part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
                .then(|| part.parse::<u64>().ok())
                .flatten()
        })
        .collect::<Option<Vec<u64>>>()?;
    let (last, leading) = parts.split_last()?;
    if leading.len() > 3 || leading.iter().any(|part| *part > 0xff) {
        return None;
    }
    // Each leading part is one byte; the last part fills what is left.
    let last_bytes = 4 - leading.len();
    if *last >> (8 * last_bytes) != 0 {
        return None;
    }
    let address = leading
        .iter()
        .fold(0_u64, |address, part| (address << 8) | part)
        << (8 * last_bytes)
        | last;
    u32::try_from(address).ok().map(std::net::Ipv4Addr::from)
}

/// The resolved host names of the image's `ip` entities, and the entity names
/// that stand for each address.
#[derive(Debug, Default)]
pub struct IpNames {
    inner: Mutex<Resolved>,
}

#[derive(Debug, Default)]
struct Resolved {
    /// Host names resolved so far, each with the address it resolved to.
    hosts: HashMap<String, IpAddr>,
    /// The `ip` entity names of the last image that stand for each address.
    names: HashMap<IpAddr, BTreeSet<String>>,
    /// The host names a lookup is running for, so that an image that arrives
    /// while one runs does not start a second.
    in_flight: HashSet<String>,
    /// The host names whose last lookup failed, with the earliest time the next
    /// one may start.
    failed: HashMap<String, Failure>,
}

/// How long one host-name lookup may run before it counts as failed.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// The wait after the first failed lookup of a host name. Each further failure
/// doubles it, up to [`RETRY_MAX`].
const RETRY_BASE: Duration = Duration::from_secs(1);

/// The longest wait between two lookups of a host name that does not resolve.
const RETRY_MAX: Duration = Duration::from_secs(300);

/// A host name that did not resolve: how often, and when it may be tried again.
#[derive(Debug, Clone, Copy)]
struct Failure {
    attempts: u32,
    retry_at: Instant,
}

impl Failure {
    /// The failure of a lookup that ended at `now`, after `previous`.
    fn after(previous: Option<Self>, now: Instant) -> Self {
        let attempts = previous.map_or(1, |failure| failure.attempts.saturating_add(1));
        let doublings = 1_u32
            .checked_shl(attempts.saturating_sub(1))
            .unwrap_or(u32::MAX);
        Self {
            attempts,
            retry_at: now + RETRY_BASE.saturating_mul(doublings).min(RETRY_MAX),
        }
    }
}

/// The first address the system resolver returns for `host`, as
/// `InetAddress.getByName` does.
pub(super) async fn system_lookup(host: String) -> Option<IpAddr> {
    let addrs = krabka_client_core::transport::resolve(&format!("{host}:0"))
        .await
        .ok()?;
    addrs.first().map(|addr| addr.ip().to_canonical())
}

impl IpNames {
    fn lock(&self) -> MutexGuard<'_, Resolved> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The entity names of the last [`Self::update`] that stand for `peer`, in
    /// name order. `peer` is canonical.
    #[must_use]
    pub(super) fn entity_names(&self, peer: IpAddr) -> Vec<String> {
        self.lock()
            .names
            .get(&peer)
            .map(|names| names.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Rebuilds the address index from `image`'s `ip` entities, and forgets
    /// the hosts whose entity is gone, so that a record applied again resolves
    /// again, as Kafka resolves it on every apply. Returns the host names of
    /// the image that have no address yet, for [`Self::resolve`].
    pub(super) fn update(&self, image: &MetadataImage) -> Vec<String> {
        let mut resolved = self.lock();
        let mut by_address: HashMap<IpAddr, BTreeSet<String>> = HashMap::new();
        let mut in_image = HashSet::new();
        let mut unresolved = Vec::new();
        for key in image.client_quotas().keys() {
            let [(entity_type, Some(name))] = key.as_slice() else {
                continue;
            };
            if entity_type != "ip" {
                continue;
            }
            in_image.insert(name.as_str());
            match parse_ip_literal(name).or_else(|| resolved.hosts.get(name).copied()) {
                Some(addr) => {
                    by_address.entry(addr).or_default().insert(name.clone());
                }
                None => unresolved.push(name.clone()),
            }
        }
        resolved
            .hosts
            .retain(|host, _| in_image.contains(host.as_str()));
        resolved
            .failed
            .retain(|host, _| in_image.contains(host.as_str()));
        resolved.names = by_address;
        unresolved
    }

    /// Takes the host names of `unresolved` that a lookup should start for at
    /// `now`, and marks them as running until [`Self::resolve`] has been through
    /// them. A name that a lookup is already running for, or that failed too
    /// recently to try again, is left out, so a stream of images neither
    /// stacks lookups of one name nor asks a resolver that is down again on
    /// every image.
    pub(super) fn claim(&self, unresolved: Vec<String>, now: Instant) -> Vec<String> {
        let mut resolved = self.lock();
        let mut claimed = Vec::new();
        for host in unresolved {
            let backing_off = resolved
                .failed
                .get(&host)
                .is_some_and(|failure| now < failure.retry_at);
            if !backing_off && resolved.in_flight.insert(host.clone()) {
                claimed.push(host);
            }
        }
        claimed
    }

    /// The earliest time a host name that failed may be looked up again, for
    /// the names of the last [`Self::update`]'s image. `None` when no name is
    /// waiting, and then nothing needs to wake up to look one up.
    pub(super) fn next_retry(&self) -> Option<Instant> {
        self.lock()
            .failed
            .values()
            .map(|failure| failure.retry_at)
            .min()
    }

    /// Resolves each host name that [`Self::claim`] returned with `lookup`, and
    /// remembers the address for the next [`Self::update`]. A lookup that fails
    /// or takes longer than [`LOOKUP_TIMEOUT`] leaves the name out and holds it
    /// back for a wait that doubles with each failure.
    pub(super) async fn resolve<Lookup, Found>(&self, hosts: &[String], lookup: Lookup)
    where
        Lookup: Fn(String) -> Found,
        Found: Future<Output = Option<IpAddr>>,
    {
        self.resolve_with(hosts, LOOKUP_TIMEOUT, lookup).await;
    }

    async fn resolve_with<Lookup, Found>(&self, hosts: &[String], timeout: Duration, lookup: Lookup)
    where
        Lookup: Fn(String) -> Found,
        Found: Future<Output = Option<IpAddr>>,
    {
        for host in hosts {
            let found = tokio::time::timeout(timeout, lookup(host.clone()))
                .await
                .ok()
                .flatten();
            let mut resolved = self.lock();
            resolved.in_flight.remove(host);
            if let Some(addr) = found {
                resolved.failed.remove(host);
                resolved.hosts.insert(host.clone(), addr);
            } else {
                let failure = Failure::after(resolved.failed.get(host).copied(), Instant::now());
                resolved.failed.insert(host.clone(), failure);
            }
        }
    }

    /// Records that `host` resolved to `addr`, for a test that needs a host
    /// name to stand for an address without asking a resolver.
    #[cfg(test)]
    pub(super) fn insert_host(&self, host: &str, addr: IpAddr) {
        self.lock().hosts.insert(host.to_owned(), addr);
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::quota::test_support::{image_with_quota, image_with_quotas, quota_record};

    /// An IP literal names one address whatever its spelling, and an
    /// IPv4-mapped IPv6 address is the IPv4 address it maps to.
    #[test]
    fn a_literal_is_the_canonical_address_of_any_spelling() {
        let v4 = |a: u8, b: u8, c: u8, d: u8| Some(IpAddr::from([a, b, c, d]));
        let cases = [
            ("127.0.0.1", v4(127, 0, 0, 1)),
            ("::1", Some(IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1]))),
            (
                "0:0:0:0:0:0:0:1",
                Some(IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1])),
            ),
            ("[::1]", Some(IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1]))),
            (
                "2001:DB8::A",
                Some(IpAddr::from([0x2001, 0xdb8, 0, 0, 0, 0, 0, 0xa])),
            ),
            ("::ffff:10.1.2.3", v4(10, 1, 2, 3)),
            ("::FFFF:A01:203", v4(10, 1, 2, 3)),
            // Java reads one to three parts the way `inet_aton` does.
            ("127.1", v4(127, 0, 0, 1)),
            ("10.1.515", v4(10, 1, 2, 3)),
            ("2130706433", v4(127, 0, 0, 1)),
            ("10.0.0.255", v4(10, 0, 0, 255)),
            ("1.16777215", v4(1, 255, 255, 255)),
            ("1.16777216", None),
            ("256.1", None),
            ("10.1.65536", None),
            ("4294967296", None),
            ("1.2.3.4.5", None),
            ("1..2", None),
            ("1.2.", None),
            ("-1", None),
            ("localhost", None),
            ("kafka.example.com", None),
            ("", None),
        ];
        for (name, want) in cases {
            check!(parse_ip_literal(name) == want, "{name:?}");
        }
    }

    /// The index maps each address to the entity names that stand for it, and
    /// a host name stands for its address once it has resolved.
    #[test]
    fn update_indexes_literals_and_resolved_hosts() {
        let names = IpNames::default();
        let image = image_with_quotas(vec![
            quota_record(crate::quota::test_support::QuotaRecordSetup {
                entity: vec![("ip", Some("0:0:0:0:0:0:0:1"))],
                key: "connection_creation_rate",
                value: crate::quota::test_support::QuotaValue(1.0),
            }),
            quota_record(crate::quota::test_support::QuotaRecordSetup {
                entity: vec![("ip", Some("::1"))],
                key: "connection_creation_rate",
                value: crate::quota::test_support::QuotaValue(2.0),
            }),
            quota_record(crate::quota::test_support::QuotaRecordSetup {
                entity: vec![("ip", Some("db"))],
                key: "connection_creation_rate",
                value: crate::quota::test_support::QuotaValue(3.0),
            }),
            quota_record(crate::quota::test_support::QuotaRecordSetup {
                entity: vec![("ip", None)],
                key: "connection_creation_rate",
                value: crate::quota::test_support::QuotaValue(4.0),
            }),
            quota_record(crate::quota::test_support::QuotaRecordSetup {
                value: crate::quota::test_support::QuotaValue(5.0),
                ..Default::default()
            }),
        ]);
        let localhost = IpAddr::from([0, 0, 0, 0, 0, 0, 0, 1]);
        let db = IpAddr::from([10, 0, 0, 9]);

        let unresolved = names.update(&image);
        names.insert_host("db", db);
        let after_resolution = names.update(&image);

        check!(
            (
                unresolved,
                after_resolution,
                names.entity_names(localhost),
                names.entity_names(db),
                names.entity_names(IpAddr::from([10, 0, 0, 1])),
            ) == (
                vec!["db".to_owned()],
                Vec::<String>::new(),
                vec!["0:0:0:0:0:0:0:1".to_owned(), "::1".to_owned()],
                vec!["db".to_owned()],
                Vec::<String>::new(),
            )
        );
    }

    /// A host whose entity is gone is forgotten, so a record applied again
    /// resolves again.
    #[test]
    fn update_forgets_a_host_whose_entity_is_gone() {
        let names = IpNames::default();
        let with_host = image_with_quota(crate::quota::test_support::QuotaRecordSetup {
            entity: vec![("ip", Some("db"))],
            key: "connection_creation_rate",
            value: crate::quota::test_support::QuotaValue(1.0),
        });
        let without = image_with_quota(crate::quota::test_support::QuotaRecordSetup {
            entity: vec![("ip", Some("other"))],
            key: "connection_creation_rate",
            value: crate::quota::test_support::QuotaValue(1.0),
        });
        let db = IpAddr::from([10, 0, 0, 9]);

        names.insert_host("db", db);
        let _ = names.update(&without);
        let unresolved_again = names.update(&with_host);

        check!((unresolved_again, names.entity_names(db)) == (vec!["db".to_owned()], vec![]));
    }

    /// A host name resolves through the system resolver to an address the
    /// resolver itself lists for it. `localhost` is in every hosts file.
    #[tokio::test]
    async fn resolve_maps_a_host_name_to_an_address_the_resolver_returns() {
        let names = IpNames::default();
        let image = image_with_quota(crate::quota::test_support::QuotaRecordSetup {
            entity: vec![("ip", Some("localhost"))],
            key: "connection_creation_rate",
            value: crate::quota::test_support::QuotaValue(1.0),
        });
        let unresolved = names.update(&image);
        names
            .resolve(
                &names.claim(unresolved.clone(), Instant::now()),
                system_lookup,
            )
            .await;
        let unresolved_after = names.update(&image);

        let listed: Vec<IpAddr> = tokio::net::lookup_host("localhost:0")
            .await
            .expect("localhost resolves")
            .map(|addr| addr.ip().to_canonical())
            .collect();
        check!(unresolved == vec!["localhost".to_owned()]);
        check!(unresolved_after.is_empty());
        check!(
            listed
                .iter()
                .any(|addr| names.entity_names(*addr) == vec!["localhost".to_owned()])
        );
    }

    fn host_image(name: &str) -> MetadataImage {
        image_with_quota(crate::quota::test_support::QuotaRecordSetup {
            entity: vec![("ip", Some(name))],
            key: "connection_creation_rate",
            value: crate::quota::test_support::QuotaValue(1.0),
        })
    }

    fn db() -> Vec<String> {
        vec!["db".to_owned()]
    }

    /// Runs a lookup of `hosts` that never answers, with a short timeout, and
    /// fails the test if the timeout does not cut it off.
    async fn hang_until_timeout(names: &IpNames, hosts: &[String]) {
        tokio::time::timeout(
            Duration::from_secs(5),
            names.resolve_with(hosts, Duration::from_millis(10), |_host| {
                std::future::pending::<Option<IpAddr>>()
            }),
        )
        .await
        .expect("the lookup is cut off at its timeout");
    }

    /// An image that arrives while a lookup of a name runs does not start a
    /// second: the name is claimed once until `resolve` has been through it.
    #[test]
    fn a_host_being_looked_up_is_not_claimed_again() {
        let names = IpNames::default();
        let now = Instant::now();

        let first = names.claim(vec!["db".to_owned(), "cache".to_owned()], now);
        let second = names.claim(vec!["db".to_owned(), "other".to_owned()], now);

        check!(
            (first, second)
                == (
                    vec!["db".to_owned(), "cache".to_owned()],
                    vec!["other".to_owned()]
                )
        );
    }

    /// A lookup that never answers is cut off at the timeout and releases its
    /// claim, and the name is held back afterwards: a resolver that hangs
    /// costs one lookup per wait, not one per image, and the next lookup is
    /// allowed once the wait is over.
    #[tokio::test]
    async fn a_lookup_that_times_out_holds_the_name_back() {
        let names = IpNames::default();
        let claimed = names.claim(db(), Instant::now());
        hang_until_timeout(&names, &claimed).await;

        let straight_after = names.claim(db(), Instant::now());
        let after_the_longest_wait = names.claim(db(), Instant::now() + RETRY_MAX + RETRY_BASE);

        check!((claimed, straight_after, after_the_longest_wait) == (db(), vec![], db()));
    }

    /// The wait after a failed lookup doubles with each failure, from
    /// [`RETRY_BASE`] up to [`RETRY_MAX`], and a count that overflows the
    /// shift stays at the cap.
    #[test]
    fn the_wait_after_a_failure_doubles_up_to_the_cap() {
        let t = Instant::now();
        // (attempts already failed, attempts after, wait after)
        for (before, attempts, wait_secs) in [
            (None, 1, 1),
            (Some(1), 2, 2),
            (Some(2), 3, 4),
            (Some(8), 9, 256),
            (Some(9), 10, 300),
            (Some(100), 101, 300),
            (Some(u32::MAX), u32::MAX, 300),
        ] {
            let previous = before.map(|attempts| Failure {
                attempts,
                retry_at: t,
            });

            let next = Failure::after(previous, t);

            check!(
                (next.attempts, next.retry_at - t) == (attempts, Duration::from_secs(wait_secs)),
                "after {before:?}"
            );
        }
    }

    /// A lookup that answers records the address and clears the name's
    /// failures, and one that failed before is looked up again once its wait
    /// has passed.
    #[tokio::test]
    async fn a_lookup_that_answers_after_a_failure_resolves_the_name() {
        let names = IpNames::default();
        let image = host_image("db");
        let address = IpAddr::from([10, 0, 0, 9]);
        let unresolved = names.update(&image);
        let claimed = names.claim(unresolved, Instant::now());
        hang_until_timeout(&names, &claimed).await;

        let retry = names.claim(db(), Instant::now() + RETRY_MAX + RETRY_BASE);
        names
            .resolve_with(&retry, Duration::from_secs(1), |_host| async move {
                Some(address)
            })
            .await;
        let unresolved_after = names.update(&image);

        check!(unresolved_after.is_empty());
        check!(names.entity_names(address) == db());
    }

    /// The next retry is the earliest wait among the names of the image, and
    /// there is none once no failed name is left in the image.
    #[tokio::test(start_paused = true)]
    async fn the_next_retry_is_the_earliest_wait_of_a_name_in_the_image() {
        let names = IpNames::default();
        let both = image_with_quotas(vec![
            quota_record(crate::quota::test_support::QuotaRecordSetup {
                entity: vec![("ip", Some("a"))],
                key: "connection_creation_rate",
                value: crate::quota::test_support::QuotaValue(1.0),
            }),
            quota_record(crate::quota::test_support::QuotaRecordSetup {
                entity: vec![("ip", Some("b"))],
                key: "connection_creation_rate",
                value: crate::quota::test_support::QuotaValue(1.0),
            }),
        ]);
        let start = Instant::now();
        let none_failed = names.next_retry();

        let a = names.claim(vec!["a".to_owned()], start);
        hang_until_timeout(&names, &a).await;
        tokio::time::advance(Duration::from_secs(10)).await;
        let b = names.claim(vec!["b".to_owned()], Instant::now());
        hang_until_timeout(&names, &b).await;
        let _ = names.update(&both);
        let earliest = names.next_retry();
        let _ = names.update(&host_image("b"));
        let after_a_left = names.next_retry();
        let _ = names.update(&host_image("c"));
        let after_both_left = names.next_retry();

        // `a` failed 10 ms after `start`, when its lookup timed out, and `b`
        // ten seconds and 10 ms after that; each waits 1 s.
        let wait = |next: Option<Instant>| next.map(|at| at - start);
        check!(
            (
                none_failed,
                wait(earliest),
                wait(after_a_left),
                after_both_left
            ) == (
                None,
                Some(Duration::from_millis(1_010)),
                Some(Duration::from_millis(11_020)),
                None
            )
        );
    }

    /// A name whose entity is gone is forgotten with its failures, so a record
    /// applied again is looked up at once, as Kafka resolves it on every
    /// apply.
    #[tokio::test]
    async fn a_failed_host_whose_entity_is_gone_is_looked_up_again_at_once() {
        let names = IpNames::default();
        let claimed = names.claim(names.update(&host_image("db")), Instant::now());
        hang_until_timeout(&names, &claimed).await;

        let _ = names.update(&host_image("other"));
        let reapplied = names.update(&host_image("db"));

        check!(names.claim(reapplied, Instant::now()) == db());
    }
}
