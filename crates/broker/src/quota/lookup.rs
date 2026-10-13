//! Quota lookup with Kafka's 8-priority entity matching.

use {
    super::IpNames,
    krabka_metadata::{EntityKey, MetadataImage},
    krabka_verified::{
        IpQuotaPrecedence, QuotaCandidatePresence, UserClientQuotaFacts, UserClientQuotaPrecedence,
        ip_quota_precedence, user_client_quota_precedence,
    },
};

/// The value of the `name` entity type (`"user"`, `"client-id"`) in `key`, or
/// `None` when `key` has no such entry or names its default entity.
pub(crate) fn entity_field(key: &EntityKey, name: &str) -> Option<String> {
    key.iter()
        .find(|(k, _)| k == name)
        .and_then(|(_, v)| v.clone())
}

/// Return the configured value for `quota_key` under the most-specific
/// matching entity for `(principal, client_id)`. First match wins, in the
/// order of Kafka's `ClientQuotaManager.DefaultQuotaCallback`:
///   1. (user=alice, client-id=app1)
///   2. (user=alice, client-id=default)
///   3. (user=alice)
///   4. (user=default, client-id=app1)
///   5. (user=default, client-id=default)
///   6. (user=default)
///   7. (client-id=app1)
///   8. (client-id=default)
///
/// All candidate keys are pre-sorted by `entity_type` ("client-id" <
/// "user" alphabetically), so the lookup runs against the image map
/// without further canonicalization.
// Disjoint from `lookup_ip_quota` (which checks `("ip", *)` candidates only).
#[must_use]
pub fn lookup_quota(
    image: &MetadataImage,
    principal: &str,
    client_id: Option<&str>,
    quota_key: &str,
) -> Option<f64> {
    lookup_quota_with_key(image, principal, client_id, quota_key).map(|(_, v)| v)
}

/// Like `lookup_quota`, but it also returns the canonical entity key
/// that matched. Enforcement code uses this to bind the lookup to a
/// bucket in `QuotaBuckets`.
///
/// The bucket key follows Kafka's metric tags: levels 1, 2, 4 and 5 share one
/// sensor per `(user, client-id)`, levels 3 and 6 one per `user`, and levels
/// 7 and 8 one per `client-id`.
///
/// An empty client id skips every level with a client-id component, and an
/// empty principal every level with a user component, as Kafka's
/// `DefaultQuotaCallback.findQuota` does: with no client id only the user
/// levels 3 and 6 can match, so a default client quota never throttles a
/// client that sends no `client.id` (#1241). With neither, nothing matches.
///
/// `client_id` is `None` when the request header's client id is null, which is
/// not an empty client id: see `lookup_null_client_quota`.
#[must_use]
pub fn lookup_quota_with_key(
    image: &MetadataImage,
    principal: &str,
    client_id: Option<&str>,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    let Some(client_id) = client_id else {
        return lookup_null_client_quota(image, principal, quota_key);
    };
    let levels = match (!principal.is_empty(), !client_id.is_empty()) {
        (true, true) => Levels::ALL,
        (true, false) => Levels::USER,
        (false, true) => Levels::CLIENT,
        (false, false) => return None,
    };
    let LevelRate {
        precedence: selected,
        rate,
    } = resolve_levels(
        image,
        RequestIdentity {
            principal,
            client_id,
        },
        quota_key,
        levels,
    )?;
    Some((bucket_key(selected, principal, client_id), rate))
}

/// The quota of a request whose client id is null, as Kafka resolves it.
///
/// `DefaultQuotaCallback.quotaMetricTags` walks the levels from the most
/// specific and takes the tags of the first one that has a quota. A `user`
/// level (3 or 6) takes an empty client-id tag, which `quotaLimit` resolves
/// through the user levels. A pair level (2 or 5) keeps the client id, and
/// `quotaLimit` returns no quota for a null client-id tag. So the first
/// configured level of `(user, <default>)`, `user`, `(<default>, <default>)`
/// and `<default>` user decides, and only a user level throttles. The levels
/// that name a client id never match a null one. An empty principal has no
/// user level either (#1241).
fn lookup_null_client_quota(
    image: &MetadataImage,
    principal: &str,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    if principal.is_empty() {
        return None;
    }
    let pair = |user: Option<&str>| -> EntityKey {
        vec![
            ("client-id".into(), None),
            ("user".into(), user.map(Into::into)),
        ]
    };
    let user = |user: Option<&str>| -> EntityKey { vec![("user".into(), user.map(Into::into))] };
    let (throttles, rate) = [
        (pair(Some(principal)), false),
        (user(Some(principal)), true),
        (pair(None), false),
        (user(None), true),
    ]
    .into_iter()
    .find_map(|(key, throttles)| {
        quota_for_key(image, &key, quota_key).map(|rate| (throttles, rate))
    })?;
    throttles.then(|| (user(Some(principal)), rate))
}

/// The rate the image configures for the quota sensor a bucket key names.
///
/// Kafka re-rates each sensor from its own metric tags
/// (`ClientQuotaManager.updateQuotaMetricConfigs` calls `quotaLimit` on the
/// sensor's tags), and `findQuota` resolves a `(user, client-id)` sensor
/// against the four pair levels only, a `user` sensor against the two user
/// levels and a `client-id` sensor against the two client levels. A bucket
/// keyed `[user=alice]` that the client `(alice, app1)` created is therefore
/// re-rated by the user levels, whatever quota `(alice, app1)` itself has now
/// (#1213). A key that is none of the three shapes has no quota.
#[must_use]
pub(super) fn lookup_bucket_rate(
    image: &MetadataImage,
    entity_key: &EntityKey,
    quota_key: &str,
) -> Option<f64> {
    let (principal, client_id, levels) = match entity_key.as_slice() {
        [(client_type, Some(client_id)), (user_type, Some(principal))]
            if client_type == "client-id" && user_type == "user" =>
        {
            (principal.as_str(), client_id.as_str(), Levels::PAIR)
        }
        [(user_type, Some(principal))] if user_type == "user" => {
            (principal.as_str(), "", Levels::USER)
        }
        [(client_type, Some(client_id))] if client_type == "client-id" => {
            ("", client_id.as_str(), Levels::CLIENT)
        }
        _ => return None,
    };
    resolve_levels(
        image,
        RequestIdentity {
            principal,
            client_id,
        },
        quota_key,
        levels,
    )
    .map(|level| level.rate)
}

/// Which of the eight levels of [`lookup_quota_with_key`] a lookup consults,
/// by the level's index in its candidate list.
#[derive(Clone, Copy)]
struct Levels([bool; 8]);

impl Levels {
    /// Every level: a request that has both a user and a client id. Kafka
    /// picks its sensor by the highest level that is set, then resolves the
    /// limit from that sensor's tags, which is this precedence.
    const ALL: Self = Self([true; 8]);
    /// `findClientQuota`: `client-id` and `<default>` client-id.
    const CLIENT: Self = Self([false, false, false, false, false, false, true, true]);
    /// `findUserClientQuota`: `(user, client-id)`, `(user, <default>)`,
    /// `(<default>, client-id)` and `(<default>, <default>)`.
    const PAIR: Self = Self([true, true, false, true, true, false, false, false]);
    /// `findUserQuota`: `user` and `<default>` user.
    const USER: Self = Self([false, false, true, false, false, true, false, false]);
}

/// The principal and client id a lookup resolves levels for. An empty one
/// stands for a component the request does not have.
#[derive(Clone, Copy)]
struct RequestIdentity<'a> {
    principal: &'a str,
    client_id: &'a str,
}

/// The level a lookup matched and the value configured there.
struct LevelRate {
    precedence: UserClientQuotaPrecedence,
    rate: f64,
}

/// The highest-precedence level of `levels` that has `quota_key` set for
/// `(principal, client_id)`, with its value.
fn resolve_levels(
    image: &MetadataImage,
    RequestIdentity {
        principal,
        client_id,
    }: RequestIdentity<'_>,
    quota_key: &str,
    levels: Levels,
) -> Option<LevelRate> {
    let candidates: [EntityKey; 8] = [
        vec![
            ("client-id".into(), Some(client_id.into())),
            ("user".into(), Some(principal.into())),
        ],
        vec![
            ("client-id".into(), None),
            ("user".into(), Some(principal.into())),
        ],
        vec![("user".into(), Some(principal.into()))],
        vec![
            ("client-id".into(), Some(client_id.into())),
            ("user".into(), None),
        ],
        vec![("client-id".into(), None), ("user".into(), None)],
        vec![("user".into(), None)],
        vec![("client-id".into(), Some(client_id.into()))],
        vec![("client-id".into(), None)],
    ];
    let mut values: [Option<f64>; 8] = [None; 8];
    for (index, key) in candidates.into_iter().enumerate() {
        if levels.0[index] {
            values[index] = quota_for_key(image, &key, quota_key);
        }
    }
    let present = |index: usize| {
        if values[index].is_some() {
            QuotaCandidatePresence::Present
        } else {
            QuotaCandidatePresence::Absent
        }
    };
    let selected = user_client_quota_precedence(UserClientQuotaFacts {
        exact_pair: present(0),
        exact_user_default_client: present(1),
        exact_user: present(2),
        default_user_exact_client: present(3),
        default_pair: present(4),
        default_user: present(5),
        exact_client: present(6),
        default_client: present(7),
    });
    let index = match selected {
        UserClientQuotaPrecedence::ExactPair => 0,
        UserClientQuotaPrecedence::ExactUserDefaultClient => 1,
        UserClientQuotaPrecedence::ExactUser => 2,
        UserClientQuotaPrecedence::DefaultUserExactClient => 3,
        UserClientQuotaPrecedence::DefaultPair => 4,
        UserClientQuotaPrecedence::DefaultUser => 5,
        UserClientQuotaPrecedence::ExactClient => 6,
        UserClientQuotaPrecedence::DefaultClient => 7,
        UserClientQuotaPrecedence::None => return None,
    };
    values[index].map(|rate| LevelRate {
        precedence: selected,
        rate,
    })
}

/// The bucket key of the sensor a matched level charges.
fn bucket_key(selected: UserClientQuotaPrecedence, principal: &str, client_id: &str) -> EntityKey {
    match selected {
        UserClientQuotaPrecedence::ExactPair
        | UserClientQuotaPrecedence::ExactUserDefaultClient
        | UserClientQuotaPrecedence::DefaultUserExactClient
        | UserClientQuotaPrecedence::DefaultPair => vec![
            ("client-id".into(), Some(client_id.into())),
            ("user".into(), Some(principal.into())),
        ],
        UserClientQuotaPrecedence::ExactUser | UserClientQuotaPrecedence::DefaultUser => {
            vec![("user".into(), Some(principal.into()))]
        }
        UserClientQuotaPrecedence::ExactClient | UserClientQuotaPrecedence::DefaultClient => {
            vec![("client-id".into(), Some(client_id.into()))]
        }
        UserClientQuotaPrecedence::None => Vec::new(),
    }
}

/// Lookup an `ip`-scoped quota for `peer_ip`. Priority order:
///   1. (ip = an entity that stands for `peer_ip`): specific
///   2. (ip = None): default
///
/// Kafka keys an IP quota by the `InetAddress` its entity name resolves to,
/// and reads it with the connection's `socket.getInetAddress` (#1214). An
/// entity therefore stands for `peer_ip` when its name is any spelling of the
/// address, or a host name that resolved to it (see [`IpNames`]), and an
/// IPv4-mapped IPv6 peer is the IPv4 address it maps to. Both IPv4 and IPv6
/// peers match this way.
///
/// It is disjoint from `lookup_quota`, which checks only `("user", *)` and
/// `("client-id", *)` candidates. KIP-612 `connection_creation_rate`
/// enforcement uses this function.
#[must_use]
pub fn lookup_ip_quota(
    image: &MetadataImage,
    names: &IpNames,
    peer_ip: std::net::IpAddr,
    quota_key: &str,
) -> Option<f64> {
    lookup_ip_quota_with_key(image, names, peer_ip, quota_key).map(|(_, v)| v)
}

/// Like [`lookup_ip_quota`], and it also returns the bucket key: the peer's
/// canonical address, whichever entity name matched, because Kafka's
/// per-address rate sensor belongs to the address and not to the name.
#[must_use]
pub fn lookup_ip_quota_with_key(
    image: &MetadataImage,
    names: &IpNames,
    peer_ip: std::net::IpAddr,
    quota_key: &str,
) -> Option<(EntityKey, f64)> {
    let peer = peer_ip.to_canonical();
    let bucket_key: EntityKey = vec![("ip".into(), Some(peer.to_string()))];
    let ip_entity = |name: Option<String>| -> EntityKey { vec![("ip".into(), name)] };
    // The entity named by the peer's own spelling, or else one whose name
    // resolves to the peer.
    let exact = quota_for_key(image, &bucket_key, quota_key).or_else(|| {
        names
            .entity_names(peer)
            .into_iter()
            .find_map(|name| quota_for_key(image, &ip_entity(Some(name)), quota_key))
    });
    let default = quota_for_key(image, &ip_entity(None), quota_key);
    match ip_quota_precedence(exact.is_some(), default.is_some()) {
        IpQuotaPrecedence::Exact => exact,
        IpQuotaPrecedence::Default => default,
        IpQuotaPrecedence::None => None,
    }
    .map(|rate| (bucket_key, rate))
}

/// The value configured for `quota_key` under the entity `key`.
fn quota_for_key(image: &MetadataImage, key: &EntityKey, quota_key: &str) -> Option<f64> {
    image
        .client_quotas()
        .get(key)
        .and_then(|configs| configs.get(quota_key))
        .copied()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::quota::test_support::{
            QuotaEntitySpec, QuotaValue, alice_app1_producer_quota, app1_producer_quota,
            image_with_quotas, producer_quota_image, quota_record as rec,
        },
        assert2::{assert, check},
        krabka_metadata::{ClientQuotaRecord, MetadataImage},
    };

    fn img_with(records: Vec<ClientQuotaRecord>) -> MetadataImage { image_with_quotas(records) }

    /// Both identities must resolve the configured 1,024-byte producer quota.
    fn assert_alice_app1_entity(entity: QuotaEntitySpec<'_>) {
        let img = img_with(vec![rec().entity(entity).value(1024.0).call()]);
        assert!(lookup_quota(&img, "alice", Some("app1"), "producer_byte_rate") == Some(1024.0));
    }

    #[test]
    fn exact_user_client_pair_match() {
        assert_alice_app1_entity(vec![("user", Some("alice")), ("client-id", Some("app1"))]);
    }

    #[test]
    fn user_default_falls_back_to_client_specific() {
        // Only (client-id=app1) configured; user=alice should still match.
        assert_alice_app1_entity(vec![("client-id", Some("app1"))]);
    }

    #[test]
    fn single_user_match_when_no_pair_exists() {
        let img = img_with(vec![rec().value(2048.0).call()]);
        assert!(
            lookup_quota(&img, "alice", Some("anyclient"), "producer_byte_rate") == Some(2048.0)
        );
    }

    #[test]
    fn single_client_id_match_when_no_user_exists() {
        let img = img_with(vec![
            rec()
                .entity(vec![("client-id", Some("app1"))])
                .value(512.0)
                .call(),
        ]);
        assert!(lookup_quota(&img, "anyuser", Some("app1"), "producer_byte_rate") == Some(512.0));
    }

    #[test]
    fn default_user_default_client_pair() {
        let img = img_with(vec![
            rec()
                .entity(vec![("user", None), ("client-id", None)])
                .value(256.0)
                .call(),
        ]);
        assert!(lookup_quota(&img, "alice", Some("app1"), "producer_byte_rate") == Some(256.0));
    }

    #[test]
    fn default_user_alone() {
        let img = img_with(vec![rec().entity(vec![("user", None)]).value(128.0).call()]);
        assert!(lookup_quota(&img, "alice", Some("app1"), "producer_byte_rate") == Some(128.0));
    }

    #[test]
    fn default_client_alone() {
        let img = img_with(vec![
            rec().entity(vec![("client-id", None)]).value(64.0).call(),
        ]);
        assert!(lookup_quota(&img, "alice", Some("app1"), "producer_byte_rate") == Some(64.0));
    }

    #[test]
    fn no_match_returns_none() {
        let img = img_with(vec![]);
        assert!(lookup_quota(&img, "alice", Some("app1"), "producer_byte_rate") == None);
    }

    #[test]
    fn pair_specific_wins_over_user_only() {
        let img = img_with(vec![
            rec().value(8192.0).call(),
            rec()
                .entity(vec![("user", Some("alice")), ("client-id", Some("app1"))])
                .value(512.0)
                .call(),
        ]);
        assert!(lookup_quota(&img, "alice", Some("app1"), "producer_byte_rate") == Some(512.0));
    }

    type Level = Vec<(&'static str, Option<&'static str>)>;

    fn user_key(user: &str) -> EntityKey { vec![("user".into(), Some(user.into()))] }

    fn client_key(client_id: &str) -> EntityKey {
        vec![("client-id".into(), Some(client_id.into()))]
    }

    fn pair_key(user: &str, client_id: &str) -> EntityKey {
        vec![
            ("client-id".into(), Some(client_id.into())),
            ("user".into(), Some(user.into())),
        ]
    }

    /// Kafka's `findQuota` skips every level with a component the request does
    /// not have (#1241): with no client id only the two user levels can match,
    /// so a default client, a `(user, <default>)` or a `(<default>,
    /// <default>)` quota never reaches a client that sends none. With no
    /// principal the user levels are skipped, and with neither nothing
    /// matches. Each row configures some levels at 64 (the level itself is the
    /// bucket key's owner) and looks one request up.
    #[test]
    fn a_missing_client_id_or_principal_skips_the_levels_that_need_it() {
        /// A label, the configured levels, the principal, the client id and
        /// the expected bucket key and rate.
        struct Row {
            label: &'static str,
            configured: Vec<Level>,
            principal: &'static str,
            client_id: &'static str,
            expected: Option<(EntityKey, f64)>,
        }
        let default_client: Level = vec![("client-id", None)];
        let user_default_client: Level = vec![("user", Some("alice")), ("client-id", None)];
        let default_pair: Level = vec![("user", None), ("client-id", None)];
        let alice: Level = vec![("user", Some("alice"))];
        let default_user: Level = vec![("user", None)];
        let app: Level = vec![("client-id", Some("app"))];
        let default_user_app: Level = vec![("user", None), ("client-id", Some("app"))];
        let cases: Vec<Row> = vec![
            Row {
                label: "a default client quota does not throttle a client with no id",
                configured: vec![default_client.clone()],
                principal: "alice",
                client_id: "",
                expected: None,
            },
            Row {
                label: "a client id makes the same default client quota apply",
                configured: vec![default_client.clone()],
                principal: "alice",
                client_id: "app",
                expected: Some((client_key("app"), 64.0)),
            },
            Row {
                label: "a (user, default client) quota needs a client id",
                configured: vec![user_default_client],
                principal: "alice",
                client_id: "",
                expected: None,
            },
            Row {
                label: "a (default, default) quota needs a client id",
                configured: vec![default_pair],
                principal: "alice",
                client_id: "",
                expected: None,
            },
            Row {
                label: "the user quota applies without a client id",
                configured: vec![alice.clone(), default_client.clone()],
                principal: "alice",
                client_id: "",
                expected: Some((user_key("alice"), 64.0)),
            },
            Row {
                label: "the default user quota applies without a client id",
                configured: vec![default_user.clone(), default_client.clone()],
                principal: "alice",
                client_id: "",
                expected: Some((user_key("alice"), 64.0)),
            },
            Row {
                label: "the client quota applies without a principal",
                configured: vec![app.clone(), alice.clone()],
                principal: "",
                client_id: "app",
                expected: Some((client_key("app"), 64.0)),
            },
            Row {
                label: "the user levels are skipped without a principal",
                configured: vec![default_user.clone(), default_user_app, default_client],
                principal: "",
                client_id: "app",
                expected: Some((client_key("app"), 64.0)),
            },
            Row {
                label: "neither a principal nor a client id matches nothing",
                configured: vec![alice, default_user, app],
                principal: "",
                client_id: "",
                expected: None,
            },
        ];
        for Row {
            label,
            configured,
            principal,
            client_id,
            expected,
        } in cases
        {
            let img = producer_quota_image(configured);
            check!(
                lookup_quota_with_key(&img, principal, Some(client_id), "producer_byte_rate")
                    == expected,
                "{label}"
            );
        }
    }

    /// A null client id is not an empty one (#1241). Kafka's
    /// `quotaMetricTags` takes the tags of the first configured level of
    /// `(user, <default>)`, `user`, `(<default>, <default>)` and `<default>`
    /// user, and `quotaLimit` finds no quota for a pair level's null client-id
    /// tag. So a pair level shadows the user level below it, where an empty
    /// client id would have skipped the pair level and reached the user one.
    /// Each row configures some levels at 64 and looks up one request.
    #[test]
    fn a_null_client_id_meets_only_the_user_levels_a_pair_level_does_not_shadow() {
        /// A label, the configured levels, the principal and the expected
        /// bucket key and rate for a null client id and for an empty one.
        struct Row {
            label: &'static str,
            configured: Vec<Level>,
            principal: &'static str,
            null_expected: Option<(EntityKey, f64)>,
            empty_expected: Option<(EntityKey, f64)>,
        }
        let default_client: Level = vec![("client-id", None)];
        let app: Level = vec![("client-id", Some("app"))];
        let user_default_client: Level = vec![("user", Some("alice")), ("client-id", None)];
        let default_pair: Level = vec![("user", None), ("client-id", None)];
        let default_user_app: Level = vec![("user", None), ("client-id", Some("app"))];
        let alice: Level = vec![("user", Some("alice"))];
        let default_user: Level = vec![("user", None)];
        let cases: Vec<Row> = vec![
            Row {
                label: "client-id levels never match a null client id",
                configured: vec![default_client.clone(), app],
                principal: "alice",
                null_expected: None,
                empty_expected: None,
            },
            Row {
                label: "the user quota applies to a null client id",
                configured: vec![alice.clone(), default_client.clone()],
                principal: "alice",
                null_expected: Some((user_key("alice"), 64.0)),
                empty_expected: Some((user_key("alice"), 64.0)),
            },
            Row {
                label: "the default user quota applies to a null client id",
                configured: vec![default_user.clone(), default_client],
                principal: "alice",
                null_expected: Some((user_key("alice"), 64.0)),
                empty_expected: Some((user_key("alice"), 64.0)),
            },
            Row {
                label: "a (user, default client) quota shadows the user quota",
                configured: vec![user_default_client.clone(), alice.clone()],
                principal: "alice",
                null_expected: None,
                empty_expected: Some((user_key("alice"), 64.0)),
            },
            Row {
                label: "a (default, default) quota shadows the default user quota",
                configured: vec![default_pair.clone(), default_user.clone()],
                principal: "alice",
                null_expected: None,
                empty_expected: Some((user_key("alice"), 64.0)),
            },
            Row {
                label: "the user quota comes before a (default, default) quota",
                configured: vec![default_pair, alice.clone(), default_user.clone()],
                principal: "alice",
                null_expected: Some((user_key("alice"), 64.0)),
                empty_expected: Some((user_key("alice"), 64.0)),
            },
            Row {
                label: "another user's pair level shadows nothing",
                configured: vec![user_default_client, default_user_app, default_user],
                principal: "bob",
                null_expected: Some((user_key("bob"), 64.0)),
                empty_expected: Some((user_key("bob"), 64.0)),
            },
            Row {
                label: "a null client id and no principal match nothing",
                configured: vec![alice],
                principal: "",
                null_expected: None,
                empty_expected: None,
            },
        ];
        for Row {
            label,
            configured,
            principal,
            null_expected,
            empty_expected,
        } in cases
        {
            let img = producer_quota_image(configured);
            check!(
                lookup_quota_with_key(&img, principal, None, "producer_byte_rate") == null_expected,
                "null: {label}"
            );
            check!(
                lookup_quota_with_key(&img, principal, Some(""), "producer_byte_rate")
                    == empty_expected,
                "empty: {label}"
            );
        }
    }

    /// A bucket is re-rated from its own entity key, as Kafka re-rates each
    /// sensor from its own metric tags (#1213). The pair `(alice, app1)`
    /// having a quota of its own does not change the rate of the `[user=alice]`
    /// bucket that the client `(alice, app2)` still draws on.
    #[test]
    fn a_bucket_is_rated_by_its_own_key_levels() {
        /// A bucket key and the rate it must be re-rated to.
        struct BucketRateCase {
            label: &'static str,
            key: EntityKey,
            expected: Option<f64>,
        }
        let img = img_with(vec![
            rec().call(),
            alice_app1_producer_quota(),
            app1_producer_quota(),
            rec().entity(vec![("user", None)]).value(700.0).call(),
            rec().entity(vec![("client-id", None)]).value(300.0).call(),
            rec()
                .entity(vec![("ip", Some("10.0.0.1"))])
                .value(9.0)
                .call(),
        ]);
        let cases: [BucketRateCase; 9] = [
            BucketRateCase {
                label: "the user bucket keeps the user quota",
                key: user_key("alice"),
                expected: Some(1_000.0),
            },
            BucketRateCase {
                label: "a user with no quota falls to the default user",
                key: user_key("bob"),
                expected: Some(700.0),
            },
            BucketRateCase {
                label: "the pair bucket has the pair quota",
                key: pair_key("alice", "app1"),
                expected: Some(5_000.0),
            },
            BucketRateCase {
                label: "a pair with no pair level has no quota, not the user's",
                key: pair_key("alice", "app2"),
                expected: None,
            },
            BucketRateCase {
                label: "the client bucket keeps the client quota",
                key: client_key("app1"),
                expected: Some(200.0),
            },
            BucketRateCase {
                label: "a client with no quota falls to the default client",
                key: client_key("app2"),
                expected: Some(300.0),
            },
            BucketRateCase {
                label: "an ip key is not a user or client bucket",
                key: vec![("ip".into(), Some("10.0.0.1".into()))],
                expected: None,
            },
            BucketRateCase {
                label: "a user bucket with no name has no quota",
                key: vec![("user".into(), None)],
                expected: None,
            },
            BucketRateCase {
                label: "an empty key has no quota",
                key: vec![],
                expected: None,
            },
        ];
        for BucketRateCase {
            label,
            key,
            expected,
        } in cases
        {
            check!(
                lookup_bucket_rate(&img, &key, "producer_byte_rate") == expected,
                "{label}"
            );
        }
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct IpQuotaSetup<'a> {
        ip: Option<&'a str>,
        #[default("connection_creation_rate")]
        key: &'a str,
        #[default(QuotaValue(1.0))]
        value: QuotaValue,
    }

    #[bon::builder]
    fn rec_ip(
        ip: Option<&'_ str>,
        #[builder(default = "connection_creation_rate")] key: &'_ str,
        #[builder(into, default = QuotaValue (1.0))] value: QuotaValue,
    ) -> ClientQuotaRecord {
        let setup = IpQuotaSetup { ip, key, value };
        rec()
            .entity(vec![("ip", setup.ip)])
            .key(setup.key)
            .value(setup.value)
            .call()
    }

    fn img_with_ip(records: Vec<ClientQuotaRecord>) -> MetadataImage { image_with_quotas(records) }

    #[test]
    fn ip_quotas_prefer_an_exact_address_and_fall_back_to_the_default() {
        let cases = [
            (
                vec![rec_ip().maybe_ip(Some("127.0.0.1")).call()],
                "127.0.0.1",
                Some(1.0),
            ),
            (vec![rec_ip().value(2.0).call()], "10.0.0.7", Some(2.0)),
            (
                vec![
                    rec_ip().value(8.0).call(),
                    rec_ip().maybe_ip(Some("127.0.0.1")).call(),
                ],
                "127.0.0.1",
                Some(1.0),
            ),
        ];
        for (records, address, expected) in cases {
            let image = img_with_ip(records);
            let ip: std::net::IpAddr = address.parse().unwrap();
            assert!(
                lookup_ip_quota(&image, &IpNames::default(), ip, "connection_creation_rate")
                    == expected,
                "{address}"
            );
        }
    }

    #[test]
    fn ip_no_match_returns_none() {
        let img = img_with_ip(vec![]);
        let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
        assert!(
            lookup_ip_quota(&img, &IpNames::default(), ip, "connection_creation_rate").is_none()
        );
    }

    /// Kafka resolves an ip entity's name to an `InetAddress` and matches it
    /// with the connection's address, so the entity stands for every spelling
    /// of its address (#1214). The bucket key is the peer's canonical address
    /// whichever entity matched.
    #[test]
    fn an_ip_entity_stands_for_every_spelling_of_its_address() {
        // (label, entity name, peer address, whether the entity matches it)
        let cases = [
            ("the canonical literal", "127.0.0.1", "127.0.0.1", true),
            (
                "an IPv6 spelling with no zero compression",
                "0:0:0:0:0:0:0:1",
                "::1",
                true,
            ),
            (
                "an upper-case IPv6 literal",
                "2001:DB8::A",
                "2001:db8::a",
                true,
            ),
            ("a bracketed IPv6 literal", "[::1]", "::1", true),
            (
                "an IPv4-mapped peer is its IPv4 address",
                "10.1.2.3",
                "::ffff:10.1.2.3",
                true,
            ),
            (
                "an IPv4-mapped entity is its IPv4 address",
                "::ffff:10.1.2.3",
                "10.1.2.3",
                true,
            ),
            ("another address", "10.1.2.4", "10.1.2.3", false),
            (
                "a host name that has no address yet",
                "db",
                "10.1.2.3",
                false,
            ),
        ];
        for (label, entity, peer, matches) in cases {
            let img = img_with_ip(vec![rec_ip().maybe_ip(Some(entity)).value(7.0).call()]);
            let names = IpNames::default();
            let _ = names.update(&img);
            let peer: std::net::IpAddr = peer.parse().unwrap();

            let got = lookup_ip_quota_with_key(&img, &names, peer, "connection_creation_rate");

            let want = matches.then(|| {
                (
                    vec![("ip".into(), Some(peer.to_canonical().to_string()))],
                    7.0,
                )
            });
            check!(got == want, "{label}");
        }
    }

    /// A host name stands for the address it resolved to, ahead of the default
    /// `ip` quota; an address it did not resolve to falls to the default.
    #[test]
    fn a_resolved_host_name_stands_for_its_address() {
        let img = img_with_ip(vec![
            rec_ip().maybe_ip(Some("db")).value(9.0).call(),
            rec_ip().value(2.0).call(),
        ]);
        let names = IpNames::default();
        let db: std::net::IpAddr = "10.0.0.9".parse().unwrap();
        let other: std::net::IpAddr = "10.0.0.1".parse().unwrap();
        let rate =
            |names: &IpNames, peer| lookup_ip_quota(&img, names, peer, "connection_creation_rate");

        let before = rate(&names, db);
        names.insert_host("db", db);
        let _ = names.update(&img);

        check!(
            (before, rate(&names, db), rate(&names, other)) == (Some(2.0), Some(9.0), Some(2.0))
        );
    }

    #[test]
    fn ipv6_specific_match() {
        // KIP-612: the connection-creation-rate quota must resolve for an
        // IPv6 peer keyed by its canonical string form, not just IPv4.
        let img = img_with_ip(vec![rec_ip().maybe_ip(Some("::1")).value(3.0).call()]);
        let ip: std::net::IpAddr = "::1".parse().unwrap();
        assert!(
            lookup_ip_quota(&img, &IpNames::default(), ip, "connection_creation_rate") == Some(3.0)
        );
    }

    #[test]
    fn ipv6_default_fallback() {
        // An IPv6 peer with no specific entry falls back to the (ip=None)
        // default, proving IPv6 is no longer skipped by the quota path.
        let img = img_with_ip(vec![rec_ip().value(5.0).call()]);
        let ip: std::net::IpAddr = "2001:db8::42".parse().unwrap();
        assert!(
            lookup_ip_quota(&img, &IpNames::default(), ip, "connection_creation_rate") == Some(5.0)
        );
    }

    // ── precedence verification: exhaustive enumeration + proptest ────────────
    //
    // The documented 8-priority (user/client) and 2-priority (IP) orders,
    // declared HERE independently of production so a reordering in
    // `lookup_quota_with_key`'s candidate array is caught. Index = priority
    // (lower wins). Parameterized by the probe `(principal, client_id)`.
    fn uc_candidates<'a>(
        principal: &'a str,
        client_id: &'a str,
    ) -> [Vec<(&'a str, Option<&'a str>)>; 8] {
        [
            vec![("user", Some(principal)), ("client-id", Some(client_id))],
            vec![("user", Some(principal)), ("client-id", None)],
            vec![("user", Some(principal))],
            vec![("user", None), ("client-id", Some(client_id))],
            vec![("user", None), ("client-id", None)],
            vec![("user", None)],
            vec![("client-id", Some(client_id))],
            vec![("client-id", None)],
        ]
    }

    /// Kafka's `ClientQuotaManager` ranks every `user=U` level above every
    /// `user=<default>` level (#677). Each row sets two or more levels and
    /// expects the higher one, with the bucket key Kafka's metric tags give.
    #[test]
    fn precedence_matches_kafkas_client_quota_manager() {
        type Level = Vec<(&'static str, Option<&'static str>)>;
        /// A level and the rate configured at it.
        struct ConfiguredLevel {
            entity: Level,
            rate: f64,
        }
        /// A case name, the configured levels and rates, and the expected
        /// bucket key and rate.
        struct Row {
            name: String,
            configured: Vec<ConfiguredLevel>,
            expected: (EntityKey, f64),
        }
        let at = |entity: Level, rate: f64| ConfiguredLevel { entity, rate };
        let pair: EntityKey = vec![
            ("client-id".into(), Some("app".into())),
            ("user".into(), Some("alice".into())),
        ];
        let user: EntityKey = vec![("user".into(), Some("alice".into()))];
        let client: EntityKey = vec![("client-id".into(), Some("app".into()))];
        let levels: [Level; 8] = uc_candidates("alice", "app");
        // Every adjacent pair: level N and N+1 both set, level N wins.
        let bucket_for_level = [&pair, &pair, &user, &pair, &pair, &user, &client, &client];
        let mut rows: Vec<Row> = (0..7)
            .map(|n| Row {
                name: format!("levels {} and {}", n + 1, n + 2),
                configured: vec![
                    at(levels[n].clone(), 1000.0),
                    at(levels[n + 1].clone(), 5000.0),
                ],
                expected: (bucket_for_level[n].clone(), 1000.0),
            })
            .collect();
        // The four examples from the issue.
        rows.extend([
            Row {
                name: "user=alice over the default pair".to_owned(),
                configured: vec![
                    at(vec![("user", Some("alice"))], 1000.0),
                    at(vec![("user", None), ("client-id", None)], 5000.0),
                ],
                expected: (user.clone(), 1000.0),
            },
            Row {
                name: "user=alice,client-id=default over user=default,client-id=app".to_owned(),
                configured: vec![
                    at(vec![("user", Some("alice")), ("client-id", None)], 1000.0),
                    at(vec![("user", None), ("client-id", Some("app"))], 5000.0),
                ],
                expected: (pair.clone(), 1000.0),
            },
            Row {
                name: "user=alice over user=default,client-id=app".to_owned(),
                configured: vec![
                    at(vec![("user", Some("alice"))], 1000.0),
                    at(vec![("user", None), ("client-id", Some("app"))], 5000.0),
                ],
                expected: (user.clone(), 1000.0),
            },
            Row {
                name: "user=default over client-id=app".to_owned(),
                configured: vec![
                    at(vec![("user", None)], 1000.0),
                    at(vec![("client-id", Some("app"))], 5000.0),
                ],
                expected: (user.clone(), 1000.0),
            },
        ]);
        for Row {
            name,
            configured,
            expected,
        } in rows
        {
            let img = img_with(
                configured
                    .into_iter()
                    .map(|ConfiguredLevel { entity, rate }| rec().entity(entity).value(rate).call())
                    .collect(),
            );
            let got = lookup_quota_with_key(&img, "alice", Some("app"), "producer_byte_rate");
            assert2::check!(got == Some(expected), "{name}");
        }
    }

    /// Distinct per-candidate sentinel values, where the index is the priority.
    /// The value alone then identifies the matched candidate, with no int→float
    /// cast.
    const CAND_VALS: [f64; 8] = [
        1000.0, 1001.0, 1002.0, 1003.0, 1004.0, 1005.0, 1006.0, 1007.0,
    ];

    #[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
    struct QuotaCandidateMask(u16);

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct QuotaCandidatesSetup<'a> {
        entities: &'a [QuotaEntitySpec<'a>],
        mask: QuotaCandidateMask,
        #[default("k")]
        key: &'a str,
    }

    /// Select the same ordered candidates and their distinct sentinel values.
    #[bon::builder]
    fn candidate_quota_records(
        #[builder(default)] entities: &'_ [QuotaEntitySpec<'_>],
        #[builder(into, default)] mask: QuotaCandidateMask,
        #[builder(default = "k")] key: &'_ str,
    ) -> Vec<ClientQuotaRecord> {
        let setup = QuotaCandidatesSetup {
            entities,
            mask,
            key,
        };
        setup
            .entities
            .iter()
            .enumerate()
            .filter(|(index, _)| setup.mask.0 & (1 << index) != 0)
            .map(|(index, entity)| {
                rec()
                    .entity(entity.clone())
                    .key(setup.key)
                    .value(CAND_VALS[index])
                    .call()
            })
            .collect()
    }

    /// Exhaustive test. Every one of the 2^8 presence configs of the 8
    /// candidates, for a fixed probe, resolves to the present candidate with
    /// the lowest index and returns its value. An empty config returns `None`.
    /// Each candidate has a distinct value (`1000 + i`), so the value
    /// identifies the matched candidate.
    #[test]
    fn quota_precedence_exhaustive() {
        let cands = uc_candidates("u", "c");
        for mask in 0u16..256 {
            let records = candidate_quota_records().entities(&cands).mask(mask).call();
            let img = img_with(records);
            let got = lookup_quota_with_key(&img, "u", Some("c"), "k");
            match (0..8usize).find(|i| mask & (1 << i) != 0) {
                None => assert!(
                    got.is_none(),
                    "mask {mask:#010b}: expected None, got {got:?}"
                ),
                Some(j) => {
                    let (_key, val) = got.expect("a candidate is present");
                    // Exact sentinel values, stored and retrieved verbatim —
                    // compare bit patterns to sidestep float_cmp.
                    assert!(
                        val.to_bits() == CAND_VALS[j].to_bits(),
                        "mask {mask:#010b}: expected candidate {j} (value {}), got {val}",
                        CAND_VALS[j]
                    );
                }
            }
            // A quota_key no candidate carries never resolves.
            assert!(lookup_quota_with_key(&img, "u", Some("c"), "absent_key").is_none());
        }
    }

    /// Exhaustive test of the 2-priority IP order. Specific beats default.
    #[test]
    fn ip_quota_precedence_exhaustive() {
        let ip: std::net::IpAddr = "10.1.2.3".parse().unwrap();
        let cands = [vec![("ip", Some("10.1.2.3"))], vec![("ip", None)]];
        for mask in 0u8..4 {
            let records = candidate_quota_records()
                .entities(&cands)
                .mask(u16::from(mask))
                .key("connection_creation_rate")
                .call();
            let img = img_with(records);
            let got =
                lookup_ip_quota_with_key(&img, &IpNames::default(), ip, "connection_creation_rate");
            match (0..2usize).find(|i| mask & (1 << i) != 0) {
                None => assert!(got.is_none(), "mask {mask:#04b}: expected None"),
                Some(j) => {
                    assert!(got.expect("present").1.to_bits() == CAND_VALS[j].to_bits());
                }
            }
        }
    }

    proptest::proptest! {
        /// Random probes, random candidate subsets, and a non-matching decoy.
        /// The present candidate with the lowest index wins, the decoy is never
        /// returned, and the user/client path never returns an IP entity.
        #[test]
        fn quota_precedence_random(
            principal in "[uv][12]",
            client_id in "[cd][12]",
            qkey_idx in 0usize..2,
            present in proptest::collection::vec(proptest::bool::ANY, 8),
            decoy in proptest::bool::ANY,
        ) {
            let qkey = ["producer_byte_rate", "consumer_byte_rate"][qkey_idx];
            let cands = uc_candidates(&principal, &client_id);
            let mut records: Vec<_> = cands
                .iter()
                .enumerate()
                .filter(|(i, _)| present[*i])
                .map(|(i, c)| {
                    rec()
                        .entity(c.clone())
                        .key(qkey)
                        .value(CAND_VALS[i])
                        .call()
                })
                .collect();
            if decoy {
                // Non-matching entity (never a candidate for a [uv][12]/[cd][12]
                // probe) — must never be returned.
                records.push(
                    rec()
                        .entity(vec![("client-id", Some("ZZZ")), ("user", Some("ZZZ"))])
                        .key(qkey)
                        .value(9999.0)
                        .call(),
                );
                records.push(
                    rec()
                        .entity(vec![("user", Some("ZZZ"))])
                        .key(qkey)
                        .value(9998.0)
                        .call(),
                );
            }
            let img = img_with(records);
            let got = lookup_quota_with_key(&img, &principal, Some(&client_id), qkey);
            match (0..8usize).find(|i| present[*i]) {
                None => proptest::prop_assert!(got.is_none(), "no candidate present, got {got:?}"),
                Some(j) => {
                    let (_k, v) = got.expect("a candidate is present");
                    proptest::prop_assert_eq!(v.to_bits(), CAND_VALS[j].to_bits());
                }
            }
        }

        /// Random IP precedence. Specific beats default, and the IP path never
        /// returns a user/client entity.
        #[test]
        fn ip_precedence_random(specific in proptest::bool::ANY, default in proptest::bool::ANY) {
            let ip: std::net::IpAddr = "10.9.8.7".parse().unwrap();
            let mut records = vec![];
            if specific {
                records.push(
                    rec()
                        .entity(vec![("ip", Some("10.9.8.7"))])
                        .key("connection_creation_rate")
                        .value(1.0)
                        .call(),
                );
            }
            if default {
                records.push(
                    rec()
                        .entity(vec![("ip", None)])
                        .key("connection_creation_rate")
                        .value(2.0)
                        .call(),
                );
            }
            // A user/client entry must not leak into the IP path.
            records.push(
                rec()
                    .entity(vec![("user", Some("u"))])
                    .key("connection_creation_rate")
                    .value(50.0)
                    .call(),
            );
            let img = img_with(records);
            let got = lookup_ip_quota_with_key(&img, &IpNames::default(), ip, "connection_creation_rate");
            if specific {
                proptest::prop_assert_eq!(got.expect("specific present").1.to_bits(), 1.0_f64.to_bits());
            } else if default {
                proptest::prop_assert_eq!(got.expect("default present").1.to_bits(), 2.0_f64.to_bits());
            } else {
                proptest::prop_assert!(got.is_none());
            }
        }
    }
}
