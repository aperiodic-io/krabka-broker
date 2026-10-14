//! Static broker config for the KIP-848 next-gen consumer group protocol.

use {
    super::{
        GroupCoordinator,
        assignor::{Assignor, RangeAssignor, UniformAssignor},
    },
    crate::time_util::system_timer,
    qubit_clock::Timer,
    std::{borrow::Cow, collections::BTreeMap, str::FromStr, sync::Arc, time::Duration},
};

/// `group.consumer.migration.policy` governs classic ↔ next-gen consumer
/// group conversion. The default is `Bidirectional`, which matches Apache
/// Kafka 4.0, verified empirically against
/// `mirror.gcr.io/apache/kafka:4.0.0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, krabka_macros::EnumStr)]
#[enum_str(case = "lowercase", parse)]
pub enum ConsumerGroupMigrationPolicy {
    /// No conversion in either direction.
    Disabled,
    /// Classic → consumer only.
    Upgrade,
    /// Consumer → classic only.
    Downgrade,
    /// Both upgrade and downgrade are enabled.
    #[default]
    Bidirectional,
}

impl ConsumerGroupMigrationPolicy {
    /// `true` if a classic group may be upgraded to a consumer group.
    #[must_use]
    pub fn allows_upgrade(self) -> bool { matches!(self, Self::Upgrade | Self::Bidirectional) }

    /// `true` if a consumer group may be downgraded to a classic group.
    #[must_use]
    pub fn allows_downgrade(self) -> bool { matches!(self, Self::Downgrade | Self::Bidirectional) }
}

impl FromStr for ConsumerGroupMigrationPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_ascii_lowercase();
        Self::parse(&lower)
            .ok_or_else(|| format!("invalid group.consumer.migration.policy: {lower}"))
    }
}

// `Debug` elides the timer (the `Timer` trait object is not `Debug`) so the
// enclosing `#[derive(Debug)]` `GroupCoordinator` still derives.
/// The settings a consumer group runs with: each `consumer.*` override in
/// the group's stored config over the broker value, clamped to the
/// broker's `group.consumer.min.*` and `group.consumer.max.*` bounds.
///
/// This is Kafka's `GroupMetadataManager.consumerGroupSessionTimeoutMs`,
/// `consumerGroupHeartbeatIntervalMs` and
/// `consumerGroupAssignmentIntervalMs`: `GroupConfigManager.groupConfig`
/// over `GroupCoordinatorConfig`, with the stored config evaluated against
/// the bounds (`GroupConfig.evaluate`). A group with no override borrows
/// the broker value.
#[derive(Clone, derive_more::Debug, krabka_macros::GroupTimingConfig)]
#[group_timing(
    session = KEY_CONSUMER_SESSION_TIMEOUT_MS,
    heartbeat = KEY_CONSUMER_HEARTBEAT_INTERVAL_MS,
    assignment = KEY_CONSUMER_ASSIGNMENT_INTERVAL_MS,
)]
pub struct NextGenConfig {
    /// Comma-separated list. "consumer" enables KIP-848. Default
    /// "classic,consumer".
    pub rebalance_protocols: Vec<RebalanceProtocol>,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    /// Kafka's `group.consumer.assignment.interval.ms`: the least time between
    /// two target assignments of a group. Zero does not wait.
    pub assignment_interval: Duration,
    /// Kafka's `group.consumer.regex.refresh.interval.ms`: how long a resolution
    /// of a subscribed regular expression stands before a heartbeat resolves it
    /// again.
    pub regex_refresh_interval: Duration,
    /// Kafka's `REGEX_BATCH_REFRESH_MIN_INTERVAL_MS`: the least time between
    /// two resolutions of the regular expressions of a group.
    pub regex_refresh_min_interval: Duration,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,
    pub session_expiry_tick: Duration,
    pub actor_mailbox_capacity: usize,
    pub shutdown_ack_timeout: Duration,
    pub classic_initial_rebalance_delay: Duration,
    /// Kafka's `group.min.session.timeout.ms`: the smallest session timeout a
    /// classic `JoinGroup` may ask for.
    pub classic_min_session_timeout: Duration,
    /// Kafka's `group.max.session.timeout.ms`: the largest session timeout a
    /// classic `JoinGroup` may ask for.
    pub classic_max_session_timeout: Duration,
    /// Kafka's `group.max.size`: the most members a classic group admits.
    pub classic_max_size: usize,
    /// Registered server-side assignors. The list IS the registry. The
    /// broker matches the client's `server_assignor` field against
    /// `Assignor::name()` by string equality. `Default` seeds the two
    /// built-ins, `uniform` and `range`. Operators add their own with
    /// `register_assignor` before `Broker::start`.
    pub assignors: Vec<Arc<dyn Assignor>>,
    pub max_size: usize,
    /// `group.consumer.migration.policy` governs classic ↔ next-gen
    /// conversion. The conversion triggers consult it.
    pub migration_policy: ConsumerGroupMigrationPolicy,
    /// Timer that drives the per-group actor's session-expiry tick cadence.
    /// Production uses `time_util::system_timer`, which is real time. Tests
    /// inject the timer of a [`qubit_clock::ManualMonotonicClock`] so the tick
    /// fires on a controlled manual timeline instead of wall-clock time.
    #[debug(skip)]
    pub timer: Arc<dyn Timer>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceProtocol {
    Classic,
    Consumer,
}

/// Returned by [`NextGenConfig::register_assignor`] when the supplied
/// assignor's `name()` collides with one that is already registered. The
/// existing entry can be a built-in or a previously-registered custom
/// assignor.
#[derive(Debug, thiserror::Error)]
pub enum AssignorRegistrationError {
    #[error("an assignor named {0} is already registered")]
    DuplicateName(String),
}

/// Default consumer session timeout: 45 s, matching Kafka's
/// `group.consumer.session.timeout.ms`.
pub const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(45);

/// Default consumer heartbeat interval: 5 s, matching Kafka's
/// `group.consumer.heartbeat.interval.ms`.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Default interval between two target assignments of a group: 1 s, matching
/// Kafka's `group.consumer.assignment.interval.ms` and
/// `group.share.assignment.interval.ms`.
pub const DEFAULT_ASSIGNMENT_INTERVAL: Duration = Duration::from_secs(1);

/// Default time a resolution of a subscribed regular expression stands: 10
/// minutes, matching Kafka's `group.consumer.regex.refresh.interval.ms`.
pub const DEFAULT_REGEX_REFRESH_INTERVAL: Duration = Duration::from_mins(10);

/// Default least time between two resolutions of the regular expressions of a
/// group: 10 s, matching Kafka's `REGEX_BATCH_REFRESH_MIN_INTERVAL_MS`.
pub const DEFAULT_REGEX_REFRESH_MIN_INTERVAL: Duration = Duration::from_secs(10);

/// Kafka's `group.{consumer,share,streams}.min.assignment.interval.ms`
/// default. krabka does not make the bound configurable, so a group's
/// `*.assignment.interval.ms` is clamped to it.
pub(crate) const MIN_ASSIGNMENT_INTERVAL: Duration = Duration::ZERO;

/// Kafka's `group.{consumer,share,streams}.max.assignment.interval.ms`
/// default: 15 s. krabka does not make the bound configurable, so a group's
/// `*.assignment.interval.ms` is clamped to it.
pub(crate) const MAX_ASSIGNMENT_INTERVAL: Duration = Duration::from_secs(15);

/// Kafka's `GroupConfig.clampToRange`: `value`, or the bound it crosses. It is
/// not `Ord::clamp`, which panics for a minimum above the maximum.
pub(crate) fn clamp_to_range<T: PartialOrd>(value: T, min: T, max: T) -> T {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}

/// The millisecond value of the group config `key` in `overrides`, the
/// group's stored override map, or `None` when the group has no override for
/// it, clamped to `min..=max`.
///
/// Kafka's `GroupConfigManager.updateGroupConfig` evaluates a stored group
/// config against the broker's current bounds (`GroupConfig.evaluate`), and
/// caps a value outside them with a warning. The config RPCs validate a value
/// against the bounds when they store it, so a value is outside them only
/// when the broker's bounds moved since. It is capped here for the same
/// reason: the group runs within the bounds of the broker it is on.
///
/// Kafka's `GroupConfig` parses the stored value as an `INT`, so a value that
/// does not parse as a whole number of milliseconds is ignored, and a
/// negative one is below every bound.
pub(crate) fn group_millis(
    overrides: Option<&BTreeMap<String, String>>,
    key: &str,
    min: Duration,
    max: Duration,
) -> Option<Duration> {
    let millis = overrides?.get(key)?.trim().parse::<i32>().ok()?;
    let value = Duration::from_millis(u64::try_from(millis).unwrap_or(0));
    Some(clamp_to_range(value, min, max))
}

/// Resolve a protocol's group overrides from one current image, or borrow broker defaults.
pub(super) fn resolve_group_config<'a, C: Clone>(
    config: &'a C,
    coordinator: &GroupCoordinator,
    group_id: &str,
    apply: impl FnOnce(&'a C, Option<&BTreeMap<String, String>>) -> Cow<'a, C>,
) -> Cow<'a, C> {
    match coordinator.metadata_source() {
        Some(source) => apply(config, source.current_image().group_config(group_id)),
        None => Cow::Borrowed(config),
    }
}

/// Lower bound on the negotiated session timeout: 45 s, matching Kafka's
/// `group.consumer.min.session.timeout.ms`.
pub const DEFAULT_MIN_SESSION_TIMEOUT: Duration = Duration::from_secs(45);

/// Upper bound on the negotiated session timeout: 60 s, matching Kafka's
/// `group.consumer.max.session.timeout.ms`.
pub const DEFAULT_MAX_SESSION_TIMEOUT: Duration = Duration::from_mins(1);

/// Lower bound on the negotiated heartbeat interval: 5 s, matching Kafka's
/// `group.consumer.min.heartbeat.interval.ms`.
pub const DEFAULT_MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// Upper bound on the negotiated heartbeat interval: 15 s, matching Kafka's
/// `group.consumer.max.heartbeat.interval.ms`.
pub const DEFAULT_MAX_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Default lower bound on a classic `JoinGroup` session timeout: 6 s, Kafka's
/// `group.min.session.timeout.ms`.
pub const DEFAULT_CLASSIC_MIN_SESSION_TIMEOUT: Duration = Duration::from_secs(6);

/// Default upper bound on a classic `JoinGroup` session timeout: 30 min,
/// Kafka's `group.max.session.timeout.ms`.
pub const DEFAULT_CLASSIC_MAX_SESSION_TIMEOUT: Duration = Duration::from_mins(30);

/// Default cap on classic-group membership: `Integer.MAX_VALUE`, Kafka's
/// `group.max.size`.
pub const DEFAULT_CLASSIC_MAX_SIZE: usize = 2_147_483_647;

/// Default cap on consumer-group membership: `Integer.MAX_VALUE`, Kafka's
/// `group.consumer.max.size` (`CONSUMER_GROUP_MAX_SIZE_DEFAULT`), so no
/// practical limit.
pub const DEFAULT_MAX_GROUP_SIZE: usize = 2_147_483_647;

impl Default for NextGenConfig {
    fn default() -> Self {
        Self {
            rebalance_protocols: vec![RebalanceProtocol::Classic, RebalanceProtocol::Consumer],
            session_timeout: DEFAULT_SESSION_TIMEOUT,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            assignment_interval: DEFAULT_ASSIGNMENT_INTERVAL,
            regex_refresh_interval: DEFAULT_REGEX_REFRESH_INTERVAL,
            regex_refresh_min_interval: DEFAULT_REGEX_REFRESH_MIN_INTERVAL,
            min_session_timeout: DEFAULT_MIN_SESSION_TIMEOUT,
            max_session_timeout: DEFAULT_MAX_SESSION_TIMEOUT,
            min_heartbeat_interval: DEFAULT_MIN_HEARTBEAT_INTERVAL,
            max_heartbeat_interval: DEFAULT_MAX_HEARTBEAT_INTERVAL,
            session_expiry_tick: Duration::from_secs(1),
            actor_mailbox_capacity: 64,
            shutdown_ack_timeout: Duration::from_secs(5),
            classic_initial_rebalance_delay: Duration::from_secs(3),
            classic_min_session_timeout: DEFAULT_CLASSIC_MIN_SESSION_TIMEOUT,
            classic_max_session_timeout: DEFAULT_CLASSIC_MAX_SESSION_TIMEOUT,
            classic_max_size: DEFAULT_CLASSIC_MAX_SIZE,
            assignors: vec![Arc::new(UniformAssignor), Arc::new(RangeAssignor)],
            max_size: DEFAULT_MAX_GROUP_SIZE,
            migration_policy: ConsumerGroupMigrationPolicy::default(),
            timer: system_timer(),
        }
    }
}

impl NextGenConfig {
    #[must_use]
    pub fn next_gen_enabled(&self) -> bool {
        self.rebalance_protocols
            .contains(&RebalanceProtocol::Consumer)
    }

    /// Register an additional assignor. Returns an error if the name is
    /// already taken. [`Default::default`] registers the built-ins
    /// `uniform` and `range`, so a `register_assignor` call with either
    /// name returns a duplicate-name error.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn register_assignor(
        &mut self,
        assignor: Arc<dyn Assignor>,
    ) -> Result<(), AssignorRegistrationError> {
        let name = assignor.name();
        if self.assignors.iter().any(|a| a.name() == name) {
            return Err(AssignorRegistrationError::DuplicateName(name.into()));
        }
        self.assignors.push(assignor);
        Ok(())
    }

    /// Resolve a registered assignor by name. An `Arc` clone is cheap.
    #[must_use]
    pub fn find_assignor(&self, name: &str) -> Option<Arc<dyn Assignor>> {
        self.assignors.iter().find(|a| a.name() == name).cloned()
    }

    /// `true` when a client may legally request this name in
    /// `ConsumerGroupHeartbeatRequest::server_assignor`.
    #[must_use]
    pub fn assignor_enabled(&self, name: &str) -> bool { self.find_assignor(name).is_some() }
}

/// Kafka's `GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG`.
const KEY_CONSUMER_SESSION_TIMEOUT_MS: &str = "consumer.session.timeout.ms";
/// Kafka's `GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG`.
const KEY_CONSUMER_HEARTBEAT_INTERVAL_MS: &str = "consumer.heartbeat.interval.ms";
/// Kafka's `GroupConfig.CONSUMER_ASSIGNMENT_INTERVAL_MS_CONFIG`.
const KEY_CONSUMER_ASSIGNMENT_INTERVAL_MS: &str = "consumer.assignment.interval.ms";

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            coordinator::unified::{
                assignor::{Assignment, GroupSpec, TopicMetadata},
                share::config::ShareGroupConfig,
                test_support::{group_config_image, make_coord},
            },
            test_support::FakeMetadataSource,
        },
        assert2::assert,
        std::{borrow::Cow, collections::HashMap, ptr},
    };

    #[derive(Debug)]
    struct TestAssignor(&'static str);
    impl Assignor for TestAssignor {
        fn name(&self) -> &'static str { self.0 }

        fn assign(&self, _group: &GroupSpec, _topics: &TopicMetadata) -> Assignment {
            HashMap::new()
        }
    }

    #[test]
    fn effective_config_without_a_source_borrows_defaults_without_applying_overrides() {
        let broker = NextGenConfig::default();
        let coordinator = make_coord();
        let effective = resolve_group_config(&broker, &coordinator, "group", |_, _| {
            panic!("no metadata source must not run the override callback")
        });
        assert!(matches!(effective, Cow::Borrowed(_)));
        assert!(ptr::eq(
            ptr::from_ref(effective.as_ref()),
            ptr::from_ref(&broker)
        ));
    }

    #[test]
    fn effective_config_observes_one_current_image_per_lookup_and_protocol_overrides() {
        let source = Arc::new(FakeMetadataSource::static_image(group_config_image(&[(
            "group",
            &[
                ("consumer.heartbeat.interval.ms", "7000"),
                ("share.heartbeat.interval.ms", "9000"),
            ],
        )])));
        let coordinator = make_coord();
        coordinator.set_metadata_source(source.clone());
        let consumer = NextGenConfig::default();
        let effective =
            resolve_group_config(&consumer, &coordinator, "group", NextGenConfig::for_group);
        assert!(source.current_image_calls() == 1);
        assert!(matches!(effective, Cow::Owned(_)));
        assert!(effective.heartbeat_interval == Duration::from_secs(7));

        let share = ShareGroupConfig::default();
        let shared =
            resolve_group_config(&share, &coordinator, "group", ShareGroupConfig::for_group);
        assert!(source.current_image_calls() == 2);
        assert!(shared.heartbeat_interval == Duration::from_secs(9));
        let defaults =
            resolve_group_config(&consumer, &coordinator, "unknown", NextGenConfig::for_group);
        assert!(source.current_image_calls() == 3);
        assert!(matches!(defaults, Cow::Borrowed(_)));
        assert!(ptr::eq(
            ptr::from_ref(defaults.as_ref()),
            ptr::from_ref(&consumer)
        ));

        source.set_image(group_config_image(&[("group", &[(
            "consumer.heartbeat.interval.ms",
            "8000",
        )])]));
        let refreshed =
            resolve_group_config(&consumer, &coordinator, "group", NextGenConfig::for_group);
        assert!(source.current_image_calls() == 4);
        assert!(refreshed.heartbeat_interval == Duration::from_secs(8));
        assert!(effective.heartbeat_interval == Duration::from_secs(7));
    }

    #[test]
    fn default_registers_uniform_and_range() {
        let cfg = NextGenConfig::default();
        assert!(cfg.assignors.len() == 2);
        let names: Vec<&str> = cfg.assignors.iter().map(|a| a.name()).collect();
        assert!(names.contains(&"uniform"));
        assert!(names.contains(&"range"));
    }

    #[test]
    fn register_assignor_succeeds_for_new_name() {
        let mut cfg = NextGenConfig::default();
        cfg.register_assignor(Arc::new(TestAssignor("custom")))
            .unwrap();
        assert!(cfg.find_assignor("custom").is_some());
    }

    #[test]
    fn register_assignor_rejects_duplicate_name() {
        let mut cfg = NextGenConfig::default();
        let err = cfg
            .register_assignor(Arc::new(TestAssignor("uniform")))
            .unwrap_err();
        match err {
            AssignorRegistrationError::DuplicateName(name) => assert!(name == "uniform"),
        }
    }

    #[test]
    fn find_assignor_returns_registered_impl() {
        let mut cfg = NextGenConfig::default();
        cfg.register_assignor(Arc::new(TestAssignor("x"))).unwrap();
        let resolved = cfg.find_assignor("x").expect("registered");
        assert!(resolved.name() == "x");
    }

    #[yare::parameterized(
        uniform = { "uniform" },
        range = { "range" },
        custom = { "y" },
        unknown = { "ghost" },
    )]
    fn assignor_enabled_matches_find_assignor(name: &str) {
        let mut cfg = NextGenConfig::default();
        cfg.register_assignor(Arc::new(TestAssignor("y"))).unwrap();
        assert2::assert!(cfg.assignor_enabled(name) == cfg.find_assignor(name).is_some());
    }

    /// Kafka's `group.consumer.max.size` defaults to `Integer.MAX_VALUE`
    /// (`CONSUMER_GROUP_MAX_SIZE_DEFAULT`), so a group of 201 members, or a
    /// classic group of that size that upgrades, is not refused.
    #[test]
    fn default_member_cap_is_kafkas_integer_max() {
        let max_size = NextGenConfig::default().max_size;

        assert!(max_size == usize::try_from(i32::MAX).unwrap());
        assert!(max_size > 200);
    }

    #[test]
    fn migration_policy_default_is_bidirectional() {
        // Matches Apache Kafka 4.0 (verified empirically).
        assert!(
            NextGenConfig::default().migration_policy
                == ConsumerGroupMigrationPolicy::Bidirectional
        );
    }

    #[yare::parameterized(
        disabled = { "disabled", ConsumerGroupMigrationPolicy::Disabled },
        upgrade = { "upgrade", ConsumerGroupMigrationPolicy::Upgrade },
        downgrade = { "downgrade", ConsumerGroupMigrationPolicy::Downgrade },
        bidirectional = { "bidirectional", ConsumerGroupMigrationPolicy::Bidirectional },
        mixed_case = { "BiDirectional", ConsumerGroupMigrationPolicy::Bidirectional },
        upper_case = { "UPGRADE", ConsumerGroupMigrationPolicy::Upgrade },
    )]
    fn migration_policy_from_str_round_trips_all_names(
        name: &str,
        want: ConsumerGroupMigrationPolicy,
    ) {
        assert2::assert!(name.parse::<ConsumerGroupMigrationPolicy>().unwrap() == want);
    }

    #[test]
    fn migration_policy_from_str_rejects_junk() {
        assert!("sideways".parse::<ConsumerGroupMigrationPolicy>().is_err());
        assert!("".parse::<ConsumerGroupMigrationPolicy>().is_err());
    }

    #[test]
    fn migration_policy_direction_truth_table() {
        use ConsumerGroupMigrationPolicy as P;
        let cases = [
            (P::Disabled, (false, false)),
            (P::Upgrade, (true, false)),
            (P::Downgrade, (false, true)),
            (P::Bidirectional, (true, true)),
        ];
        for (policy, want) in cases {
            assert!(
                (policy.allows_upgrade(), policy.allows_downgrade()) == want,
                "policy {policy:?}"
            );
        }
    }

    /// Kafka's `consumerGroupSessionTimeoutMs`,
    /// `consumerGroupHeartbeatIntervalMs` and
    /// `consumerGroupAssignmentIntervalMs`: a `consumer.*` override replaces
    /// the broker value, capped to the broker's bounds as
    /// `GroupConfig.evaluate` caps it; another coordinator's key, and a value
    /// that does not parse, leave it.
    #[test]
    fn for_group_applies_each_consumer_override() {
        /// A `consumer.*` or other coordinator's override key and its value.
        #[derive(Debug)]
        struct ConfigOverride {
            key: &'static str,
            value: &'static str,
        }

        /// The overrides, and the session timeout, heartbeat interval and
        /// assignment interval they give.
        struct Row {
            overrides: &'static [ConfigOverride],
            session: Duration,
            heartbeat: Duration,
            assignment: Duration,
        }

        let broker = NextGenConfig::default();
        let rows: [Row; 12] = [
            Row {
                overrides: &[ConfigOverride {
                    key: "consumer.session.timeout.ms",
                    value: "1000",
                }],
                session: DEFAULT_MIN_SESSION_TIMEOUT,
                heartbeat: DEFAULT_HEARTBEAT_INTERVAL,
                assignment: DEFAULT_ASSIGNMENT_INTERVAL,
            },
            Row {
                overrides: &[ConfigOverride {
                    key: "consumer.session.timeout.ms",
                    value: "3600000",
                }],
                session: DEFAULT_MAX_SESSION_TIMEOUT,
                heartbeat: DEFAULT_HEARTBEAT_INTERVAL,
                assignment: DEFAULT_ASSIGNMENT_INTERVAL,
            },
            Row {
                overrides: &[ConfigOverride {
                    key: "consumer.heartbeat.interval.ms",
                    value: "0",
                }],
                session: DEFAULT_SESSION_TIMEOUT,
                heartbeat: DEFAULT_MIN_HEARTBEAT_INTERVAL,
                assignment: DEFAULT_ASSIGNMENT_INTERVAL,
            },
            Row {
                overrides: &[ConfigOverride {
                    key: "consumer.heartbeat.interval.ms",
                    value: "60000",
                }],
                session: DEFAULT_SESSION_TIMEOUT,
                heartbeat: DEFAULT_MAX_HEARTBEAT_INTERVAL,
                assignment: DEFAULT_ASSIGNMENT_INTERVAL,
            },
            Row {
                overrides: &[ConfigOverride {
                    key: "consumer.assignment.interval.ms",
                    value: "3600000",
                }],
                session: DEFAULT_SESSION_TIMEOUT,
                heartbeat: DEFAULT_HEARTBEAT_INTERVAL,
                assignment: MAX_ASSIGNMENT_INTERVAL,
            },
            Row {
                overrides: &[ConfigOverride {
                    key: "consumer.assignment.interval.ms",
                    value: "-5",
                }],
                session: DEFAULT_SESSION_TIMEOUT,
                heartbeat: DEFAULT_HEARTBEAT_INTERVAL,
                assignment: MIN_ASSIGNMENT_INTERVAL,
            },
            Row {
                overrides: &[],
                session: DEFAULT_SESSION_TIMEOUT,
                heartbeat: DEFAULT_HEARTBEAT_INTERVAL,
                assignment: DEFAULT_ASSIGNMENT_INTERVAL,
            },
            Row {
                overrides: &[ConfigOverride {
                    key: "consumer.session.timeout.ms",
                    value: "50000",
                }],
                session: Duration::from_secs(50),
                heartbeat: DEFAULT_HEARTBEAT_INTERVAL,
                assignment: DEFAULT_ASSIGNMENT_INTERVAL,
            },
            Row {
                overrides: &[ConfigOverride {
                    key: "consumer.heartbeat.interval.ms",
                    value: "7000",
                }],
                session: DEFAULT_SESSION_TIMEOUT,
                heartbeat: Duration::from_secs(7),
                assignment: DEFAULT_ASSIGNMENT_INTERVAL,
            },
            Row {
                overrides: &[ConfigOverride {
                    key: "consumer.assignment.interval.ms",
                    value: "0",
                }],
                session: DEFAULT_SESSION_TIMEOUT,
                heartbeat: DEFAULT_HEARTBEAT_INTERVAL,
                assignment: Duration::ZERO,
            },
            Row {
                overrides: &[
                    ConfigOverride {
                        key: "share.session.timeout.ms",
                        value: "50000",
                    },
                    ConfigOverride {
                        key: "streams.heartbeat.interval.ms",
                        value: "7000",
                    },
                ],
                session: DEFAULT_SESSION_TIMEOUT,
                heartbeat: DEFAULT_HEARTBEAT_INTERVAL,
                assignment: DEFAULT_ASSIGNMENT_INTERVAL,
            },
            Row {
                overrides: &[
                    ConfigOverride {
                        key: "consumer.session.timeout.ms",
                        value: "soon",
                    },
                    ConfigOverride {
                        key: "consumer.heartbeat.interval.ms",
                        value: "-1",
                    },
                ],
                session: DEFAULT_SESSION_TIMEOUT,
                heartbeat: DEFAULT_HEARTBEAT_INTERVAL,
                assignment: DEFAULT_ASSIGNMENT_INTERVAL,
            },
        ];
        for Row {
            overrides: entries,
            session,
            heartbeat,
            assignment,
        } in rows
        {
            let overrides: BTreeMap<String, String> = entries
                .iter()
                .map(|entry| (entry.key.to_owned(), entry.value.to_owned()))
                .collect();
            let group = broker.for_group(Some(&overrides));
            assert!(
                (
                    group.session_timeout,
                    group.heartbeat_interval,
                    group.assignment_interval
                ) == (session, heartbeat, assignment),
                "{entries:?}"
            );
        }
        assert!(matches!(broker.for_group(None), Cow::Borrowed(_)));
    }

    #[test]
    fn debug_renders_operator_fields_and_elides_timer() {
        // The manual `Debug` impl exists because the `Timer` trait object is
        // not `Debug`. Assert on the rendered content — a stubbed-out `fmt`
        // body (writing nothing) must not pass. Distinctive values on a subset
        // of fields prove the real fields are emitted, not a static placeholder.
        let cfg = NextGenConfig {
            session_timeout: Duration::from_secs(37),
            max_size: 4242,
            migration_policy: ConsumerGroupMigrationPolicy::Downgrade,
            ..Default::default()
        };

        let rendered = format!("{cfg:?}");

        assert!(rendered.starts_with("NextGenConfig"), "got {rendered}");
        for needle in [
            "rebalance_protocols",
            "session_timeout",
            "37s",
            "heartbeat_interval",
            "min_session_timeout",
            "max_session_timeout",
            "min_heartbeat_interval",
            "max_heartbeat_interval",
            "assignors",
            "max_size",
            "4242",
            "migration_policy",
            "Downgrade",
        ] {
            assert!(
                rendered.contains(needle),
                "Debug output missing {needle:?}: {rendered}"
            );
        }
        // `finish_non_exhaustive` elides the timer with a trailing `..`; the
        // elided field's name must not leak into the output. No other field
        // name or rendered value contains "timer" — the timeout fields spell
        // "timeout" — so the needle can only match the elided field.
        assert!(
            rendered.contains(".."),
            "expected non-exhaustive marker: {rendered}"
        );
        assert!(
            !rendered.contains("timer"),
            "timer must be elided: {rendered}"
        );
    }
}
