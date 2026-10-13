//! KIP-1071 Streams rebalance-protocol configuration.
use {
    super::description::TopologyDescriptionPlugin,
    crate::{
        api_catalog::UnstableApiVersions,
        coordinator::unified::config::{
            MAX_ASSIGNMENT_INTERVAL, MIN_ASSIGNMENT_INTERVAL, clamp_to_range,
        },
    },
    std::{collections::BTreeMap, time::Duration},
};

/// Kafka's `group.streams.max.standby.replicas` default, which krabka does not
/// make configurable: the most standby replicas a group's
/// `streams.num.standby.replicas` can ask for.
const MAX_NUM_STANDBY_REPLICAS: i32 = 2;

pub const KEY_SESSION_TIMEOUT_MS: &str = "streams.session.timeout.ms";
pub const KEY_HEARTBEAT_INTERVAL_MS: &str = "streams.heartbeat.interval.ms";
pub const KEY_ACCEPTABLE_RECOVERY_LAG: &str = "streams.acceptable.recovery.lag";
pub const KEY_NUM_WARMUP_REPLICAS: &str = "streams.num.warmup.replicas";
pub const KEY_NUM_STANDBY_REPLICAS: &str = "streams.num.standby.replicas";
pub const KEY_TASK_OFFSET_INTERVAL_MS: &str = "streams.task.offset.interval.ms";
pub const KEY_ASSIGNOR_NAME: &str = "streams.assignor.name";
pub const KEY_RACK_AWARE_ASSIGNMENT_TAGS: &str = "streams.rack.aware.assignment.tags";
pub const KEY_INITIAL_REBALANCE_DELAY_MS: &str = "streams.initial.rebalance.delay.ms";
pub const KEY_ASSIGNMENT_INTERVAL_MS: &str = "streams.assignment.interval.ms";
pub const KEY_SHARE_AUTO_OFFSET_RESET: &str = "share.auto.offset.reset";

/// The `streams.assignor.name` value of a streams group.
///
/// Every value runs the same assignor, a port of Kafka's `StickyTaskAssignor`,
/// the only built-in streams assignor in Kafka. The value is kept only for
/// the configuration surface.
///
/// `from_config_name` also accepts `highly-available`, the spelling of the
/// runtime flag and of the file config.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, krabka_macros::EnumStr)]
#[enum_str(case = "snake_case", as_str = config_name, parse = from_config_name)]
pub enum StreamsAssignorKind {
    #[default]
    Auto,
    Sticky,
    #[enum_str(alias = "highly-available")]
    HighlyAvailable,
}

impl StreamsAssignorKind {
    fn parse(value: &str) -> Result<Self, String> {
        Self::from_config_name(value).ok_or_else(|| {
            format!("{KEY_ASSIGNOR_NAME} must be `auto`, `sticky`, or `highly_available`")
        })
    }
}

/// KIP-932 `share.auto.offset.reset`: where a share partition of a group
/// starts when the share coordinator holds no state for it.
///
/// Kafka's `ShareGroupAutoOffsetResetStrategy` accepts `latest`, `earliest`,
/// and `by_duration:<ISO-8601 duration>`, and defaults to `latest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ShareAutoOffsetReset {
    /// Start at the partition's high watermark: records produced before the
    /// group's first fetch are not delivered.
    #[default]
    Latest,
    /// Start at the partition's log start offset.
    Earliest,
    /// Start at the first record whose timestamp is at or after
    /// `now - duration`. When no record qualifies, Kafka fails the share
    /// partition's initialization with `OFFSET_NOT_AVAILABLE`, and a fetch on
    /// it gets that error until a record inside the window exists.
    ByDuration(Duration),
}

impl ShareAutoOffsetReset {
    /// The value `DescribeConfigs` reports for this strategy.
    ///
    /// The duration renders the way `java.time.Duration::toString` does, so a
    /// round trip through [`Self::parse`] is lossless: days fold into hours,
    /// zero components drop out, and a zero duration renders as `PT0S`.
    #[must_use]
    pub fn config_value(self) -> String {
        match self {
            Self::Latest => "latest".to_owned(),
            Self::Earliest => "earliest".to_owned(),
            Self::ByDuration(duration) => format!("by_duration:{}", iso8601(duration)),
        }
    }

    /// Parses one `share.auto.offset.reset` value.
    ///
    /// # Errors
    /// Returns a message suitable for `INVALID_CONFIG` when the value names no
    /// strategy, when the `by_duration:` prefix carries no duration, or when
    /// the duration is not ISO-8601. As in Kafka, a negative duration is
    /// rejected and a zero duration (`by_duration:PT0S`) is accepted.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "latest" => return Ok(Self::Latest),
            "earliest" => return Ok(Self::Earliest),
            _ => {}
        }
        let duration = value
            .strip_prefix("by_duration:")
            .ok_or_else(invalid_share_auto_offset_reset)
            .and_then(|iso| parse_iso8601(iso).ok_or_else(invalid_share_auto_offset_reset))?;
        Ok(Self::ByDuration(duration))
    }

    /// The strategy a group runs with, given its persisted override map.
    ///
    /// A group with no override, or with one this broker cannot parse, runs
    /// the Kafka default. `IncrementalAlterConfigs` rejects an unparseable
    /// value before it reaches the metadata log, so the fallback covers only a
    /// value written by some other path.
    #[must_use]
    pub fn from_group_overrides(overrides: &BTreeMap<String, String>) -> Self {
        overrides
            .get(KEY_SHARE_AUTO_OFFSET_RESET)
            .and_then(|value| Self::parse(value).ok())
            .unwrap_or_default()
    }
}

fn invalid_share_auto_offset_reset() -> String {
    format!(
        "{KEY_SHARE_AUTO_OFFSET_RESET} must be `latest`, `earliest`, or \
         `by_duration:<PnDTnHnMn.nS>` with a non-negative duration"
    )
}

/// Renders `duration` the way `java.time.Duration::toString` does.
fn iso8601(duration: Duration) -> String {
    let secs = duration.as_secs();
    let nanos = duration.subsec_nanos();
    let (hours, minutes, seconds) = (secs / 3_600, (secs % 3_600) / 60, secs % 60);
    let hours_part = if hours > 0 {
        format!("{hours}H")
    } else {
        String::new()
    };
    let minutes_part = if minutes > 0 {
        format!("{minutes}M")
    } else {
        String::new()
    };
    // A zero duration still renders its seconds, so `PT` never stands alone.
    let seconds_part = match (seconds, nanos, hours, minutes) {
        (0, 0, hours, minutes) if hours > 0 || minutes > 0 => String::new(),
        (seconds, 0, _, _) => format!("{seconds}S"),
        (seconds, nanos, _, _) => {
            let fraction = format!("{nanos:09}");
            format!("{seconds}.{}S", fraction.trim_end_matches('0'))
        }
    };
    format!("PT{hours_part}{minutes_part}{seconds_part}")
}

/// Parses the ISO-8601 duration forms `java.time.Duration::parse` accepts:
/// `PnDTnHnMn.nS`, case-insensitive, each component optionally signed, at
/// least one component present, and a `T` section that is non-empty when it is
/// present. Weeks, months, and years are not duration components, exactly as
/// in Java. Returns `None` for a malformed or negative duration.
fn parse_iso8601(text: &str) -> Option<Duration> {
    let lowered = text.to_ascii_lowercase();
    let (negate, body) = match lowered.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, lowered.strip_prefix('+').unwrap_or(&lowered)),
    };
    let body = body.strip_prefix('p')?;
    let (days_part, time_part) = match body.split_once('t') {
        Some((days, time)) => (days, Some(time)),
        None => (body, None),
    };

    let mut total_nanos: i128 = 0;
    let mut components = 0_usize;
    let mut rest = days_part;
    if !rest.is_empty() {
        let SignedPrefix { value, tail } = take_signed_number(rest, 'd')?;
        total_nanos = total_nanos.checked_add(value.checked_mul(86_400_000_000_000)?)?;
        components += 1;
        rest = tail;
        if !rest.is_empty() {
            return None;
        }
    }
    if let Some(time) = time_part {
        let mut rest = time;
        let mut time_components = 0_usize;
        for (unit, unit_nanos) in [('h', 3_600_000_000_000_i128), ('m', 60_000_000_000)] {
            if rest.is_empty() || !rest.contains(unit) {
                continue;
            }
            let SignedPrefix { value, tail } = take_signed_number(rest, unit)?;
            total_nanos = total_nanos.checked_add(value.checked_mul(unit_nanos)?)?;
            time_components += 1;
            rest = tail;
        }
        if !rest.is_empty() {
            total_nanos = total_nanos.checked_add(take_seconds(rest)?)?;
            time_components += 1;
        }
        if time_components == 0 {
            return None;
        }
        components += time_components;
    }
    if components == 0 {
        return None;
    }
    if negate {
        total_nanos = -total_nanos;
    }
    let total_nanos = u128::try_from(total_nanos).ok()?;
    let secs = u64::try_from(total_nanos / 1_000_000_000).ok()?;
    let subsec = u32::try_from(total_nanos % 1_000_000_000).ok()?;
    Some(Duration::new(secs, subsec))
}

/// A signed integer taken off the front of a duration text, and the rest of
/// the text.
struct SignedPrefix<'a> {
    value: i128,
    tail: &'a str,
}

/// Splits `text` at `unit`, returning the signed integer before it and the
/// remainder after it.
fn take_signed_number(text: &str, unit: char) -> Option<SignedPrefix<'_>> {
    let (number, tail) = text.split_once(unit)?;
    Some(SignedPrefix {
        value: parse_signed(number)?,
        tail,
    })
}

/// Parses the seconds component, `n` or `n.n`, into nanoseconds.
fn take_seconds(text: &str) -> Option<i128> {
    let seconds = text.strip_suffix('s')?;
    let (whole, fraction) = match seconds.split_once('.') {
        Some((whole, fraction)) => (whole, fraction),
        None => (seconds, ""),
    };
    // A bare sign, an empty seconds field, or an over-long fraction is not a
    // duration Java would parse.
    if fraction.len() > 9 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let whole_nanos = parse_signed(whole)?.checked_mul(1_000_000_000)?;
    if fraction.is_empty() {
        return Some(whole_nanos);
    }
    let scaled = format!("{fraction:0<9}").parse::<i128>().ok()?;
    // The fraction carries the sign of the whole part, as in Java.
    let signed = if whole.starts_with('-') {
        -scaled
    } else {
        scaled
    };
    whole_nanos.checked_add(signed)
}

/// Parses an optionally signed decimal integer, rejecting an empty digit run.
fn parse_signed(text: &str) -> Option<i128> {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let magnitude = digits.parse::<i128>().ok()?;
    Some(if text.starts_with('-') {
        -magnitude
    } else {
        magnitude
    })
}

/// KIP-1071 streams-group membership and assignment configuration. Static
/// broker values provide defaults; GROUP resources can override the supported
/// `streams.*` keys for one group through `IncrementalAlterConfigs`.
#[krabka_macros::membership_defaults]
#[derive(Debug, Clone, PartialEq, Eq, krabka_macros::FieldDefaults)]
pub struct StreamsGroupConfig {
    /// Config-level kill switch. The real gate is the `streams.version`
    /// feature (KIP-1071 early access, default-disabled). This switch lets an
    /// operator turn the protocol off even where the feature is finalized.
    #[default(true)]
    pub enable: bool,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,

    /// Max members per group: Kafka's `group.streams.max.size`, whose default
    /// is `Integer.MAX_VALUE`.
    #[default(MAX_SIZE_DEFAULT)]
    pub max_size: usize,
    /// Kafka's `group.streams.initial.rebalance.delay.ms`: how long the first
    /// assignment of a group that a member joins while it is empty waits for
    /// more members. Zero assigns at once.
    #[default(Duration::from_secs(3))]
    pub initial_rebalance_delay: Duration,
    /// Kafka's `group.streams.assignment.interval.ms`: the least time between
    /// two target assignments. Zero does not wait.
    #[default(Duration::from_secs(1))]
    pub assignment_interval: Duration,
    /// `num.standby.replicas`: standby copies per stateful task.
    // Kafka GA defaults: no standby copies, up to 2 warmups,
    // acceptable lag 10k records.
    pub num_standby_replicas: i32,
    /// `max.warmup.replicas`: cap on concurrent warmup tasks. A warmup task
    /// migrates state.
    #[default(2)]
    pub num_warmup_replicas: i32,
    /// `acceptable.recovery.lag`: the maximum changelog lag in records at which
    /// a warmup task is caught up. The assignor can then promote the task to
    /// active or standby.
    #[default(10_000)]
    pub acceptable_recovery_lag: i64,
    /// How often a member reports task offsets, so the assignor can evaluate
    /// warmup catch-up. This is `task_offset_interval_ms` in the heartbeat
    /// response.
    #[default(Duration::from_mins(1))]
    pub task_offset_interval: Duration,
    /// Server-side assignor selection.
    #[default(StreamsAssignorKind::Auto)]
    pub assignor: StreamsAssignorKind,
    /// Kafka's `group.streams.rack.aware.assignment.tags`, which a group
    /// overrides with `streams.rack.aware.assignment.tags`: the client tag
    /// keys every member must send. A version 1 heartbeat of a member that
    /// sends only some of them carries the `MISSING_CLIENT_TAGS` status.
    pub rack_aware_assignment_tags: Vec<String>,
    /// KIP-932 share-partition start strategy for a group with no persisted
    /// share state.
    #[default(ShareAutoOffsetReset::Latest)]
    pub share_auto_offset_reset: ShareAutoOffsetReset,
    /// Kafka's internal `unstable.api.versions.enable`. While it is
    /// [`Disabled`][UnstableApiVersions::Disabled], the default, the group
    /// answers as Kafka 4.3.1 does: a heartbeat response leaves
    /// `TaskOffsetIntervalMs` at 0, `partitionsByUserEndpoint` follows 4.3.1's
    /// `EndpointToPartitionsManager`, and a join at a full group is refused even
    /// for a member already in it. [`Enabled`][UnstableApiVersions::Enabled]
    /// serves Kafka trunk's behavior for those.
    #[default(UnstableApiVersions::Disabled)]
    pub unstable_api_versions: UnstableApiVersions,
    /// Kafka trunk's `group.streams.topology.description.plugin.class`
    /// (KIP-1331): the plugin that stores the topology descriptions the
    /// members push. With none, the default, the broker never asks for one.
    #[default(TopologyDescriptionPlugin::None)]
    pub topology_description_plugin: TopologyDescriptionPlugin,
    #[default(64)]
    pub actor_mailbox_capacity: usize,
}

/// Kafka's `STREAMS_GROUP_MAX_SIZE_DEFAULT`, `Integer.MAX_VALUE`.
const MAX_SIZE_DEFAULT: usize = 2_147_483_647;

impl StreamsGroupConfig {
    /// Apply a persisted GROUP resource override map to these broker defaults.
    ///
    /// A key this coordinator does not apply is left alone: the map holds
    /// every group key Kafka's `GroupConfig` defines, the consumer and share
    /// coordinators' among them, and reading one must not cost the group its
    /// streams overrides.
    ///
    /// A value outside the broker's bounds is capped to them, as Kafka's
    /// `GroupConfig.evaluate` caps a stored group config: the session
    /// timeout, the heartbeat interval and the assignment interval to their
    /// minimum and maximum, and the standby replicas to
    /// `group.streams.max.standby.replicas`. The config RPCs refuse such a
    /// value when they store it, so it is stored only when the broker's
    /// bounds moved since, and the group must still run within them.
    ///
    /// # Errors
    /// Returns a message suitable for `INVALID_CONFIG` when a value cannot be
    /// parsed.
    pub fn with_group_overrides(
        &self,
        overrides: &BTreeMap<String, String>,
    ) -> Result<Self, String> {
        let mut out = self.clone();
        for (key, value) in overrides {
            match key.as_str() {
                KEY_SESSION_TIMEOUT_MS => {
                    out.session_timeout = parse_clamped_millis(
                        key,
                        value,
                        self.min_session_timeout,
                        self.max_session_timeout,
                    )?;
                }
                KEY_HEARTBEAT_INTERVAL_MS => {
                    out.heartbeat_interval = parse_clamped_millis(
                        key,
                        value,
                        self.min_heartbeat_interval,
                        self.max_heartbeat_interval,
                    )?;
                }
                KEY_ACCEPTABLE_RECOVERY_LAG => {
                    out.acceptable_recovery_lag = parse_nonnegative(key, value)?;
                }
                KEY_NUM_WARMUP_REPLICAS => {
                    out.num_warmup_replicas = parse_nonnegative(key, value)?;
                }
                KEY_NUM_STANDBY_REPLICAS => {
                    out.num_standby_replicas =
                        parse_nonnegative::<i32>(key, value)?.min(MAX_NUM_STANDBY_REPLICAS);
                }
                KEY_TASK_OFFSET_INTERVAL_MS => {
                    out.task_offset_interval = parse_positive_millis(key, value)?;
                }
                KEY_ASSIGNOR_NAME => out.assignor = StreamsAssignorKind::parse(value)?,
                KEY_RACK_AWARE_ASSIGNMENT_TAGS => {
                    out.rack_aware_assignment_tags = parse_tag_list(value)?;
                }
                KEY_SHARE_AUTO_OFFSET_RESET => {
                    out.share_auto_offset_reset = ShareAutoOffsetReset::parse(value)?;
                }
                KEY_INITIAL_REBALANCE_DELAY_MS => {
                    out.initial_rebalance_delay =
                        Duration::from_millis(parse_nonnegative::<u64>(key, value)?);
                }
                KEY_ASSIGNMENT_INTERVAL_MS => {
                    out.assignment_interval = parse_clamped_millis(
                        key,
                        value,
                        MIN_ASSIGNMENT_INTERVAL,
                        MAX_ASSIGNMENT_INTERVAL,
                    )?;
                }
                _ => {}
            }
        }
        Ok(out)
    }

    /// Effective values exposed by `DescribeConfigs` for a GROUP resource.
    #[must_use]
    pub fn group_config_values(&self) -> BTreeMap<String, String> {
        maplit::btreemap! {
        KEY_SESSION_TIMEOUT_MS.into() => self.session_timeout.as_millis().to_string(),
        KEY_HEARTBEAT_INTERVAL_MS.into() => self.heartbeat_interval.as_millis().to_string(),
        KEY_ACCEPTABLE_RECOVERY_LAG.into() => self.acceptable_recovery_lag.to_string(),
        KEY_NUM_WARMUP_REPLICAS.into() => self.num_warmup_replicas.to_string(),
        KEY_NUM_STANDBY_REPLICAS.into() => self.num_standby_replicas.to_string(),
        KEY_TASK_OFFSET_INTERVAL_MS.into() => self.task_offset_interval.as_millis().to_string(),
        KEY_INITIAL_REBALANCE_DELAY_MS.into() => self.initial_rebalance_delay.as_millis().to_string(),
        KEY_ASSIGNMENT_INTERVAL_MS.into() => self.assignment_interval.as_millis().to_string(),
        KEY_ASSIGNOR_NAME.into() => self.assignor.config_name().into(),
        KEY_RACK_AWARE_ASSIGNMENT_TAGS.into() => self.rack_aware_assignment_tags.join(","),
        KEY_SHARE_AUTO_OFFSET_RESET.into() => self.share_auto_offset_reset.config_value()}
    }
}

/// Kafka's broker-level `group.streams.rack.aware.assignment.tags`, the
/// default of every group's [`KEY_RACK_AWARE_ASSIGNMENT_TAGS`].
pub const BROKER_KEY_RACK_AWARE_ASSIGNMENT_TAGS: &str = "group.streams.rack.aware.assignment.tags";

/// Parses the broker's `group.streams.rack.aware.assignment.tags`, given as
/// the entries of the Kafka property value, which is those entries joined
/// with commas.
///
/// Kafka checks the two failures in the reverse of the group order.
/// `KafkaConfig`'s `ConfigDef` parse removes repeated entries and then refuses
/// an empty one with `ValidList`'s message. After that,
/// `GroupCoordinatorConfig` re-splits the raw value and refuses a repeated
/// tag key.
///
/// # Errors
///
/// Returns Kafka's message for an empty or a repeated tag key.
pub fn parse_broker_rack_aware_assignment_tags(entries: &[String]) -> Result<Vec<String>, String> {
    let tags = split_tag_list(&entries.join(","));
    if tags.iter().any(String::is_empty) {
        return Err(empty_tag_message(BROKER_KEY_RACK_AWARE_ASSIGNMENT_TAGS));
    }
    if has_duplicate_tag(&tags) {
        return Err(duplicate_tag_message(BROKER_KEY_RACK_AWARE_ASSIGNMENT_TAGS));
    }
    Ok(tags)
}

/// Parses `streams.rack.aware.assignment.tags` as Kafka's `GroupConfig`
/// does. A repeated tag key is refused first
/// (`validateNoDuplicateRackAwareAssignmentTags` reads the raw value), then an
/// empty one (`ValidList.anyNonDuplicateValues(true, false)`).
fn parse_tag_list(value: &str) -> Result<Vec<String>, String> {
    let tags = split_tag_list(value);
    if has_duplicate_tag(&tags) {
        return Err(duplicate_tag_message(KEY_RACK_AWARE_ASSIGNMENT_TAGS));
    }
    if tags.iter().any(String::is_empty) {
        return Err(empty_tag_message(KEY_RACK_AWARE_ASSIGNMENT_TAGS));
    }
    Ok(tags)
}

/// Splits a Kafka `LIST` value: the trimmed value split on commas, with the
/// whitespace next to each comma removed. A blank value is the empty list.
fn split_tag_list(value: &str) -> Vec<String> {
    let trimmed = value.trim_matches(|c: char| c <= ' ');
    if trimmed.is_empty() {
        return Vec::new();
    }
    trimmed
        .split(',')
        .map(|tag| {
            tag.trim_matches([' ', '\t', '\n', '\u{b}', '\u{c}', '\r'])
                .to_owned()
        })
        .collect()
}

fn has_duplicate_tag(tags: &[String]) -> bool {
    let distinct: std::collections::BTreeSet<&String> = tags.iter().collect();
    distinct.len() != tags.len()
}

/// The `require` message of Kafka's `GroupConfig` and
/// `GroupCoordinatorConfig` for a repeated tag key.
fn duplicate_tag_message(key: &str) -> String {
    format!("{key} must not contain duplicate tag keys.")
}

/// `ValidList`'s message for an empty entry.
fn empty_tag_message(key: &str) -> String {
    format!("Configuration '{key}' values must not be empty.")
}

/// The value of an `INT` millisecond key, capped to `min..=max` as Kafka's
/// `GroupConfig.clampToRange` caps it. A negative value is below every bound.
fn parse_clamped_millis(
    key: &str,
    value: &str,
    min: Duration,
    max: Duration,
) -> Result<Duration, String> {
    let millis = value
        .trim()
        .parse::<i32>()
        .map_err(|_| format!("{key} must be an integer"))?;
    Ok(clamp_to_range(
        Duration::from_millis(u64::try_from(millis).unwrap_or(0)),
        min,
        max,
    ))
}

fn parse_positive_millis(key: &str, value: &str) -> Result<Duration, String> {
    let millis = value
        .parse::<u64>()
        .map_err(|_| format!("{key} must be a positive integer"))?;
    if millis == 0 {
        return Err(format!("{key} must be positive"));
    }
    Ok(Duration::from_millis(millis))
}

fn parse_nonnegative<T>(key: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr + PartialOrd + Default,
{
    let parsed = value
        .parse::<T>()
        .map_err(|_| format!("{key} must be a nonnegative integer"))?;
    if parsed < T::default() {
        return Err(format!("{key} must be nonnegative"));
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn defaults_are_kafka_ga() {
        assert!(
            StreamsGroupConfig::default()
                == StreamsGroupConfig {
                    enable: true,
                    session_timeout: Duration::from_secs(45),
                    heartbeat_interval: Duration::from_secs(5),
                    min_session_timeout: Duration::from_secs(45),
                    max_session_timeout: Duration::from_mins(1),
                    min_heartbeat_interval: Duration::from_secs(5),
                    max_heartbeat_interval: Duration::from_secs(15),
                    max_size: 2_147_483_647,
                    initial_rebalance_delay: Duration::from_secs(3),
                    assignment_interval: Duration::from_secs(1),
                    num_standby_replicas: 0,
                    num_warmup_replicas: 2,
                    acceptable_recovery_lag: 10_000,
                    task_offset_interval: Duration::from_mins(1),
                    assignor: StreamsAssignorKind::Auto,
                    rack_aware_assignment_tags: Vec::new(),
                    share_auto_offset_reset: ShareAutoOffsetReset::Latest,
                    unstable_api_versions: UnstableApiVersions::Disabled,
                    topology_description_plugin: TopologyDescriptionPlugin::None,
                    actor_mailbox_capacity: 64,
                }
        );
    }

    /// Kafka trunk's `GroupConfig` parses `streams.rack.aware.assignment.tags`
    /// as a `LIST` and refuses a repeated or an empty tag key, and
    /// `DescribeConfigs` reports the parsed list joined with commas.
    #[test]
    fn rack_aware_assignment_tags_parse_as_kafkas_group_config_does() {
        /// The tag keys that the value parses to, and the value that
        /// `DescribeConfigs` reports.
        struct Parsed {
            tags: Vec<&'static str>,
            reported: &'static str,
        }

        const DUPLICATE: &str =
            "streams.rack.aware.assignment.tags must not contain duplicate tag keys.";
        const EMPTY: &str =
            "Configuration 'streams.rack.aware.assignment.tags' values must not be empty.";
        for (value, want) in [
            (
                "",
                Ok(Parsed {
                    tags: vec![],
                    reported: "",
                }),
            ),
            (
                "  ",
                Ok(Parsed {
                    tags: vec![],
                    reported: "",
                }),
            ),
            (
                "zone",
                Ok(Parsed {
                    tags: vec!["zone"],
                    reported: "zone",
                }),
            ),
            (
                " zone ,\track ",
                Ok(Parsed {
                    tags: vec!["zone", "rack"],
                    reported: "zone,rack",
                }),
            ),
            ("rack,zone,rack", Err(DUPLICATE)),
            ("zone, zone", Err(DUPLICATE)),
            ("zone,,rack", Err(EMPTY)),
            ("zone,", Err(EMPTY)),
        ] {
            let overrides =
                maplit::btreemap! {KEY_RACK_AWARE_ASSIGNMENT_TAGS.into() => value.to_owned()};
            let got = StreamsGroupConfig::default()
                .with_group_overrides(&overrides)
                .map(|config| {
                    let reported =
                        config.group_config_values()[KEY_RACK_AWARE_ASSIGNMENT_TAGS].clone();
                    (config.rack_aware_assignment_tags, reported)
                });
            let want = want
                .map(|Parsed { tags, reported }| {
                    (
                        tags.into_iter().map(str::to_owned).collect::<Vec<_>>(),
                        reported.to_owned(),
                    )
                })
                .map_err(str::to_owned);
            assert!(got == want, "{value:?}");
        }
    }

    #[test]
    fn group_overrides_are_validated_and_applied() {
        let overrides = maplit::btreemap! {
        KEY_SESSION_TIMEOUT_MS.into() => "50000".into(),
        KEY_HEARTBEAT_INTERVAL_MS.into() => "6000".into(),
        KEY_NUM_STANDBY_REPLICAS.into() => "1".into(),
        KEY_ASSIGNOR_NAME.into() => "highly_available".into()};
        let got = StreamsGroupConfig::default()
            .with_group_overrides(&overrides)
            .expect("valid overrides");
        assert!(got.session_timeout == Duration::from_secs(50));
        assert!(got.heartbeat_interval == Duration::from_secs(6));
        assert!(got.num_standby_replicas == 1);
        assert!(got.assignor == StreamsAssignorKind::HighlyAvailable);
    }

    #[test]
    fn share_auto_offset_reset_parses_the_three_kafka_forms() {
        // Kafka 4.3.1 types this key STRING with
        // `[latest, earliest, by_duration:PnDTnHnMn.nS]`, defaults it to
        // `latest`, and validates it with
        // `ShareGroupAutoOffsetResetStrategy`, which delegates the duration to
        // `java.time.Duration::parse` and rejects only a negative one.
        for (value, want) in [
            ("latest", Some(ShareAutoOffsetReset::Latest)),
            ("earliest", Some(ShareAutoOffsetReset::Earliest)),
            (
                "by_duration:PT1H",
                Some(ShareAutoOffsetReset::ByDuration(Duration::from_secs(3_600))),
            ),
            (
                "by_duration:P1DT2H3M4.5S",
                Some(ShareAutoOffsetReset::ByDuration(Duration::new(
                    93_784,
                    500_000_000,
                ))),
            ),
            (
                "by_duration:PT0S",
                Some(ShareAutoOffsetReset::ByDuration(Duration::ZERO)),
            ),
            ("by_duration:-PT1H", None),
            ("by_duration:PT-1H", None),
            ("by_duration:PT", None),
            ("by_duration:P", None),
            ("by_duration:", None),
            ("by_duration", None),
            ("by_duration:1H", None),
            ("by_duration:P1H", None),
            ("by_duration:PT1X", None),
            ("by_duration:P1W", None),
            ("by_duration:PT1M1H", None),
            ("Earliest", None),
            ("none", None),
            ("", None),
        ] {
            let overrides =
                maplit::btreemap! {KEY_SHARE_AUTO_OFFSET_RESET.into() => value.to_owned()};
            let got = StreamsGroupConfig::default()
                .with_group_overrides(&overrides)
                .ok()
                .map(|config| config.share_auto_offset_reset);
            assert!(got == want, "{KEY_SHARE_AUTO_OFFSET_RESET}={value}");
        }
    }

    #[test]
    fn share_auto_offset_reset_is_echoed_by_describe_configs() {
        // The default the key reports, and the round trip a configured value
        // takes: `DescribeConfigs` renders the duration the way
        // `java.time.Duration::toString` does, so the value it reports parses
        // back to the same strategy.
        assert!(
            StreamsGroupConfig::default().group_config_values()[KEY_SHARE_AUTO_OFFSET_RESET]
                == "latest"
        );
        for (value, want) in [
            ("earliest", "earliest"),
            ("latest", "latest"),
            ("by_duration:PT1H", "by_duration:PT1H"),
            ("by_duration:P1DT2H3M4.5S", "by_duration:PT26H3M4.5S"),
            ("by_duration:PT0S", "by_duration:PT0S"),
            ("by_duration:pt90m", "by_duration:PT1H30M"),
        ] {
            let overrides =
                maplit::btreemap! {KEY_SHARE_AUTO_OFFSET_RESET.into() => value.to_owned()};
            let reported = StreamsGroupConfig::default()
                .with_group_overrides(&overrides)
                .expect("valid strategy")
                .group_config_values()[KEY_SHARE_AUTO_OFFSET_RESET]
                .clone();
            assert!(reported == want, "{KEY_SHARE_AUTO_OFFSET_RESET}={value}");
            assert!(ShareAutoOffsetReset::parse(&reported) == ShareAutoOffsetReset::parse(value));
        }
    }

    #[test]
    fn share_auto_offset_reset_from_group_overrides_defaults_to_latest() {
        assert!(
            ShareAutoOffsetReset::from_group_overrides(&BTreeMap::new())
                == ShareAutoOffsetReset::Latest
        );
        let configured =
            maplit::btreemap! {KEY_SHARE_AUTO_OFFSET_RESET.into() => "earliest".to_owned()};
        assert!(
            ShareAutoOffsetReset::from_group_overrides(&configured)
                == ShareAutoOffsetReset::Earliest
        );
        // A value this broker cannot parse cannot reach the metadata log
        // through `IncrementalAlterConfigs`; the strategy falls back to the
        // Kafka default rather than failing a fetch.
        let bogus = maplit::btreemap! {KEY_SHARE_AUTO_OFFSET_RESET.into() => "bogus".to_owned()};
        assert!(ShareAutoOffsetReset::from_group_overrides(&bogus) == ShareAutoOffsetReset::Latest);
    }

    /// Kafka's `GroupConfig.evaluate` caps a stored group config to the
    /// broker's bounds, and keeps every other override of the group. A
    /// heartbeat interval of 0, which the actor's ticker cannot run, is capped
    /// to the minimum like any other value below it.
    #[test]
    fn group_overrides_are_capped_to_the_broker_bounds() {
        let broker = StreamsGroupConfig::default();
        let rows = [
            (KEY_SESSION_TIMEOUT_MS, "1000", StreamsGroupConfig {
                session_timeout: broker.min_session_timeout,
                ..broker.clone()
            }),
            (KEY_SESSION_TIMEOUT_MS, "3600000", StreamsGroupConfig {
                session_timeout: broker.max_session_timeout,
                ..broker.clone()
            }),
            (KEY_HEARTBEAT_INTERVAL_MS, "0", StreamsGroupConfig {
                heartbeat_interval: broker.min_heartbeat_interval,
                ..broker.clone()
            }),
            (KEY_HEARTBEAT_INTERVAL_MS, "-1", StreamsGroupConfig {
                heartbeat_interval: broker.min_heartbeat_interval,
                ..broker.clone()
            }),
            (KEY_HEARTBEAT_INTERVAL_MS, "60000", StreamsGroupConfig {
                heartbeat_interval: broker.max_heartbeat_interval,
                ..broker.clone()
            }),
            (KEY_ASSIGNMENT_INTERVAL_MS, "3600000", StreamsGroupConfig {
                assignment_interval: MAX_ASSIGNMENT_INTERVAL,
                ..broker.clone()
            }),
            (KEY_NUM_STANDBY_REPLICAS, "5", StreamsGroupConfig {
                num_standby_replicas: MAX_NUM_STANDBY_REPLICAS,
                ..broker.clone()
            }),
        ];
        for (key, value, expected) in rows {
            let overrides = maplit::btreemap! {key.into() => value.into()};
            assert!(
                broker.with_group_overrides(&overrides) == Ok(expected),
                "{key}={value}"
            );
        }
    }

    /// A stored value that is not a whole number of milliseconds is still
    /// refused, where a bound only caps.
    #[test]
    fn group_overrides_refuse_a_value_that_does_not_parse() {
        let overrides = maplit::btreemap! {KEY_SESSION_TIMEOUT_MS.into() => "soon".into()};
        assert!(
            StreamsGroupConfig::default()
                .with_group_overrides(&overrides)
                .is_err()
        );
    }

    /// The group override map holds every group key, the consumer and share
    /// coordinators' among them. A key this coordinator does not apply must
    /// not cost the group the streams overrides stored beside it.
    #[test]
    fn a_key_of_another_coordinator_leaves_the_streams_overrides_intact() {
        let overrides = maplit::btreemap! {
            KEY_NUM_STANDBY_REPLICAS.into() => "1".into(),
            "consumer.session.timeout.ms".into() => "50000".into(),
            "share.record.lock.duration.ms".into() => "45000".into(),
            "errors.deadletterqueue.topic.name".into() => "dlq".into(),
        };
        let got = StreamsGroupConfig::default()
            .with_group_overrides(&overrides)
            .expect("foreign keys are ignored");
        assert!(
            got == StreamsGroupConfig {
                num_standby_replicas: 1,
                ..StreamsGroupConfig::default()
            }
        );
    }

    /// `streams.initial.rebalance.delay.ms` and `streams.assignment.interval.ms`
    /// reach the fields the streams actor waits on, and are reported.
    #[test]
    fn the_rebalance_delay_and_assignment_interval_are_applied_and_reported() {
        let overrides = maplit::btreemap! {
            KEY_INITIAL_REBALANCE_DELAY_MS.into() => "0".into(),
            KEY_ASSIGNMENT_INTERVAL_MS.into() => "250".into(),
        };
        let got = StreamsGroupConfig::default()
            .with_group_overrides(&overrides)
            .expect("valid overrides");
        assert!(got.initial_rebalance_delay == Duration::ZERO);
        assert!(got.assignment_interval == Duration::from_millis(250));
        let reported = got.group_config_values();
        assert!(reported[KEY_INITIAL_REBALANCE_DELAY_MS] == "0");
        assert!(reported[KEY_ASSIGNMENT_INTERVAL_MS] == "250");
    }
}
