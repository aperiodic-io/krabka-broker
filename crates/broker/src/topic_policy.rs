//! KIP-108 / KIP-133: the operator-declared rule set a topic must satisfy
//! before the controller commits its creation or its config change.
//!
//! Kafka names a Java class in `create.topic.policy.class.name` and
//! `alter.config.policy.class.name`, and the broker calls
//! `CreateTopicPolicy.validate` / `AlterConfigPolicy.validate` after config
//! validation and before the records are generated. A `PolicyViolationException`
//! becomes [`POLICY_VIOLATION`](crate::codes::POLICY_VIOLATION) on that row,
//! carrying the exception message.
//!
//! krabka cannot load a Java class, so the hook is this declared rule set,
//! configured through `[topic_policy]`. An absent table leaves every field
//! unset, which is Kafka's default of no policy class: [`check`] then accepts
//! everything. The rules are the ones a managed-Kafka policy class actually
//! contains — a replication-factor floor, a partition ceiling, a
//! `min.insync.replicas` floor, and required / forbidden config values.
//!
//! # What the rules can and cannot see
//!
//! [`check`] reads the request and nothing else, which is what Kafka's two
//! policy interfaces see:
//!
//! * `CreateTopics` passes the effective partition count and replication
//!   factor — the placement has already resolved the `-1` defaults — plus the
//!   topic's requested config overrides.
//! * The two `AlterConfigs` paths pass `None` for both numbers, because
//!   `AlterConfigPolicy.RequestMetadata` carries only the resource and the
//!   resolved config map. Neither number can change on that path anyway.
//! * A config rule reads the map alone. A key that is absent from it leaves
//!   the cluster default in force, and [`check`] does not resolve that
//!   default: the operator who writes the policy also writes the default, so
//!   an absent key passes. Resolving it would need the metadata image here,
//!   which is the upgrade path if a cluster ever needs the stricter reading.
//!
//! A richer rule than this table can express — one that reads the principal,
//! or the broker set — would go in the OPA authorizer input
//! (`authorizer::opa`'s private `wire` module) rather than here. Nothing
//! needs it yet.

use std::collections::BTreeMap;

use crate::config_keys;

/// The `[topic_policy]` rule set. Every field defaults to "no rule", so
/// [`TopicPolicy::default`] accepts every topic and every config change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopicPolicy {
    /// Lowest replication factor a new topic may be created with.
    pub min_replication_factor: Option<usize>,
    /// Highest partition count a new topic may be created with.
    pub max_partitions: Option<usize>,
    /// Lowest `min.insync.replicas` a topic may carry, when the topic sets
    /// the key at all.
    pub min_insync_replicas: Option<usize>,
    /// Config keys whose value the topic must carry exactly.
    pub required: BTreeMap<String, String>,
    /// Config keys whose value the topic must not carry.
    pub forbidden: BTreeMap<String, String>,
}

impl TopicPolicy {
    /// Whether no rule is declared, in which case [`check`] is a no-op.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

/// Check one topic against the policy. `Err` carries the reason, which the
/// caller returns as the `error_message` beside
/// [`POLICY_VIOLATION`](crate::codes::POLICY_VIOLATION).
///
/// `partitions` and `replication_factor` are the effective values, or `None`
/// on the `AlterConfigs` paths where neither is part of the request.
///
/// # Errors
///
/// Returns the operator-facing reason string for the first rule the topic
/// breaks.
pub fn check(
    policy: &TopicPolicy,
    topic: &str,
    partitions: Option<usize>,
    replication_factor: Option<usize>,
    configs: &BTreeMap<String, String>,
) -> Result<(), String> {
    if let (Some(limit), Some(count)) = (policy.max_partitions, partitions)
        && count > limit
    {
        return Err(format!(
            "topic `{topic}` asks for {count} partitions, but the cluster topic policy allows at \
             most {limit}"
        ));
    }
    if let (Some(floor), Some(factor)) = (policy.min_replication_factor, replication_factor)
        && factor < floor
    {
        return Err(format!(
            "topic `{topic}` has replication factor {factor}, but the cluster topic policy \
             requires at least {floor}"
        ));
    }
    if let Some(floor) = policy.min_insync_replicas
        && let Some(value) = configs.get(config_keys::MIN_INSYNC_REPLICAS)
    {
        let parsed = value.parse::<usize>().map_err(|_| {
            format!(
                "topic `{topic}` has {} = `{value}`, which is not a number",
                config_keys::MIN_INSYNC_REPLICAS
            )
        })?;
        if parsed < floor {
            return Err(format!(
                "topic `{topic}` has {} = {parsed}, but the cluster topic policy requires at \
                 least {floor}",
                config_keys::MIN_INSYNC_REPLICAS
            ));
        }
    }
    for (key, want) in &policy.required {
        if configs.get(key) != Some(want) {
            return Err(format!(
                "topic `{topic}` must set `{key}` to `{want}`, which the cluster topic policy \
                 requires"
            ));
        }
    }
    for (key, refused) in &policy.forbidden {
        if configs.get(key) == Some(refused) {
            return Err(format!(
                "topic `{topic}` sets `{key}` to `{refused}`, which the cluster topic policy \
                 forbids"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::test_support::config_map as configs;

    fn policy() -> TopicPolicy {
        TopicPolicy {
            min_replication_factor: Some(3),
            max_partitions: Some(64),
            min_insync_replicas: Some(2),
            required: configs(&[("cleanup.policy", "delete")]),
            forbidden: configs(&[("unclean.leader.election.enable", "true")]),
        }
    }

    #[test]
    fn an_absent_table_accepts_everything() {
        let empty = TopicPolicy::default();
        check!(empty.is_empty());
        check!(
            check(
                &empty,
                "orders",
                Some(4096),
                Some(1),
                &configs(&[("unclean.leader.election.enable", "true")]),
            )
            .is_ok()
        );
    }

    #[test]
    fn each_rule_refuses_what_it_names_and_passes_what_it_allows() {
        /// Label, partitions, replication factor, configs, and the substring
        /// the refusal must name — `None` for a topic the policy accepts.
        type Case<'a> = (
            &'a str,
            Option<usize>,
            Option<usize>,
            Vec<(&'a str, &'a str)>,
            Option<&'a str>,
        );

        let ok = vec![("cleanup.policy", "delete"), ("min.insync.replicas", "2")];
        let cases: Vec<Case<'_>> = vec![
            (
                "a topic that satisfies every rule",
                Some(64),
                Some(3),
                ok.clone(),
                None,
            ),
            (
                "one partition over the ceiling",
                Some(65),
                Some(3),
                ok.clone(),
                Some("at most 64"),
            ),
            (
                "one replica under the floor",
                Some(1),
                Some(2),
                ok.clone(),
                Some("at least 3"),
            ),
            (
                "min.insync.replicas under the floor",
                Some(1),
                Some(3),
                vec![("cleanup.policy", "delete"), ("min.insync.replicas", "1")],
                Some("min.insync.replicas = 1"),
            ),
            (
                "min.insync.replicas absent leaves the cluster default in force",
                Some(1),
                Some(3),
                vec![("cleanup.policy", "delete")],
                None,
            ),
            (
                "a required key with the wrong value",
                Some(1),
                Some(3),
                vec![("cleanup.policy", "compact"), ("min.insync.replicas", "2")],
                Some("must set `cleanup.policy` to `delete`"),
            ),
            (
                "a required key that is absent",
                Some(1),
                Some(3),
                vec![("min.insync.replicas", "2")],
                Some("must set `cleanup.policy` to `delete`"),
            ),
            (
                "a forbidden key at its forbidden value",
                Some(1),
                Some(3),
                vec![
                    ("cleanup.policy", "delete"),
                    ("min.insync.replicas", "2"),
                    ("unclean.leader.election.enable", "true"),
                ],
                Some("forbids"),
            ),
            (
                "a forbidden key at another value",
                Some(1),
                Some(3),
                vec![
                    ("cleanup.policy", "delete"),
                    ("min.insync.replicas", "2"),
                    ("unclean.leader.election.enable", "false"),
                ],
                None,
            ),
        ];

        for (label, partitions, replication_factor, pairs, want) in cases {
            let result = check(
                &policy(),
                "orders",
                partitions,
                replication_factor,
                &configs(&pairs),
            );
            let got = result.err();
            match want {
                None => {
                    check!(got.is_none(), "{label}: {got:?}");
                }
                Some(fragment) => {
                    let reason = got.unwrap_or_default();
                    check!(reason.contains(fragment), "{label}: {reason}");
                    check!(reason.contains("orders"), "{label}: {reason}");
                }
            }
        }
    }

    #[test]
    fn the_alter_paths_pass_none_and_skip_the_two_numeric_rules() {
        check!(
            check(
                &policy(),
                "orders",
                None,
                None,
                &configs(&[("cleanup.policy", "delete"), ("min.insync.replicas", "2")]),
            )
            .is_ok()
        );
    }

    #[test]
    fn a_non_numeric_min_insync_replicas_is_refused_rather_than_ignored() {
        let reason = check(
            &policy(),
            "orders",
            None,
            None,
            &configs(&[("cleanup.policy", "delete"), ("min.insync.replicas", "two")]),
        )
        .expect_err("a value the floor cannot be compared against must not pass");
        check!(reason.contains("not a number"), "{reason}");
    }
}
