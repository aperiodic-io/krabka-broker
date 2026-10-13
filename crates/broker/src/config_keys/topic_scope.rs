//! The topic-scoped config key that only the controller writes: KFC-9's
//! synthesised write-freeze key and the refusal that both alter paths give
//! for it. This is the topic-side
//! counterpart of [`super::broker_scope`].

/// KFC-9: the write-freeze state of one topic.
///
/// The broker synthesises this key. It is never stored in `V1TopicConfig`,
/// and it never reaches the topic-config record in the metadata log. The
/// freeze itself lives in the metadata log as
/// [`krabka_metadata::TopicFreezeRecord`], so no snapshot and no restore can
/// bring back a stale freeze through a topic config.
///
/// The key is controller-managed and read-only. `DescribeConfigs` reports it
/// with `read_only` set, and both `AlterConfigs` and
/// `IncrementalAlterConfigs` refuse it with `INVALID_CONFIG`. The
/// krabka-private `SetTopicFreeze` API (key 1015) and the `krabka-guard` CLI
/// are the one path that sets and clears it.
///
/// An operator who holds only the JVM tools reads the freeze with
/// `kafka-configs --entity-type topics --describe`.
pub(crate) const WRITE_FREEZE: &str = "write.freeze";

/// Removed private ELR topic-config key, retained only for migration tests.
#[cfg(test)]
pub(crate) const ELIGIBLE_LEADER_REPLICAS: &str = "krabka.elr";

/// Topic-scoped config keys that only the controller writes. This is the
/// topic-side analogue of [`super::broker_scope::CONTROLLER_MANAGED_BROKER_CONFIGS`]:
/// `AlterConfigs` and `IncrementalAlterConfigs` must reject every key in this
/// list, and `DescribeConfigs` must report each one as read-only.
pub(crate) const CONTROLLER_MANAGED_TOPIC_CONFIGS: [&str; 1] = [WRITE_FREEZE];

/// `true` when `key` is a topic config that only the controller writes.
pub(crate) fn is_controller_managed_topic_config(key: &str) -> bool {
    CONTROLLER_MANAGED_TOPIC_CONFIGS.contains(&key)
}

/// The refusal both alter paths give for a controller-managed topic config.
///
/// Both handlers build the message here, so an operator reads one wording
/// from `AlterConfigs` and from `IncrementalAlterConfigs`. The message names
/// what does change the key. A refusal that names nothing leaves the operator
/// with no next step.
pub(crate) fn controller_managed_topic_config_message(key: &str) -> String {
    let remedy =
        "use `krabka-guard freeze set` to set it and `krabka-guard freeze clear` to clear it";
    format!("topic config {key} is controller-managed and read-only; {remedy}")
}

#[cfg(test)]
mod tests {
    use {
        super::{
            super::{
                RETENTION_MS,
                broker_scope::BROKER_WITNESS,
                validation::{is_recognized, validate_topic_config},
            },
            *,
        },
        assert2::check,
    };

    #[test]
    fn a_controller_managed_topic_config_is_named_as_one() {
        for (label, key, expected) in [
            ("the write-freeze key", WRITE_FREEZE, true),
            (
                "the removed ELR config key",
                ELIGIBLE_LEADER_REPLICAS,
                false,
            ),
            ("an ordinary topic key", RETENTION_MS, false),
            (
                "a broker-scoped controller-managed key",
                BROKER_WITNESS,
                false,
            ),
            ("an unknown key", "not.a.topic.config", false),
            ("an empty key", "", false),
        ] {
            check!(
                is_controller_managed_topic_config(key) == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn a_controller_managed_key_stays_outside_the_stored_whitelist() {
        // Both are written by the controller alone, so the validator must not
        // accept either as an ordinary override an alter path could set.
        for key in CONTROLLER_MANAGED_TOPIC_CONFIGS {
            check!(!is_recognized(key), "{key}");
            check!(validate_topic_config(key, "true").is_err(), "{key}");
        }
    }

    #[test]
    fn the_refusal_names_the_key_and_the_commands_that_change_it() {
        let message = controller_managed_topic_config_message(WRITE_FREEZE);

        check!(message.contains(WRITE_FREEZE), "got: {message}");
        check!(
            message.contains("krabka-guard freeze set"),
            "got: {message}"
        );
        check!(
            message.contains("krabka-guard freeze clear"),
            "got: {message}"
        );
    }
}
