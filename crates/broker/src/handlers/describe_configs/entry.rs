//! Shaping one `DescribeConfigsResourceResult` from the layers a value can
//! come from.
//!
//! Kafka answers `DescribeConfigs` with more than a value. Each entry carries
//! the `ConfigDef` type the JVM `AdminClient` parses the value with, the
//! documentation string when the request asked for it, whether the value is
//! one the broker must not disclose, and the *synonym chain*: every layer that
//! holds a value for the key, most specific first. `ConfigEntry.source()` is
//! the head of that chain, which is how `kafka-configs --describe --all` tells
//! a dynamic override from an inherited default.
//!
//! The chain's order is Kafka's, from `ConfigHelper`:
//!
//! 1. `DYNAMIC_TOPIC_CONFIG` — the topic's own override.
//! 2. `DYNAMIC_BROKER_CONFIG` — this node's per-broker override.
//! 3. `DYNAMIC_DEFAULT_BROKER_CONFIG` — the cluster-wide default.
//! 4. `STATIC_BROKER_CONFIG` — the node's static configuration.
//! 5. `DEFAULT_CONFIG` — the built-in default.
//!
//! A layer with no value is left out, so a chain is usually short. Krabka
//! populates the layers it genuinely reads: a topic resolves an override over
//! the cluster-wide default over the built-in default, and a broker resource
//! resolves a per-node override over the cluster-wide default, with `node.id`
//! arriving from the static configuration. Krabka stores no per-broker
//! *topic* setting, so a topic never reports a `DYNAMIC_BROKER_CONFIG`
//! synonym, and Kafka reports none either when the layer holds nothing.
//!
//! The type, the documentation, the read-only flag, and the sensitivity all
//! come from [`crate::config_keys::registry`], the one table every config
//! surface reads.

use {
    super::wire::CONFIG_SOURCE_DEFAULT,
    crate::config_keys::registry::ConfigKey,
    krabka_protocol::owned::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsSynonym,
    },
};

/// `ConfigDef.Type::UNKNOWN` wire byte, for a key no registry row covers.
const CONFIG_TYPE_UNKNOWN: i8 = 0;

/// One layer of the precedence chain that holds a value for a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Layer<'a> {
    /// The `ConfigSource` byte the layer reports.
    pub(super) source: i8,
    /// The config key the layer holds the value under. A topic key that
    /// inherits from a broker config names the broker config here, the way
    /// Kafka names `log.cleanup.policy` beneath `cleanup.policy`.
    pub(super) name: &'a str,
    pub(super) value: &'a str,
}

/// The bottom of the chain: the value an unset key reports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct DefaultLayer<'a> {
    /// The value the key reports when no layer above holds one.
    pub(super) value: Option<&'a str>,
    /// The config the default is stated on. `None` keeps the default off the
    /// synonym chain, which is what Kafka does for a key it names no
    /// broker-level config for.
    pub(super) name: Option<&'a str>,
}

/// The two request flags that decide how much of an entry the client wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct EntryOptions {
    pub(super) include_synonyms: bool,
    pub(super) include_documentation: bool,
}

impl EntryOptions {
    /// The flags a `DescribeConfigs` request carries.
    pub(super) const fn from_request(
        req: &krabka_protocol::owned::describe_configs_request::DescribeConfigsRequest,
    ) -> Self {
        Self {
            include_synonyms: req.include_synonyms,
            include_documentation: req.include_documentation,
        }
    }
}

/// Build one entry from its precedence chain.
///
/// `overrides` are the layers above the default, most specific first, and
/// each one holds a value. The reported value and `config_source` are the head
/// of that chain, or `default` and `DEFAULT_CONFIG` when the chain is empty.
///
/// A key the registry marks sensitive reports a null value, in the entry and
/// in every synonym, which is how Kafka keeps a password-valued config off the
/// wire. A `row` of `None` is a name Kafka does not define, which
/// `KafkaConfig.maybeSensitive` treats the same way, with type `UNKNOWN`, and
/// which is read-only, because it is in no dynamic set
/// (`ALL_DYNAMIC_CONFIGS.contains(name)` is false).
pub(super) fn config_entry(
    row: Option<&ConfigKey>,
    name: &str,
    overrides: &[Layer<'_>],
    default: DefaultLayer<'_>,
    options: EntryOptions,
) -> DescribeConfigsResourceResult {
    let sensitive = row.is_none_or(ConfigKey::is_sensitive);
    let disclose = |value: &str| (!sensitive).then(|| value.to_owned());

    let (value, config_source) = overrides.first().map_or_else(
        || (default.value.and_then(disclose), CONFIG_SOURCE_DEFAULT),
        |head| (disclose(head.value), head.source),
    );

    let synonyms =
        if options.include_synonyms {
            overrides
                .iter()
                .map(|layer| DescribeConfigsSynonym {
                    name: layer.name.to_owned(),
                    value: disclose(layer.value),
                    source: layer.source,
                    ..Default::default()
                })
                .chain(default.name.zip(default.value).map(|(name, value)| {
                    DescribeConfigsSynonym {
                        name: name.to_owned(),
                        value: disclose(value),
                        source: CONFIG_SOURCE_DEFAULT,
                        ..Default::default()
                    }
                }))
                .collect()
        } else {
            Vec::new()
        };

    DescribeConfigsResourceResult {
        name: name.to_owned(),
        value,
        read_only: row.is_none_or(|row| row.read_only),
        config_source,
        is_sensitive: sensitive,
        synonyms,
        config_type: row.map_or(CONFIG_TYPE_UNKNOWN, |row| row.config_type.wire()),
        // A row with no documentation string, such as a `KafkaConfig` key
        // krabka has no row of its own for, reports none rather than "".
        documentation: options
            .include_documentation
            .then(|| {
                row.filter(|row| !row.doc.is_empty())
                    .map(|row| row.doc.to_owned())
            })
            .flatten(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests;
