//! The idle window `DescribeConfigs` reports beside the dynamic overrides the
//! metadata image holds: `connections.max.idle.ms` and its per-listener
//! overrides.
//!
//! Kafka answers a named BROKER resource with every value the node actually
//! runs with, static ones included, and answers the cluster-default
//! (`--entity-default`) resource with the dynamic cluster-wide values only.
//! These keys go out beside the static `node.id` entry and the KIP-211
//! retention keys, for the same resources they do, and through the same
//! [`config_entry`] builder, so one precedence chain shapes every entry.
//!
//! Every shape below was read off `apache/kafka:4.3.1` running with
//! `connections.max.idle.ms=30000` and
//! `listener.name.plaintext.connections.max.idle.ms=15000`:
//!
//! - `kafka-configs --describe --entity-type brokers --entity-name 1 --all`
//!   reports `connections.max.idle.ms=30000` with the synonym chain
//!   `STATIC_BROKER_CONFIG:…=30000, DEFAULT_CONFIG:…=600000`. So the key is
//!   spelled that way, an operator-set value reads `STATIC_BROKER_CONFIG`, an
//!   unset one falls back to `DEFAULT_CONFIG`, and the default is 600000.
//! - The same describe reports
//!   `listener.name.plaintext.connections.max.idle.ms=15000` as a separate
//!   entry, also at `STATIC_BROKER_CONFIG`, under a lowercased listener name.
//! - `kafka-configs --alter … --add-config connections.max.idle.ms=20000`
//!   fails with `InvalidRequestException: Cannot update these configs
//!   dynamically`, so the key is read-only.
//!
//! Two finer points about the source and the chain were settled the same way,
//! against the same image, by running it twice — once with
//! `connections.max.idle.ms=600000` spelled out and once with the key absent —
//! and diffing the same `--all` describe:
//!
//! - The source is provenance, never a value comparison. The explicit
//!   `connections.max.idle.ms=600000` — the default, spelled out — answers
//!   `synonyms={STATIC_BROKER_CONFIG:connections.max.idle.ms=600000,
//!   DEFAULT_CONFIG:connections.max.idle.ms=600000}`. With the key absent the
//!   same describe answers `synonyms={DEFAULT_CONFIG:…=600000}`. The two
//!   values are identical and the two chains are not, and since an entry's
//!   `config_source` is the source of its chain's head, the two sources
//!   differ too — so an auditor can tell a pinned setting from an inherited
//!   one.
//! - A per-listener entry's chain is its own value first, then the same
//!   broker-wide tail: with both keys set it reads
//!   `{STATIC_BROKER_CONFIG:listener.name.plaintext.connections.max.idle.ms
//!   =15000, STATIC_BROKER_CONFIG:connections.max.idle.ms=600000,
//!   DEFAULT_CONFIG:connections.max.idle.ms=600000}`, and with the
//!   broker-wide key absent the middle element is simply gone. `node.id`,
//!   which has no key behind it, gets the one-element chain
//!   `{STATIC_BROKER_CONFIG:node.id=1}`.
//!
//! The chain itself is conditional: Kafka builds it only for a request that
//! sets `include_synonyms`, and answers every entry with an empty list
//! otherwise, so a caller that was not asked for synonyms must not
//! synthesise any. [`config_entry`] is what holds that rule.
//!
//! A per-listener key is a key per listener rather than a registry row, so
//! every one of them is reported under the broker-wide
//! [`CONNECTIONS_MAX_IDLE_MS`] row: it is the same config under Kafka's
//! `ListenerName.configPrefix`, and it is that row that carries the type the
//! JVM `AdminClient` parses the value with, the documentation, and the
//! read-only flag.
//!
//! One behaviour is krabka's own, and the container is what settled it: with
//! the per-listener key set to 15000 above, a raw TCP connection to the
//! PLAINTEXT listener that sent nothing was closed after 30 seconds, not 15.
//! Kafka reports `listener.name.<name>.connections.max.idle.ms` and then
//! ignores it -- its `DataPlaneAcceptor` takes `max.connections` and
//! `max.connection.creation.rate` per listener, but the idle window only
//! broker-wide. krabka honours the override, because a listener facing the
//! internet and a listener facing the cluster want different windows. The
//! reported shape is Kafka's; only the effect is a superset, and nothing on
//! the wire changes.

use {
    super::{
        entry::{DefaultLayer, EntryOptions, Layer, config_entry},
        resources::StaticBrokerConfigs,
        wire::CONFIG_SOURCE_STATIC_BROKER,
    },
    crate::{
        config::DEFAULT_CONNECTIONS_MAX_IDLE,
        config_keys::{
            CONNECTIONS_MAX_IDLE_MS,
            registry::{self, ConfigScope},
        },
    },
    krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult,
    krabka_units::convert::TimeExt as _,
};

/// The per-listener key for `listener_name`. The name in the middle is
/// lowercased, which is what Kafka's `ListenerName.configPrefix` does.
fn listener_connections_max_idle_key(listener_name: &str) -> String {
    format!(
        "listener.name.{}.{CONNECTIONS_MAX_IDLE_MS}",
        listener_name.to_ascii_lowercase()
    )
}

/// The idle-window entries a named broker resource reports, filtered by the
/// request's `configuration_keys`. The caller sorts them in with the rest.
pub(super) fn idle_window_entries(
    configs: StaticBrokerConfigs<'_>,
    wanted: &impl Fn(&str) -> bool,
    options: EntryOptions,
) -> Vec<DescribeConfigsResourceResult> {
    let row = registry::lookup(ConfigScope::Broker, CONNECTIONS_MAX_IDLE_MS);
    let built_in = DEFAULT_CONNECTIONS_MAX_IDLE.millis_i64().to_string();
    let default = DefaultLayer {
        value: Some(built_in.as_str()),
        name: Some(CONNECTIONS_MAX_IDLE_MS),
    };
    // Provenance, not a value comparison: an operator who spells out Kafka's
    // own 600000 has still set the key statically, and Kafka says so — so the
    // layer exists exactly when `connections_max_idle` holds a value.
    let broker_wide = configs
        .connections_max_idle
        .unwrap_or(DEFAULT_CONNECTIONS_MAX_IDLE)
        .millis_i64()
        .to_string();
    let broker_wide_layer = configs.connections_max_idle.map(|_| Layer {
        source: CONFIG_SOURCE_STATIC_BROKER,
        name: CONNECTIONS_MAX_IDLE_MS,
        value: broker_wide.as_str(),
    });

    let mut entries = Vec::with_capacity(1 + configs.connections_max_idle_overrides.len());
    if wanted(CONNECTIONS_MAX_IDLE_MS) {
        let layers: Vec<Layer<'_>> = broker_wide_layer.into_iter().collect();
        entries.push(config_entry(
            row,
            CONNECTIONS_MAX_IDLE_MS,
            &layers,
            default,
            options,
        ));
    }
    for (listener, idle) in configs.connections_max_idle_overrides {
        let key = listener_connections_max_idle_key(listener);
        if !wanted(&key) {
            continue;
        }
        let value = idle.millis_i64().to_string();
        // The listener's own value first, then the broker-wide tail it falls
        // back through.
        let layers: Vec<Layer<'_>> = std::iter::once(Layer {
            source: CONFIG_SOURCE_STATIC_BROKER,
            name: &key,
            value: &value,
        })
        .chain(broker_wide_layer)
        .collect();
        entries.push(config_entry(row, &key, &layers, default, options));
    }
    entries
}

#[cfg(test)]
mod tests;
