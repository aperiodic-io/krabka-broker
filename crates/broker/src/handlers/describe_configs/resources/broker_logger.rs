//! The `BROKER_LOGGER` (resource type 8) view of a node's live `tracing`
//! filter.
//!
//! A JVM broker answers this resource out of its log4j2 `LoggerContext`: one
//! entry per logger that exists, plus `root`, each stamped
//! `DYNAMIC_BROKER_LOGGER_CONFIG`, none sensitive, none read-only, and none
//! carrying a synonym chain or a `ConfigDef` type — a logger level is not a
//! config key the broker has a `ConfigDef` for. Krabka answers it out of
//! [`krabka_telemetry::LogLevelController`], which lists `tracing` targets in
//! place of logger names.
//!
//! The resource is node-local, so the resource name must be this node's id.
//! The three refusals below are Kafka's, message for message, because
//! `kafka-configs` prints them straight through to the operator.

use {
    super::super::wire::CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER,
    krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult,
    krabka_telemetry::LogLevelController,
};

#[cfg(test)]
mod tests;

/// Which node's loggers a `BROKER_LOGGER` request may name, and where to read
/// them.
#[derive(Clone, Copy)]
pub(in crate::handlers::describe_configs) struct BrokerLoggers<'a> {
    /// This node's broker id. A request that names another one is refused.
    pub(in crate::handlers::describe_configs) node_id: i32,
    /// The live filter this node runs with.
    pub(in crate::handlers::describe_configs) levels: &'a LogLevelController,
}

/// Check that `resource_name` names this node, and say why not when it does
/// not.
///
/// Kafka refuses an empty name outright rather than treating it as the
/// cluster-default resource the way it does for `BROKER`: there is no
/// cluster-wide logger configuration to describe.
pub(super) fn validate_resource_name(resource_name: &str, node_id: i32) -> Result<(), String> {
    if resource_name.is_empty() {
        return Err("Broker id must not be empty".to_owned());
    }
    let Ok(requested) = resource_name.parse::<i32>() else {
        return Err(format!(
            "Broker id must be an integer, but it is: {resource_name}"
        ));
    };
    if requested == node_id {
        Ok(())
    } else {
        Err(format!(
            "Unexpected broker id, expected {node_id} but received {resource_name}"
        ))
    }
}

/// Every logger this node knows, as `DescribeConfigs` entries.
pub(super) fn logger_configs(
    levels: &LogLevelController,
    wanted: &impl Fn(&str) -> bool,
) -> Vec<DescribeConfigsResourceResult> {
    levels
        .loggers()
        .into_iter()
        .filter(|(name, _)| wanted(name))
        .map(|(name, level)| {
            tagged_wire!(DescribeConfigsResourceResult {
                name,
                value: Some(level.kafka_name().to_owned()),
                read_only: false,
                config_source: CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER,
                is_sensitive: false,
                synonyms: Vec::new(),
                config_type: 0,
                // Kafka's `ConfigHelper` builds a logger entry without calling
                // `setDocumentation`, so the field goes out as the generated
                // default: the empty string, not null.
                documentation: Some(String::new()),
            })
        })
        .collect()
}
