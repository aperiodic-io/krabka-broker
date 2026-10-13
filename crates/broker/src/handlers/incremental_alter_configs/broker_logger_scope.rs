//! `BROKER_LOGGER` resources for `IncrementalAlterConfigs`: the node-local
//! log level control behind `kafka-configs --entity-type broker-loggers
//! --alter`.
//!
//! Nothing here reaches the metadata log. A JVM broker applies the change to
//! its live log4j2 context and to nothing else, so the level is gone when the
//! node restarts and no other node in the cluster sees it. Krabka applies it
//! to the `tracing` filter this process installed, which has the same
//! lifetime and the same reach.
//!
//! The rules and the messages are Kafka's `RuntimeLoggerManager`:
//!
//! - The resource name must parse as an integer and be this node's id.
//! - SET names a logger that exists and a level in
//!   [`krabka_telemetry::VALID_LOG_LEVELS`].
//! - DELETE names a logger that exists and is not the root logger, which has
//!   no level of its own to remove.
//! - APPEND and SUBTRACT are refused: a level is not a list.
//!
//! Validation runs over every config in the resource before any of them is
//! applied, so a request that names one bad level changes nothing. That is
//! also what makes `--dry-run` (`validate_only`) meaningful here.

use {
    super::{OP_DELETE, OP_SET},
    crate::codes,
    krabka_protocol::owned::{
        incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
        incremental_alter_configs_response::AlterConfigsResourceResponse,
    },
    krabka_telemetry::{LogLevel, LogLevelController, ROOT_LOGGER, VALID_LOG_LEVELS},
};

/// `AlterConfigOp.OpType::APPEND`, one of the two list operations a level is
/// not.
const OP_APPEND: i8 = 2;
/// `AlterConfigOp.OpType::SUBTRACT`.
const OP_SUBTRACT: i8 = 3;

#[cfg(test)]
mod tests;

/// One validated operation, ready to apply.
#[derive(Debug)]
enum LoggerChange<'a> {
    /// Pin `target` to `level`.
    Set { target: &'a str, level: LogLevel },
    /// Drop `target`'s own level so it inherits again.
    Clear { target: &'a str },
}

/// Apply a `BROKER_LOGGER` resource to this node's live filter.
///
/// `out` carries the refusal when one of the configs does not validate;
/// nothing is applied in that case. `validate_only` stops after validation,
/// which is what `kafka-configs --alter --dry-run` asks for.
pub(super) fn handle_broker_logger_scoped(
    resource: &AlterConfigsResource,
    node_id: i32,
    levels: &LogLevelController,
    validate_only: bool,
    out: &mut AlterConfigsResourceResponse,
) {
    if let Err((code, message)) = validate_resource_name(&resource.resource_name, node_id) {
        out.error_code = code;
        out.error_message = Some(message);
        return;
    }

    let mut changes = Vec::with_capacity(resource.configs.len());
    for config in &resource.configs {
        match validate_config(config, levels) {
            Ok(change) => changes.push(change),
            Err((code, message)) => {
                out.error_code = code;
                out.error_message = Some(message);
                return;
            }
        }
    }

    if validate_only {
        return;
    }
    for change in changes {
        match change {
            LoggerChange::Set { target, level } => levels.set_level(target, level),
            LoggerChange::Clear { target } => {
                if !levels.clear_level(target) {
                    // The logger exists but never had a level of its own, so
                    // it was already inheriting. Kafka logs the same
                    // no-op and answers the request without an error.
                    tracing::debug!(logger = target, "logger had no level of its own to clear");
                }
            }
        }
    }
}

/// The resource name must be this node's id. Kafka calls a name that is not
/// an integer out separately from one that is another node's.
fn validate_resource_name(resource_name: &str, node_id: i32) -> Result<(), (i16, String)> {
    let Ok(requested) = resource_name.parse::<i32>() else {
        return Err((
            codes::INVALID_REQUEST,
            format!("Node id must be an integer, but it is: {resource_name}"),
        ));
    };
    if requested == node_id {
        Ok(())
    } else {
        Err((
            codes::INVALID_REQUEST,
            format!("Unexpected node id. Expected {node_id}, but received {requested}"),
        ))
    }
}

/// Check one config and say what it would do.
fn validate_config<'a>(
    config: &'a AlterableConfig,
    levels: &LogLevelController,
) -> Result<LoggerChange<'a>, (i16, String)> {
    let target = config.name.as_str();
    match config.config_operation {
        OP_SET => {
            logger_must_exist(target, levels)?;
            // A missing value renders as `null`, which is what the JVM's
            // string concatenation puts in the same message.
            let value = config.value.as_deref().unwrap_or("null");
            let Some(level) = LogLevel::from_kafka_name(value) else {
                return Err((
                    codes::INVALID_CONFIG,
                    format!(
                        "Cannot set the log level of {target} to {value} as it is not a supported log level. Valid log levels are {}",
                        VALID_LOG_LEVELS.join(", ")
                    ),
                ));
            };
            Ok(LoggerChange::Set { target, level })
        }
        OP_DELETE => {
            logger_must_exist(target, levels)?;
            if target == ROOT_LOGGER {
                return Err((
                    codes::INVALID_REQUEST,
                    "Removing the log level of the root logger is not allowed".to_owned(),
                ));
            }
            Ok(LoggerChange::Clear { target })
        }
        op => Err((
            codes::INVALID_REQUEST,
            operation_refusal(op).unwrap_or_else(|| {
                format!("Unknown operation type {op} is not allowed for the BROKER_LOGGER resource")
            }),
        )),
    }
}

/// The refusal for the two list-valued operations, which a level is not.
fn operation_refusal(op: i8) -> Option<String> {
    let name = match op {
        OP_APPEND => "APPEND",
        OP_SUBTRACT => "SUBTRACT",
        _ => return None,
    };
    Some(format!(
        "{name} operation is not allowed for the BROKER_LOGGER resource"
    ))
}

/// A level may only be written to a logger this node actually has, which is
/// how a typo in a target name reaches the operator instead of taking effect
/// silently.
fn logger_must_exist(target: &str, levels: &LogLevelController) -> Result<(), (i16, String)> {
    if levels.contains(target) {
        Ok(())
    } else {
        Err((
            codes::INVALID_CONFIG,
            format!("Logger {target} does not exist!"),
        ))
    }
}
