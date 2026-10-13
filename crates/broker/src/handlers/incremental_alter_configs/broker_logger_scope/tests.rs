//! What a `BROKER_LOGGER` alter accepts, what it refuses, and what it does to
//! the live filter.

use {
    super::handle_broker_logger_scoped,
    crate::{codes, config::DEFAULT_LOG_FILTER},
    assert2::{assert, check},
    krabka_protocol::owned::{
        incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
        incremental_alter_configs_response::AlterConfigsResourceResponse,
    },
    krabka_telemetry::{LogLevel, LogLevelController},
};

/// This node's id in every case below.
const NODE_ID: i32 = 3;

/// One `AlterableConfig`, spelled the way the wire carries it.
fn config(name: &str, operation: i8, value: Option<&str>) -> AlterableConfig {
    AlterableConfig {
        name: name.to_owned(),
        config_operation: operation,
        value: value.map(str::to_owned),
        ..Default::default()
    }
}

/// The response of one alter beside the controller it changed, so a test can
/// read the levels back.
struct Applied {
    response: AlterConfigsResourceResponse,
    levels: LogLevelController,
}

/// One refused alter: what it does, its resource name and operations, and the
/// code and message of the refusal.
struct Refusal<'a> {
    what: &'a str,
    resource_name: &'a str,
    configs: Vec<AlterableConfig>,
    code: i16,
    message: &'a str,
}

/// Apply `configs` to a fresh controller seeded with `spec` and return the
/// response beside the controller, so a test can read the levels back.
fn apply(
    spec: &str,
    resource_name: &str,
    configs: Vec<AlterableConfig>,
    validate_only: bool,
) -> Applied {
    let (levels, _filter) = LogLevelController::new(spec);
    let resource = AlterConfigsResource {
        resource_type: 8,
        resource_name: resource_name.to_owned(),
        configs,
        ..Default::default()
    };
    let mut out = AlterConfigsResourceResponse {
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
        error_code: codes::NONE,
        error_message: None,
        ..Default::default()
    };
    handle_broker_logger_scoped(&resource, NODE_ID, &levels, validate_only, &mut out);
    Applied {
        response: out,
        levels,
    }
}

#[test]
fn a_set_retargets_the_named_logger() {
    let Applied {
        response: out,
        levels,
    } = apply(
        "info,krabka_broker=info",
        "3",
        vec![config("krabka_broker", 0, Some("DEBUG"))],
        false,
    );

    assert!(out.error_code == codes::NONE, "{out:?}");
    assert!(levels.level("krabka_broker") == Some(LogLevel::Debug));
}

#[test]
fn a_delete_puts_the_logger_back_on_the_root_level() {
    let Applied {
        response: out,
        levels,
    } = apply(
        "warn,krabka_broker=trace",
        "3",
        vec![config("krabka_broker", 1, None)],
        false,
    );

    assert!(out.error_code == codes::NONE, "{out:?}");
    assert!(levels.level("krabka_broker") == Some(LogLevel::Warn));
}

#[test]
fn a_set_of_the_root_logger_moves_every_unpinned_target() {
    let Applied {
        response: out,
        levels,
    } = apply("info", "3", vec![config("root", 0, Some("TRACE"))], false);

    assert!(out.error_code == codes::NONE, "{out:?}");
    assert!(levels.level("root") == Some(LogLevel::Trace));
}

#[test]
fn validate_only_reports_the_verdict_and_changes_nothing() {
    let Applied {
        response: out,
        levels,
    } = apply(
        "info,krabka_broker=info",
        "3",
        vec![config("krabka_broker", 0, Some("TRACE"))],
        true,
    );

    assert!(out.error_code == codes::NONE, "{out:?}");
    assert!(levels.level("krabka_broker") == Some(LogLevel::Info));
}

#[test]
fn one_bad_config_leaves_the_whole_resource_unapplied() {
    let Applied {
        response: out,
        levels,
    } = apply(
        "info,krabka_broker=info,krabka_log=info",
        "3",
        vec![
            config("krabka_broker", 0, Some("DEBUG")),
            config("krabka_log", 0, Some("VERBOSE")),
        ],
        false,
    );

    assert!(out.error_code == codes::INVALID_CONFIG);
    assert!(levels.level("krabka_broker") == Some(LogLevel::Info));
}

#[test]
fn refusals_carry_kafkas_own_message() {
    let cases: Vec<Refusal<'_>> = vec![
        Refusal {
            what: "a name that is not a node id",
            resource_name: "not-a-number",
            configs: vec![config("krabka_broker", 0, Some("DEBUG"))],
            code: codes::INVALID_REQUEST,
            message: "Node id must be an integer, but it is: not-a-number",
        },
        Refusal {
            what: "another node's id",
            resource_name: "4",
            configs: vec![config("krabka_broker", 0, Some("DEBUG"))],
            code: codes::INVALID_REQUEST,
            message: "Unexpected node id. Expected 3, but received 4",
        },
        Refusal {
            what: "a logger this node does not have",
            resource_name: "3",
            configs: vec![config("no_such_target", 0, Some("DEBUG"))],
            code: codes::INVALID_CONFIG,
            message: "Logger no_such_target does not exist!",
        },
        Refusal {
            what: "a level that is not a level",
            resource_name: "3",
            configs: vec![config("krabka_broker", 0, Some("VERBOSE"))],
            code: codes::INVALID_CONFIG,
            message: "Cannot set the log level of krabka_broker to VERBOSE as it is not a supported log level. Valid log levels are DEBUG, ERROR, FATAL, INFO, TRACE, WARN",
        },
        Refusal {
            what: "a set with no value at all",
            resource_name: "3",
            configs: vec![config("krabka_broker", 0, None)],
            code: codes::INVALID_CONFIG,
            message: "Cannot set the log level of krabka_broker to null as it is not a supported log level. Valid log levels are DEBUG, ERROR, FATAL, INFO, TRACE, WARN",
        },
        Refusal {
            what: "a delete of the root logger",
            resource_name: "3",
            configs: vec![config("root", 1, None)],
            code: codes::INVALID_REQUEST,
            message: "Removing the log level of the root logger is not allowed",
        },
        Refusal {
            what: "an append",
            resource_name: "3",
            configs: vec![config("krabka_broker", 2, Some("DEBUG"))],
            code: codes::INVALID_REQUEST,
            message: "APPEND operation is not allowed for the BROKER_LOGGER resource",
        },
        Refusal {
            what: "a subtract",
            resource_name: "3",
            configs: vec![config("krabka_broker", 3, Some("DEBUG"))],
            code: codes::INVALID_REQUEST,
            message: "SUBTRACT operation is not allowed for the BROKER_LOGGER resource",
        },
        Refusal {
            what: "an operation byte no version defines",
            resource_name: "3",
            configs: vec![config("krabka_broker", 9, Some("DEBUG"))],
            code: codes::INVALID_REQUEST,
            message: "Unknown operation type 9 is not allowed for the BROKER_LOGGER resource",
        },
    ];

    for Refusal {
        what,
        resource_name,
        configs,
        code,
        message,
    } in cases
    {
        let Applied { response: out, .. } =
            apply("info,krabka_broker=info", resource_name, configs, false);
        check!(out.error_code == code, "{what}");
        check!(out.error_message.as_deref() == Some(message), "{what}");
    }
}

#[test]
fn the_default_filter_names_the_loggers_an_operator_reaches_for() {
    // A `tracing` callsite registers under its module path, never under the
    // bare crate root, so the crate-root names are loggers only because the
    // starting spec seeds them. If that seed stops naming them, the headline
    // `--add-config krabka_broker=DEBUG` is refused on a real broker.
    for target in ["krabka_broker", "krabka_log"] {
        let Applied {
            response: out,
            levels,
        } = apply(
            DEFAULT_LOG_FILTER,
            "3",
            vec![config(target, 0, Some("DEBUG"))],
            false,
        );
        check!(out.error_code == codes::NONE, "{target}: {out:?}");
        check!(levels.level(target) == Some(LogLevel::Debug), "{target}");
    }
}

#[test]
fn a_lowercase_level_is_not_a_level() {
    // Kafka tests the value against a set of uppercase names, so `debug` is
    // rejected there and must be rejected here.
    let Applied { response: out, .. } = apply(
        "info,krabka_broker=info",
        "3",
        vec![config("krabka_broker", 0, Some("debug"))],
        false,
    );
    assert!(out.error_code == codes::INVALID_CONFIG);
}
