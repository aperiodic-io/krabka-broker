//! What a `BROKER_LOGGER` describe answers, and what it refuses.

use {
    super::{logger_configs, validate_resource_name},
    crate::handlers::describe_configs::wire::CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER,
    assert2::{assert, check},
    krabka_telemetry::{LogLevel, LogLevelController},
};

#[test]
fn a_resource_name_must_be_this_node() {
    for (name, want) in [
        ("7", Ok(())),
        ("", Err("Broker id must not be empty".to_owned())),
        (
            "not-a-number",
            Err("Broker id must be an integer, but it is: not-a-number".to_owned()),
        ),
        (
            "8",
            Err("Unexpected broker id, expected 7 but received 8".to_owned()),
        ),
    ] {
        check!(validate_resource_name(name, 7) == want, "name {name}");
    }
}

#[test]
fn every_logger_reports_its_level_at_the_broker_logger_source() {
    let (levels, _filter) = LogLevelController::new("info,krabka_broker=debug");
    levels.set_level("krabka_log", LogLevel::Fatal);

    let configs = logger_configs(&levels, &|_| true);

    let entry = |name: &str| {
        configs
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("missing {name} in {configs:?}"))
            .clone()
    };
    let check_entry = |name: &str, value: &str| {
        let config = entry(name);
        assert!(config.value.as_deref() == Some(value));
        assert!(!config.read_only);
        assert!(config.config_source == CONFIG_SOURCE_DYNAMIC_BROKER_LOGGER);
        assert!(!config.is_sensitive);
        assert!(config.synonyms.is_empty());
        assert!(config.config_type == 0);
        // Kafka never sets it for a logger, so the generated default is empty.
        assert!(config.documentation.as_deref() == Some(""));
    };
    check_entry("root", "INFO");
    check_entry("krabka_broker", "DEBUG");
    check_entry("krabka_log", "FATAL");
}

#[test]
fn the_configuration_keys_filter_selects_loggers() {
    let (levels, _filter) = LogLevelController::new("info,krabka_broker=debug");

    let configs = logger_configs(&levels, &|name| name == "krabka_broker");

    let names: Vec<&str> = configs.iter().map(|c| c.name.as_str()).collect();
    assert!(names == vec!["krabka_broker"]);
}
