use {super::*, crate::test_support::env_from};

#[test]
fn disabled_when_no_env() {
    let cfg =
        OtlpConfig::from_env(env_from(&[]), "1", "0.1.1", "krabka-broker").expect("valid config");
    assert2::assert!(cfg.is_none());
}

#[test]
fn env_truthy_rejects_falsy_values() {
    for value in ["", "0", "false", "no", "off", " definitely "] {
        assert2::assert!(!env_truthy(value));
    }
}

#[test]
fn falsy_env_flags_do_not_enable_or_disable_otlp() {
    let cfg = OtlpConfig::from_env(
        env_from(&[("KRABKA_OTLP_ENABLED", "false")]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config");
    assert2::assert!(cfg.is_none());

    let cfg = OtlpConfig::from_env(
        env_from(&[
            ("KRABKA_OTLP_ENDPOINT", "http://collector:4317"),
            ("OTEL_SDK_DISABLED", "off"),
        ]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config");
    assert2::assert!(cfg.is_some());
}

#[test]
fn enabled_by_krabka_endpoint() {
    let cfg = OtlpConfig::from_env(
        env_from(&[("KRABKA_OTLP_ENDPOINT", "http://collector:4317")]),
        "7",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!(cfg.endpoint.as_str() == "http://collector:4317");
    assert2::assert!(cfg.protocol == OtlpProtocol::Grpc);
    assert2::assert!((cfg.sample_ratio - 1.0).abs() < f64::EPSILON);
    assert2::assert!(cfg.service_name.as_str() == "krabka-broker");
    assert2::assert!(cfg.service_instance_id.as_str() == "7");
    assert2::assert!(cfg.service_version.as_str() == "0.1.1");
}

#[test]
fn enabled_flag_uses_protocol_default_endpoint() {
    let cfg = OtlpConfig::from_env(
        env_from(&[
            ("KRABKA_OTLP_ENABLED", "true"),
            ("KRABKA_OTLP_PROTOCOL", "http/protobuf"),
        ]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!(cfg.protocol == OtlpProtocol::HttpProtobuf);
    assert2::assert!(cfg.endpoint.as_str() == "http://localhost:4318");
}

#[test]
fn grpc_is_the_default_protocol() {
    let cfg = OtlpConfig::from_env(
        env_from(&[("KRABKA_OTLP_ENABLED", "1")]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!(cfg.protocol == OtlpProtocol::Grpc);
    assert2::assert!(cfg.endpoint.as_str() == "http://localhost:4317");
}

#[test]
fn sdk_disabled_overrides_endpoint() {
    let cfg = OtlpConfig::from_env(
        env_from(&[
            ("KRABKA_OTLP_ENDPOINT", "http://collector:4317"),
            ("OTEL_SDK_DISABLED", "true"),
        ]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config");
    assert2::assert!(cfg.is_none());
}

#[test]
fn endpoint_precedence_and_standard_vars() {
    // Standard OTLP env (no KRABKA_ override) still enables export.
    let cfg = OtlpConfig::from_env(
        env_from(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel:4317")]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!(cfg.endpoint == "http://otel:4317");

    // Traces-specific endpoint wins over the generic one.
    let cfg = OtlpConfig::from_env(
        env_from(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://generic:4317"),
            ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://traces:4317"),
        ]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!(cfg.endpoint == "http://traces:4317");

    // KRABKA override wins over everything.
    let cfg = OtlpConfig::from_env(
        env_from(&[
            ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://traces:4317"),
            ("KRABKA_OTLP_ENDPOINT", "http://krabka:4317"),
        ]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!(cfg.endpoint == "http://krabka:4317");
}

#[test]
fn sample_ratio_parsed_and_clamped() {
    let cfg = OtlpConfig::from_env(
        env_from(&[
            ("KRABKA_OTLP_ENDPOINT", "http://c:4317"),
            ("KRABKA_OTLP_SAMPLE_RATIO", "0.25"),
        ]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!((cfg.sample_ratio - 0.25).abs() < f64::EPSILON);

    // Out-of-range clamps to [0,1].
    let cfg = OtlpConfig::from_env(
        env_from(&[
            ("KRABKA_OTLP_ENABLED", "true"),
            ("KRABKA_OTLP_SAMPLE_RATIO", "9.0"),
        ]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!((cfg.sample_ratio - 1.0).abs() < f64::EPSILON);
}

#[test]
fn heartbeat_interval_is_disabled_by_default_and_ignores_zero() {
    let cfg = OtlpConfig::from_env(
        env_from(&[("KRABKA_OTLP_ENDPOINT", "http://c:4317")]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!(cfg.heartbeat_interval.is_none());

    let cfg = OtlpConfig::from_env(
        env_from(&[
            ("KRABKA_OTLP_ENDPOINT", "http://c:4317"),
            ("KRABKA_OTLP_HEARTBEAT_INTERVAL", "0s"),
        ]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!(cfg.heartbeat_interval.is_none());
}

#[test]
fn heartbeat_interval_parses_from_env() {
    let cfg = OtlpConfig::from_env(
        env_from(&[
            ("KRABKA_OTLP_ENDPOINT", "http://c:4317"),
            ("KRABKA_OTLP_HEARTBEAT_INTERVAL", "15s"),
        ]),
        "1",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!(cfg.heartbeat_interval == Some(secs(15)));
}

#[test]
fn custom_otlp_times_reject_malformed_values() {
    for name in ["KRABKA_OTLP_TIMEOUT", "KRABKA_OTLP_HEARTBEAT_INTERVAL"] {
        for value in [
            "5",
            "1MiB",
            "NaNs",
            "-1s",
            "999999999999999999999999999999999999999999999999999999999999s",
        ] {
            let error = OtlpConfig::from_env(
                env_from(&[("KRABKA_OTLP_ENDPOINT", "http://c:4317"), (name, value)]),
                "1",
                "0.1.1",
                "krabka-broker",
            )
            .expect_err("invalid custom OTLP time must fail startup");
            assert2::assert!(
                matches!(
                    error,
                    TelemetryError::InvalidConfig {
                        name: actual_name,
                        ..
                    } if actual_name == name
                ),
                "{name}={value}: {error}"
            );
        }
    }
}

#[test]
fn service_name_and_timeout_overrides() {
    let cfg = OtlpConfig::from_env(
        env_from(&[
            ("KRABKA_OTLP_ENDPOINT", "http://c:4317"),
            ("OTEL_SERVICE_NAME", "my-kafka"),
            ("KRABKA_OTLP_TIMEOUT", "3s"),
        ]),
        "9",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!(cfg.service_name.as_str() == "my-kafka");
    assert2::assert!(cfg.timeout == secs(3));
}

#[test]
fn standard_otlp_timeout_secs_remains_compatible() {
    for (value, expected) in [("7", 7), ("7s", 10)] {
        let cfg = OtlpConfig::from_env(
            env_from(&[
                ("KRABKA_OTLP_ENDPOINT", "http://c:4317"),
                ("OTEL_EXPORTER_OTLP_TIMEOUT_SECS", value),
            ]),
            "9",
            "0.1.1",
            "krabka-broker",
        )
        .expect("standard OTLP timeout remains non-failing")
        .expect("enabled");
        assert2::assert!(cfg.timeout == secs(expected));
    }
}

#[test]
fn unit_suffixed_krabka_otlp_time_names_are_not_aliases() {
    let cfg = OtlpConfig::from_env(
        env_from(&[
            ("KRABKA_OTLP_ENDPOINT", "http://c:4317"),
            ("KRABKA_OTLP_TIMEOUT_SECS", "3"),
            ("KRABKA_OTLP_HEARTBEAT_INTERVAL_SECS", "15"),
        ]),
        "9",
        "0.1.1",
        "krabka-broker",
    )
    .expect("valid config")
    .expect("enabled");
    assert2::assert!(cfg.timeout == secs(10));
    assert2::assert!(cfg.heartbeat_interval.is_none());
}

#[test]
fn protocol_parse_variants() {
    for (input, want) in [
        ("grpc", OtlpProtocol::Grpc),
        ("http/protobuf", OtlpProtocol::HttpProtobuf),
        ("HTTP", OtlpProtocol::HttpProtobuf),
        ("nonsense", OtlpProtocol::Grpc),
    ] {
        assert2::assert!(OtlpProtocol::parse(input) == want);
    }
}
