//! `krabka-bench-driver` runs one scenario × one stack and writes a
//! `RunOutput` JSON file.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use krabka_bench_driver::{
    scenario::{Scenario, Stack},
    workload::{
        self, ConsumerBuildAttempts, ConsumerBuildRetryPolicy, DriverConfig,
        MAX_CLIENT_REQUEST_TIMEOUT,
    },
};
use krabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity, DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
};
use krabka_units::{fmt::Human as _, parse, prelude::*};
use tracing_subscriber::EnvFilter;

/// The driver's flags. A field without an `#[arg(...)]` of its own gets
/// `--<field>` and `BENCH_<FIELD>` from `#[krabka_env]`.
#[krabka_macros::krabka_env(prefix = "BENCH_")]
#[derive(Debug, Parser)]
#[command(name = "krabka-bench-driver", version, about)]
struct Cli {
    /// Path to the scenario YAML.
    #[arg(long, env = "BENCH_SCENARIO_PATH")]
    scenario: PathBuf,
    /// Kafka bootstrap servers (host:port).
    #[arg(long, env = "BENCH_BOOTSTRAP_SERVERS")]
    bootstrap: String,
    #[arg(
        long,
        env = "BENCH_CLIENT_DISPATCH_QUEUE_CAPACITY",
        default_value_t = DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY,
        value_parser = parse_client_dispatch_queue_capacity
    )]
    client_dispatch_queue_capacity: usize,
    #[arg(
        long,
        env = "BENCH_CLIENT_FRAME_MAX",
        default_value = "100MiB",
        value_parser = parse_client_frame_max
    )]
    client_frame_max: ByteSize,
    /// Which Kafka stack this is. This is metadata only and does not change
    /// behaviour.
    #[arg(long, env = "BENCH_STACK", value_enum)]
    stack: Stack,
    /// Topic name. The topic must already exist. The orchestrator creates it
    /// with a `KafkaTopic` CR.
    #[arg(long, env = "BENCH_TOPIC", default_value = "bench-topic")]
    topic: String,
    /// Namespace the brokers live in. The Prometheus pod regex uses it.
    #[arg(long, env = "BENCH_NAMESPACE", default_value = "default")]
    namespace: String,
    /// Prometheus base URL. Without it, the resource fields are zero and
    /// `notes` records the skip.
    #[arg(long, env = "BENCH_PROMETHEUS_URL")]
    prometheus: Option<String>,
    /// HTTP request timeout for Prometheus queries.
    #[arg(
        long,
        env = "BENCH_PROMETHEUS_REQUEST_TIMEOUT",
        default_value = "15s",
        value_parser = parse::positive_time
    )]
    prometheus_request_timeout: Time,
    /// Producer request timeout.
    #[arg(
        long,
        env = "BENCH_PRODUCER_REQUEST_TIMEOUT",
        default_value = "2s",
        value_parser = parse_client_request_timeout
    )]
    producer_request_timeout: Time,
    /// Maximum time to drain outstanding producer sends.
    #[arg(
        long,
        env = "BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT",
        default_value = "10s",
        value_parser = parse::positive_time
    )]
    producer_final_drain_timeout: Time,
    /// Consumer request timeout. Defaults to 5s for Krabka and 30s
    /// for Kafka.
    #[arg(
        long,
        env = "BENCH_CONSUMER_REQUEST_TIMEOUT",
        value_parser = parse_client_request_timeout
    )]
    consumer_request_timeout: Option<Time>,
    /// Maximum attempts when building each consumer.
    #[arg(
        long,
        env = "BENCH_CONSUMER_BUILD_ATTEMPTS",
        default_value_t = ConsumerBuildAttempts::default()
    )]
    consumer_build_attempts: ConsumerBuildAttempts,
    /// Initial consumer-build retry backoff.
    #[arg(
        long,
        env = "BENCH_CONSUMER_BUILD_INITIAL_BACKOFF",
        default_value = "100ms",
        value_parser = parse::positive_time
    )]
    consumer_build_initial_backoff: Time,
    /// Maximum consumer-build retry backoff.
    #[arg(
        long,
        env = "BENCH_CONSUMER_BUILD_MAX_BACKOFF",
        default_value = "2s",
        value_parser = parse::positive_time
    )]
    consumer_build_max_backoff: Time,
    /// Consumer poll timeout.
    #[arg(
        long,
        env = "BENCH_CONSUMER_POLL_TIMEOUT",
        default_value = "50ms",
        value_parser = parse::positive_time
    )]
    consumer_poll_timeout: Time,
    /// Sleep after a consumer poll error.
    #[arg(
        long,
        env = "BENCH_CONSUMER_POLL_ERROR_BACKOFF",
        default_value = "100ms",
        value_parser = parse::positive_time
    )]
    consumer_poll_error_backoff: Time,
    /// Time-series sample interval.
    #[arg(
        long,
        env = "BENCH_SAMPLE_INTERVAL",
        default_value = "2s",
        value_parser = parse_sample_interval
    )]
    sample_interval: Time,
    /// Configured broker count. The driver uses this to gate RF=3-only
    /// scenarios.
    #[arg(long, env = "BENCH_BROKER_COUNT", default_value_t = 1)]
    broker_count: u32,
    /// Output path for the `RunOutput` JSON.
    #[arg(long, env = "BENCH_OUTPUT_PATH", default_value = "/results/run.json")]
    out: PathBuf,
    /// Enable a TLS-encrypted data path, so producers and consumers dial the
    /// broker's `Ssl` listener instead of plaintext. The default is unset, which
    /// keeps the plaintext benchmark path unchanged.
    #[arg(long, env = "BENCH_TLS_ENABLED", default_value_t = false)]
    tls_enabled: bool,
    /// PEM CA bundle that the client trusts to verify the broker serving cert.
    /// This is required when `--tls-enabled`. It is mounted from the per-stack
    /// cluster-CA Secret, for example `/etc/bench-ca/ca.crt`.
    tls_ca_path: Option<PathBuf>,
    /// SNI server name that the client presents in the TLS handshake. The broker
    /// matches it against a SAN on its serving cert. LOAD-BEARING: the client
    /// resolves the bootstrap to a pod IP and dials by IP, so you must set the
    /// SNI to a SAN name. For krabka that name is
    /// `demo-broker-headless.<ns>.svc.cluster.local`, and for Strimzi it is
    /// `demo-kafka-bootstrap`. This is required when `--tls-enabled`.
    tls_server_name: Option<String>,
    /// Optional mTLS client certificate in PEM form. One-way TLS is enough for
    /// the benchmark. Set this, together with `--tls-client-key`, only when the
    /// listener requires client auth.
    tls_client_cert: Option<PathBuf>,
    /// Optional mTLS client private key in PEM form. It pairs with
    /// `--tls-client-cert`.
    tls_client_key: Option<PathBuf>,
}

fn parse_client_request_timeout(input: &str) -> Result<Time, String> {
    let value = parse::positive_time(input).map_err(|error| error.to_string())?;
    let millis = value.millis_i64();
    if Time::from_millis(millis) == value && value <= MAX_CLIENT_REQUEST_TIMEOUT {
        Ok(value)
    } else {
        Err("request timeout must be a whole positive i32 millisecond count".to_string())
    }
}

krabka_macros::dispatch_capacity_parser!(parse_client_dispatch_queue_capacity);

fn parse_client_frame_max(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    ClientFrameMax::try_from(value).map(ClientFrameMax::size)
}

fn parse_sample_interval(input: &str) -> Result<Time, String> {
    let value = parse::positive_time(input).map_err(|error| error.to_string())?;
    let millis = value.millis_i64();
    if millis > 0 && Time::from_millis(millis) == value {
        Ok(value)
    } else {
        Err("sample interval must be a whole positive millisecond count".to_string())
    }
}

fn resolve_consumer_request_timeout(stack: Stack, configured: Option<Time>) -> Time {
    configured.unwrap_or_else(|| workload::default_consumer_request_timeout(stack))
}

fn resolve_consumer_build_retry_policy(cli: &Cli) -> Result<ConsumerBuildRetryPolicy> {
    ConsumerBuildRetryPolicy::new(
        cli.consumer_build_attempts,
        cli.consumer_build_initial_backoff,
        cli.consumer_build_max_backoff,
    )
    .map_err(anyhow::Error::msg)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .init();

    // rustls's process-global crypto provider needs to be installed
    // exactly once before any TLS code runs. The `kube` and `reqwest`
    // rustls features both rely on this. Failing to install means TLS
    // operations panic with "no process-level CryptoProvider available".
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();
    let consumer_build_retry_policy = resolve_consumer_build_retry_policy(&cli)?;

    let yaml = tokio::fs::read_to_string(&cli.scenario)
        .await
        .with_context(|| format!("read scenario {}", cli.scenario.display()))?;
    let scenario: Scenario = serde_yaml::from_str(&yaml).context("parse scenario yaml")?;

    let scenario_id = hash_str(&scenario.name);

    // Assemble the TLS data-path config from the CLI/env knobs. When
    // `--tls-enabled` is set, a CA path and a server-name (SNI) are mandatory:
    // without the explicit SNI the handshake validates against a pod IP that is
    // not on the cert and fails. mTLS is optional (one-way TLS by default).
    let tls = if cli.tls_enabled {
        let ca_path = cli
            .tls_ca_path
            .context("BENCH_TLS_ENABLED set but BENCH_TLS_CA_PATH missing")?;
        let server_name = cli
            .tls_server_name
            .context("BENCH_TLS_ENABLED set but BENCH_TLS_SERVER_NAME missing")?;
        let client_identity = match (cli.tls_client_cert, cli.tls_client_key) {
            (Some(cert), Some(key)) => Some((cert, key)),
            (None, None) => None,
            _ => anyhow::bail!(
                "BENCH_TLS_CLIENT_CERT and BENCH_TLS_CLIENT_KEY must be set together (mTLS) or both unset (one-way TLS)"
            ),
        };
        Some(workload::TlsParams {
            ca_path,
            server_name,
            client_identity,
        })
    } else {
        None
    };

    let stack = cli.stack;
    let consumer_request_timeout =
        resolve_consumer_request_timeout(stack, cli.consumer_request_timeout);
    let cfg = DriverConfig {
        bootstrap: cli.bootstrap,
        client_dispatch_queue_capacity: ConnectionDispatchQueueCapacity::new(
            cli.client_dispatch_queue_capacity,
        )
        .expect("validated by clap"),
        client_frame_max: ClientFrameMax::try_from(cli.client_frame_max)
            .expect("validated by clap"),
        topic: cli.topic,
        stack,
        namespace: cli.namespace,
        prometheus_url: cli.prometheus,
        prometheus_request_timeout: cli.prometheus_request_timeout,
        producer_request_timeout: cli.producer_request_timeout,
        producer_final_drain_timeout: cli.producer_final_drain_timeout,
        sample_interval: cli.sample_interval,
        consumer_request_timeout,
        consumer_build_retry_policy,
        consumer_poll_timeout: cli.consumer_poll_timeout,
        consumer_poll_error_backoff: cli.consumer_poll_error_backoff,
        broker_count: cli.broker_count,
        scenario_id,
        tls,
    };

    let out = workload::run(scenario, cfg).await?;

    if let Some(parent) = cli.out.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }
    let json = serde_json::to_string_pretty(&out).context("encode run output")?;
    tokio::fs::write(&cli.out, json)
        .await
        .with_context(|| format!("write run output to {}", cli.out.display()))?;

    // Brief stdout summary so `kubectl logs` shows progress. The dimensioned
    // values print in the operator form (`2.4GiB`, `4.25ms`).
    println!(
        "stack={:?} scenario={} produced={} consumed={} bytes_in={} p99={}",
        out.stack,
        out.scenario.name,
        out.throughput.msgs_produced,
        out.throughput.msgs_consumed,
        out.throughput.bytes_in.human(),
        out.producer_latency.p99.human(),
    );
    Ok(())
}

/// Stable, simple hash so producer-stamped magic IDs match across pods.
fn hash_str(s: &str) -> u64 {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use clap::Parser;

    // `use super::*` already carries this module's `krabka_units` prelude.
    use super::*;

    fn required_args(stack: &'static str) -> Vec<&'static str> {
        vec![
            "krabka-bench-driver",
            "--scenario",
            "scenario.yaml",
            "--bootstrap",
            "broker:9092",
            "--stack",
            stack,
        ]
    }

    // Each environment case executes in a separate process, preserving parallel test isolation.
    fn run_environment_child(marker: &str, test: &str, env: &[(&str, &str)]) -> bool {
        if std::env::var_os(marker).is_some() {
            return false;
        }
        let status = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", test])
            .env(marker, "1")
            .envs(env.iter().copied())
            .status()
            .expect("child test");
        assert!(status.success());
        true
    }

    fn check_invalid_cli_values(option: &'static str, values: &[&'static str]) {
        for &invalid in values {
            let mut args = required_args("krabka");
            args.extend([option, invalid]);
            check!(Cli::try_parse_from(args).is_err(), "{option}={invalid}");
        }
    }

    #[test]
    fn client_request_timeout_defaults_follow_active_stack() {
        let krabka = Cli::try_parse_from(required_args("krabka")).expect("Krabka timeout defaults");
        let kafka = Cli::try_parse_from(required_args("kafka")).expect("Kafka timeout defaults");

        check!(krabka.producer_request_timeout == secs(2));
        check!(
            resolve_consumer_request_timeout(Stack::Krabka, krabka.consumer_request_timeout)
                == secs(5)
        );
        check!(
            resolve_consumer_request_timeout(Stack::Kafka, kafka.consumer_request_timeout)
                == secs(30)
        );
    }

    #[test]
    fn client_resource_policy_parses_defaults_and_overrides() {
        let defaults = Cli::try_parse_from(required_args("krabka")).unwrap();
        assert!(defaults.client_dispatch_queue_capacity == 64);
        assert!(defaults.client_frame_max == mebibytes(100));

        let mut args = required_args("krabka");
        args.extend([
            "--client-dispatch-queue-capacity",
            "7",
            "--client-frame-max",
            "32KiB",
        ]);
        let custom = Cli::try_parse_from(args).unwrap();
        assert!(custom.client_dispatch_queue_capacity == 7);
        assert!(custom.client_frame_max == kibibytes(32));

        for (option, invalid) in [
            ("--client-dispatch-queue-capacity", "0"),
            ("--client-frame-max", "101MiB"),
        ] {
            let mut args = required_args("krabka");
            args.extend([option, invalid]);
            assert!(Cli::try_parse_from(args).is_err());
        }
    }

    #[test]
    fn client_resource_policy_reads_environment_and_prefers_cli() {
        if run_environment_child(
            "BENCH_CLIENT_RESOURCE_POLICY_CHILD",
            "tests::client_resource_policy_reads_environment_and_prefers_cli",
            &[
                ("BENCH_CLIENT_DISPATCH_QUEUE_CAPACITY", "7"),
                ("BENCH_CLIENT_FRAME_MAX", "32KiB"),
            ],
        ) {
            return;
        }

        let from_env = Cli::try_parse_from(required_args("krabka")).unwrap();
        assert!(from_env.client_dispatch_queue_capacity == 7);
        assert!(from_env.client_frame_max == kibibytes(32));

        let mut args = required_args("krabka");
        args.extend([
            "--client-dispatch-queue-capacity",
            "9",
            "--client-frame-max",
            "64KiB",
        ]);
        let from_cli = Cli::try_parse_from(args).unwrap();
        assert!(from_cli.client_dispatch_queue_capacity == 9);
        assert!(from_cli.client_frame_max == kibibytes(64));
    }

    #[test]
    fn client_request_timeout_rejects_invalid_cli_values() {
        for option in ["--producer-request-timeout", "--consumer-request-timeout"] {
            check_invalid_cli_values(option, &["0s", "not-a-number", "-1s", "2147483648ms", "1"]);
        }
    }

    #[test]
    fn consumer_build_retry_cli_defaults_preserve_policy() {
        let cli = Cli::try_parse_from(required_args("krabka")).expect("retry defaults");
        let policy = resolve_consumer_build_retry_policy(&cli).expect("valid defaults");

        check!(
            (
                policy.attempts(),
                policy.initial_backoff(),
                policy.max_backoff()
            ) == (6, millis(100), secs(2))
        );
    }

    #[test]
    fn consumer_build_retry_rejects_invalid_cli_values() {
        let cases = [
            ("--consumer-build-attempts", "0"),
            ("--consumer-build-attempts", "4294967296"),
            ("--consumer-build-initial-backoff", "0ms"),
            ("--consumer-build-initial-backoff", "1"),
            ("--consumer-build-max-backoff", "0ms"),
            ("--consumer-build-max-backoff", "1"),
        ];
        for (option, invalid) in cases {
            let mut args = required_args("krabka");
            args.extend([option, invalid]);
            check!(Cli::try_parse_from(args).is_err(), "{option}={invalid}");
        }
    }

    #[test]
    fn consumer_build_retry_rejects_inverted_cli_range() {
        let mut args = required_args("krabka");
        args.extend([
            "--consumer-build-initial-backoff",
            "2ms",
            "--consumer-build-max-backoff",
            "1ms",
        ]);
        let cli = Cli::try_parse_from(args).expect("individual values are valid");

        assert!(resolve_consumer_build_retry_policy(&cli).is_err());
    }

    #[test]
    fn consumer_build_retry_reads_environment_and_prefers_cli() {
        if run_environment_child(
            "KRABKA_BENCH_CONSUMER_BUILD_RETRY_CHILD",
            "tests::consumer_build_retry_reads_environment_and_prefers_cli",
            &[
                ("BENCH_CONSUMER_BUILD_ATTEMPTS", "2"),
                ("BENCH_CONSUMER_BUILD_INITIAL_BACKOFF", "11ms"),
                ("BENCH_CONSUMER_BUILD_MAX_BACKOFF", "12ms"),
            ],
        ) {
            return;
        }

        let from_env = Cli::try_parse_from(required_args("krabka")).expect("environment");
        let environment_policy =
            resolve_consumer_build_retry_policy(&from_env).expect("valid environment");
        check!(environment_policy.attempts() == 2);
        check!(environment_policy.initial_backoff() == millis(11));
        check!(environment_policy.max_backoff() == millis(12));

        let mut args = required_args("krabka");
        args.extend([
            "--consumer-build-attempts",
            "3",
            "--consumer-build-initial-backoff",
            "21ms",
            "--consumer-build-max-backoff",
            "22ms",
        ]);
        let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
        let cli_policy = resolve_consumer_build_retry_policy(&from_cli).expect("valid CLI");
        check!(cli_policy.attempts() == 3);
        check!(cli_policy.initial_backoff() == millis(21));
        check!(cli_policy.max_backoff() == millis(22));
    }

    #[test]
    fn consumer_poll_timing_cli_defaults_preserve_behavior() {
        let cli = Cli::try_parse_from(required_args("krabka")).expect("poll defaults");

        check!(cli.consumer_poll_timeout == millis(50));
        check!(cli.consumer_poll_error_backoff == millis(100));
    }

    #[test]
    fn consumer_poll_timing_rejects_invalid_cli_values() {
        for option in ["--consumer-poll-timeout", "--consumer-poll-error-backoff"] {
            check_invalid_cli_values(option, &["0ms", "not-a-number", "-1ms", "1"]);
        }
    }

    #[test]
    fn consumer_poll_timing_reads_environment_and_prefers_cli() {
        if run_environment_child(
            "KRABKA_BENCH_CONSUMER_POLL_TIMING_CHILD",
            "tests::consumer_poll_timing_reads_environment_and_prefers_cli",
            &[
                ("BENCH_CONSUMER_POLL_TIMEOUT", "11ms"),
                ("BENCH_CONSUMER_POLL_ERROR_BACKOFF", "12ms"),
            ],
        ) {
            return;
        }

        let from_env = Cli::try_parse_from(required_args("krabka")).expect("environment");
        check!(from_env.consumer_poll_timeout == millis(11));
        check!(from_env.consumer_poll_error_backoff == millis(12));

        let mut args = required_args("krabka");
        args.extend([
            "--consumer-poll-timeout",
            "21ms",
            "--consumer-poll-error-backoff",
            "22ms",
        ]);
        let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
        check!(from_cli.consumer_poll_timeout == millis(21));
        check!(from_cli.consumer_poll_error_backoff == millis(22));
    }

    #[test]
    fn producer_final_drain_timeout_cli_default_preserves_behavior() {
        let cli = Cli::try_parse_from(required_args("krabka")).expect("drain default");

        assert!(cli.producer_final_drain_timeout == secs(10));
    }

    #[test]
    fn producer_final_drain_timeout_rejects_invalid_cli_values() {
        for invalid in ["0s", "not-a-number", "-1s", "1"] {
            let mut args = required_args("krabka");
            args.extend(["--producer-final-drain-timeout", invalid]);
            assert!(Cli::try_parse_from(args).is_err(), "{invalid}");
        }
    }

    #[test]
    fn producer_final_drain_timeout_reads_environment_and_prefers_cli() {
        if run_environment_child(
            "KRABKA_BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_CHILD",
            "tests::producer_final_drain_timeout_reads_environment_and_prefers_cli",
            &[("BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT", "11s")],
        ) {
            return;
        }

        let from_env = Cli::try_parse_from(required_args("krabka")).expect("environment");
        assert!(from_env.producer_final_drain_timeout == secs(11));

        let mut args = required_args("krabka");
        args.extend(["--producer-final-drain-timeout", "21s"]);
        let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
        assert!(from_cli.producer_final_drain_timeout == secs(21));
    }

    #[test]
    fn sample_interval_cli_default_preserves_behavior() {
        let cli = Cli::try_parse_from(required_args("krabka")).expect("sample default");

        assert!(cli.sample_interval == secs(2));
    }

    #[test]
    fn sample_interval_rejects_invalid_cli_values() {
        for invalid in ["0ms", "not-a-number", "-1ms", "0.5ms", "1"] {
            let mut args = required_args("krabka");
            args.extend(["--sample-interval", invalid]);
            assert!(Cli::try_parse_from(args).is_err(), "{invalid}");
        }
    }

    #[test]
    fn sample_interval_reads_environment_and_prefers_cli() {
        if run_environment_child(
            "KRABKA_BENCH_SAMPLE_INTERVAL_CHILD",
            "tests::sample_interval_reads_environment_and_prefers_cli",
            &[("BENCH_SAMPLE_INTERVAL", "11ms")],
        ) {
            return;
        }

        let from_env = Cli::try_parse_from(required_args("krabka")).expect("environment");
        assert!(from_env.sample_interval == millis(11));

        let mut args = required_args("krabka");
        args.extend(["--sample-interval", "21ms"]);
        let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
        assert!(from_cli.sample_interval == millis(21));
    }

    #[test]
    fn client_request_timeouts_read_environment_and_prefer_cli() {
        if run_environment_child(
            "KRABKA_BENCH_CLIENT_TIMEOUTS_CHILD",
            "tests::client_request_timeouts_read_environment_and_prefer_cli",
            &[
                ("BENCH_PRODUCER_REQUEST_TIMEOUT", "11s"),
                ("BENCH_CONSUMER_REQUEST_TIMEOUT", "12s"),
            ],
        ) {
            return;
        }

        let from_env = Cli::try_parse_from(required_args("krabka")).expect("environment");
        check!(from_env.producer_request_timeout == secs(11));
        check!(
            resolve_consumer_request_timeout(Stack::Krabka, from_env.consumer_request_timeout,)
                == secs(12)
        );

        let mut args = required_args("krabka");
        args.extend([
            "--producer-request-timeout",
            "21s",
            "--consumer-request-timeout",
            "22s",
        ]);
        let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
        check!(from_cli.producer_request_timeout == secs(21));
        check!(
            resolve_consumer_request_timeout(Stack::Krabka, from_cli.consumer_request_timeout,)
                == secs(22)
        );
    }

    #[test]
    fn prometheus_request_timeout_environment_and_cli_precedence() {
        if run_environment_child(
            "KRABKA_BENCH_PROMETHEUS_TIMEOUT_CHILD",
            "tests::prometheus_request_timeout_environment_and_cli_precedence",
            &[("BENCH_PROMETHEUS_REQUEST_TIMEOUT", "32s")],
        ) {
            return;
        }

        let from_env = Cli::try_parse_from(required_args("krabka")).expect("environment");
        check!(from_env.prometheus_request_timeout == secs(32));

        let mut args = required_args("krabka");
        args.extend(["--prometheus-request-timeout", "64s"]);
        let from_cli = Cli::try_parse_from(args).expect("CLI over environment");
        check!(from_cli.prometheus_request_timeout == secs(64));
    }

    /// Values offered to every flag's value parser by [`flag_shapes`].
    const PROBES: [&str; 14] = [
        "0",
        "1",
        "-1",
        "1.5",
        "0ms",
        "1ms",
        "3000000000ms",
        "1MiB",
        "true",
        "maybe",
        "krabka",
        "broker:9092",
        "/tmp/ca.crt",
        "",
    ];

    krabka_macros::flag_metadata_fixture!(flag_metadata);

    /// One line per flag of [`Cli`]: its long name, its environment variable,
    /// its default values, its action, and which of [`PROBES`] its value
    /// parser accepts (`+`) or refuses (`-`).
    fn flag_shapes() -> String {
        use clap::CommandFactory as _;

        let command = Cli::command();
        command
            .get_arguments()
            .filter(|arg| {
                arg.get_long()
                    .is_some_and(|long| long != "help" && long != "version")
            })
            .map(|arg| {
                let (long, env, defaults) = flag_metadata(arg);
                let action = arg.get_action();
                let accepted = PROBES
                    .iter()
                    .map(|probe| {
                        let required = [
                            ("scenario", "scenario.yaml"),
                            ("bootstrap", "broker:9092"),
                            ("stack", "krabka"),
                        ]
                        .into_iter()
                        .filter(|(flag, _)| *flag != long)
                        .map(|(flag, value)| format!("--{flag}={value}"));
                        let args = std::iter::once("krabka-bench-driver".to_owned())
                            .chain(required)
                            .chain([format!("--{long}={probe}")]);
                        if command.clone().try_get_matches_from(args).is_ok() {
                            '+'
                        } else {
                            '-'
                        }
                    })
                    .collect::<String>();
                format!("{long} {env} {defaults:?} {action:?} {accepted}")
            })
            .flat_map(|line| [line, "\n".to_owned()])
            .collect()
    }

    /// The output of [`flag_shapes`], pinned so that a change to how a flag is
    /// declared cannot change its name, variable, default or parser unseen.
    const FLAG_SHAPES: &str = "\
        scenario BENCH_SCENARIO_PATH [] Set +++++++++++++-\n\
        bootstrap BENCH_BOOTSTRAP_SERVERS [] Set ++++++++++++++\n\
        client-dispatch-queue-capacity BENCH_CLIENT_DISPATCH_QUEUE_CAPACITY [\"64\"] Set -+------------\n\
        client-frame-max BENCH_CLIENT_FRAME_MAX [\"100MiB\"] Set -------+------\n\
        stack BENCH_STACK [] Set ----------+---\n\
        topic BENCH_TOPIC [\"bench-topic\"] Set ++++++++++++++\n\
        namespace BENCH_NAMESPACE [\"default\"] Set ++++++++++++++\n\
        prometheus BENCH_PROMETHEUS_URL [] Set ++++++++++++++\n\
        prometheus-request-timeout BENCH_PROMETHEUS_REQUEST_TIMEOUT [\"15s\"] Set -----++-------\n\
        producer-request-timeout BENCH_PRODUCER_REQUEST_TIMEOUT [\"2s\"] Set -----+--------\n\
        producer-final-drain-timeout BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT [\"10s\"] Set -----++-------\n\
        consumer-request-timeout BENCH_CONSUMER_REQUEST_TIMEOUT [] Set -----+--------\n\
        consumer-build-attempts BENCH_CONSUMER_BUILD_ATTEMPTS [\"6\"] Set -+------------\n\
        consumer-build-initial-backoff BENCH_CONSUMER_BUILD_INITIAL_BACKOFF [\"100ms\"] Set -----++-------\n\
        consumer-build-max-backoff BENCH_CONSUMER_BUILD_MAX_BACKOFF [\"2s\"] Set -----++-------\n\
        consumer-poll-timeout BENCH_CONSUMER_POLL_TIMEOUT [\"50ms\"] Set -----++-------\n\
        consumer-poll-error-backoff BENCH_CONSUMER_POLL_ERROR_BACKOFF [\"100ms\"] Set -----++-------\n\
        sample-interval BENCH_SAMPLE_INTERVAL [\"2s\"] Set -----++-------\n\
        broker-count BENCH_BROKER_COUNT [\"1\"] Set ++------------\n\
        out BENCH_OUTPUT_PATH [\"/results/run.json\"] Set +++++++++++++-\n\
        tls-enabled BENCH_TLS_ENABLED [\"false\"] SetTrue --------------\n\
        tls-ca-path BENCH_TLS_CA_PATH [] Set +++++++++++++-\n\
        tls-server-name BENCH_TLS_SERVER_NAME [] Set ++++++++++++++\n\
        tls-client-cert BENCH_TLS_CLIENT_CERT [] Set +++++++++++++-\n\
        tls-client-key BENCH_TLS_CLIENT_KEY [] Set +++++++++++++-\n";

    #[test]
    fn flags_keep_their_shape() {
        assert!(flag_shapes() == FLAG_SHAPES);
    }
}
