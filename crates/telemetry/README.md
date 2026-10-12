# krabka-telemetry

[API documentation](https://krabka-io.github.io/krabka-broker/krabka-telemetry/krabka_telemetry/index.html)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Generic OTLP distributed-tracing pipeline for Krabka services.

This crate is part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add krabka-telemetry --path ../krabka-broker/crates/telemetry
```

This crate is not published by this repository. Adjust the path to your local checkout. Workspace development uses the path dependencies declared in this repository.

## Usage example

Install stdout tracing plus optional OTLP export for a Krabka service process:

```rust,no_run
use krabka_telemetry::{init, OtlpConfig};

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let otlp = OtlpConfig::from_env(
    |key| std::env::var(key).ok(),
    "broker-1",
    env!("CARGO_PKG_VERSION"),
    "krabka-broker",
);
let guard = init(otlp, "info", "info", "krabka-broker")?;
tracing::info!("tracing is configured");
guard.shutdown();
# Ok(())
# }
```

## Runtime log levels

`init` returns a guard that hands out a `LogLevelController`. It reads and
retargets the stdout layer's filter while the process runs, which is what a
Kafka broker exposes as the `BROKER_LOGGER` config resource so
`kafka-configs --entity-type broker-loggers` can raise a level without a
restart. A change applies to the process that served it and is not persisted.

```rust,no_run
use krabka_telemetry::{init, LogLevel};

# fn run() -> Result<(), Box<dyn std::error::Error>> {
let guard = init(None, "krabka_broker=info,info", "info", "krabka-broker")?;
let levels = guard.log_levels();

// Every logger this process knows, with its effective level.
for (logger, level) in levels.loggers() {
    println!("{logger}={}", level.kafka_name());
}

// Raise one target. `krabka_broker` covers the targets below it.
levels.set_level("krabka_broker", LogLevel::Debug);
# Ok(())
# }
```

## Documentation

Read the [krabka-telemetry API documentation](https://krabka-io.github.io/krabka-broker/krabka-telemetry/krabka_telemetry/index.html). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
