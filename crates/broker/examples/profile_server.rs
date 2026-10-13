//! A single-node broker on a fixed port, for CPU profiling.
//!
//! The broker binds one address and runs until you stop it. It prints its own
//! process id on the first line, so you can attach a profiler to it:
//!
//! ```text
//! cargo run --release -p krabka-broker --example profile_server
//! perf record -F 999 -g -p <pid>
//! ```
//!
//! Drive it with the `loadgen` example beside this one. One process makes the
//! traffic and the other is the process you measure, so the profile holds no
//! client work.
//!
//! This is a profiling harness and not a test. Nothing in CI runs it.
//!
//! # Environment
//!
//! | Variable | Default | What it sets |
//! | :--- | :--- | :--- |
//! | `PROFILE_LISTEN` | `127.0.0.1:9092` | The bound and advertised address. |
//! | `PROFILE_DATA_DIR` | `/tmp/krabka-profile-data` | The log directory. It is erased at start. |
//! | `PROFILE_FLUSH` | unset | `1` calls `fsync` on every append. |

use {
    krabka_broker::{Broker, BrokerConfig},
    std::path::PathBuf,
};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let listen = std::env::var("PROFILE_LISTEN").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let data_dir =
        std::env::var("PROFILE_DATA_DIR").unwrap_or_else(|_| "/tmp/krabka-profile-data".into());
    let data_dir = PathBuf::from(data_dir);
    // A profile of a warm log and a profile of a cold one are different
    // measurements. Start cold every time, so two runs compare.
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).expect("create the log directory");

    let mut config = BrokerConfig::for_tests(data_dir);
    config.listen_addr = listen.parse().expect("PROFILE_LISTEN must be host:port");
    config.advertised_listener = listen.clone();
    // `PROFILE_FLUSH=1` puts an fsync on every append, which is Kafka's
    // durability mode. It is off by default, which matches Kafka's
    // `flush.messages` default. On this setting the write path waits on the
    // real disk, and that is where the profile differs most.
    let flush = std::env::var("PROFILE_FLUSH").ok().as_deref() == Some("1");
    config.log_config.flush_on_append = flush;

    let broker = Broker::start(config).await.expect("broker start");
    let addr = broker.listen_addr().to_string();
    println!(
        "PROFILE_SERVER pid={} listen={addr} flush_on_append={flush}",
        std::process::id()
    );

    tokio::signal::ctrl_c().await.expect("wait for ctrl-c");
    broker.shutdown().await;
}
