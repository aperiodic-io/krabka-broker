//! `krabka-bench-local`: start a krabka cluster on this machine, run one
//! scenario against it, print the result, and stop the cluster.
//!
//! ```text
//! cargo build --release -p krabka-broker -p krabka-format -p krabka-bench-driver
//! target/release/krabka-bench-local --scenario bench/scenarios/local-rf3-saturate.yaml
//! ```
//!
//! The cluster is three combined controller/broker processes on loopback,
//! started from the release binaries in `--bin-dir`. Its directories and the
//! per-node logs are under `--work-dir`, which every run wipes first.

use {
    anyhow::{Context, Result},
    clap::Parser,
    krabka_bench_driver::{
        hist,
        local_cluster::{LocalCluster, LocalClusterSpec, RETENTION_BYTES, SEGMENT_BYTES},
        raw_load::{self, RawOutcome},
        scenario::{RunOutput, Scenario, Stack},
        workload::{self, ConsumerBuildRetryPolicy, DriverConfig},
    },
    krabka_client_core::{ClientFrameMax, ConnectionDispatchQueueCapacity},
    krabka_units::{fmt::Human as _, prelude::*},
    num_traits::ToPrimitive as _,
    std::path::PathBuf,
    tracing_subscriber::EnvFilter,
};

#[derive(Debug, Parser)]
#[command(
    name = "krabka-bench-local",
    about = "Benchmark a local krabka cluster"
)]
struct Cli {
    /// The scenario YAML to run.
    #[arg(long)]
    scenario: PathBuf,
    /// Directory holding the release `krabka-broker` and `krabka-format`.
    #[arg(long, default_value = "target/release")]
    bin_dir: PathBuf,
    /// Root of the node directories and logs. Wiped at the start of a run.
    ///
    /// The default is on tmpfs. On one machine all three brokers write every
    /// record to one disk, three times over, and that disk -- not the
    /// brokers -- then sets both the ceiling and the latency tail. A real
    /// cluster gives each broker its own disk.
    #[arg(long, default_value = "/dev/shm/krabka-bench-local")]
    work_dir: PathBuf,
    /// Number of broker nodes.
    #[arg(long, default_value_t = 3)]
    brokers: u16,
    /// First port of the cluster's port range.
    #[arg(long, default_value_t = 19_100)]
    base_port: u16,
    /// Override the scenario's measurement window, such as `20s`.
    #[arg(long, value_parser = krabka_units::parse::positive_time)]
    duration: Option<Time>,
    /// Override the scenario's warmup, such as `5s`.
    #[arg(long, value_parser = krabka_units::parse::non_negative_time)]
    warmup: Option<Time>,
    /// Extra argument for every broker. Repeat it for several.
    #[arg(long = "broker-arg", allow_hyphen_values = true)]
    broker_args: Vec<String>,
    /// Extra environment variable for every broker, as `KEY=VALUE`, such as
    /// `TOKIO_WORKER_THREADS=2`. Repeat it for several.
    #[arg(long = "broker-env")]
    broker_env: Vec<String>,
    /// The benchmark topic's `retention.bytes`, per partition.
    #[arg(long, value_parser = krabka_units::parse::positive_byte_size)]
    retention: Option<ByteSize>,
    /// The benchmark topic's `segment.bytes`.
    #[arg(long, value_parser = krabka_units::parse::positive_byte_size)]
    segment: Option<ByteSize>,
    /// `RUST_LOG` for the brokers.
    #[arg(long, default_value = "warn")]
    broker_log: String,
    /// Where to write the `RunOutput` JSON of a `client` run.
    #[arg(long)]
    out: Option<PathBuf>,
    /// The load generator: `raw` produces pre-encoded batches over the
    /// protocol and measures the cluster; `client` runs the producer and
    /// consumer libraries, whose own CPU cost is then part of the result.
    #[arg(long, value_enum, default_value_t = Generator::Raw)]
    generator: Generator,
    /// Cap a saturating raw run at this many records a second, such as
    /// `1500000/s`.
    #[arg(long, value_parser = krabka_units::parse::frequency)]
    rate: Option<Frequency>,
    /// Search for the highest rate the cluster sustains with a saturating
    /// scenario's batches, starting from this rate: committed throughput keeps
    /// pace with the produce rate and the consumer stays caught up.
    #[arg(long, value_parser = krabka_units::parse::frequency)]
    find_max: Option<Frequency>,
}

/// The share of the target rate a sustained probe must produce and commit.
const SUSTAINED_SHARE: f64 = 0.95;
/// The end-to-end p50 a sustained probe must stay under: the consumer reads
/// what is produced as it is produced, rather than falling ever further back.
/// A median, because on a shared machine a single scheduling stall moves the
/// tail of an otherwise caught-up run.
const SUSTAINED_E2E_P50: Time = millis(50);
/// How many probes the search makes at most.
const MAX_PROBES: usize = 8;

/// Whether a run at `rate` sustained it.
fn sustained(out: &RawOutcome, rate: f64) -> bool {
    let window = out.window.secs_f64();
    out.errors == 0
        && to_rate(out.produced, window) >= rate * SUSTAINED_SHARE
        && out.committed_per_sec >= rate * SUSTAINED_SHARE
        && hist::percentiles(&out.end_to_end).p50 <= SUSTAINED_E2E_P50
}

/// Starts a cluster, runs `scenario` against it with the raw generator, and
/// stops it.
async fn raw_once(
    spec: &LocalClusterSpec,
    scenario: &Scenario,
    rate: Option<Frequency>,
) -> Result<RawOutcome> {
    let mut cluster = LocalCluster::start(spec).await?;
    cluster
        .create_topic(TOPIC, scenario.partitions, scenario.replication_factor)
        .await?;
    let out = raw_load::run(scenario, cluster.bootstrap(), TOPIC, rate).await?;
    if let Err(error) = cluster.save_metrics(spec).await {
        tracing::warn!(%error, "could not save the broker metrics");
    }
    Ok(out)
}

/// Bisects for the highest sustained rate, from `start`: up by half while
/// probes hold, then halving the gap to the first rate that did not.
async fn find_max(spec: &LocalClusterSpec, scenario: &Scenario, start: Frequency) -> Result<()> {
    let mut held: Option<(f64, RawOutcome)> = None;
    let mut failed: Option<f64> = None;
    let mut rate = start.per_sec_f64();
    for _ in 0..MAX_PROBES {
        let out = raw_once(spec, scenario, Some(Frequency::from_per_sec(rate))).await?;
        let ok = sustained(&out, rate);
        let e2e = hist::percentiles(&out.end_to_end);
        println!(
            "probe {rate:.0} msg/s: produced {:.0} committed {:.0} e2e p50 {} p99 {} -> {}",
            to_rate(out.produced, out.window.secs_f64()),
            out.committed_per_sec,
            e2e.p50.human(),
            e2e.p99.human(),
            if ok { "sustained" } else { "not sustained" }
        );
        let floor = held.as_ref().map_or(0.0, |(rate, _)| *rate);
        if ok {
            held = Some((rate, out));
            rate = failed.map_or(rate * 1.5, |ceiling| f64::midpoint(rate, ceiling));
        } else {
            failed = Some(rate);
            rate = f64::midpoint(floor, rate);
        }
        if let (Some((floor, _)), Some(ceiling)) = (&held, failed)
            && (ceiling - floor) / floor < 0.05
        {
            break;
        }
    }
    match held {
        Some((rate, out)) => {
            println!("max sustained       {rate:.0} msg/s");
            print_raw_summary(&scenario.name, &out);
        }
        None => println!("max sustained       none of the probed rates held"),
    }
    Ok(())
}

/// The topic every run produces to.
const TOPIC: &str = "bench-topic";

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Generator {
    Raw,
    Client,
}

fn print_raw_summary(name: &str, out: &RawOutcome) {
    let window = out.window.secs_f64();
    let produced = to_rate(out.produced, window);
    let p = hist::percentiles(&out.ack);
    let e = hist::percentiles(&out.end_to_end);
    println!("scenario            {name}");
    println!("produce             {produced:.0} msg/s");
    println!(
        "consume             {:.0} msg/s",
        to_rate(out.consumed, window)
    );
    println!("committed           {:.0} msg/s", out.committed_per_sec);
    println!(
        "produce ack latency p50 {}  p99 {}  p999 {}  max {}",
        p.p50.human(),
        p.p99.human(),
        p.p999.human(),
        p.max.human()
    );
    println!(
        "end-to-end latency  p50 {}  p99 {}  p999 {}  max {}",
        e.p50.human(),
        e.p99.human(),
        e.p999.human(),
        e.max.human()
    );
    if out.errors > 0 {
        println!(
            "errors              {} (first: {})",
            out.errors,
            out.first_error.as_deref().unwrap_or("")
        );
    }
}

/// `count` per second over `window` seconds.
fn to_rate(count: u64, window: f64) -> f64 { count.to_f64().unwrap_or(0.0) / window }

fn print_summary(out: &RunOutput) {
    let produced = out.throughput.producer_rate.per_sec_f64();
    // MiB/s as a plain number: the human form of an arbitrary rate prints
    // bytes with a fraction, which is hard to compare across runs.
    let mebibytes_per_sec = produced * out.scenario.msg_size.bytes_f64() / 1_048_576.0;
    let p = &out.producer_latency;
    let e = &out.consumer_e2e_latency;
    println!("scenario            {}", out.scenario.name);
    println!("produce             {produced:.0} msg/s  {mebibytes_per_sec:.1} MiB/s");
    println!(
        "consume             {:.0} msg/s",
        out.throughput.consumer_rate.per_sec_f64()
    );
    println!(
        "produce ack latency p50 {}  p99 {}  p999 {}  max {}",
        p.p50.human(),
        p.p99.human(),
        p.p999.human(),
        p.max.human()
    );
    println!(
        "end-to-end latency  p50 {}  p99 {}  p999 {}  max {}",
        e.p50.human(),
        e.p99.human(),
        e.p999.human(),
        e.max.human()
    );
    for error in &out.errors {
        println!("error               {error}");
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .init();
    let cli = Cli::parse();

    let yaml = std::fs::read_to_string(&cli.scenario)
        .with_context(|| format!("read scenario {}", cli.scenario.display()))?;
    let mut scenario: Scenario = serde_saphyr::from_str(&yaml).context("parse scenario yaml")?;
    if let Some(duration) = cli.duration {
        scenario.duration = duration;
    }
    if let Some(warmup) = cli.warmup {
        scenario.warmup = warmup;
    }

    let spec = LocalClusterSpec::builder()
        .bin_dir(cli.bin_dir)
        .work_dir(cli.work_dir)
        .nodes(cli.brokers)
        .base_port(cli.base_port)
        .broker_args(cli.broker_args)
        .broker_log(cli.broker_log)
        .broker_env(cli.broker_env)
        .retention_bytes(cli.retention.map_or(RETENTION_BYTES, ByteSize::bytes_u64))
        .segment_bytes(cli.segment.map_or(SEGMENT_BYTES, ByteSize::bytes_u64))
        .build();
    if let Some(start) = cli.find_max {
        return find_max(&spec, &scenario, start).await;
    }
    if cli.generator == Generator::Raw {
        let out = raw_once(&spec, &scenario, cli.rate).await?;
        print_raw_summary(&scenario.name, &out);
        return Ok(());
    }

    let mut cluster = LocalCluster::start(&spec).await?;
    let topic = TOPIC;
    cluster
        .create_topic(topic, scenario.partitions, scenario.replication_factor)
        .await?;

    let cfg = DriverConfig {
        bootstrap: cluster.bootstrap().to_owned(),
        client_dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
        client_frame_max: ClientFrameMax::default(),
        topic: topic.to_owned(),
        stack: Stack::Krabka,
        namespace: "local".to_owned(),
        prometheus_url: None,
        prometheus_request_timeout: secs(5),
        producer_request_timeout: workload::default_producer_request_timeout(),
        producer_final_drain_timeout: secs(10),
        sample_interval: secs(1),
        consumer_request_timeout: workload::default_consumer_request_timeout(Stack::Krabka),
        consumer_build_retry_policy: ConsumerBuildRetryPolicy::default(),
        consumer_poll_timeout: workload::default_consumer_poll_timeout(),
        consumer_poll_error_backoff: workload::default_consumer_poll_error_backoff(),
        broker_count: u32::from(cli.brokers),
        scenario_id: 0,
        tls: None,
    };
    let out = workload::run(scenario, cfg).await?;
    if let Err(error) = cluster.save_metrics(&spec).await {
        tracing::warn!(%error, "could not save the broker metrics");
    }
    drop(cluster);

    if let Some(path) = &cli.out {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_string_pretty(&out)?)
            .with_context(|| format!("write {}", path.display()))?;
    }
    print_summary(&out);
    Ok(())
}
