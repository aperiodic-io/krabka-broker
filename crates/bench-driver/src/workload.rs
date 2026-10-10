//! Producer and consumer workload runners. This module builds N producers and
//! M consumers as members of one group, runs the warmup phase and then the
//! measurement phase, triggers an optional failover mid-measurement, and merges
//! the per-task histograms into the public `LatencyPercentiles` shape.

use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result};
use chrono::Utc;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use hdrhistogram::Histogram;
use krabka_client_consumer::{AutoOffsetReset, Consumer};
use krabka_client_core::{
    ClientFrameMax, ConnectionDispatchQueueCapacity,
    security::{ClientSecurity, KeyStore, TlsConnectorConfig, TrustStore},
};
use krabka_client_producer::{Producer, ProducerError, ProducerRecord, RecordMetadata};
use krabka_macros::RefinedNewtype;
use krabka_security::ListenerProtocol;
use krabka_units::prelude::*;
use refined_type::rule::GreaterU32;
use tokio::{task::JoinSet, time::Instant};
use tracing::{info, warn};

use crate::{
    hist,
    ids::{MessageCount, TimeOffsetMs, WallclockMs},
    numeric::{
        event_rate, nonnegative_i64_to_u64, saturating_u64_to_i64, saturating_u128_to_u64, to_f64,
    },
    payload,
    prom::PromClient,
    rate::Pacer,
    scenario::{
        BrokerSample, Disturbance, LoadMode, ModeTag, Resource, RunOutput, Sample, Scenario, Stack,
        Throughput, Topology,
    },
};

/// Width of one time-series sample bucket. The driver splits the measurement
/// window into slices of the configured interval. Each producer task and
/// consumer task tallies its per-slice counts and its per-slice latency
/// histogram locally, so the hot path holds no shared locks. `run()` then merges
/// them into the `samples` series.
type AckResult = Result<RecordMetadata, ProducerError>;
type AckFuture = Pin<Box<dyn Future<Output = (AckResult, Instant)> + Send>>;

pub const MAX_CLIENT_REQUEST_TIMEOUT: Time = millis(2_147_483_647);
pub const DEFAULT_PRODUCER_REQUEST_TIMEOUT: Time = secs(2);
pub const DEFAULT_KRABKA_CONSUMER_REQUEST_TIMEOUT: Time = secs(5);
pub const DEFAULT_KAFKA_CONSUMER_REQUEST_TIMEOUT: Time = secs(30);
pub const DEFAULT_CONSUMER_BUILD_ATTEMPTS: u32 = 6;
pub const DEFAULT_CONSUMER_BUILD_INITIAL_BACKOFF: Time = millis(100);
pub const DEFAULT_CONSUMER_BUILD_MAX_BACKOFF: Time = secs(2);
pub const DEFAULT_CONSUMER_POLL_TIMEOUT: Time = millis(50);
pub const DEFAULT_CONSUMER_POLL_ERROR_BACKOFF: Time = millis(100);
pub const DEFAULT_PRODUCER_FINAL_DRAIN_TIMEOUT: Time = secs(10);
pub const DEFAULT_SAMPLE_INTERVAL: Time = secs(2);

/// Return the validated producer request-timeout default.
///
/// # Panics
///
/// Panics if the named default is not protocol-safe.
#[must_use]
pub const fn default_producer_request_timeout() -> Time {
    DEFAULT_PRODUCER_REQUEST_TIMEOUT
}

/// Return the validated consumer request-timeout default for `stack`.
///
/// # Panics
///
/// Panics if the selected named default is not protocol-safe.
#[must_use]
pub const fn default_consumer_request_timeout(stack: Stack) -> Time {
    match stack {
        Stack::Krabka => DEFAULT_KRABKA_CONSUMER_REQUEST_TIMEOUT,
        Stack::Kafka => DEFAULT_KAFKA_CONSUMER_REQUEST_TIMEOUT,
    }
}

/// How many times the driver tries to build a consumer before it gives up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, RefinedNewtype)]
#[refined(
    rule(GreaterU32<0>),
    string_error,
    default = DEFAULT_CONSUMER_BUILD_ATTEMPTS,
    from_str,
    display
)]
pub struct ConsumerBuildAttempts(u32);

#[must_use]
pub const fn default_consumer_build_initial_backoff() -> Time {
    DEFAULT_CONSUMER_BUILD_INITIAL_BACKOFF
}

#[must_use]
pub const fn default_consumer_build_max_backoff() -> Time {
    DEFAULT_CONSUMER_BUILD_MAX_BACKOFF
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ConsumerBuildRetryPolicy {
    attempts: ConsumerBuildAttempts,
    initial_backoff: Time,
    max_backoff: Time,
}

impl ConsumerBuildRetryPolicy {
    /// Validate a complete consumer-build retry policy.
    ///
    /// # Errors
    ///
    /// Returns an error when the initial backoff exceeds the maximum.
    pub fn new(
        attempts: ConsumerBuildAttempts,
        initial_backoff: Time,
        max_backoff: Time,
    ) -> Result<Self, String> {
        if initial_backoff > max_backoff {
            return Err("consumer-build initial backoff exceeds maximum".to_owned());
        }
        Ok(Self {
            attempts,
            initial_backoff,
            max_backoff,
        })
    }

    #[must_use]
    pub const fn attempts(self) -> u32 {
        self.attempts.into_value()
    }

    #[must_use]
    pub fn initial_backoff(self) -> Time {
        self.initial_backoff
    }

    #[must_use]
    pub fn max_backoff(self) -> Time {
        self.max_backoff
    }
}

impl Default for ConsumerBuildRetryPolicy {
    fn default() -> Self {
        Self::new(
            ConsumerBuildAttempts::default(),
            default_consumer_build_initial_backoff(),
            default_consumer_build_max_backoff(),
        )
        .expect("default consumer-build retry range is ordered")
    }
}

#[must_use]
pub const fn default_consumer_poll_timeout() -> Time {
    DEFAULT_CONSUMER_POLL_TIMEOUT
}

#[must_use]
pub const fn default_consumer_poll_error_backoff() -> Time {
    DEFAULT_CONSUMER_POLL_ERROR_BACKOFF
}

/// The fixed sampling grid. Every task gets a copy, so all tasks bucket into
/// the same slices.
#[derive(Clone, Copy)]
struct Grid {
    /// When the measurement window starts, which is the end of warmup.
    meas_start: Instant,
    /// The same instant as an epoch nanosecond, and the close of the window.
    ///
    /// A consumer classifies a record by the epoch stamp the producer wrote
    /// into it, not by the monotonic clock at poll time, so it needs the window
    /// on the same scale the payload carries.
    meas_start_ns: u64,
    meas_end_ns: u64,
    interval_ms: u64,
    /// Number of slices covering the measurement window.
    n: usize,
}

impl Grid {
    /// The grid that covers `scenario`'s measurement window. That window opens
    /// after its warmup has elapsed from `started_at`. The grid is always at
    /// least one slice wide, so a run shorter than a slice still has somewhere
    /// to tally into.
    ///
    /// `started_ns` is the epoch nanosecond that `started_at` was read at. The
    /// two are one instant on two clocks.
    fn new(started_at: Instant, started_ns: u64, scenario: &Scenario, interval: Time) -> Self {
        let interval_ms = nonnegative_i64_to_u64(interval.millis_i64());
        let duration_ms = nonnegative_i64_to_u64(scenario.duration.millis_i64());
        let warmup = nonnegative_i64_to_u64(scenario.warmup.nanos_i64());
        let width = nonnegative_i64_to_u64(scenario.duration.nanos_i64());
        let meas_start_ns = started_ns.saturating_add(warmup);
        Self {
            meas_start: started_at + scenario.warmup.to_std(),
            meas_start_ns,
            meas_end_ns: meas_start_ns.saturating_add(width),
            interval_ms,
            n: usize::try_from(duration_ms.div_ceil(interval_ms).max(1)).unwrap_or(usize::MAX),
        }
    }

    /// Whether a record sent at epoch nanosecond `send_ns` belongs to the
    /// measurement window.
    ///
    /// A saturating consumer can still be draining warmup backlog when the
    /// window opens. Counting those records would report warmup throughput and
    /// warmup-aged latency as measurement, which is what the scenario
    /// discarded the warmup to avoid.
    fn sent_in_measurement(&self, send_ns: u64) -> bool {
        send_ns >= self.meas_start_ns && send_ns < self.meas_end_ns
    }

    /// Slice index for a record sent at epoch nanosecond `send_ns`, clamped
    /// into `[0, n-1]`. Callers check [`Self::sent_in_measurement`] first.
    fn idx_for_send_ns(&self, send_ns: u64) -> usize {
        let elapsed_ms = send_ns.saturating_sub(self.meas_start_ns) / 1_000_000;
        usize::try_from(elapsed_ms / self.interval_ms)
            .unwrap_or(usize::MAX)
            .min(self.n.saturating_sub(1))
    }

    /// Slice index for an event observed at `now`, clamped into `[0, n-1]`.
    fn idx(&self, now: Instant) -> usize {
        let elapsed_ms =
            saturating_u128_to_u64(now.saturating_duration_since(self.meas_start).as_millis());
        usize::try_from(elapsed_ms / self.interval_ms)
            .unwrap_or(usize::MAX)
            .min(self.n.saturating_sub(1))
    }

    /// The width of one slice, as the extent the per-slice rates divide by.
    fn interval(&self) -> Time {
        Time::from_millis(saturating_u64_to_i64(self.interval_ms))
    }
}

/// Parameters wired in from `main` and the CLI. This struct is distinct from
/// `Scenario` because the scenario YAML describes *what* to run, and this struct
/// describes *where* it runs.
pub struct DriverConfig {
    pub bootstrap: String,
    pub client_dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    pub client_frame_max: ClientFrameMax,
    pub topic: String,
    pub stack: Stack,
    pub namespace: String,
    pub prometheus_url: Option<String>,
    pub prometheus_request_timeout: Time,
    pub producer_request_timeout: Time,
    pub producer_final_drain_timeout: Time,
    pub sample_interval: Time,
    pub consumer_request_timeout: Time,
    pub consumer_build_retry_policy: ConsumerBuildRetryPolicy,
    /// How long one consumer `poll` waits for records.
    pub consumer_poll_timeout: Time,
    /// How long the consumer loop sleeps after a poll error before retrying.
    pub consumer_poll_error_backoff: Time,
    pub broker_count: u32,
    pub scenario_id: u64,
    /// TLS data-path config. `None` means plaintext, the default benchmark path.
    pub tls: Option<TlsParams>,
}

/// TLS knobs for the producer and consumer data path. When this is present, both
/// clients dial the broker's TLS listener and the produce and fetch byte stream
/// is encrypted. Krabka can serve that path with kTLS sendfile, and Strimzi
/// serves it with JVM SSL.
#[derive(Debug, Clone)]
pub struct TlsParams {
    /// Mounted CA bundle (`ca.crt`) that the client trusts to verify the broker
    /// serving cert. It comes from the per-stack cluster-CA Secret.
    pub ca_path: PathBuf,
    /// SNI server name that the client presents in the TLS `ClientHello`. The
    /// broker matches it against a SAN on its serving cert. LOAD-BEARING: the
    /// client DNS-resolves the bootstrap to a pod IP and dials by IP, so the SNI
    /// is NOT derived from the bootstrap host. You must set it to a cert-SAN
    /// name. For krabka that name is
    /// `demo-broker-headless.<ns>.svc.cluster.local`, and for Strimzi it is
    /// `demo-kafka-bootstrap`.
    pub server_name: String,
    /// Optional mTLS client identity `(cert_pem, key_pem)`. One-way TLS with
    /// `None` is enough for the benchmark. Set this only if the listener
    /// requires client auth.
    pub client_identity: Option<(PathBuf, PathBuf)>,
}

impl TlsParams {
    /// Builds the [`ClientSecurity`] for an `Ssl` listener. The default is
    /// one-way TLS, which authenticates the server against the trust roots from
    /// the mounted CA. It becomes mutual TLS when [`Self::client_identity`] is
    /// set.
    #[must_use]
    pub fn to_security(&self) -> ClientSecurity {
        let mut tls = TlsConnectorConfig::default();
        tls.trust_store = TrustStore::PemFile(self.ca_path.clone());
        tls.server_name.clone_from(&self.server_name);
        tls.key_store = self
            .client_identity
            .clone()
            .map(|(certificate_chain, private_key)| KeyStore::PemFiles {
                certificate_chain,
                private_key,
                key_password: None,
            });
        ClientSecurity {
            protocol: ListenerProtocol::Ssl,
            tls: Some(tls),
            sasl: None,
            sasl_host: None,
        }
    }
}

/// Top-level entry point that `main` calls. Returns the populated
/// `RunOutput`. The caller must serialise it to disk.
/// # Errors
/// Returns an error when input data is invalid, required I/O fails, or the destination rejects the generated report or audit event.
/// # Panics
/// Panics if synchronized state is poisoned or validated input is missing a field required to produce the output.
pub async fn run(scenario: Scenario, cfg: DriverConfig) -> Result<RunOutput> {
    // One reading of the wall clock, kept at both scales. The nanosecond form
    // is what a consumer compares a record's embedded send stamp against.
    let started_ns = nonnegative_i64_to_u64(Utc::now().timestamp_nanos_opt().unwrap_or_default());
    let wallclock_start = saturating_u64_to_i64(started_ns / 1_000_000);
    let t_start = Instant::now();

    // Time-series sampling grid: split the measurement window into fixed
    // slices. Computed up front (meas_start is t_start + warmup) so it can be
    // handed to each task at spawn time.
    let grid = Grid::new(t_start, started_ns, &scenario, cfg.sample_interval);
    let n_intervals = grid.n;

    let mut notes: Vec<String> = Vec::new();
    let mut errors: Vec<String> = Vec::new();

    let (failover_active, skipped) =
        validate_topology(&scenario, &cfg, wallclock_start, &mut notes);
    if let Some(output) = skipped {
        return Ok(output);
    }

    let security = client_security(&cfg);

    let mut prod_set: JoinSet<ProducerOut> = JoinSet::new();
    let stop = Arc::new(AtomicU8::new(STATE_RUN));
    let first_ack_unix_ms = Arc::new(AtomicU64::new(0));
    // The one marker every producer reads to learn that the broker is gone.
    // It stays zero until the delete succeeds, so it is created before the
    // producers start and not beside `spawn_failover`.
    let kill_at_ms = Arc::new(AtomicU64::new(0));

    spawn_producers(
        &mut prod_set,
        &scenario,
        &cfg,
        security.as_ref(),
        grid,
        (&stop, &first_ack_unix_ms, &kill_at_ms),
    );

    let mut cons_set: JoinSet<ConsumerOut> = JoinSet::new();
    spawn_consumers(
        &mut cons_set,
        &scenario,
        &cfg,
        security.as_ref(),
        grid,
        &stop,
    );

    spawn_failover(
        failover_active,
        &scenario,
        &cfg,
        security.clone(),
        t_start,
        kill_at_ms.clone(),
    );

    let warmup_end = t_start + scenario.warmup.to_std();
    tokio::time::sleep_until(warmup_end).await;
    stop.store(STATE_MEASURING, Ordering::SeqCst);

    let meas_end = warmup_end + scenario.duration.to_std();
    tokio::time::sleep_until(meas_end).await;
    stop.store(STATE_STOP, Ordering::SeqCst);

    // ── Join all tasks ──────────────────────────────────────────────────────
    let mut prod_hist = hist::new();
    let mut prod_msgs = 0u64;
    let mut prod_bytes = 0u64;
    let mut prod_dropped = 0u64;
    let mut earliest_recovery_ms = 0u64;
    let mut max_spike = Time::ZERO;
    let mut prod_iv_msgs = vec![0u64; n_intervals];
    let mut prod_iv_hist: Vec<Histogram<u64>> = (0..n_intervals).map(|_| hist::new()).collect();
    while let Some(j) = prod_set.join_next().await {
        match j {
            Ok(t) => {
                prod_hist.add(&t.latency).ok();
                prod_msgs += t.msgs;
                prod_bytes += t.bytes;
                prod_dropped += t.dropped;
                for iv in 0..n_intervals {
                    if let Some(m) = t.interval_msgs.get(iv) {
                        prod_iv_msgs[iv] += *m;
                    }
                    if let Some(h) = t.interval_hist.get(iv) {
                        prod_iv_hist[iv].add(h).ok();
                    }
                }
                if t.latency_spike_max > max_spike {
                    max_spike = t.latency_spike_max;
                }
                if t.recovery_unix_ms > 0
                    && (earliest_recovery_ms == 0 || t.recovery_unix_ms < earliest_recovery_ms)
                {
                    earliest_recovery_ms = t.recovery_unix_ms;
                }
                if !t.error.is_empty() {
                    errors.push(t.error);
                }
            }
            Err(e) => errors.push(format!("producer-task-panic: {e}")),
        }
    }

    let mut cons_hist = hist::new();
    let mut cons_msgs = 0u64;
    let mut cons_bytes = 0u64;
    let mut cons_iv_msgs = vec![0u64; n_intervals];
    let mut cons_iv_hist: Vec<Histogram<u64>> = (0..n_intervals).map(|_| hist::new()).collect();
    while let Some(j) = cons_set.join_next().await {
        match j {
            Ok(t) => {
                cons_hist.add(&t.latency).ok();
                cons_msgs += t.msgs;
                cons_bytes += t.bytes;
                for iv in 0..n_intervals {
                    if let Some(m) = t.interval_msgs.get(iv) {
                        cons_iv_msgs[iv] += *m;
                    }
                    if let Some(h) = t.interval_hist.get(iv) {
                        cons_iv_hist[iv].add(h).ok();
                    }
                }
                if !t.error.is_empty() {
                    errors.push(t.error);
                }
            }
            Err(e) => errors.push(format!("consumer-task-panic: {e}")),
        }
    }

    let samples = build_samples(
        grid.interval(),
        (&prod_iv_msgs, &prod_iv_hist),
        (&cons_iv_msgs, &cons_iv_hist),
    );

    let wallclock_end = Utc::now().timestamp_millis();
    let (resource, broker_samples) = capture_resources(
        &scenario,
        &cfg,
        (wallclock_start, wallclock_end),
        grid.meas_end_ns,
        prod_msgs,
        &mut notes,
    )
    .await;

    let (disturbance, first_ack) = finalize_timing(
        failover_active,
        (
            kill_at_ms.load(Ordering::SeqCst),
            earliest_recovery_ms,
            prod_dropped,
            max_spike,
        ),
        first_ack_unix_ms.load(Ordering::SeqCst),
        wallclock_start,
    );
    note_missing_disturbance(
        failover_active,
        disturbance.as_ref(),
        kill_at_ms.load(Ordering::SeqCst),
        &mut notes,
    );

    Ok(RunOutput {
        scenario: scenario.clone(),
        stack: cfg.stack,
        topology: Topology {
            partitions: scenario.partitions,
            replication_factor: scenario.replication_factor,
            broker_count: cfg.broker_count,
        },
        wallclock_start_unix_ms: WallclockMs(wallclock_start),
        wallclock_end_unix_ms: WallclockMs(wallclock_end),
        throughput: Throughput {
            msgs_produced: MessageCount(prod_msgs),
            msgs_consumed: MessageCount(cons_msgs),
            bytes_in: ByteSize::from_bytes(prod_bytes),
            bytes_out: ByteSize::from_bytes(cons_bytes),
            producer_rate: event_rate(prod_msgs, scenario.duration),
            consumer_rate: event_rate(cons_msgs, scenario.duration),
        },
        producer_latency: hist::percentiles(&prod_hist),
        consumer_e2e_latency: hist::percentiles(&cons_hist),
        resource,
        disturbance,
        startup: None,
        first_ack,
        errors,
        notes,
        samples,
        broker_samples,
    })
}

fn client_security(cfg: &DriverConfig) -> Option<ClientSecurity> {
    let security = cfg.tls.as_ref().map(TlsParams::to_security);
    if let Some(value) = &security {
        info!(
            server_name = value
                .tls
                .as_ref()
                .map_or("", |tls| tls.server_name.as_str()),
            "TLS data path enabled (protocol=Ssl)"
        );
    }
    security
}

fn build_samples(
    interval: Time,
    producer: (&[u64], &[Histogram<u64>]),
    consumer: (&[u64], &[Histogram<u64>]),
) -> Vec<Sample> {
    let interval_ms = nonnegative_i64_to_u64(interval.millis_i64());
    // The histogram counts microseconds; an empty slice measured nothing.
    let percentile = |histogram: &Histogram<u64>, quantile: f64| {
        if histogram.is_empty() {
            Time::ZERO
        } else {
            Time::from_micros(saturating_u64_to_i64(histogram.value_at_quantile(quantile)))
        }
    };
    producer
        .0
        .iter()
        .enumerate()
        .map(|(index, messages)| Sample {
            t_offset_ms: TimeOffsetMs(
                u64::try_from(index)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(interval_ms),
            ),
            producer_rate: event_rate(*messages, interval),
            consumer_rate: event_rate(consumer.0[index], interval),
            producer_p50: percentile(&producer.1[index], 0.50),
            producer_p99: percentile(&producer.1[index], 0.99),
            consumer_e2e_p99: percentile(&consumer.1[index], 0.99),
        })
        .collect()
}

/// The disturbance a failover run measured, and the run's first-ack extent.
///
/// A disturbance is written only when the delete actually landed and a record
/// was acknowledged after it. `failover.0` is the shared kill marker, which
/// stays zero when the pod delete failed; `failover.1` is the first ack at or
/// after that marker. Either one at zero means the run has no failover
/// evidence, and an empty disturbance is the honest answer. The report's gate
/// reads that as a violation rather than as an instant recovery.
/// Say in the artifact why a failover run carries no disturbance.
///
/// Without this the JSON looks like an ordinary run and the reader has to
/// guess whether the schedule ran at all.
/// Spawn one producer task per `scenario.producers`.
///
/// The three shared handles travel as a tuple because they are one thing: the
/// run-phase signal, the first-ack stamp, and the kill marker every task reads.
fn spawn_producers(
    tasks: &mut JoinSet<ProducerOut>,
    scenario: &Scenario,
    cfg: &DriverConfig,
    security: Option<&ClientSecurity>,
    grid: Grid,
    shared: (&Arc<AtomicU8>, &Arc<AtomicU64>, &Arc<AtomicU64>),
) {
    let (stop, first_ack, kill_at) = shared;
    for idx in 0..scenario.producers {
        tasks.spawn(run_producer(ProducerTask {
            idx,
            scenario: scenario.clone(),
            bootstrap: cfg.bootstrap.clone(),
            topic: cfg.topic.clone(),
            scenario_id: cfg.scenario_id,
            stop: Arc::clone(stop),
            first_ack: Arc::clone(first_ack),
            kill_at: Arc::clone(kill_at),
            security: security.cloned(),
            grid,
            request_timeout: cfg.producer_request_timeout,
            final_drain_timeout: cfg.producer_final_drain_timeout,
            dispatch_queue_capacity: cfg.client_dispatch_queue_capacity,
            frame_max: cfg.client_frame_max,
        }));
    }
}

/// Spawn one consumer task per `scenario.consumers`.
fn spawn_consumers(
    tasks: &mut JoinSet<ConsumerOut>,
    scenario: &Scenario,
    cfg: &DriverConfig,
    security: Option<&ClientSecurity>,
    grid: Grid,
    stop: &Arc<AtomicU8>,
) {
    for idx in 0..scenario.consumers {
        tasks.spawn(run_consumer(ConsumerTask {
            idx,
            scenario: scenario.clone(),
            bootstrap: cfg.bootstrap.clone(),
            topic: cfg.topic.clone(),
            scenario_id: cfg.scenario_id,
            stop: Arc::clone(stop),
            security: security.cloned(),
            grid,
            request_timeout: cfg.consumer_request_timeout,
            build_retry_policy: cfg.consumer_build_retry_policy,
            poll_timeout: cfg.consumer_poll_timeout,
            poll_error_backoff: cfg.consumer_poll_error_backoff,
            dispatch_queue_capacity: cfg.client_dispatch_queue_capacity,
            frame_max: cfg.client_frame_max,
        }));
    }
}

fn note_missing_disturbance(
    failover_active: bool,
    disturbance: Option<&Disturbance>,
    kill_at_ms: u64,
    notes: &mut Vec<String>,
) {
    if !failover_active || disturbance.is_some() {
        return;
    }
    notes.push(if kill_at_ms == 0 {
        "failover:no-broker-deleted".to_owned()
    } else {
        "failover:no-ack-after-kill".to_owned()
    });
}

fn finalize_timing(
    failover_active: bool,
    failover: (u64, u64, u64, Time),
    first_ack: u64,
    wallclock_start: i64,
) -> (Option<Disturbance>, Time) {
    let measured_kill = failover.0 > 0 && failover.1 >= failover.0;
    let disturbance = (failover_active && measured_kill).then_some(Disturbance {
        kill_at_ms: TimeOffsetMs(failover.0),
        recovery_at_ms: TimeOffsetMs(failover.1),
        dropped: MessageCount(failover.2),
        latency_spike_max: failover.3,
    });
    // Both stamps are epoch milliseconds — coordinates — so their difference is
    // the extent from run start to the first ack. No ack at all means no extent.
    let first_ack = if first_ack == 0 {
        Time::ZERO
    } else {
        Time::from_millis(
            i64::try_from(first_ack)
                .unwrap_or(i64::MAX)
                .saturating_sub(wallclock_start)
                .max(0),
        )
    };
    (disturbance, first_ack)
}

fn validate_topology(
    scenario: &Scenario,
    cfg: &DriverConfig,
    wallclock_start: i64,
    notes: &mut Vec<String>,
) -> (bool, Option<RunOutput>) {
    let failover_active =
        scenario.failover.is_some() && scenario.replication_factor >= 3 && cfg.broker_count >= 3;
    if scenario.failover.is_some() && !failover_active {
        notes.push("skipped:failover-needs-rf3".into());
    }
    let topology_mismatch = matches!(scenario.mode_tag, ModeTag::Cluster)
        && scenario.replication_factor >= 3
        && cfg.broker_count < 3;
    if !topology_mismatch {
        return (failover_active, None);
    }
    notes.push(format!(
        "skipped:topology-mismatch (rf={} brokers={})",
        scenario.replication_factor, cfg.broker_count
    ));
    let output = empty_output(
        scenario,
        cfg,
        wallclock_start,
        std::mem::take(notes),
        Vec::new(),
    );
    (failover_active, Some(output))
}

/// Capture broker CPU and memory for the window the run measured.
///
/// `measured_until_ns` is the epoch nanosecond at which the measurement window
/// closed, which is not the same instant as this call: joining the tasks and
/// draining the producers happens in between, and a failover run can wait out
/// its final-drain timeout there. The queries are evaluated at the close of the
/// window, so the lookback covers what was measured.
async fn capture_resources(
    scenario: &Scenario,
    cfg: &DriverConfig,
    wallclock: (i64, i64),
    measured_until_ns: u64,
    produced: u64,
    notes: &mut Vec<String>,
) -> (Resource, Vec<BrokerSample>) {
    let Some(url) = &cfg.prometheus_url else {
        notes.push("prometheus-url-not-set".into());
        return (Resource::default(), Vec::new());
    };
    let client = match PromClient::new(url, cfg.prometheus_request_timeout) {
        Ok(client) => client,
        Err(error) => {
            notes.push(format!("prometheus-client-failed: {error}"));
            return (Resource::default(), Vec::new());
        }
    };
    let resource = match client
        .capture_resource(
            cfg.stack,
            &cfg.namespace,
            scenario.duration,
            to_f64(measured_until_ns) / 1e9,
            MessageCount(produced),
        )
        .await
    {
        Ok(resource) => resource,
        Err(error) => {
            warn!(%error, "prometheus capture failed");
            notes.push(format!("prometheus-capture-failed: {error}"));
            Resource::default()
        }
    };
    let broker_samples = client
        .capture_resource_series(
            cfg.stack,
            &cfg.namespace,
            to_f64(wallclock.0) / 1000.0,
            to_f64(wallclock.1) / 1000.0,
            15,
        )
        .await
        .unwrap_or_default();
    (resource, broker_samples)
}

fn spawn_failover(
    active: bool,
    scenario: &Scenario,
    cfg: &DriverConfig,
    security: Option<ClientSecurity>,
    started_at: Instant,
    kill_at: Arc<AtomicU64>,
) {
    if !active {
        return;
    }
    let spec = scenario.failover.clone().expect("checked above");
    let stack = cfg.stack;
    let namespace = cfg.namespace.clone();
    let bootstrap = cfg.bootstrap.clone();
    let topic = cfg.topic.clone();
    let dispatch_queue_capacity = cfg.client_dispatch_queue_capacity;
    let frame_max = cfg.client_frame_max;
    tokio::spawn(async move {
        tokio::time::sleep_until(started_at + spec.kill_after.to_std()).await;
        let client = match crate::failover::try_client().await {
            Ok(client) => client,
            Err(error) => {
                warn!(%error, "failover: in-cluster client unavailable");
                return;
            }
        };
        let leader_id = if spec.target == "partition0_leader" {
            match crate::failover::partition0_leader_from_metadata(
                &bootstrap,
                &topic,
                security,
                dispatch_queue_capacity,
                frame_max,
            )
            .await
            {
                Ok(id) => Some(id),
                Err(error) => {
                    warn!(%error, "failover: leader lookup failed; using first broker pod");
                    None
                }
            }
        } else {
            None
        };
        match crate::failover::kill_broker_pod(&client, stack, &namespace, leader_id).await {
            Ok(name) => {
                // The marker goes up only now. Everything above it -- client
                // discovery, the leader lookup, the delete itself -- can fail,
                // and a marker published before them would make a producer
                // error that had nothing to do with a kill look like the kill.
                // A run whose delete failed leaves the marker at zero, and
                // `finalize_timing` then records no disturbance at all.
                kill_at.store(
                    nonnegative_i64_to_u64(Utc::now().timestamp_millis()),
                    Ordering::SeqCst,
                );
                info!(pod = %name, "failover: killed broker");
            }
            Err(error) => warn!(%error, "failover: broker kill failed"),
        }
    });
}

const STATE_RUN: u8 = 0; // warmup phase, record-but-discard
const STATE_MEASURING: u8 = 1;
const STATE_STOP: u8 = 2;

struct ProducerOut {
    latency: Histogram<u64>,
    msgs: u64,
    bytes: u64,
    dropped: u64,
    recovery_unix_ms: u64,
    latency_spike_max: Time,
    error: String,
    /// Measurement-window tallies, one per slice of the [`Grid`]. This is empty
    /// on a build failure. `run()` merges by index with bounds checks.
    interval_msgs: Vec<u64>,
    interval_hist: Vec<Histogram<u64>>,
}

struct ConsumerOut {
    latency: Histogram<u64>,
    msgs: u64,
    bytes: u64,
    error: String,
    interval_msgs: Vec<u64>,
    interval_hist: Vec<Histogram<u64>>,
}

fn empty_output(
    scenario: &Scenario,
    cfg: &DriverConfig,
    start: i64,
    notes: Vec<String>,
    errors: Vec<String>,
) -> RunOutput {
    RunOutput {
        scenario: scenario.clone(),
        stack: cfg.stack,
        topology: Topology {
            partitions: scenario.partitions,
            replication_factor: scenario.replication_factor,
            broker_count: cfg.broker_count,
        },
        wallclock_start_unix_ms: WallclockMs(start),
        wallclock_end_unix_ms: WallclockMs(start),
        throughput: Throughput::default(),
        producer_latency: crate::scenario::LatencyPercentiles::default(),
        consumer_e2e_latency: crate::scenario::LatencyPercentiles::default(),
        resource: Resource::default(),
        disturbance: None,
        startup: None,
        first_ack: Time::ZERO,
        errors,
        notes,
        samples: Vec::new(),
        broker_samples: Vec::new(),
    }
}

// ── Producer task ───────────────────────────────────────────────────────────

struct ProducerTask {
    idx: usize,
    scenario: Scenario,
    bootstrap: String,
    topic: String,
    scenario_id: u64,
    stop: Arc<AtomicU8>,
    first_ack: Arc<AtomicU64>,
    /// Epoch millisecond at which the failover task deleted the broker pod, or
    /// zero while no delete has succeeded. This is the only thing that says a
    /// kill happened. A producer error says the send failed and nothing more.
    kill_at: Arc<AtomicU64>,
    security: Option<ClientSecurity>,
    grid: Grid,
    request_timeout: Time,
    final_drain_timeout: Time,
    dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    frame_max: ClientFrameMax,
}

struct TaskMeasurements {
    latency: Histogram<u64>,
    interval_msgs: Vec<u64>,
    interval_hist: Vec<Histogram<u64>>,
    msgs: u64,
    bytes: u64,
}

impl TaskMeasurements {
    fn new(intervals: usize) -> Self {
        Self {
            latency: hist::new(),
            interval_msgs: vec![0; intervals],
            interval_hist: (0..intervals).map(|_| hist::new()).collect(),
            msgs: 0,
            bytes: 0,
        }
    }
}

async fn run_producer(task: ProducerTask) -> ProducerOut {
    let ProducerTask {
        idx,
        scenario,
        bootstrap,
        topic,
        scenario_id,
        stop,
        first_ack,
        kill_at,
        security,
        grid,
        request_timeout,
        final_drain_timeout,
        dispatch_queue_capacity,
        frame_max,
    } = task;
    // Idempotence forces acks=All; turn it off whenever the scenario
    // requested something weaker.
    let enable_idempotence = matches!(scenario.acks, crate::scenario::Acks::All);
    let producer = match Producer::builder()
        .bootstrap(bootstrap.clone())
        .dispatch_queue_capacity(dispatch_queue_capacity.get())
        .frame_max(frame_max.size())
        .client_id(format!("bench-producer-{idx}"))
        .acks(scenario.acks.into_producer())
        .compression(scenario.compression.into_producer())
        .enable_idempotence(enable_idempotence)
        .linger(scenario.linger.to_std())
        .batch_size(scenario.batch_size.bytes_usize())
        // The producer pipelines one in-flight request PER PARTITION and uses
        // this value as the cross-partition fan-out cap (how many distinct
        // partitions it services concurrently per drain cycle). The default (5)
        // throttles topics with more partitions than that — our scenarios run 6
        // to 100 partitions — so raise it well past the max partition count to
        // keep every partition's pipeline busy.
        .max_in_flight_per_connection(128)
        // A produce to a healthy broker completes in low single-digit ms, so a
        // 2s ceiling should not trip under normal load — but it bounds how long a
        // send to a *killed* leader blocks before the client re-routes, so the
        // measured failover recovery reflects the cluster's leader re-election
        // rather than the 30s default request timeout.
        .request_timeout(request_timeout.to_std())
        // `None` → plaintext (default). `Some` → all produce traffic for this
        // task goes over the broker's TLS listener.
        .maybe_security(security)
        .build()
        .await
        .context("build producer")
    {
        Ok(p) => p,
        Err(e) => {
            return ProducerOut {
                latency: hist::new(),
                msgs: 0,
                bytes: 0,
                dropped: 0,
                recovery_unix_ms: 0,
                latency_spike_max: Time::ZERO,
                error: format!("producer-{idx}-build: {e:#}"),
                interval_msgs: Vec::new(),
                interval_hist: Vec::new(),
            };
        }
    };

    let mut tmpl = payload::template(scenario.msg_size);
    // Counter behind each record key, so keys vary within this task. The task
    // index is folded in, so two tasks never mint the same key.
    let mut sent_records = (idx as u64) << 40;
    let mut measurements = TaskMeasurements::new(grid.n);
    let mut dropped = 0u64;
    let mut recovery_unix_ms = 0u64;
    let mut latency_spike_max = Time::ZERO;
    let mut error = String::new();

    let mut pacer = match scenario.mode {
        LoadMode::Saturate => None,
        // The scenario rate is for the whole run, so each task paces at its
        // share of it. `Pacer` floors the result at one message a second.
        LoadMode::FixedRate { rate } => Some(Pacer::new(rate / to_f64(scenario.producers.max(1)))),
    };

    // Pipeline depth: how many records may be in flight (awaiting their ack)
    // before the loop applies back-pressure. Without pipelining the loop sent
    // one record and awaited its ack before the next, capping a task's
    // throughput at 1/RTT regardless of cluster capacity — so the numbers
    // measured the driver, not the cluster. A bounded window lets throughput
    // track the cluster while keeping memory in hand.
    let max_inflight: usize = 512;
    let mut inflight: FuturesUnordered<AckFuture> = FuturesUnordered::new();

    // Record one *settled* send (`Ok` = ack, `Err` = producer error). A macro,
    // not a closure, so it can mutate the per-task accumulators in place.
    macro_rules! handle_ack {
        ($res:expr, $t0:expr) => {
            match $res {
                Ok(_meta) => {
                    let latency = $t0.elapsed().as_time();
                    if stop.load(Ordering::Relaxed) == STATE_MEASURING {
                        hist::record(&mut measurements.latency, latency);
                        measurements.msgs += 1;
                        measurements.bytes += scenario.msg_size.bytes_u64();
                        let iv = grid.idx(Instant::now());
                        measurements.interval_msgs[iv] += 1;
                        hist::record(&mut measurements.interval_hist[iv], latency);
                        // Recovery and the spike are measured against the
                        // shared kill marker, not against this task's own
                        // errors. The marker is published only after the pod
                        // delete succeeds, so an ack before it belongs to the
                        // healthy cluster and is not a recovery.
                        let killed_at = kill_at.load(Ordering::Relaxed);
                        if killed_at > 0 {
                            let now_ms = nonnegative_i64_to_u64(Utc::now().timestamp_millis());
                            if recovery_unix_ms == 0 && now_ms >= killed_at {
                                recovery_unix_ms = now_ms;
                            }
                            if latency > latency_spike_max {
                                latency_spike_max = latency;
                            }
                        }
                    }
                    if first_ack.load(Ordering::Relaxed) == 0 {
                        let now_ms = nonnegative_i64_to_u64(Utc::now().timestamp_millis());
                        let _ = first_ack.compare_exchange(
                            0,
                            now_ms,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                        );
                    }
                }
                Err(e) => {
                    if stop.load(Ordering::Relaxed) == STATE_MEASURING {
                        dropped += 1;
                    }
                    if dropped == 1 && error.is_empty() {
                        error = format!("producer-{idx}-err: {e}");
                    }
                }
            }
        };
    }
    loop {
        let state = stop.load(Ordering::Relaxed);
        if state == STATE_STOP {
            break;
        }
        // Drain every ack that has already settled — promptly, so the recorded
        // latency is the real send→ack time, not how long a record waited in
        // the in-flight set behind an older stuck send.
        while let Some((res, t0)) = inflight.next().now_or_never().flatten() {
            handle_ack!(res, t0);
        }
        // Back-pressure: at capacity, block until any send completes. Waiting
        // for the oldest can make one dead-leader send hide live-partition acks.
        while inflight.len() >= max_inflight {
            if let Some((res, t0)) = inflight.next().await {
                handle_ack!(res, t0);
            }
        }
        if let Some(p) = pacer.as_mut() {
            p.await_token().await;
        }
        let value = payload::stamp_into(&mut tmpl, scenario_id);
        // A scenario that sets `key_size` gets keyed records. The key changes
        // per record, so the partitioner spreads them the way a real keyed
        // workload spreads, and the key's bytes are on the wire the way the
        // scenario asked for. `key_size: 0` stays keyless.
        let key = payload::key(scenario.key_size, sent_records);
        sent_records += 1;
        let rec = ProducerRecord {
            topic: topic.clone(),
            key,
            value: Some(value),
            ..Default::default()
        };
        let t0 = Instant::now();
        match producer.enqueue(rec).await {
            Ok(delivery) => inflight.push(Box::pin(async move { (delivery.await, t0) })),
            Err(error) => handle_ack!(Err::<RecordMetadata, _>(error), t0),
        }
    }

    // Drain anything still outstanding when the measurement window closed, but
    // do not let a stuck producer retry slot keep the whole benchmark job alive
    // forever. Unsettled sends are counted as drops so failover reports capture
    // the stall instead of timing out without a JSON result.
    let drain_until = Instant::now() + final_drain_timeout.to_std();
    while !inflight.is_empty() {
        let now = Instant::now();
        if now >= drain_until {
            let unresolved = inflight.len() as u64;
            dropped += unresolved;
            if error.is_empty() {
                error = format!("producer-{idx}-final-drain-timeout:{unresolved}");
            }
            break;
        }
        match tokio::time::timeout_at(drain_until, inflight.next()).await {
            Ok(Some((res, t0))) => handle_ack!(res, t0),
            Ok(None) => break,
            Err(_) => {
                let unresolved = inflight.len() as u64;
                dropped += unresolved;
                if error.is_empty() {
                    error = format!("producer-{idx}-final-drain-timeout:{unresolved}");
                }
                break;
            }
        }
    }

    let _ = producer.flush().await;
    let _ = producer.close().await;

    ProducerOut {
        latency: measurements.latency,
        msgs: measurements.msgs,
        bytes: measurements.bytes,
        dropped,
        recovery_unix_ms,
        latency_spike_max,
        error,
        interval_msgs: measurements.interval_msgs,
        interval_hist: measurements.interval_hist,
    }
}

// ── Consumer task ───────────────────────────────────────────────────────────

struct ConsumerTask {
    idx: usize,
    scenario: Scenario,
    bootstrap: String,
    topic: String,
    scenario_id: u64,
    stop: Arc<AtomicU8>,
    security: Option<ClientSecurity>,
    grid: Grid,
    request_timeout: Time,
    build_retry_policy: ConsumerBuildRetryPolicy,
    poll_timeout: Time,
    poll_error_backoff: Time,
    dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    frame_max: ClientFrameMax,
}

async fn run_consumer(task: ConsumerTask) -> ConsumerOut {
    let ConsumerTask {
        idx,
        scenario,
        bootstrap,
        topic,
        scenario_id,
        stop,
        security,
        grid,
        request_timeout,
        build_retry_policy,
        poll_timeout,
        poll_error_backoff,
        dispatch_queue_capacity,
        frame_max,
    } = task;
    let group_id = format!("krabka-bench-{}", scenario.name);
    let mut consumer = match build_consumer_with_retry(
        idx,
        &bootstrap,
        group_id,
        &topic,
        security,
        ConsumerBuildPolicy {
            request_timeout,
            retry: build_retry_policy,
            dispatch_queue_capacity,
            frame_max,
        },
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            return ConsumerOut {
                latency: hist::new(),
                msgs: 0,
                bytes: 0,
                error: format!("consumer-{idx}-build: {e:#}"),
                interval_msgs: Vec::new(),
                interval_hist: Vec::new(),
            };
        }
    };

    let mut measurements = TaskMeasurements::new(grid.n);
    let mut error = String::new();

    loop {
        if stop.load(Ordering::Relaxed) == STATE_STOP {
            break;
        }
        match consumer.poll(poll_timeout).await {
            Ok(records) => {
                let now_ns =
                    nonnegative_i64_to_u64(Utc::now().timestamp_nanos_opt().unwrap_or_default());
                let phase = stop.load(Ordering::Relaxed);
                for r in records {
                    if let Some(val) = &r.value {
                        let bytes = val.len() as u64;
                        if let Some(send_nanos) = payload::read_send_nanos(val, scenario_id) {
                            // Both stamps are epoch nanoseconds; their
                            // difference is the end-to-end extent.
                            let latency = Time::from_nanos(saturating_u64_to_i64(
                                now_ns.saturating_sub(send_nanos),
                            ));
                            // Classify by when the producer SENT the record,
                            // not by the phase this poll returned in. A lagging
                            // consumer drains warmup backlog after the window
                            // opens, and counting it reports warmup throughput
                            // and warmup-aged latency as measurement.
                            if grid.sent_in_measurement(send_nanos) {
                                let slice = grid.idx_for_send_ns(send_nanos);
                                hist::record(&mut measurements.latency, latency);
                                measurements.msgs += 1;
                                measurements.bytes += bytes;
                                measurements.interval_msgs[slice] += 1;
                                hist::record(&mut measurements.interval_hist[slice], latency);
                            }
                        } else if phase == STATE_MEASURING {
                            // Non-bench record, left over from an earlier run.
                            // It carries no send stamp, so the poll phase is
                            // the only thing that can place it. Count bytes and
                            // not end-to-end latency.
                            measurements.bytes += bytes;
                        }
                    }
                }
            }
            Err(e) => {
                if error.is_empty() {
                    error = format!("consumer-{idx}-poll: {e}");
                }
                tokio::time::sleep(poll_error_backoff.to_std()).await;
            }
        }
    }
    let _ = consumer.close().await;
    ConsumerOut {
        latency: measurements.latency,
        msgs: measurements.msgs,
        bytes: measurements.bytes,
        error,
        interval_msgs: measurements.interval_msgs,
        interval_hist: measurements.interval_hist,
    }
}

#[derive(Clone, Copy)]
struct ConsumerBuildPolicy {
    request_timeout: Time,
    retry: ConsumerBuildRetryPolicy,
    dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    frame_max: ClientFrameMax,
}

async fn build_consumer_with_retry(
    idx: usize,
    bootstrap: &str,
    group_id: String,
    topic: &str,
    security: Option<ClientSecurity>,
    policy: ConsumerBuildPolicy,
) -> Result<Consumer> {
    let backoff = exponential_backoff::Backoff::new(
        policy.retry.attempts(),
        policy.retry.initial_backoff().to_std(),
        Some(policy.retry.max_backoff().to_std()),
    );
    for (attempt_idx, delay) in backoff.into_iter().enumerate() {
        let attempt = attempt_idx + 1;
        let result = Consumer::builder()
            .bootstrap(bootstrap.to_string())
            .dispatch_queue_capacity(policy.dispatch_queue_capacity.get())
            .frame_max(policy.frame_max.size())
            .client_id(format!("bench-consumer-{idx}"))
            .group_id(group_id.clone())
            .subscribe(vec![topic.to_string()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .request_timeout(policy.request_timeout)
            // `None` → plaintext (default). `Some` → all fetch traffic for this
            // task goes over the broker's TLS listener (kTLS sendfile on krabka).
            .maybe_security(security.clone())
            .build()
            .await
            .context("build consumer");

        match result {
            Ok(consumer) => return Ok(consumer),
            Err(e) => match delay {
                Some(delay) => {
                    warn!(
                        attempt,
                        retry_after_ms = delay.as_millis(),
                        error = %e,
                        "consumer build failed; retrying"
                    );
                    tokio::time::sleep(delay).await;
                }
                None => {
                    return Err(
                        e.context(format!("build consumer failed after {attempt} attempts"))
                    );
                }
            },
        }
    }
    unreachable!("exponential_backoff::Backoff yields a terminal attempt");
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::scenario::{FailoverSpec, ModeTag};

    fn cfg(broker_count: u32) -> DriverConfig {
        DriverConfig {
            bootstrap: "broker:9092".into(),
            client_dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
            client_frame_max: ClientFrameMax::default(),
            topic: "t".into(),
            stack: Stack::Krabka,
            namespace: "default".into(),
            prometheus_url: None,
            prometheus_request_timeout: crate::prom::DEFAULT_PROMETHEUS_REQUEST_TIMEOUT,
            producer_request_timeout: default_producer_request_timeout(),
            producer_final_drain_timeout: DEFAULT_PRODUCER_FINAL_DRAIN_TIMEOUT,
            sample_interval: DEFAULT_SAMPLE_INTERVAL,
            consumer_request_timeout: default_consumer_request_timeout(Stack::Krabka),
            consumer_build_retry_policy: ConsumerBuildRetryPolicy::default(),
            consumer_poll_timeout: default_consumer_poll_timeout(),
            consumer_poll_error_backoff: default_consumer_poll_error_backoff(),
            broker_count,
            scenario_id: 0,
            tls: None,
        }
    }

    fn scenario(rf: i16) -> Scenario {
        crate::scenario::fixture::short_scenario(rf)
    }

    async fn producer_broker(
        metadata_error: i16,
        delivery_error: i16,
    ) -> (
        krabka_client_core::MockBroker,
        tokio::sync::oneshot::Receiver<()>,
        Arc<AtomicU64>,
    ) {
        use bytes::BytesMut;
        use krabka_protocol::{
            Decode as _, Encode as _,
            owned::{
                api_versions_request,
                api_versions_response::{ApiVersion, ApiVersionsResponse},
                metadata_request::{self, MetadataRequest},
                metadata_response::{
                    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
                    MetadataResponseTopic,
                },
                produce_request::{self, ProduceRequest},
                produce_response::{
                    PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
                },
            },
            records::RecordsPayload,
        };

        let port = Arc::new(std::sync::atomic::AtomicU16::new(0));
        let handler_port = Arc::clone(&port);
        let largest_batch = Arc::new(AtomicU64::new(0));
        let observed_batch = Arc::clone(&largest_batch);
        let (started, receiving) = tokio::sync::oneshot::channel();
        let mut started = Some(started);
        let broker = krabka_client_core::MockBroker::start(move |api, version, _, body| {
            let mut encoded = BytesMut::new();
            match api {
                api_versions_request::API_KEY => ApiVersionsResponse {
                    api_keys: [
                        (metadata_request::API_KEY, 0),
                        (produce_request::API_KEY, 3),
                    ]
                    .into_iter()
                    .map(|(api_key, max_version)| ApiVersion {
                        api_key,
                        min_version: max_version,
                        max_version,
                        ..Default::default()
                    })
                    .collect(),
                    ..Default::default()
                }
                .encode(&mut encoded, 0)
                .unwrap(),
                metadata_request::API_KEY => {
                    let mut body = &body[2 + "bench-producer-0".len()..];
                    let request = MetadataRequest::decode(&mut body, version).unwrap();
                    let named = request.topics.is_some_and(|topics| !topics.is_empty());
                    if named && let Some(started) = started.take() {
                        let _ = started.send(());
                    }
                    MetadataResponse {
                        brokers: vec![MetadataResponseBroker {
                            node_id: 1,
                            host: "127.0.0.1".into(),
                            port: i32::from(handler_port.load(Ordering::SeqCst)),
                            ..Default::default()
                        }],
                        topics: named
                            .then(|| MetadataResponseTopic {
                                name: Some("t".into()),
                                error_code: metadata_error,
                                partitions: vec![MetadataResponsePartition {
                                    partition_index: 0,
                                    leader_id: 1,
                                    replica_nodes: vec![1],
                                    isr_nodes: vec![1],
                                    ..Default::default()
                                }],
                                ..Default::default()
                            })
                            .into_iter()
                            .collect(),
                        ..Default::default()
                    }
                    .encode(&mut encoded, version)
                    .unwrap();
                }
                produce_request::API_KEY => {
                    let mut body = &body[2 + "bench-producer-0".len()..];
                    let request = ProduceRequest::decode(&mut body, version).unwrap();
                    ProduceResponse {
                        responses: request
                            .topic_data
                            .into_iter()
                            .map(|topic| TopicProduceResponse {
                                name: topic.name,
                                partition_responses: topic
                                    .partition_data
                                    .into_iter()
                                    .map(|partition| {
                                        let Some(RecordsPayload::V2(batches)) = partition.records
                                        else {
                                            panic!("benchmark sends v2 record batches");
                                        };
                                        for batch in batches {
                                            observed_batch.fetch_max(
                                                batch.records.len() as u64,
                                                Ordering::SeqCst,
                                            );
                                        }
                                        PartitionProduceResponse {
                                            index: partition.index,
                                            error_code: delivery_error,
                                            base_offset: 0,
                                            ..Default::default()
                                        }
                                    })
                                    .collect(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    }
                    .encode(&mut encoded, version)
                    .unwrap();
                }
                _ => panic!("unexpected benchmark API {api}"),
            }
            Some(encoded.to_vec())
        })
        .await;
        port.store(broker.addr.port(), Ordering::SeqCst);
        (broker, receiving, largest_batch)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn producer_pipeline_counts_delivery_and_admission_outcomes() {
        for (metadata_error, delivery_error) in [(0, 0), (0, 29), (29, 0)] {
            let (broker, started, largest_batch) =
                producer_broker(metadata_error, delivery_error).await;
            let mut scenario = scenario(1);
            scenario.linger = millis(20);
            let stop = Arc::new(AtomicU8::new(STATE_MEASURING));
            let first_ack = Arc::new(AtomicU64::new(0));
            let producer = tokio::spawn(run_producer(ProducerTask {
                idx: 0,
                grid: Grid::new(Instant::now(), 0, &scenario, DEFAULT_SAMPLE_INTERVAL),
                scenario,
                bootstrap: broker.addr.to_string(),
                topic: "t".into(),
                scenario_id: 0,
                stop: Arc::clone(&stop),
                first_ack: Arc::clone(&first_ack),
                kill_at: Arc::new(AtomicU64::new(0)),
                security: None,
                request_timeout: secs(1),
                final_drain_timeout: secs(1),
                dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
                frame_max: ClientFrameMax::default(),
            }));
            tokio::time::timeout(secs(10).to_std(), started)
                .await
                .unwrap()
                .expect("producer reaches topic admission");
            // A short measurement window exercises the bounded pipeline and its
            // final drain, using the same phase signal as a benchmark run. When
            // delivery succeeds the window opens at the first ack, so a slow
            // runner still measures something.
            if metadata_error == 0 && delivery_error == 0 {
                tokio::time::timeout(secs(10).to_std(), async {
                    while first_ack.load(Ordering::SeqCst) == 0 {
                        tokio::time::sleep(millis(10).to_std()).await;
                    }
                })
                .await
                .expect("the first record is acknowledged");
            }
            tokio::time::sleep(millis(200).to_std()).await;
            stop.store(STATE_STOP, Ordering::SeqCst);
            let output = tokio::time::timeout(secs(10).to_std(), producer)
                .await
                .unwrap()
                .unwrap();
            broker.stop();

            assert!(output.interval_msgs.iter().sum::<u64>() == output.msgs);
            assert!(output.latency.len() == output.msgs);
            assert!(output.bytes == output.msgs * 100);
            if metadata_error == 0 && delivery_error == 0 {
                assert!(output.msgs > 0);
                assert!((output.dropped, output.error.as_str()) == (0, ""));
                assert!(first_ack.load(Ordering::SeqCst) > 0);
            } else {
                assert!((output.msgs, output.bytes, first_ack.load(Ordering::SeqCst)) == (0, 0, 0));
                assert!(output.dropped > 0);
                assert!(output.error == "producer-0-err: broker error_code 29");
            }
            if metadata_error == 0 {
                assert!(
                    largest_batch.load(Ordering::SeqCst) > 1,
                    "records are queued before awaiting delivery"
                );
            } else {
                assert!(
                    largest_batch.load(Ordering::SeqCst) == 0,
                    "admission failures never reach Produce"
                );
            }
        }
    }

    #[test]
    fn client_request_timeout_defaults_preserve_policy() {
        check!(default_producer_request_timeout() == secs(2));
        check!(default_consumer_request_timeout(Stack::Krabka) == secs(5));
        check!(default_consumer_request_timeout(Stack::Kafka) == secs(30));
    }

    #[test]
    fn consumer_build_retry_defaults_preserve_policy() {
        let policy = ConsumerBuildRetryPolicy::default();

        check!(
            (
                policy.attempts(),
                policy.initial_backoff(),
                policy.max_backoff()
            ) == (6, millis(100), secs(2))
        );
    }

    #[test]
    fn consumer_build_retry_accepts_positive_minimum_and_equal_backoffs() {
        let attempts = ConsumerBuildAttempts::new(1).expect("one attempt is valid");
        let policy = ConsumerBuildRetryPolicy::new(attempts, millis(1), millis(1))
            .expect("equal bounds are valid");

        check!(policy.attempts() == 1);
        check!(policy.initial_backoff() == millis(1));
        check!(policy.max_backoff() == millis(1));
    }

    #[test]
    fn consumer_build_retry_rejects_invalid_primitive_values() {
        for invalid in ["0", "not-a-number", "-1", "4294967296"] {
            check!(
                invalid.parse::<ConsumerBuildAttempts>().is_err(),
                "attempts {invalid:?} must be rejected"
            );
        }
    }

    #[test]
    fn consumer_build_retry_rejects_inverted_backoff_range() {
        let attempts = ConsumerBuildAttempts::new(1).expect("valid attempts");
        assert!(ConsumerBuildRetryPolicy::new(attempts, millis(2), millis(1)).is_err());
    }

    #[test]
    fn consumer_poll_timing_defaults_preserve_behavior() {
        check!(default_consumer_poll_timeout() == millis(50));
        check!(default_consumer_poll_error_backoff() == millis(100));
    }

    #[test]
    fn producer_final_drain_timeout_default_preserves_behavior() {
        check!(DEFAULT_PRODUCER_FINAL_DRAIN_TIMEOUT == secs(10));
    }

    #[test]
    fn sample_interval_default_preserves_behavior() {
        assert!(DEFAULT_SAMPLE_INTERVAL == secs(2));
    }

    // TLS enabled: a CA path + server_name must build a `ClientSecurity` whose
    // protocol is `Ssl`, whose TLS config carries the mounted CA as trust roots
    // and the explicit SNI, and (one-way TLS) no client identity. This is the
    // exact shape passed to `.maybe_security(...)` on both clients.
    #[test]
    fn tls_params_build_ssl_security_with_server_name() {
        let params = TlsParams {
            ca_path: "/etc/bench-ca/ca.crt".into(),
            server_name: "demo-broker-headless.default.svc.cluster.local".into(),
            client_identity: None,
        };
        let sec = params.to_security();
        assert2::assert!(sec.protocol == ListenerProtocol::Ssl);
        assert2::assert!(sec.protocol.requires_tls());
        assert2::assert!(sec.sasl.is_none());
        let tls = sec.tls.expect("Ssl security carries a TLS config");
        assert2::assert!(
            tls.server_name.as_str() == "demo-broker-headless.default.svc.cluster.local"
        );
        assert2::assert!(tls.trust_store == TrustStore::PemFile("/etc/bench-ca/ca.crt".into()));
        assert2::assert!(tls.key_store == None);
    }

    // mTLS variant: a client identity threads through to the TLS config.
    #[test]
    fn tls_params_carry_client_identity_for_mtls() {
        let params = TlsParams {
            ca_path: "/etc/bench-ca/ca.crt".into(),
            server_name: "demo-kafka-bootstrap".into(),
            client_identity: Some(("/c/tls.crt".into(), "/c/tls.key".into())),
        };
        let sec = params.to_security();
        let tls = sec.tls.expect("TLS config present");
        assert2::assert!(
            tls.key_store
                == Some(KeyStore::PemFiles {
                    certificate_chain: "/c/tls.crt".into(),
                    private_key: "/c/tls.key".into(),
                    key_password: None,
                })
        );
    }

    // Disabled: when `DriverConfig.tls` is `None`, the derived security is
    // `None` (plaintext) — the unchanged default benchmark path.
    #[test]
    fn no_tls_params_means_plaintext_security_is_none() {
        let c = cfg(1);
        assert2::assert!(c.tls.is_none());
        let security: Option<ClientSecurity> = c.tls.as_ref().map(TlsParams::to_security);
        assert2::assert!(security.is_none());
    }

    /// A record produced during warmup but polled after the window opens
    /// belongs to the warmup. The grid says so from the record's own send
    /// stamp, whatever the consumer's lag was.
    #[test]
    fn grid_places_a_record_by_its_send_stamp() {
        let mut s = scenario(1);
        s.warmup = secs(10);
        s.duration = secs(60);
        let started_ns = 1_000_000_000_000u64;
        let grid = Grid::new(Instant::now(), started_ns, &s, secs(2));

        let warmup_send = started_ns + nonnegative_i64_to_u64(secs(5).nanos_i64());
        let first_slice = started_ns + nonnegative_i64_to_u64(secs(11).nanos_i64());
        let last_slice = started_ns + nonnegative_i64_to_u64(secs(69).nanos_i64());
        let after_window = started_ns + nonnegative_i64_to_u64(secs(71).nanos_i64());

        check!(!grid.sent_in_measurement(warmup_send));
        check!(grid.sent_in_measurement(first_slice));
        check!(grid.sent_in_measurement(last_slice));
        check!(!grid.sent_in_measurement(after_window));

        // 1s into a 2s-slice grid is slice 0; 59s in is the last slice.
        check!(grid.idx_for_send_ns(first_slice) == 0);
        check!(grid.idx_for_send_ns(last_slice) == grid.n - 1);
    }

    /// A failover run whose pod delete failed leaves the shared marker at
    /// zero. No marker means no measured kill, so the run carries no
    /// disturbance and the gate treats it as missing evidence.
    #[test]
    fn finalize_timing_records_no_disturbance_without_a_kill() {
        let (disturbance, _) = finalize_timing(true, (0, 5_000, 3, millis(9)), 1_000, 0);
        check!(disturbance.is_none());
    }

    /// An ack before the marker is not a recovery. The producer only stamps a
    /// recovery at or after the marker, so a zero here means no ack followed
    /// the kill at all.
    #[test]
    fn finalize_timing_records_no_disturbance_without_an_ack_after_the_kill() {
        let (disturbance, _) = finalize_timing(true, (5_000, 0, 3, millis(9)), 1_000, 0);
        check!(disturbance.is_none());
    }

    #[test]
    fn finalize_timing_records_a_measured_kill() {
        let (disturbance, _) = finalize_timing(true, (5_000, 7_500, 3, millis(9)), 1_000, 0);
        let disturbance = disturbance.expect("a measured kill gives a disturbance");
        check!(disturbance.kill_at_ms == TimeOffsetMs(5_000));
        check!(disturbance.recovery_at_ms == TimeOffsetMs(7_500));
        check!(disturbance.dropped == MessageCount(3));
    }

    #[test]
    fn empty_output_preserves_inputs() {
        let s = scenario(1);
        let c = cfg(1);
        let out = empty_output(&s, &c, 42, vec!["a-note".into()], vec!["an-error".into()]);
        check!(out.wallclock_start_unix_ms == WallclockMs(42));
        check!(out.wallclock_end_unix_ms == WallclockMs(42));
        check!(out.topology.broker_count == 1);
        check!(out.notes == vec!["a-note".to_owned()]);
        check!(out.errors == vec!["an-error".to_owned()]);
        check!(out.first_ack == Time::ZERO);
        check!(out.disturbance.is_none());
    }

    // The state byte is shared across producer/consumer tasks; verify the
    // three values are pairwise distinct so a flat AtomicU8 can encode them
    // without ambiguity.
    #[test]
    fn state_constants_are_distinct() {
        check!(STATE_RUN != STATE_MEASURING);
        check!(STATE_MEASURING != STATE_STOP);
        check!(STATE_RUN != STATE_STOP);
    }

    #[tokio::test(start_paused = true)]
    async fn cluster_mode_rf3_with_one_broker_is_skipped() {
        let mut s = scenario(3);
        s.mode_tag = ModeTag::Cluster;
        let out = run(s, cfg(1)).await.expect("run returned");
        assert2::assert!(out.throughput.msgs_produced == 0);
        assert2::assert!(out.notes.iter().any(|n| n.contains("topology-mismatch")));
    }

    #[tokio::test(start_paused = true)]
    async fn failover_request_without_rf3_records_skip_note() {
        // Scenario asks for failover, but RF=1 + 1 broker → driver must
        // record a skip note. The scenario has no producers or consumers, so
        // the run builds no client and needs no broker. A consumer pointed at
        // the unresolvable "broker:9092" retries its startup for five minutes
        // and looks the name up again on every attempt: the paused clock makes
        // the backoff free, but each lookup takes real time, a few
        // milliseconds on one machine and far longer on another.
        let mut s = scenario(1);
        s.failover = Some(FailoverSpec {
            kill_after: secs(1),
            target: "partition0_leader".into(),
        });
        s.producers = 0;
        s.consumers = 0;
        s.warmup = Time::ZERO;
        s.duration = Time::ZERO;
        let out = run(s, cfg(1)).await.expect("run returned");
        check!(
            out.notes
                .iter()
                .any(|n| n.contains("skipped:failover-needs-rf3"))
        );
    }
}
