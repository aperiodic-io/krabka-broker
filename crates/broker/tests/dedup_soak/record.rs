//! What a run measured: the samples taken every few seconds, one row per
//! cycle, and the bounds every sample is checked against as it is taken.
//!
//! Each sample and each cycle is also printed as one comma-separated line, so
//! `--nocapture` output can be cut into a file and plotted. The first field
//! names the line's kind and the `# sample,` and `# cycle,` lines name the
//! columns.

use {
    crate::{
        cluster::SoakCluster,
        drift::Series,
        gauges::{CounterSums, DedupGauges},
        process::ProcessSample,
        settings::{BROKERS, Settings},
        workload::{LatencySink, Tally},
    },
    std::{
        collections::BTreeMap,
        fmt,
        time::{Duration, Instant},
    },
};

/// The share of a limit a gauge must reach for the run to count the limit
/// as reached: the filters start trimming a tenth below each limit.
const NEAR_LIMIT_PERCENT: u64 = 90;

/// Where in its cycle a sample was taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Steady,
    HotPartition,
    Saturated,
    Drain,
    Drained,
    Final,
}

impl fmt::Display for Phase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Steady => "steady",
            Self::HotPartition => "hot",
            Self::Saturated => "saturated",
            Self::Drain => "drain",
            Self::Drained => "drained",
            Self::Final => "final",
        })
    }
}

/// A value as a float, exactly for any value below 2^53.
pub fn real(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).expect("the high half fits");
    let low = u32::try_from(value & u64::from(u32::MAX)).expect("the low half fits");
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

fn micros(duration: Duration) -> u64 { u64::try_from(duration.as_micros()).unwrap_or(u64::MAX) }

/// The median, 99th percentile and maximum of a set of request latencies.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LatencySummary {
    pub count: usize,
    pub p50: Duration,
    pub p99: Duration,
    pub max: Duration,
}

impl LatencySummary {
    pub fn of(mut latencies: Vec<Duration>) -> Self {
        latencies.sort_unstable();
        // The nearest-rank percentile: the smallest latency at or above which
        // `percent` percent of the requests finished.
        let rank = |percent: usize| {
            let at = (latencies.len() * percent).div_ceil(100).saturating_sub(1);
            latencies.get(at).copied().unwrap_or_default()
        };
        Self {
            count: latencies.len(),
            p50: rank(50),
            p99: rank(99),
            max: latencies.last().copied().unwrap_or_default(),
        }
    }
}

impl fmt::Display for LatencySummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{},{},{},{}",
            self.count,
            micros(self.p50),
            micros(self.p99),
            micros(self.max)
        )
    }
}

/// One reading of the process and of every broker's KFC-10 series.
#[derive(Debug, Clone)]
pub struct Sample {
    pub process: ProcessSample,
    pub brokers: Vec<(u64, DedupGauges)>,
}

impl Sample {
    pub fn entries_sum(&self) -> u64 { self.brokers.iter().map(|(_, g)| g.entries_sum()).sum() }

    pub fn heap_sum(&self) -> u64 { self.brokers.iter().map(|(_, g)| g.heap_sum()).sum() }

    pub fn registry_series(&self) -> u64 {
        self.brokers.iter().map(|(_, g)| g.registry_series).sum()
    }

    /// One column for each broker, empty for a broker that is not running.
    fn per_broker(&self, value: impl Fn(&DedupGauges) -> u64) -> String {
        (1..=BROKERS)
            .map(|node| {
                self.brokers
                    .iter()
                    .find(|(id, _)| *id == node)
                    .map(|(_, gauges)| value(gauges).to_string())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// What one cycle did and where it left the process.
#[derive(Debug, Clone)]
pub struct CycleRow {
    pub cycle: usize,
    /// The churn made after the cycle's drain.
    pub churn: String,
    /// How long the indexes took to empty once the load stopped.
    pub drained_in: Duration,
    /// The process right after the drain.
    pub drained: ProcessSample,
    /// The estimated index heap right after the drain, summed over brokers.
    pub drained_heap: u64,
    /// The registry's sample lines right after the drain, summed over
    /// brokers.
    pub drained_series: u64,
    pub tally: Tally,
    pub latency: LatencySummary,
}

/// Which of the paths the soak is for the run reached at least once.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Exercised {
    pub partition_limit: bool,
    pub broker_limit: bool,
    pub counters: CounterSums,
}

/// The samples and cycles of a run, and every bound a sample broke.
pub struct Recorder {
    settings: Settings,
    started: Instant,
    next_due: Instant,
    sink: LatencySink,
    /// Latencies taken since the cycle's load began.
    cycle_latencies: Vec<Duration>,
    /// Each broker's counter sums at the previous sample.
    counters: BTreeMap<u64, CounterSums>,
    pub rss: Series,
    /// Descriptors other than segment files.
    pub other_fds: Series,
    pub segment_fds: Series,
    pub deleted_fds: Series,
    pub heap: Series,
    pub cycles: Vec<CycleRow>,
    pub violations: Vec<String>,
    pub exercised: Exercised,
}

impl Recorder {
    pub fn new(settings: &Settings, sink: LatencySink) -> Self {
        println!(
            "# sample,elapsed_ms,cycle,phase,rss_kib,peak_rss_kib,anon_kib,fds,segment_fds,\
             deleted_fds,threads,entries_b1,entries_b2,entries_b3,heap_b1,heap_b2,heap_b3,series_b1,series_b2,\
             series_b3,entries,heap,requests,p50_us,p99_us,max_us"
        );
        println!(
            "# cycle,cycle,churn,drained_in_ms,rss_kib,anon_kib,fds,segment_fds,deleted_fds,\
             threads,heap,series,requests,records,latency_requests,p50_us,p99_us,max_us"
        );
        let now = Instant::now();
        Self {
            settings: settings.clone(),
            started: now,
            next_due: now,
            sink,
            cycle_latencies: Vec::new(),
            counters: BTreeMap::new(),
            rss: Series::new("process VmRSS", "KiB"),
            other_fds: Series::new("process open fds, segment files excluded", "fds"),
            segment_fds: Series::new("process open segment files", "fds"),
            deleted_fds: Series::new("process open deleted files", "fds"),
            heap: Series::new("index heap, all brokers", "bytes"),
            cycles: Vec::new(),
            violations: Vec::new(),
            exercised: Exercised::default(),
        }
    }

    /// Sample now, whatever the schedule.
    pub async fn sample(&mut self, cluster: &SoakCluster, cycle: usize, phase: Phase) -> Sample {
        let taken = self.sink.take();
        self.cycle_latencies.extend_from_slice(&taken);
        let interval = LatencySummary::of(taken);
        let brokers = cluster.gauges().await;
        let sample = Sample {
            process: ProcessSample::read(),
            brokers,
        };
        let elapsed = self.started.elapsed();
        self.check(&sample, cycle, phase);
        self.rss.push(elapsed, real(sample.process.rss_kib));
        let fds = sample.process.fds;
        self.other_fds.push(elapsed, real(fds.other()));
        self.segment_fds.push(elapsed, real(fds.segments));
        self.deleted_fds.push(elapsed, real(fds.deleted));
        self.heap.push(elapsed, real(sample.heap_sum()));
        let process = sample.process;
        println!(
            "sample,{},{cycle},{phase},{},{},{},{},{},{},{},{},{},{},{interval}",
            elapsed.as_millis(),
            process.rss_kib,
            process.peak_rss_kib,
            process.anon_kib,
            process.fds,
            process.threads,
            sample.per_broker(DedupGauges::entries_sum),
            sample.per_broker(DedupGauges::heap_sum),
            sample.per_broker(|gauges| gauges.registry_series),
            sample.entries_sum(),
            sample.heap_sum(),
        );
        self.next_due = Instant::now() + self.settings.sample_every;
        sample
    }

    /// Sample if the schedule says one is due.
    pub async fn sample_if_due(&mut self, cluster: &SoakCluster, cycle: usize, phase: Phase) {
        if Instant::now() >= self.next_due {
            self.sample(cluster, cycle, phase).await;
        }
    }

    /// The latencies of every request since the last call, sampled or not.
    pub fn take_cycle_latency(&mut self) -> LatencySummary {
        let mut latencies = std::mem::take(&mut self.cycle_latencies);
        latencies.extend(self.sink.take());
        LatencySummary::of(latencies)
    }

    /// Note a broken bound, and print it at once so a long run shows it when
    /// it happens rather than at the end.
    pub fn violation(&mut self, what: String) {
        println!("VIOLATION {what}");
        self.violations.push(what);
    }

    pub fn push_cycle(&mut self, row: CycleRow) {
        let drained = row.drained;
        println!(
            "cycle,{},{},{},{},{},{},{},{},{},{},{},{}",
            row.cycle,
            row.churn,
            row.drained_in.as_millis(),
            drained.rss_kib,
            drained.anon_kib,
            drained.fds,
            drained.threads,
            row.drained_heap,
            row.drained_series,
            row.tally.requests,
            row.tally.records,
            row.latency,
        );
        self.cycles.push(row);
    }

    /// Check one sample against the configured limits, and note which limits
    /// it shows reached.
    fn check(&mut self, sample: &Sample, cycle: usize, phase: Phase) {
        let per_partition = self.settings.max_entries_per_partition;
        let broker_ceiling = self.settings.broker_entry_ceiling();
        let heap_ceiling = self.settings.heap_ceiling();
        let near = |limit: u64| limit * NEAR_LIMIT_PERCENT / 100;
        let mut broken = Vec::new();
        for (node, gauges) in &sample.brokers {
            let at = format!("cycle {cycle} {phase}, broker {node}");
            if let Some((partition, entries)) = gauges
                .entries
                .iter()
                .find(|(_, entries)| **entries > per_partition)
            {
                broken.push(format!(
                    "{at}: {partition:?} holds {entries} entries, over the {per_partition} limit"
                ));
            }
            if gauges.broker_entries > broker_ceiling {
                broken.push(format!(
                    "{at}: the broker holds {} entries, over {broker_ceiling}",
                    gauges.broker_entries
                ));
            }
            if gauges.broker_max_entries != self.settings.max_entries {
                broken.push(format!(
                    "{at}: the broker reports a limit of {}, not {}",
                    gauges.broker_max_entries, self.settings.max_entries
                ));
            }
            if gauges.heap_sum() > heap_ceiling {
                broken.push(format!(
                    "{at}: the indexes report {} heap bytes, over {heap_ceiling}",
                    gauges.heap_sum()
                ));
            }
            self.exercised.partition_limit |= gauges.fullest_partition() >= near(per_partition);
            self.exercised.broker_limit |= gauges.broker_entries >= near(self.settings.max_entries);
            let earlier = self
                .counters
                .insert(*node, gauges.counters)
                .unwrap_or_default();
            let added = gauges.counters.since(earlier);
            let total = &mut self.exercised.counters;
            total.dropped += added.dropped;
            total.unindexed += added.unindexed;
            total.evicted += added.evicted;
            total.rebuilds += added.rebuilds;
        }
        for what in broken {
            self.violation(what);
        }
    }
}

mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    fn ms(n: u64) -> Duration { Duration::from_millis(n) }

    #[test]
    fn the_latency_summary_takes_nearest_rank_percentiles() {
        let latencies: Vec<Duration> = (1..=200).rev().map(ms).collect();
        assert!(
            LatencySummary::of(latencies)
                == LatencySummary {
                    count: 200,
                    p50: ms(100),
                    p99: ms(198),
                    max: ms(200),
                }
        );
        assert!(LatencySummary::of(Vec::new()) == LatencySummary::default());
        assert!(LatencySummary::of(vec![ms(7)]).to_string() == "1,7000,7000,7000");
    }

    #[yare::parameterized(
        zero = { 0 },
        small = { 1_234 },
        past_u32 = { 5_000_000_000 },
        large = { (1 << 52) + 3 },
    )]
    fn a_count_converts_to_a_float_exactly(value: u64) {
        check!(real(value).to_string() == value.to_string());
    }
}
