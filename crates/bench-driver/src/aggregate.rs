//! Cross-run aggregation, the "accurate averages" layer.
//!
//! A benchmark *cell* is one [`CellKey`]: a scenario name with the whole
//! topology it ran at. The harness runs
//! each cell `N` times per stack. This module reduces those `N` [`RunOutput`]s
//! into a mean ± sample-stddev per metric, and averages the per-interval time
//! series across runs at each time offset. Both the Markdown summary and the
//! Plotly graphs read these aggregates, so one place computes "the average" and
//! no renderer re-derives it.

use std::collections::BTreeMap;

use krabka_units::prelude::*;

use crate::{
    ids::TimeOffsetMs,
    numeric::{mebibytes_f64, millis_f64, to_f64},
    scenario::{BrokerSample, RunOutput, Sample, Stack},
};

/// Mean and spread of a single scalar metric over the runs of one
/// `(cell, stack)`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Stat {
    pub mean: f64,
    /// Bessel-corrected sample standard deviation. This is `0.0` for fewer than
    /// two runs, because a single run has no measurable spread.
    pub stddev: f64,
    pub n: usize,
}

impl Stat {
    /// Coefficient of variation as a percentage (`stddev ÷ mean × 100`). This is
    /// `0` when the mean is `0`, so an absent or zero metric renders cleanly.
    #[must_use]
    pub fn cv_percent(&self) -> f64 {
        if self.mean == 0.0 {
            0.0
        } else {
            (self.stddev / self.mean).abs() * 100.0
        }
    }
}

/// Mean, sample stddev, and count over a sample of run-level values.
#[must_use]
pub fn stat(values: &[f64]) -> Stat {
    let n = values.len();
    if n == 0 {
        return Stat::default();
    }
    let mean = values.iter().sum::<f64>() / to_f64(n);
    let stddev = if n < 2 {
        0.0
    } else {
        let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (to_f64(n) - 1.0);
        var.sqrt()
    };
    Stat { mean, stddev, n }
}

/// A headline scalar metric. It holds its key, a way to pull it from a run, and
/// which direction counts as "better", so a renderer can colour and ratio it
/// correctly.
///
/// `extract` returns a plain number in the metric's stated `unit`, because the
/// statistics below and the Plotly axes above are dimension-agnostic. A mean and
/// a stddev run over whatever unit the axis carries. The quantity-to-number seam
/// is in each metric's `extract`.
#[derive(Clone, Copy)]
pub struct ScalarMetric {
    pub key: &'static str,
    pub label: &'static str,
    pub unit: &'static str,
    pub higher_better: bool,
    extract: fn(&RunOutput) -> f64,
}

impl ScalarMetric {
    #[must_use]
    pub fn value(&self, r: &RunOutput) -> f64 {
        (self.extract)(r)
    }
}

/// The headline scalar metrics rendered as krabka-vs-kafka bars.
#[must_use]
pub fn scalar_metrics() -> Vec<ScalarMetric> {
    vec![
        ScalarMetric {
            key: "producer_msgs_per_sec",
            label: "Producer throughput",
            unit: "msgs/s",
            higher_better: true,
            extract: |r| r.throughput.producer_rate.per_sec_f64(),
        },
        ScalarMetric {
            key: "consumer_msgs_per_sec",
            label: "Consumer throughput",
            unit: "msgs/s",
            higher_better: true,
            extract: |r| r.throughput.consumer_rate.per_sec_f64(),
        },
        ScalarMetric {
            key: "producer_p99_ms",
            label: "Producer ack p99",
            unit: "ms",
            higher_better: false,
            extract: |r| millis_f64(r.producer_latency.p99),
        },
        ScalarMetric {
            key: "consumer_e2e_p99_ms",
            label: "Consumer e2e p99",
            unit: "ms",
            higher_better: false,
            extract: |r| millis_f64(r.consumer_e2e_latency.p99),
        },
        ScalarMetric {
            key: "msgs_per_cpu_core",
            label: "Efficiency",
            unit: "msgs/s per CPU-core",
            higher_better: true,
            extract: |r| r.resource.msgs_per_cpu_second.per_sec_f64(),
        },
        ScalarMetric {
            key: "mem_working_set_mb",
            label: "Broker working set",
            unit: "MiB",
            higher_better: false,
            extract: |r| mebibytes_f64(r.resource.mem_cgroup_working_set),
        },
    ]
}

/// Per-metric aggregates for one stack within a cell.
#[derive(Debug, Clone, Default)]
pub struct StackAgg {
    pub n_runs: usize,
    /// A map from metric key to the [`Stat`] across this stack's runs in the
    /// cell.
    pub metrics: BTreeMap<&'static str, Stat>,
}

/// What makes two runs comparable: one scenario at one topology.
///
/// The key carries the whole topology and not only the broker count. A
/// scenario rerun after a partition-count or replication-factor change leaves
/// both sets of JSON in `bench/results`, and averaging those two together
/// reports a number that no single experiment produced. Every grouping in the
/// report, the graphs and the CSVs keys on this, so a cell holds one
/// experiment.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CellKey {
    pub scenario: String,
    pub broker_count: u32,
    pub partitions: i32,
    pub replication_factor: i16,
}

impl CellKey {
    /// The cell one run belongs to.
    #[must_use]
    pub fn of(run: &RunOutput) -> Self {
        Self {
            scenario: run.scenario.name.clone(),
            broker_count: run.topology.broker_count,
            partitions: run.topology.partitions,
            replication_factor: run.topology.replication_factor,
        }
    }

    /// The heading a report, a chart or a violation message names the cell by.
    #[must_use]
    pub fn label(&self) -> String {
        format!(
            "{} @ {} broker(s), {} partitions, RF={}",
            self.scenario, self.broker_count, self.partitions, self.replication_factor
        )
    }
}

/// One benchmark cell. It holds a `(scenario, topology)` with the krabka and
/// kafka aggregates side by side.
#[derive(Debug, Clone)]
pub struct CellAgg {
    pub key: CellKey,
    pub krabka: StackAgg,
    pub kafka: StackAgg,
}

fn runs_by_cell(runs: &[RunOutput]) -> BTreeMap<CellKey, Vec<&RunOutput>> {
    let mut cells: BTreeMap<CellKey, Vec<&RunOutput>> = BTreeMap::new();
    for run in runs {
        cells.entry(CellKey::of(run)).or_default().push(run);
    }
    cells
}

/// Groups runs by [`CellKey`] and reduces each stack's runs to a per-metric
/// [`Stat`]. This returns the cells in key order.
#[must_use]
pub fn aggregate_cells(runs: &[RunOutput]) -> Vec<CellAgg> {
    let metrics = scalar_metrics();
    let by_cell = runs_by_cell(runs);

    let mut out = Vec::with_capacity(by_cell.len());
    for (key, cell_runs) in by_cell {
        let agg_stack = |stack: Stack| -> StackAgg {
            let stack_runs: Vec<&RunOutput> = cell_runs
                .iter()
                .copied()
                .filter(|r| r.stack == stack)
                .collect();
            let mut by_metric = BTreeMap::new();
            for m in &metrics {
                let vals: Vec<f64> = stack_runs.iter().map(|r| m.value(r)).collect();
                by_metric.insert(m.key, stat(&vals));
            }
            StackAgg {
                n_runs: stack_runs.len(),
                metrics: by_metric,
            }
        };
        out.push(CellAgg {
            key,
            krabka: agg_stack(Stack::Krabka),
            kafka: agg_stack(Stack::Kafka),
        });
    }
    out
}

/// One averaged point of a time series. It holds the across-run mean of a metric
/// at a fixed offset into the run, and how many runs gave that offset.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TsPoint {
    pub t_offset_ms: TimeOffsetMs,
    pub mean: f64,
    pub n: usize,
}

/// An across-run-averaged time series for one `(cell, stack, metric)`.
#[derive(Debug, Clone)]
pub struct TsSeries {
    pub key: CellKey,
    pub stack: Stack,
    pub metric: &'static str,
    pub points: Vec<TsPoint>,
}

type SampleExtract = fn(&Sample) -> f64;
type BrokerExtract = fn(&BrokerSample) -> f64;

fn client_ts_metrics() -> Vec<(&'static str, SampleExtract)> {
    vec![
        ("producer_msgs_per_sec", |s| s.producer_rate.per_sec_f64()),
        ("consumer_msgs_per_sec", |s| s.consumer_rate.per_sec_f64()),
        ("producer_p99_ms", |s| millis_f64(s.producer_p99)),
        ("consumer_e2e_p99_ms", |s| millis_f64(s.consumer_e2e_p99)),
    ]
}

fn broker_ts_metrics() -> Vec<(&'static str, BrokerExtract)> {
    vec![
        ("broker_cpu_cores", |b| b.cpu_cores),
        ("broker_mem_working_set_mb", |b| {
            mebibytes_f64(b.mem_working_set)
        }),
    ]
}

/// For each `(cell, stack, metric)`, averages the per-interval samples across
/// all runs at each shared time offset. A ragged run that ended early gives
/// fewer points to the tail offsets. `TsPoint::n` records how many runs backed
/// each averaged point.
///
/// The cell is a [`CellKey`], so two runs of one scenario at different
/// partition counts stay in separate series.
#[must_use]
pub fn averaged_timeseries(runs: &[RunOutput]) -> Vec<TsSeries> {
    let client_metrics = client_ts_metrics();
    let broker_metrics = broker_ts_metrics();

    let by_cell = runs_by_cell(runs);

    let mut out = Vec::new();
    for (key, cell_runs) in by_cell {
        for stack in [Stack::Krabka, Stack::Kafka] {
            let stack_runs: Vec<&RunOutput> = cell_runs
                .iter()
                .copied()
                .filter(|r| r.stack == stack)
                .collect();
            if stack_runs.is_empty() {
                continue;
            }

            for &(metric, extract) in &client_metrics {
                let mut buckets: BTreeMap<TimeOffsetMs, Vec<f64>> = BTreeMap::new();
                for r in &stack_runs {
                    for s in &r.samples {
                        buckets.entry(s.t_offset_ms).or_default().push(extract(s));
                    }
                }
                if let Some(series) = series_from_buckets(&key, stack, metric, buckets) {
                    out.push(series);
                }
            }

            for &(metric, extract) in &broker_metrics {
                let mut buckets: BTreeMap<TimeOffsetMs, Vec<f64>> = BTreeMap::new();
                for r in &stack_runs {
                    for b in &r.broker_samples {
                        buckets.entry(b.t_offset_ms).or_default().push(extract(b));
                    }
                }
                if let Some(series) = series_from_buckets(&key, stack, metric, buckets) {
                    out.push(series);
                }
            }
        }
    }
    out
}

/// Averages each time-offset bucket across runs into a [`TsSeries`]. Returns
/// `None` when no run in the group produced any sample for this metric.
fn series_from_buckets(
    key: &CellKey,
    stack: Stack,
    metric: &'static str,
    buckets: BTreeMap<TimeOffsetMs, Vec<f64>>,
) -> Option<TsSeries> {
    if buckets.is_empty() {
        return None;
    }
    let points = buckets
        .into_iter()
        .map(|(t_offset_ms, vals)| {
            let s = stat(&vals);
            TsPoint {
                t_offset_ms,
                mean: s.mean,
                n: s.n,
            }
        })
        .collect();
    Some(TsSeries {
        key: key.clone(),
        stack,
        metric,
        points,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::{
        Topology,
        fixture::{RunSetup, empty_run as run},
    };

    fn sample(t: TimeOffsetMs, producer_rate: Frequency) -> Sample {
        Sample {
            t_offset_ms: t,
            producer_rate,
            ..Sample::default()
        }
    }

    fn producer_series(series: &[TsSeries], stack: Stack) -> &TsSeries {
        series
            .iter()
            .find(|s| s.metric == "producer_msgs_per_sec" && s.stack == stack)
            .expect("producer series present")
    }

    #[test]
    fn stat_computes_mean_and_sample_stddev() {
        let s = stat(&[2.0, 4.0, 6.0]);
        assert2::assert!(s.n == 3);
        assert2::assert!((s.mean - 4.0).abs() < f64::EPSILON);
        // sample variance = ((−2)²+0²+2²)/(3−1) = 8/2 = 4 → stddev 2.
        assert2::assert!((s.stddev - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn stat_single_and_empty() {
        assert2::assert!(
            stat(&[5.0])
                == Stat {
                    mean: 5.0,
                    stddev: 0.0,
                    n: 1
                }
        );
        assert2::assert!(stat(&[]) == Stat::default());
    }

    #[test]
    fn cv_percent_is_zero_when_mean_zero() {
        assert2::assert!(stat(&[0.0, 0.0]).cv_percent().abs() < f64::EPSILON);
        let s = stat(&[100.0, 200.0, 300.0]); // mean 200, stddev 100 → cv 50%
        assert2::assert!((s.cv_percent() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aggregate_cells_groups_and_averages_each_stack() {
        let runs = vec![
            run(RunSetup {
                scenario: "sat",
                producer_rate: per_sec(100),
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "sat",
                producer_rate: per_sec(200),
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "sat",
                producer_rate: per_sec(300),
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "sat",
                stack: Stack::Kafka,
                producer_rate: per_sec(50),
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "sat",
                stack: Stack::Kafka,
                producer_rate: per_sec(50),
                ..Default::default()
            }),
        ];
        let cells = aggregate_cells(&runs);
        assert2::assert!(cells.len() == 1);
        let c = &cells[0];
        assert2::assert!(
            c.key
                == CellKey {
                    scenario: "sat".into(),
                    broker_count: 6,
                    partitions: 100,
                    replication_factor: 3,
                }
        );
        assert2::assert!(c.krabka.n_runs == 3);
        assert2::assert!(c.kafka.n_runs == 2);
        let cm = c.krabka.metrics["producer_msgs_per_sec"];
        assert2::assert!((cm.mean - 200.0).abs() < f64::EPSILON);
        assert2::assert!((cm.stddev - 100.0).abs() < f64::EPSILON);
        let km = c.kafka.metrics["producer_msgs_per_sec"];
        assert2::assert!((km.mean - 50.0).abs() < f64::EPSILON);
        assert2::assert!(km.stddev.abs() < f64::EPSILON);
        assert2::assert!(km.n == 2);
    }

    #[test]
    fn aggregate_cells_separates_topologies() {
        let runs = vec![
            run(RunSetup {
                scenario: "sat",
                topology: Topology {
                    broker_count: 3,
                    ..RunSetup::default().topology
                },
                producer_rate: per_sec(10),
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "sat",
                producer_rate: per_sec(20),
                ..Default::default()
            }),
        ];
        let cells = aggregate_cells(&runs);
        assert2::assert!(cells.len() == 2);
        assert2::assert!(cells[0].key.broker_count == 3);
        assert2::assert!(cells[1].key.broker_count == 6);
    }

    /// A rerun after a partition-count or replication-factor change leaves both
    /// sets of JSON in the results directory. The two are different
    /// experiments, so they must not average into one cell.
    #[test]
    fn aggregate_cells_separates_partition_and_replication_changes() {
        let topology = |partitions, replication_factor| Topology {
            partitions,
            replication_factor,
            broker_count: 6,
        };
        let runs = vec![
            run(RunSetup {
                scenario: "sat",
                topology: topology(12, 3),
                producer_rate: per_sec(10),
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "sat",
                topology: topology(100, 3),
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "sat",
                topology: topology(100, 1),
                producer_rate: per_sec(5000),
                ..Default::default()
            }),
        ];
        let cells = aggregate_cells(&runs);
        assert2::assert!(cells.len() == 3);
        let means: Vec<f64> = cells
            .iter()
            .map(|c| c.krabka.metrics["producer_msgs_per_sec"].mean)
            .collect();
        // Each cell holds exactly its own run, so no mean is the average of
        // two topologies.
        assert2::assert!(cells.iter().all(|c| c.krabka.n_runs == 1));
        assert2::assert!(means.contains(&10.0));
        assert2::assert!(means.contains(&1000.0));
        assert2::assert!(means.contains(&5000.0));
    }

    /// `averaged_timeseries` keys on the same cell, so two partition counts
    /// give two series and neither point is the mean of both.
    #[test]
    fn averaged_timeseries_separates_partition_changes() {
        let topology = |partitions| Topology {
            partitions,
            replication_factor: 3,
            broker_count: 6,
        };
        let runs = vec![
            run(RunSetup {
                scenario: "sat",
                topology: topology(12),
                producer_rate: Frequency::ZERO,
                samples: vec![sample(TimeOffsetMs(0), per_sec(100))],
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "sat",
                topology: topology(100),
                producer_rate: Frequency::ZERO,
                samples: vec![sample(TimeOffsetMs(0), per_sec(900))],
                ..Default::default()
            }),
        ];
        let series = averaged_timeseries(&runs);
        let producer: Vec<&TsSeries> = series
            .iter()
            .filter(|s| s.metric == "producer_msgs_per_sec")
            .collect();
        assert2::assert!(producer.len() == 2);
        let mut means: Vec<f64> = producer.iter().map(|s| s.points[0].mean).collect();
        means.sort_by(f64::total_cmp);
        assert2::assert!((means[0] - 100.0).abs() < f64::EPSILON);
        assert2::assert!((means[1] - 900.0).abs() < f64::EPSILON);
        assert2::assert!(producer.iter().all(|s| s.points[0].n == 1));
    }

    #[test]
    fn averaged_timeseries_averages_across_runs_per_offset() {
        let runs = vec![
            run(RunSetup {
                scenario: "sat",
                producer_rate: Frequency::ZERO,
                samples: vec![
                    sample(TimeOffsetMs(0), per_sec(1000)),
                    sample(TimeOffsetMs(2000), per_sec(2000)),
                ],
                broker_samples: vec![BrokerSample {
                    t_offset_ms: TimeOffsetMs(0),
                    cpu_cores: 2.0,
                    mem_working_set: mebibytes(1),
                }],
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "sat",
                producer_rate: Frequency::ZERO,
                samples: vec![
                    sample(TimeOffsetMs(0), per_sec(3000)),
                    sample(TimeOffsetMs(2000), per_sec(4000)),
                ],
                broker_samples: vec![BrokerSample {
                    t_offset_ms: TimeOffsetMs(0),
                    cpu_cores: 4.0,
                    mem_working_set: mebibytes(3),
                }],
                ..Default::default()
            }),
        ];
        let series = averaged_timeseries(&runs);
        let prod = producer_series(&series, Stack::Krabka);
        assert2::assert!(
            prod.points
                == vec![
                    TsPoint {
                        t_offset_ms: TimeOffsetMs(0),
                        mean: 2000.0,
                        n: 2
                    },
                    TsPoint {
                        t_offset_ms: TimeOffsetMs(2000),
                        mean: 3000.0,
                        n: 2
                    },
                ]
        );
        let cpu = series
            .iter()
            .find(|s| s.metric == "broker_cpu_cores")
            .expect("broker cpu series present");
        assert2::assert!(
            cpu.points[0]
                == TsPoint {
                    t_offset_ms: TimeOffsetMs(0),
                    mean: 3.0,
                    n: 2
                }
        );
        // mem in MiB: (1 + 3) / 2 = 2.0
        let mem = series
            .iter()
            .find(|s| s.metric == "broker_mem_working_set_mb")
            .expect("broker mem series present");
        assert2::assert!((mem.points[0].mean - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn averaged_timeseries_handles_ragged_runs() {
        let runs = vec![
            run(RunSetup {
                scenario: "sat",
                stack: Stack::Kafka,
                producer_rate: Frequency::ZERO,
                samples: vec![
                    sample(TimeOffsetMs(0), per_sec(10)),
                    sample(TimeOffsetMs(2000), per_sec(20)),
                ],
                ..Default::default()
            }),
            run(RunSetup {
                scenario: "sat",
                stack: Stack::Kafka,
                producer_rate: Frequency::ZERO,
                samples: vec![sample(TimeOffsetMs(0), per_sec(30))],
                ..Default::default()
            }),
        ];
        let series = averaged_timeseries(&runs);
        let prod = producer_series(&series, Stack::Kafka);
        assert2::assert!(
            prod.points[0]
                == TsPoint {
                    t_offset_ms: TimeOffsetMs(0),
                    mean: 20.0,
                    n: 2
                }
        );
        // Only one run reached t=2000.
        assert2::assert!(
            prod.points[1]
                == TsPoint {
                    t_offset_ms: TimeOffsetMs(2000),
                    mean: 20.0,
                    n: 1
                }
        );
    }
}
