//! The memory bound, sampled while the run goes on: every few hundred
//! milliseconds, every running broker's `/metrics` body is read for the
//! per-partition and broker-wide KFC-10 entry gauges.

use {
    crate::topics::{BROKER_LIMIT, PER_PARTITION_LIMIT},
    krabka_broker::metrics::BrokerMetrics,
    std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex, PoisonError,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
    tokio::task::JoinHandle,
};

const PARTITION_GAUGE: &str = "krabka_broker_dedup_index_entries{";
const BROKER_GAUGE: &str = "krabka_broker_dedup_broker_index_entries ";
const BROKER_LIMIT_GAUGE: &str = "krabka_broker_dedup_broker_index_max_entries ";

/// How often the sampler reads the gauges.
const PERIOD: Duration = Duration::from_millis(250);

/// The metrics of each running broker, by node id. The cluster updates it
/// as brokers stop and start.
#[derive(Clone, Default)]
pub struct Meters(Arc<Mutex<BTreeMap<u64, BrokerMetrics>>>);

impl Meters {
    pub fn set(&self, node: u64, metrics: Option<BrokerMetrics>) {
        let mut meters = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        match metrics {
            Some(metrics) => meters.insert(node, metrics),
            None => meters.remove(&node),
        };
    }

    fn snapshot(&self) -> Vec<(u64, BrokerMetrics)> {
        let meters = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        meters
            .iter()
            .map(|(node, metrics)| (*node, metrics.clone()))
            .collect()
    }
}

/// What the sampler saw.
#[derive(Debug, Default)]
pub struct GaugeReport {
    pub samples: u64,
    /// The highest per-partition gauge, and the series and broker it came
    /// from.
    pub max_partition: (u64, String),
    pub max_broker: (u64, u64),
    /// Every sample above its limit, and every broker that reported a
    /// broker-wide limit other than the configured one.
    pub violations: Vec<String>,
}

/// A running sampler.
pub struct Sampler {
    stop: Arc<AtomicBool>,
    task: JoinHandle<GaugeReport>,
}

impl Sampler {
    pub fn start(meters: Meters) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let halt = Arc::clone(&stop);
        let task = tokio::spawn(async move {
            let mut report = GaugeReport::default();
            while !halt.load(Ordering::Acquire) {
                for (node, metrics) in meters.snapshot() {
                    let mut rendered = String::new();
                    {
                        let registry = metrics.registry.lock().await;
                        prometheus_client::encoding::text::encode(&mut rendered, &registry)
                            .expect("encode the registry");
                    }
                    report.read(node, &rendered);
                }
                report.samples += 1;
                tokio::time::sleep(PERIOD).await;
            }
            report
        });
        Self { stop, task }
    }

    pub async fn finish(self) -> GaugeReport {
        self.stop.store(true, Ordering::Release);
        self.task.await.expect("the gauge sampler")
    }
}

impl GaugeReport {
    /// Fold one `/metrics` body of broker `node` into the report.
    fn read(&mut self, node: u64, rendered: &str) {
        for line in rendered.lines() {
            let Some((series, value)) = line.rsplit_once(' ') else {
                continue;
            };
            let Ok(value) = value.parse::<f64>() else {
                continue;
            };
            // Every KFC-10 gauge is a whole number of entries.
            let entries = value.to_string().parse::<u64>().unwrap_or(u64::MAX);
            let line = format!("{series} ");
            if let Some(labels) = line.strip_prefix(PARTITION_GAUGE) {
                if entries > self.max_partition.0 {
                    self.max_partition =
                        (entries, format!("broker {node} {{{}", labels.trim_end()));
                }
                if entries > PER_PARTITION_LIMIT {
                    self.violations.push(format!(
                        "broker {node}: {series} = {entries} > {PER_PARTITION_LIMIT}"
                    ));
                }
            } else if line == BROKER_GAUGE {
                if entries > self.max_broker.0 {
                    self.max_broker = (entries, node);
                }
                if entries > BROKER_LIMIT {
                    self.violations.push(format!(
                        "broker {node}: {series} = {entries} > {BROKER_LIMIT}"
                    ));
                }
            } else if line == BROKER_LIMIT_GAUGE && entries != BROKER_LIMIT {
                self.violations.push(format!(
                    "broker {node}: {series} = {entries}, configured {BROKER_LIMIT}"
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// A `/metrics` body with one partition gauge of topic `a`, the
    /// broker-wide gauge and the broker-wide limit.
    fn body(partition: u64, broker: u64, limit: u64) -> String {
        format!(
            "# TYPE krabka_broker_dedup_index_entries gauge\n\
             {PARTITION_GAUGE}topic=\"a\",partition=\"0\"}} {partition}\n\
             {PARTITION_GAUGE}topic=\"b\",partition=\"0\"}} 7\n\
             {BROKER_GAUGE}{broker}\n\
             {BROKER_LIMIT_GAUGE}{limit}\n"
        )
    }

    #[test]
    fn samples_above_a_limit_are_violations() {
        let mut report = GaugeReport::default();
        report.read(2, &body(PER_PARTITION_LIMIT, BROKER_LIMIT, BROKER_LIMIT));
        assert!(report.violations.is_empty());
        assert!(
            report.max_partition
                == (
                    PER_PARTITION_LIMIT,
                    "broker 2 {topic=\"a\",partition=\"0\"}".to_owned()
                )
        );
        assert!(report.max_broker == (BROKER_LIMIT, 2));
        report.read(3, &body(PER_PARTITION_LIMIT + 1, BROKER_LIMIT + 1, 1));
        assert!(report.violations.len() == 3);
    }
}
