//! Load driver and report aggregator for the Krabka vs Strimzi benchmark
//! harness on Kubernetes.
//!
//! Scenarios describe a target Kafka stack, a workload shape, and optional
//! disturbance windows. The driver applies the scenario to Kubernetes, runs
//! the producer/consumer load, samples Prometheus, and writes a `RunOutput`
//! JSON artifact. The report binary reads those artifacts back and renders
//! the side-by-side Markdown summary used in benchmark reports.
//!
//! The crate ships two binaries:
//!
//! - `krabka-bench-driver` — runs one scenario against one Kafka stack,
//!   either Krabka or Strimzi/Kafka. It captures throughput, latency, and
//!   disturbance data, queries Prometheus for resource usage, and writes a
//!   single `RunOutput` JSON file.
//! - `krabka-bench-report` — walks a directory of `RunOutput` files,
//!   groups them by scenario name, and writes a side-by-side Markdown
//!   summary.
//!
//! ## Dimensioned values
//!
//! Sizes, durations, and rates are [`krabka_units`] quantities throughout, and
//! not bare numbers. A scenario's `msg_size` is a `ByteSize`, its `linger` and
//! `duration` are a `Time`, and a paced run's `rate` is a `Frequency`. The
//! operator writes them with units (`512B`, `5ms`, `20000/s`), and the measured
//! `RunOutput` encodes them as exact integers. See [`scenario`] for the
//! encoding of each field, and the [code style guide] for the vocabulary.
//!
//! [code style guide]: https://github.com/krabka-io/krabka-broker/blob/main/docs/style_guides/code_style_guide.md
//!
//! ## Command-line workflow
//!
//! ```text
//! krabka-bench-driver \
//!   --scenario bench/scenarios/small-msg-saturate.yaml \
//!   --stack krabka \
//!   --namespace kafka-bench \
//!   --out runs/krabka-steady.json
//!
//! krabka-bench-report --input runs --out report.md
//! ```
//!
//! ## Programmatic report aggregation
//!
//! ```no_run
//! use {krabka_bench_driver::report, std::path::Path};
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let markdown = report::render_markdown(Path::new("runs"), true)?;
//! std::fs::write("report.md", markdown)?;
//! # Ok(())
//! # }
//! ```

pub mod aggregate;
pub mod failover;
pub mod graph;
pub mod hist;
pub mod ids;
pub mod local_cluster;
mod numeric;
pub mod payload;
pub mod prom;
pub mod rate;
pub mod raw_load;
pub mod report;
pub mod scenario;
pub mod workload;
