//! Parses Criterion benchmark output in bencher format into structured JSON
//! summaries.
//!
//! Note: If a shared `krabka-tools` repository is established across the
//! `krabka-io` organization, this crate can be migrated there as a common
//! benchmark utility for `krabka-broker`, `krabka-protocol`, and `krabka-client-rs`.

use {
    clap::Parser,
    regex::Regex,
    serde::{Deserialize, Serialize},
    std::{
        collections::BTreeMap,
        fs::{self, File},
        io::{BufRead, BufReader, Write},
        path::{Path, PathBuf},
        sync::LazyLock,
        time::SystemTime,
    },
    thiserror::Error,
};

static BENCHER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"test\s+([\w\/\-\.]+)\s+\.\.\.\s+bench:\s+([\d,]+)\s+ns\/iter\s+\(\+\/-\s+([\d,]+)\)",
    )
    .expect("static regex pattern is valid")
});

/// Errors encountered while parsing benchmark output.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ParseBenchesError {
    /// Benchmark results directory does not exist.
    #[error("directory '{0}' does not exist")]
    DirectoryNotFound(PathBuf),

    /// No `.txt` output files were found in the results directory.
    #[error("no benchmark output files (*.txt) found in '{0}'")]
    NoTxtFiles(PathBuf),

    /// Output files were found, but no benchmark metrics could be parsed.
    #[error("parsed 0 benchmark metrics from '{0}'")]
    NoMetricsParsed(PathBuf),

    /// A duplicate benchmark identifier was found.
    #[error("duplicate benchmark metric '{name}' found in '{file}'")]
    DuplicateBenchmark {
        /// Name of the duplicated benchmark identifier.
        name: String,
        /// Path of the file containing the collision.
        file: PathBuf,
    },

    /// Failed to parse a numeric value from benchmark output.
    #[error("failed to parse numeric benchmark value: {0}")]
    InvalidNumber(String),

    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Underlying JSON serialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Fewer than three repeated summaries were supplied for one side.
    #[error("{side} needs at least 3 repeated benchmark summaries, got {count}")]
    TooFewSamples {
        /// The comparison side.
        side: &'static str,
        /// Number of supplied summaries.
        count: usize,
    },

    /// Reference and candidate summaries did not contain the same benchmarks.
    #[error("reference and candidate benchmark sets differ")]
    BenchmarkSetMismatch,

    /// A sample was non-finite or non-positive without every sample being zero.
    #[error("invalid sample for benchmark '{0}'")]
    InvalidSample(String),

    /// A supplied summary had no benchmark metrics.
    #[error("benchmark summaries contain no metrics")]
    EmptyBenchmarkSet,

    /// The configured tolerance was negative, NaN, or infinite.
    #[error("minimum tolerance must be a finite non-negative ratio")]
    InvalidTolerance,
}

/// A single benchmark measurement in nanoseconds per iteration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkMetric {
    /// Mean nanoseconds per iteration.
    pub ns_per_iter: f64,
    /// Variance in nanoseconds per iteration (+/- bound).
    pub variance_ns: f64,
}

/// Aggregated benchmark summary document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    /// Benchmark suite name (e.g. `krabka-broker`).
    pub suite: String,
    /// Git commit SHA of the run.
    pub commit: String,
    /// ISO8601/RFC3339 UTC timestamp when the summary was generated.
    pub timestamp: String,
    /// Map of canonical benchmark names to metrics, sorted alphabetically.
    pub benchmarks: BTreeMap<String, BenchmarkMetric>,
}

/// One benchmark's variance-calibrated reference/candidate decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkComparison {
    /// Median reference time in nanoseconds.
    pub reference_median_ns: f64,
    /// Median candidate time in nanoseconds.
    pub candidate_median_ns: f64,
    /// Candidate/reference ratio.
    pub ratio: f64,
    /// Allowed relative slowdown, derived from repeated-run MAD with a floor.
    pub tolerance: f64,
    /// Whether this benchmark stayed within its tolerance.
    pub passed: bool,
}

/// Machine-readable verdict over repeated same-host benchmark runs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkVerdict {
    /// Exact commits represented by the reference samples.
    pub reference_commits: Vec<String>,
    /// Exact commits represented by the candidate samples.
    pub candidate_commits: Vec<String>,
    /// Per-benchmark decisions.
    pub benchmarks: BTreeMap<String, BenchmarkComparison>,
    /// True only when every benchmark passed.
    pub passed: bool,
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}

fn relative_mad(values: &[f64], center: f64) -> f64 {
    median(values.iter().map(|value| (value - center).abs()).collect()) / center
}

/// Compare at least three repeated reference and candidate summaries.
///
/// The tolerance is three times the larger side's median absolute deviation,
/// with `minimum_tolerance` as a floor. Bilateral all-zero samples are tied
/// below bencher's integer-nanosecond resolution. Other missing, non-finite, or
/// non-positive samples are errors and therefore cannot silently pass.
///
/// # Errors
///
/// Returns [`ParseBenchesError`] for too few samples, mismatched benchmark
/// sets, or invalid measurements.
pub fn compare_summaries(
    reference: &[BenchmarkSummary],
    candidate: &[BenchmarkSummary],
    minimum_tolerance: f64,
) -> Result<BenchmarkVerdict, ParseBenchesError> {
    if !minimum_tolerance.is_finite() || minimum_tolerance < 0.0 {
        return Err(ParseBenchesError::InvalidTolerance);
    }
    for (side, summaries) in [("reference", reference), ("candidate", candidate)] {
        if summaries.len() < 3 {
            return Err(ParseBenchesError::TooFewSamples {
                side,
                count: summaries.len(),
            });
        }
    }
    let reference_names: Vec<_> = reference[0].benchmarks.keys().collect();
    if reference_names.is_empty() {
        return Err(ParseBenchesError::EmptyBenchmarkSet);
    }
    if reference.iter().any(|summary| {
        summary
            .benchmarks
            .keys()
            .ne(reference_names.iter().copied())
    }) || candidate.iter().any(|summary| {
        summary
            .benchmarks
            .keys()
            .ne(reference_names.iter().copied())
    }) {
        return Err(ParseBenchesError::BenchmarkSetMismatch);
    }

    let mut benchmarks = BTreeMap::new();
    for name in reference_names {
        let reference_values: Vec<_> = reference
            .iter()
            .map(|summary| summary.benchmarks[name].ns_per_iter)
            .collect();
        let candidate_values: Vec<_> = candidate
            .iter()
            .map(|summary| summary.benchmarks[name].ns_per_iter)
            .collect();
        let mut values = reference_values.iter().chain(&candidate_values);
        if values.clone().all(|value| *value == 0.0) {
            benchmarks.insert(name.clone(), BenchmarkComparison {
                reference_median_ns: 0.0,
                candidate_median_ns: 0.0,
                ratio: 1.0,
                tolerance: minimum_tolerance,
                passed: true,
            });
            continue;
        }
        if values.any(|value| !value.is_finite() || *value <= 0.0) {
            return Err(ParseBenchesError::InvalidSample(name.clone()));
        }
        let reference_median_ns = median(reference_values.clone());
        let candidate_median_ns = median(candidate_values.clone());
        let tolerance = minimum_tolerance.max(
            3.0 * relative_mad(&reference_values, reference_median_ns)
                .max(relative_mad(&candidate_values, candidate_median_ns)),
        );
        let ratio = candidate_median_ns / reference_median_ns;
        benchmarks.insert(name.clone(), BenchmarkComparison {
            reference_median_ns,
            candidate_median_ns,
            ratio,
            tolerance,
            passed: ratio <= 1.0 + tolerance,
        });
    }
    let passed = benchmarks.values().all(|benchmark| benchmark.passed);
    Ok(BenchmarkVerdict {
        reference_commits: reference
            .iter()
            .map(|summary| summary.commit.clone())
            .collect(),
        candidate_commits: candidate
            .iter()
            .map(|summary| summary.commit.clone())
            .collect(),
        benchmarks,
        passed,
    })
}

/// Read every JSON summary in `directory`, sorted by filename.
///
/// # Errors
///
/// Returns [`ParseBenchesError`] when the directory is missing, contains fewer
/// than three summaries, or a summary is unreadable or invalid JSON.
pub fn read_summaries(
    directory: &Path,
    side: &'static str,
) -> Result<Vec<BenchmarkSummary>, ParseBenchesError> {
    if !directory.is_dir() {
        return Err(ParseBenchesError::DirectoryNotFound(
            directory.to_path_buf(),
        ));
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    paths.sort();
    if paths.len() < 3 {
        return Err(ParseBenchesError::TooFewSamples {
            side,
            count: paths.len(),
        });
    }
    paths
        .into_iter()
        .map(|path| Ok(serde_json::from_reader(File::open(path)?)?))
        .collect()
}

/// CLI configuration arguments for parsing benchmarks.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "krabka-parse-benches",
    about = "Parses Criterion benchmark output in bencher format into a structured JSON summary"
)]
pub struct Args {
    /// Directory containing benchmark `.txt` output files.
    #[arg(long, default_value = "bench-results")]
    pub results_dir: PathBuf,

    /// Output JSON summary file path (defaults to `<results_dir>/broker-benchmarks.json`).
    #[arg(long)]
    pub output: Option<PathBuf>,

    /// Suite identifier for the benchmark summary.
    #[arg(long, default_value = "krabka-broker")]
    pub suite: String,

    /// Git commit SHA (defaults to `GITHUB_SHA` env var or `"unknown"`).
    #[arg(long)]
    pub commit: Option<String>,

    /// Directory of repeated reference summary JSON files to compare.
    #[arg(long, requires = "candidate_dir")]
    pub reference_dir: Option<PathBuf>,

    /// Directory of repeated candidate summary JSON files to compare.
    #[arg(long, requires = "reference_dir")]
    pub candidate_dir: Option<PathBuf>,

    /// Minimum allowed slowdown percentage for comparison mode.
    #[arg(long, default_value_t = 3.0)]
    pub minimum_tolerance_percent: f64,
}

/// Parses a single line of Criterion bencher output.
///
/// Matches patterns of the shape:
/// `test log/append/100rec_1024B ... bench: 95,979 ns/iter (+/- 1,377,729)`
///
/// # Errors
///
/// Returns [`ParseBenchesError::InvalidNumber`] if matched numeric fields cannot
/// be parsed into floats.
pub fn parse_bencher_line(
    line: &str,
) -> Result<Option<(String, BenchmarkMetric)>, ParseBenchesError> {
    let Some(caps) = BENCHER_REGEX.captures(line) else {
        return Ok(None);
    };

    let name = caps[1].to_string();
    let ns_raw = caps[2].replace(',', "");
    let variance_raw = caps[3].replace(',', "");

    let ns_per_iter: f64 = ns_raw
        .parse()
        .map_err(|_| ParseBenchesError::InvalidNumber(caps[2].to_string()))?;
    let variance_ns: f64 = variance_raw
        .parse()
        .map_err(|_| ParseBenchesError::InvalidNumber(caps[3].to_string()))?;

    Ok(Some((name, BenchmarkMetric {
        ns_per_iter,
        variance_ns,
    })))
}

/// Parses all benchmark metrics from a `.txt` file into the target map.
///
/// # Errors
///
/// Returns an error if the file cannot be read, numeric parsing fails, or a
/// duplicate benchmark name is encountered.
pub fn parse_bencher_file(
    path: &Path,
    benchmarks: &mut BTreeMap<String, BenchmarkMetric>,
) -> Result<(), ParseBenchesError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        if let Some((name, metric)) = parse_bencher_line(&line)?
            && benchmarks.insert(name.clone(), metric).is_some()
        {
            return Err(ParseBenchesError::DuplicateBenchmark {
                name,
                file: path.to_path_buf(),
            });
        }
    }

    Ok(())
}

/// Scans the given directory for all `*.txt` files and parses their benchmark metrics.
///
/// # Errors
///
/// Returns an error if:
/// - The directory does not exist ([`ParseBenchesError::DirectoryNotFound`]).
/// - No `*.txt` files are found ([`ParseBenchesError::NoTxtFiles`]).
/// - No metrics were parsed from any found files ([`ParseBenchesError::NoMetricsParsed`]).
/// - Duplicate benchmark metrics are detected across files.
pub fn parse_benchmark_dir(
    results_dir: &Path,
) -> Result<BTreeMap<String, BenchmarkMetric>, ParseBenchesError> {
    if !results_dir.exists() || !results_dir.is_dir() {
        return Err(ParseBenchesError::DirectoryNotFound(
            results_dir.to_path_buf(),
        ));
    }

    let mut txt_files = Vec::new();
    for entry in fs::read_dir(results_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("txt") {
            txt_files.push(path);
        }
    }

    if txt_files.is_empty() {
        return Err(ParseBenchesError::NoTxtFiles(results_dir.to_path_buf()));
    }

    txt_files.sort();

    let mut benchmarks = BTreeMap::new();
    for file_path in &txt_files {
        parse_bencher_file(file_path, &mut benchmarks)?;
    }

    if benchmarks.is_empty() {
        return Err(ParseBenchesError::NoMetricsParsed(
            results_dir.to_path_buf(),
        ));
    }

    Ok(benchmarks)
}

/// Formats a [`SystemTime`] as an ISO8601 / RFC3339 UTC string (`YYYY-MM-DDTHH:MM:SSZ`).
#[must_use]
pub fn format_rfc3339_utc(time: SystemTime) -> String {
    let offset_time = time::OffsetDateTime::from(time)
        .replace_nanosecond(0)
        .unwrap_or_else(|_| time::OffsetDateTime::from(time));
    offset_time
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

/// Resolves commit SHA from an optional CLI argument, falling back to an optional environment SHA.
#[must_use]
pub fn resolve_commit_sha(commit_arg: Option<&str>, env_sha: Option<&str>) -> String {
    let raw = commit_arg
        .and_then(|s| {
            let t = s.trim();
            (!t.is_empty()).then_some(t)
        })
        .or_else(|| {
            env_sha.and_then(|s| {
                let t = s.trim();
                (!t.is_empty()).then_some(t)
            })
        });

    let Some(trimmed) = raw else {
        return "unknown".to_string();
    };

    trimmed.to_string()
}

/// Generates a [`BenchmarkSummary`] from the provided command-line arguments.
///
/// # Errors
///
/// Returns [`ParseBenchesError`] if directory scanning or metric parsing fails.
pub fn generate_summary(args: &Args) -> Result<BenchmarkSummary, ParseBenchesError> {
    let benchmarks = parse_benchmark_dir(&args.results_dir)?;
    let env_sha = std::env::var("GITHUB_SHA").ok();
    let commit = resolve_commit_sha(args.commit.as_deref(), env_sha.as_deref());
    let timestamp = format_rfc3339_utc(SystemTime::now());

    Ok(BenchmarkSummary {
        suite: args.suite.clone(),
        commit,
        timestamp,
        benchmarks,
    })
}

/// Executes benchmark parsing and writes the resulting JSON summary to disk.
///
/// # Errors
///
/// Returns [`ParseBenchesError`] if parsing, directory creation, or file write fails.
pub fn run_from_args(args: &Args) -> Result<BenchmarkSummary, ParseBenchesError> {
    let summary = generate_summary(args)?;

    let out_path = args
        .output
        .clone()
        .unwrap_or_else(|| args.results_dir.join("broker-benchmarks.json"));

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let json_bytes = serde_json::to_vec_pretty(&summary)?;
    let mut file = File::create(&out_path)?;
    file.write_all(&json_bytes)?;
    file.write_all(b"\n")?;

    Ok(summary)
}
