//! Running one scenario in a child process, and measuring its memory there.
//!
//! The parent test runs this test binary again, filtered to [`entry`] alone,
//! with [`SCENARIO_ENV`] naming the scenario. [`entry`] does nothing when the
//! variable is unset, so in an ordinary run it is an empty test. In the child
//! it runs the scenario and prints one `STRESS-REPORT name=value` line for
//! each number the scenario measured. The parent reads those lines and the
//! child's exit status: an abort, an out-of-memory kill, or a panic in the
//! child is a failed scenario and nothing worse.

use {
    crate::{amplification, malformed},
    std::{collections::BTreeMap, io::ErrorKind, process::Command},
};

/// The environment variable that names the scenario a child runs.
pub const SCENARIO_ENV: &str = "KRABKA_DEDUP_STRESS_SCENARIO";

/// The numbers one scenario measured, by name.
pub type Report = Vec<(&'static str, u64)>;

/// A named scenario a child can run.
type Scenario = (&'static str, fn() -> Report);

/// Every scenario a child can run.
const SCENARIOS: &[Scenario] = &[
    ("records_count_bomb", malformed::records_count_bomb),
    ("partial_drop_dedup", amplification::partial_drop_dedup),
    ("partial_drop_plain", amplification::partial_drop_plain),
    ("header_refusal", amplification::header_refusal),
    ("idle_filters", amplification::idle_filters),
];

/// The child side: run the scenario that [`SCENARIO_ENV`] names.
#[test]
fn entry() {
    let Ok(name) = std::env::var(SCENARIO_ENV) else {
        return;
    };
    let (_, scenario) = SCENARIOS
        .iter()
        .find(|(known, _)| *known == name)
        .unwrap_or_else(|| panic!("unknown scenario {name}"));
    for (key, value) in scenario() {
        println!("STRESS-REPORT {key}={value}");
    }
}

/// What a child run left behind.
#[derive(Debug)]
pub struct ChildRun {
    /// Whether the child exited with status 0.
    pub success: bool,
    /// How the child exited, as the platform describes it.
    pub status: String,
    /// The numbers the scenario reported.
    pub report: BTreeMap<String, u64>,
    /// The end of the child's standard error, for a failure message.
    pub stderr_tail: String,
}

impl ChildRun {
    /// One reported number. Panics with the child's output when it is absent.
    pub fn get(&self, key: &str) -> u64 {
        *self
            .report
            .get(key)
            .unwrap_or_else(|| panic!("the child reported no {key}: {self:?}"))
    }
}

/// Run `scenario` in a child process, with its address space capped at
/// `address_space_bytes` when that is set and `prlimit` is installed.
pub fn run(scenario: &str, address_space_bytes: Option<u64>) -> ChildRun {
    let exe = std::env::current_exe().expect("the test binary's path");
    let filter = ["child::entry", "--exact", "--nocapture", "--test-threads=1"];
    let capped = address_space_bytes.map(|bytes| {
        let mut command = Command::new("prlimit");
        command
            .arg(format!("--as={bytes}"))
            .arg("--")
            .arg(&exe)
            .args(filter);
        command
    });
    let output = capped
        .and_then(
            |mut command| match command.env(SCENARIO_ENV, scenario).output() {
                Err(error) if error.kind() == ErrorKind::NotFound => None,
                other => Some(other),
            },
        )
        .unwrap_or_else(|| {
            Command::new(&exe)
                .args(filter)
                .env(SCENARIO_ENV, scenario)
                .output()
        })
        .expect("run the child");
    let stdout = String::from_utf8_lossy(&output.stdout);
    // The test harness prints "test child::entry ... " without a trailing
    // newline under --nocapture, so the first report line does not start the
    // line. Find the marker anywhere in the line.
    let report = stdout
        .lines()
        .filter_map(|line| line.split_once("STRESS-REPORT "))
        .filter_map(|(_, pair)| pair.split_once('='))
        .filter_map(|(key, value)| Some((key.to_owned(), value.trim().parse().ok()?)))
        .collect();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let tail_start = stderr.len().saturating_sub(4_000);
    let tail_start = (tail_start..stderr.len())
        .find(|&at| stderr.is_char_boundary(at))
        .unwrap_or(stderr.len());
    ChildRun {
        success: output.status.success(),
        status: output.status.to_string(),
        report,
        stderr_tail: stderr[tail_start..].to_owned(),
    }
}

/// Run `future` to completion on a fresh multi-thread runtime, as a broker
/// test does.
pub fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("a tokio runtime")
        .block_on(future)
}

/// A field of `/proc/self/status` in KiB, such as `VmRSS` or `VmHWM`.
pub fn status_kib(field: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    status
        .lines()
        .filter_map(|line| line.strip_prefix(field))
        .filter_map(|rest| rest.strip_prefix(':'))
        .find_map(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap_or_else(|| panic!("no {field} in /proc/self/status"))
}

/// Reset this process's peak resident size (`VmHWM`) to its current size, so
/// that the next read of it measures only what follows.
pub fn reset_peak() { std::fs::write("/proc/self/clear_refs", "5").expect("reset VmHWM"); }

/// A measurement of peak resident growth from the moment it starts.
pub struct Peak {
    rss_kib: u64,
}

impl Peak {
    /// Note the current resident size and reset the peak to it.
    pub fn start() -> Self {
        let rss_kib = status_kib("VmRSS");
        reset_peak();
        Self { rss_kib }
    }

    /// How far, in KiB, the peak resident size rose above the start.
    pub fn growth_kib(&self) -> u64 { status_kib("VmHWM").saturating_sub(self.rss_kib) }
}
