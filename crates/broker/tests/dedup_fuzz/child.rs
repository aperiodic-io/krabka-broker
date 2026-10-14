//! Running the fuzz loop in a child process under an address-space cap.
//!
//! The parent test runs this test binary again, filtered to one test, with
//! [`CHILD_ENV`] set, under `prlimit --as`. A lying count that turns into a
//! huge allocation then aborts the child on allocation failure instead of
//! taking the test runner down, and the parent reports it as a failed test
//! with the child's output. The child reports its peak resident and virtual
//! sizes on standard output, as `DEDUP-FUZZ name=value` lines.

use std::{collections::BTreeMap, process::Command};

/// Set in the child; its value is ignored.
pub const CHILD_ENV: &str = "KRABKA_DEDUP_FUZZ_CHILD";

/// The marker of a report line.
const REPORT: &str = "DEDUP-FUZZ ";

/// Whether this process is the child.
pub fn is_child() -> bool { std::env::var_os(CHILD_ENV).is_some() }

/// Print one reported number for the parent.
pub fn report(name: &str, value: u64) {
    println!("{REPORT}{name}={value}");
}

/// A `/proc/self/status` size field, such as `VmHWM`, in KiB.
pub fn proc_status_kib(field: &str) -> u64 {
    let prefix = format!("{field}:");
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            let line = status.lines().find(|line| line.starts_with(&prefix))?;
            line[prefix.len()..].split_whitespace().next()?.parse().ok()
        })
        .unwrap_or(0)
}

/// How the child ended.
#[derive(Debug)]
pub struct ChildOutcome {
    pub success: bool,
    pub status: String,
    pub numbers: BTreeMap<String, u64>,
    pub stderr: String,
}

/// Run the test named `test` in a child capped at `address_space_bytes`.
/// Without `prlimit` on the path, the child runs uncapped and says so.
pub fn run(test: &str, address_space_bytes: u64) -> ChildOutcome {
    let exe = std::env::current_exe().expect("the test binary's path");
    let harness_args = [test, "--exact", "--nocapture", "--test-threads=1"];
    let capped = Command::new("prlimit")
        .arg(format!("--as={address_space_bytes}"))
        .arg("--")
        .arg(&exe)
        .args(harness_args)
        .env(CHILD_ENV, "1")
        .output();
    let output = capped.or_else(|error| {
        eprintln!("prlimit unavailable ({error}): the fuzz child runs without a cap");
        Command::new(&exe)
            .args(harness_args)
            .env(CHILD_ENV, "1")
            .output()
    });
    let output = output.expect("start the fuzz child");
    let numbers = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| Some(&line[line.find(REPORT)? + REPORT.len()..]))
        .filter_map(|pair| {
            let (name, value) = pair.split_once('=')?;
            Some((name.to_owned(), value.trim().parse().ok()?))
        })
        .collect();
    ChildOutcome {
        success: output.status.success(),
        status: output.status.to_string(),
        numbers,
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}
