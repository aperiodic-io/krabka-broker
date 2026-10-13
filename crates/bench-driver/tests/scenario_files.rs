//! Every checked-in benchmark scenario loads through the driver's own loader.
//!
//! `bench/scenarios/*.yaml` is an operator-written corpus that lives outside
//! this crate, so nothing linked it to the `Scenario` type. When the dimensioned
//! fields grew units, all twelve files stopped parsing and no test noticed.
//! This harness closes that gap. It runs each file through the same `serde_saphyr`
//! path that `main.rs` uses, with one nextest process per scenario, so a broken
//! file names itself.
//!
//! The harness is `libtest-mimic` directly rather than `datatest-stable`, which
//! wraps it. `datatest-stable` walks the corpus with `walkdir` and keeps only
//! entries whose `file_type()` is a file. It does not follow links, and Bazel
//! stages every runfile as a symbolic link, so under Bazel it saw no scenarios
//! at all:
//!
//! ```text
//! no test cases found for test 'scenario_file' -- scanned directory:
//!   `.../scenario_files_test.runfiles/_main/bench/scenarios`
//! ```
//!
//! `scenario_paths` lists the directory itself and asks `Path::is_file`, which
//! follows the link, so both runners see the same twelve files.

use {
    assert2::{assert, check},
    krabka_bench_driver::scenario::{LoadMode, Scenario},
    krabka_units::prelude::*,
    std::{
        path::{Path, PathBuf},
        process::ExitCode,
    },
};

/// Where the scenario corpus is, under whichever runner started this test.
///
/// Bazel is asked first, and that order matters. Both runners export
/// `CARGO_MANIFEST_DIR`, so reading it first answers for Bazel too, and under
/// Bazel it holds the package-relative `crates/bench-driver` rather than an
/// absolute path. Walking `..` out of that depends on the working directory and
/// on whether the runfiles entry is a symbolic link.
///
/// `TEST_SRCDIR` is set by Bazel alone and is absolute. Joined with
/// `TEST_WORKSPACE` it is the runfiles root, and Bazel stages a target's `data`
/// under it by package path, so `//bench:scenarios` is exactly
/// `bench/scenarios` there. No `..`, and no dependence on the working
/// directory.
///
/// Cargo sets no `TEST_SRCDIR`, and it runs a test with the crate directory as
/// the working directory, so the relative form is right there.
///
/// # Panics
///
/// Panics when neither Bazel's pair nor Cargo's variable is set, which means
/// the test was launched by something that stages data differently again.
fn scenario_root() -> PathBuf {
    if let Ok(srcdir) = std::env::var("TEST_SRCDIR") {
        let workspace =
            std::env::var("TEST_WORKSPACE").expect("TEST_WORKSPACE accompanies TEST_SRCDIR");
        return [srcdir.as_str(), workspace.as_str(), "bench", "scenarios"]
            .iter()
            .collect();
    }
    let manifest = std::env::var("CARGO_MANIFEST_DIR")
        .expect("TEST_SRCDIR (bazel) or CARGO_MANIFEST_DIR (cargo) must be set");
    [manifest.as_str(), "..", "..", "bench", "scenarios"]
        .iter()
        .collect()
}

/// Every `*.yaml` scenario under [`scenario_root`], sorted so the trial order
/// does not depend on the file system.
///
/// `Path::is_file` reads the metadata of the link target, so a scenario that
/// Bazel staged as a symbolic link counts the same as a plain file.
///
/// # Panics
///
/// Panics when the directory cannot be read or holds no scenario. An empty
/// corpus is almost always a wrong path, and a harness that ran zero trials
/// would pass silently.
fn scenario_paths() -> Vec<PathBuf> {
    let root = scenario_root();
    let entries = std::fs::read_dir(&root)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", root.display()));
    let mut paths: Vec<PathBuf> = entries
        .map(|entry| {
            entry
                .unwrap_or_else(|error| {
                    panic!("cannot read an entry in {}: {error}", root.display())
                })
                .path()
        })
        .filter(|path| path.extension().is_some_and(|ext| ext == "yaml") && path.is_file())
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no scenario found in {}", root.display());
    paths
}

/// Loads one scenario file and checks it describes a runnable benchmark.
fn scenario_file(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = std::fs::read_to_string(path)?;
    let scenario: Scenario = serde_saphyr::from_str(&yaml)
        .map_err(|error| format!("{} does not load: {error}", path.display()))?;

    check!(!scenario.name.is_empty());
    // A zero-length measurement window or a zero-byte record would run but
    // measure nothing, so these are load-bearing rather than decorative.
    check!(scenario.duration > Time::ZERO);
    check!(scenario.msg_size > ByteSize::ZERO);
    check!(scenario.batch_size > ByteSize::ZERO);
    check!(scenario.partitions > 0);
    check!(scenario.producers > 0);

    if let LoadMode::FixedRate { rate } = scenario.mode {
        check!(rate > Frequency::ZERO);
    }

    // A kill scheduled at or after the end of the run would never fire.
    if let Some(failover) = &scenario.failover {
        check!(failover.kill_after < scenario.duration + scenario.warmup);
    }

    // The quantities survive a round trip through the operator-facing encoding,
    // so a scenario echoed into a report reads back as the same benchmark.
    let reencoded = serde_saphyr::to_string(&scenario)?;
    let reparsed: Scenario = serde_saphyr::from_str(&reencoded)?;
    assert!(reparsed == scenario);

    Ok(())
}

fn main() -> ExitCode {
    let args = libtest_mimic::Arguments::from_args();
    let trials = scenario_paths()
        .into_iter()
        .map(|path| {
            // The same `scenario_file::<file>` names `datatest-stable` produced,
            // so nextest filters and CI history keep matching.
            let file = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            libtest_mimic::Trial::test(format!("scenario_file::{file}"), move || {
                scenario_file(&path).map_err(libtest_mimic::Failed::from)
            })
        })
        .collect();
    libtest_mimic::run(&args, trials).exit_code()
}
