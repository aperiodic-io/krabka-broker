//! Whole-format tests: the exit code each argv produces, and the files a run
//! leaves behind.
//!
//! These drive the command end to end through [`crate::run_from_args`], which
//! is the only way the mode selection, the voter-set validation, and the
//! writers become observable. The tests that pin one parser or one resolution
//! rule live beside that code in the sibling modules.

use {
    super::*,
    crate::{
        Cli, format::output::ZERO_CHECKPOINT_NAME, meta_properties::META_PROPERTIES, run_from_args,
    },
    assert2::check,
};

/// What a run leaves in one directory.
#[derive(Debug, PartialEq, Eq)]
struct DirectoryWant<Dir, Expected> {
    dir: Dir,
    expected: Expected,
}

/// A run's directory layout, and what each directory should hold: what it is,
/// `--metadata-log-dir`, the `--log-dir` entries, the quorum flags, and the
/// directories.
struct DirectoryCase<'a, Expected> {
    what: &'a str,
    metadata: Option<&'a str>,
    log_dirs: &'a [&'a str],
    extra: &'a [&'a str],
    want: &'a [DirectoryWant<&'a str, Expected>],
}

/// A temporary root and the data directory path inside it.
struct DataDirectory {
    root: tempfile::TempDir,
    path: std::path::PathBuf,
}

fn fresh_data_directory() -> DataDirectory {
    let directory = tempfile::tempdir().expect("tempdir");
    let data = directory.path().join("data");
    DataDirectory {
        root: directory,
        path: data,
    }
}

fn format_argv(log_dir: &std::path::Path, node_id: &str, extra: &[&str]) -> Vec<String> {
    let mut argv = vec![
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        log_dir.display().to_string(),
        "--node-id".to_owned(),
        node_id.to_owned(),
    ];
    argv.extend(extra.iter().map(|arg| (*arg).to_owned()));
    argv
}

/// The node id of every run below that does not test the node id.
const NODE_ID: &str = "1";

/// The exit code `run` returns for each argv it can be given.
///
/// Neither `run` nor `run_from_args` had a unit test, so a mutant making
/// either return a constant survived -- and with them the whole of
/// `is_dynamic_format` and `build_initial_voters`, whose only visible
/// effect is which of these codes comes back.
#[tokio::test]
async fn exit_code_for_each_argv() {
    const STANDALONE: &[&str] = &["--standalone", "--controller-listener", "controller-1:9093"];
    /// What it is, the extra argv, and the expected exit, with `--node-id 1`.
    struct Case<'a> {
        what: &'a str,
        extra: &'a [&'a str],
        want: i32,
    }
    let cases: &[Case] = &[
        Case {
            what: "static, no flags at all",
            extra: &[],
            want: EXIT_OK,
        },
        Case {
            what: "standalone",
            extra: STANDALONE,
            want: EXIT_OK,
        },
        Case {
            what: "no-initial-controllers",
            extra: &["--no-initial-controllers"],
            want: EXIT_OK,
        },
        // is_dynamic_format: the kraft.version rules.
        Case {
            what: "kraft.version=1 with no quorum flag",
            extra: &["--feature", "kraft.version=1"],
            want: EXIT_INVALID_FEATURE,
        },
        Case {
            what: "standalone with kraft.version=0",
            extra: &[
                "--standalone",
                "--controller-listener",
                "c:9093",
                "--feature",
                "kraft.version=0",
            ],
            want: EXIT_INVALID_FEATURE,
        },
        Case {
            what: "kraft.version given twice",
            extra: &[
                "--no-initial-controllers",
                "--feature",
                "kraft.version=1",
                "--feature",
                "kraft.version=1",
            ],
            want: EXIT_INVALID_FEATURE,
        },
        Case {
            what: "kraft.version above its range",
            extra: &["--feature", "kraft.version=2"],
            want: EXIT_INVALID_FEATURE,
        },
        // build_initial_voters: every way the standalone voter can be wrong.
        Case {
            what: "standalone without --controller-listener",
            extra: &["--standalone"],
            want: EXIT_BOOTSTRAP_FAIL,
        },
        Case {
            what: "listener with no port",
            extra: &["--standalone", "--controller-listener", "hostonly"],
            want: EXIT_BOOTSTRAP_FAIL,
        },
        Case {
            what: "listener with an empty host",
            extra: &["--standalone", "--controller-listener", ":9093"],
            want: EXIT_BOOTSTRAP_FAIL,
        },
        Case {
            what: "listener on port zero",
            extra: &["--standalone", "--controller-listener", "c:0"],
            want: EXIT_BOOTSTRAP_FAIL,
        },
    ];

    for Case { what, extra, want } in cases {
        let FreshArgv { dir: _tmp, argv } = fresh_argv(extra);
        let got = run_from_args(argv).await;
        check!(got == *want, "{what}: exit {got}, want {want}");
    }
}

/// `Formatter.run` resolves the release and the feature names before it
/// reconciles `kraft.version` with the quorum flags, and it reconciles the mode
/// before it checks that the feature defines the level. So the first error a
/// command line with several wrong things reports is fixed.
#[test]
fn the_first_error_follows_kafkas_order() {
    use clap::Parser as _;

    /// What is wrong, the argv, and the start of the message it is reported as.
    struct Case<'a> {
        what: &'a str,
        extra: &'a [&'a str],
        want: &'a str,
    }
    let cases: &[Case] = &[
        Case {
            what: "an unknown release beats a kraft.version level that does not exist",
            extra: &[
                "--release-version",
                "9.9-IV0",
                "--feature",
                "kraft.version=9",
            ],
            want: "krabka format: Unknown metadata.version '9.9-IV0'.",
        },
        Case {
            what: "an unknown feature name beats a kraft.version level that does not exist",
            extra: &[
                "--feature",
                "nope.version=1",
                "--feature",
                "kraft.version=9",
            ],
            want: "krabka format: Unsupported feature: nope.version.",
        },
        Case {
            what: "a kraft.version level above 1 without a quorum flag is a mode conflict",
            extra: &["--feature", "kraft.version=9"],
            want: "krabka format: kraft.version=9 requires --standalone,",
        },
        Case {
            what: "a kraft.version level above 1 with a quorum flag does not exist",
            extra: &["--no-initial-controllers", "--feature", "kraft.version=9"],
            want: "krabka format: No feature:kraft.version with feature level 9",
        },
    ];
    for Case { what, extra, want } in cases {
        let FreshArgv { dir: _tmp, argv } = fresh_argv(extra);
        let cli = Cli::try_parse_from(argv).expect("parse");

        let Err(Failure { code, message }) = plan(cli.args, Vec::new()) else {
            panic!("{what}: the plan was accepted");
        };

        check!(code == EXIT_INVALID_FEATURE, "{what}: exit {code}");
        check!(message.starts_with(want), "{what}: {message}");
    }
}

/// The exit code of a run, and the directory it formatted.
struct FormatRun {
    code: i32,
    log_dir: std::path::PathBuf,
}

/// Formatting into a fresh directory as node `node_id`, returning its path
/// for inspection.
async fn format_into(tmp: &std::path::Path, node_id: &str, extra: &[&str]) -> FormatRun {
    let log_dir = tmp.join("data");
    let argv = format_argv(&log_dir, node_id, extra);
    FormatRun {
        code: run_from_args(argv).await,
        log_dir,
    }
}

fn checkpoint_len(log_dir: &std::path::Path) -> u64 {
    let path = krabka_raft::metadata_partition_dir(log_dir).join(ZERO_CHECKPOINT_NAME);
    std::fs::metadata(path).map_or(0, |m| m.len())
}

/// Any one of the three quorum flags selects a dynamic format, and their
/// absence selects the static one. The offset-zero checkpoint is written
/// only for a dynamic format, so its presence is the observable.
#[tokio::test]
async fn each_quorum_flag_on_its_own_selects_a_dynamic_format() {
    const EXPLICIT: &[&str] = &[
        "--initial-controllers",
        "1@host:9093:00000000-0000-0000-0000-000000000003",
    ];
    /// What it is, the argv, and whether the format is dynamic.
    struct Case<'a> {
        what: &'a str,
        argv: &'a [&'a str],
        dynamic: bool,
    }
    let cases: &[Case] = &[
        Case {
            what: "no quorum flag",
            argv: &[],
            dynamic: false,
        },
        Case {
            what: "--standalone",
            argv: STANDALONE,
            dynamic: true,
        },
        Case {
            what: "--initial-controllers",
            argv: EXPLICIT,
            dynamic: true,
        },
        Case {
            what: "--no-initial-controllers",
            argv: &["--no-initial-controllers"],
            dynamic: true,
        },
    ];
    for Case {
        what,
        argv,
        dynamic,
    } in cases
    {
        let tmp = tempfile::tempdir().expect("tempdir");
        let FormatRun { code, log_dir } = format_into(tmp.path(), NODE_ID, argv).await;
        check!(code == EXIT_OK, "{what}: exit {code}");
        check!(
            (checkpoint_len(&log_dir) > 0) == *dynamic,
            "{what}: checkpoint present should be {dynamic}"
        );
    }
}

/// The voter set rides in the checkpoint only when there is one. Both of
/// these formats are dynamic, so both write a checkpoint -- what separates
/// them is whether it also carries voters, and the one that does is bigger.
#[tokio::test]
async fn the_checkpoint_carries_voters_only_when_the_quorum_has_them() {
    let tmp_with = tempfile::tempdir().expect("tempdir");
    let FormatRun {
        code,
        log_dir: with_voters,
    } = format_into(tmp_with.path(), NODE_ID, STANDALONE).await;
    check!(code == EXIT_OK);

    let tmp_without = tempfile::tempdir().expect("tempdir");
    let FormatRun {
        code,
        log_dir: without_voters,
    } = format_into(tmp_without.path(), NODE_ID, &["--no-initial-controllers"]).await;
    check!(code == EXIT_OK);

    let (a, b) = (
        checkpoint_len(&with_voters),
        checkpoint_len(&without_voters),
    );
    check!(
        a > b,
        "checkpoint with voters ({a}) should exceed one without ({b})"
    );
}

/// The explicit quorum must name each controller once and must include
/// this node.
#[tokio::test]
async fn an_explicit_quorum_is_checked_for_duplicates_and_for_this_node() {
    const A: &str = "1@host-a:9093:00000000-0000-0000-0000-000000000001";
    const B: &str = "2@host-b:9093:00000000-0000-0000-0000-000000000002";
    // Same id as A on a different host, and same directory id as A on a
    // different node: each is rejected by its own check.
    const DUP_ID: &str = "1@host-c:9093:00000000-0000-0000-0000-00000000000c";
    const DUP_DIR: &str = "3@host-d:9093:00000000-0000-0000-0000-000000000001";

    /// What it is, `--node-id`, `--initial-controllers`, and the expected exit.
    struct Case<'a> {
        what: &'a str,
        node_id: &'a str,
        controllers: &'a str,
        want: i32,
    }
    let cases: &[Case] = &[
        Case {
            what: "a well-formed pair",
            node_id: "1",
            controllers: &joined(A, B),
            want: EXIT_OK,
        },
        Case {
            what: "a repeated node id",
            node_id: "1",
            controllers: &joined(A, DUP_ID),
            want: EXIT_BOOTSTRAP_FAIL,
        },
        Case {
            what: "a repeated directory id",
            node_id: "1",
            controllers: &joined(A, DUP_DIR),
            want: EXIT_BOOTSTRAP_FAIL,
        },
        Case {
            what: "a quorum without this node",
            node_id: "9",
            controllers: &joined(A, B),
            want: EXIT_BOOTSTRAP_FAIL,
        },
    ];
    for Case {
        what,
        node_id,
        controllers,
        want,
    } in cases
    {
        let tmp = tempfile::tempdir().expect("tempdir");
        let FormatRun { code, .. } =
            format_into(tmp.path(), node_id, &["--initial-controllers", controllers]).await;
        check!(code == *want, "{what}: exit {code}, want {want}");
    }
}

/// `--initial-controllers` takes one comma-separated value.
fn joined(a: &str, b: &str) -> String { format!("{a},{b}") }

/// `--directory-id` is only checked against the quorum entry when it was
/// given, and only rejected when the two disagree.
#[tokio::test]
async fn an_explicit_directory_id_must_match_this_node_s_quorum_entry() {
    // Outside the 100 lowest ids, which `--directory-id` refuses.
    const CONTROLLER: &str = "1@host-a:9093:00000000-0000-0001-0000-000000000001";
    /// What it is, `--directory-id`, and the expected exit.
    struct Case<'a> {
        what: &'a str,
        directory_id: &'a str,
        want: i32,
    }
    let cases: &[Case] = &[
        Case {
            what: "matching the quorum entry",
            directory_id: "00000000-0000-0001-0000-000000000001",
            want: EXIT_OK,
        },
        Case {
            what: "disagreeing with the quorum entry",
            directory_id: "00000000-0000-0000-0000-0000000000ff",
            want: EXIT_BOOTSTRAP_FAIL,
        },
    ];
    for Case {
        what,
        directory_id,
        want,
    } in cases
    {
        let tmp = tempfile::tempdir().expect("tempdir");
        let FormatRun { code, .. } = format_into(tmp.path(), NODE_ID, &[
            "--initial-controllers",
            CONTROLLER,
            "--directory-id",
            directory_id,
        ])
        .await;
        check!(code == *want, "{what}: exit {code}, want {want}");
    }
}

/// The SCRAM iteration floor is inclusive: the minimum itself is allowed
/// and one below it is not.
#[tokio::test]
async fn scram_iterations_are_checked_against_an_inclusive_minimum() {
    let min = u32::try_from(MIN_SCRAM_ITERATIONS).expect("SCRAM minimum is positive");
    for (iterations, want) in [(min, EXIT_OK), (min - 1, EXIT_LOW_ITERATIONS)] {
        let tmp = tempfile::tempdir().expect("tempdir");
        let spec = format!("SCRAM-SHA-256=[name=alice,password=hunter2,iterations={iterations}]");
        let FormatRun { code, .. } =
            format_into(tmp.path(), NODE_ID, &["--add-scram", &spec]).await;
        check!(
            code == want,
            "iterations={iterations}: exit {code}, want {want}"
        );
    }
}

/// A directory holding anything at all is refused rather than overwritten.
#[tokio::test]
async fn a_non_empty_log_dir_is_refused() {
    let DataDirectory {
        root: _tmp,
        path: log_dir,
    } = fresh_data_directory();
    std::fs::create_dir_all(&log_dir).expect("mkdir");
    std::fs::write(log_dir.join("someone-elses.txt"), b"x").expect("write");

    let code = run_from_args([
        "krabka-format",
        "--log-dir",
        &log_dir.display().to_string(),
        "--node-id",
        NODE_ID,
    ])
    .await;
    check!(code == EXIT_DIRTY_LOG_DIR);
}

/// The writers are only observable through the files they leave, so a
/// mutant emptying one out to `Ok(())` survives until something reads the
/// directory. A boot needs all of these.
#[tokio::test]
async fn a_standalone_format_writes_what_a_boot_reads() {
    let DataDirectory {
        root: _tmp,
        path: log_dir,
    } = fresh_data_directory();

    let code = run_from_args(standalone_argv(&log_dir, &[])).await;
    check!(code == EXIT_OK);

    for name in [META_PROPERTIES, "bootstrap.records.bin", "bootstrap.json"] {
        let path = log_dir.join(name);
        let len = std::fs::metadata(&path).map_or(0, |m| m.len());
        check!(len > 0, "{name} should exist and carry bytes, got {len}");
    }

    // KIP-853 dynamic quorum: the voter set lives in the offset-zero
    // checkpoint, not in the bootstrap record stream.
    let len = checkpoint_len(&log_dir);
    check!(len > 0, "offset-zero checkpoint should carry the voter set");
}

fn standalone_argv(log_dir: &std::path::Path, extra: &[&str]) -> Vec<String> {
    let mut argv = [
        "krabka-format",
        "--log-dir",
        &log_dir.display().to_string(),
        "--standalone",
        "--node-id",
        "1",
        "--controller-listener",
        "controller-1:9093",
    ]
    .map(str::to_owned)
    .to_vec();
    argv.extend(extra.iter().map(|s| (*s).to_owned()));
    argv
}

/// `--ignore-formatted` is what lets a Kubernetes init container run the
/// formatter unconditionally: the second run is a no-op that exits 0 and
/// leaves the first run's identity in place, while the same directory without
/// the flag is still refused.
#[tokio::test]
async fn ignore_formatted_makes_a_second_format_a_no_op() {
    let DataDirectory {
        root: _tmp,
        path: log_dir,
    } = fresh_data_directory();
    let argv = |extra: &[&str]| standalone_argv(&log_dir, extra);

    check!(crate::run_from_args(argv(&[])).await == EXIT_OK);
    let formatted = std::fs::read(log_dir.join(META_PROPERTIES)).expect("meta properties");

    // Without the flag the same directory is still a dirty log dir.
    check!(crate::run_from_args(argv(&[])).await == EXIT_DIRTY_LOG_DIR);

    // With it the run succeeds and rewrites nothing: a regenerated cluster or
    // directory id would strand the node's replicated identity.
    check!(crate::run_from_args(argv(&["--ignore-formatted"])).await == EXIT_OK);
    let after = std::fs::read(log_dir.join(META_PROPERTIES)).expect("meta properties");
    check!(after == formatted);
}

/// An unformatted directory is formatted normally under the flag: it means
/// "ignore an existing format", not "skip formatting".
#[tokio::test]
async fn ignore_formatted_still_formats_a_fresh_directory() {
    let DataDirectory {
        root: _tmp,
        path: log_dir,
    } = fresh_data_directory();

    let code = run_from_args(standalone_argv(&log_dir, &["--ignore-formatted"])).await;
    check!(code == EXIT_OK);
    check!(log_dir.join(META_PROPERTIES).is_file());
}

/// A format that fails partway leaves no marker, so the next run redoes it
/// rather than treating the half-written directory as formatted.
///
/// `--ignore-formatted` is what makes the formatter safe to run
/// unconditionally, and the price of that is that whatever it recognises as
/// "already formatted" has to mean the whole format landed. A rejected
/// `--add-scram` fails after the point the identity is resolved and before
/// any output is written, which is exactly the window that would otherwise
/// strand a directory carrying an identity and no seed records: the next
/// init-container attempt would exit 0 on it and the broker would boot with
/// no offset-zero checkpoint and no voter set.
#[tokio::test]
async fn a_failed_format_is_not_mistaken_for_a_finished_one() {
    let DataDirectory {
        root: _tmp,
        path: log_dir,
    } = fresh_data_directory();
    let argv = |extra: &[&str]| standalone_argv(&log_dir, extra);

    let weak = "SCRAM-SHA-256=[name=alice,password=hunter2,iterations=1]";
    check!(crate::run_from_args(argv(&["--add-scram", weak])).await == EXIT_LOW_ITERATIONS);
    check!(!log_dir.join(META_PROPERTIES).exists());

    // The retry an init container makes formats the directory for real.
    check!(crate::run_from_args(argv(&["--ignore-formatted"])).await == EXIT_OK);
    check!(log_dir.join(META_PROPERTIES).is_file());
    check!(log_dir.join("bootstrap.json").is_file());
    check!(log_dir.join("bootstrap.records.bin").is_file());
    check!(checkpoint_len(&log_dir) > 0, "the voter set must be seeded");
}

/// A directory whose `meta.properties` does not read is skipped with Kafka's
/// message and the others are formatted, unless it is the metadata log
/// directory, which Kafka refuses to continue without.
#[tokio::test]
async fn an_unreadable_marker_is_skipped_unless_it_is_the_metadata_directory() {
    let (tmp, meta_dir, data_dir) = metadata_and_data_directories();
    let unreadable = |dir: &std::path::Path| {
        std::fs::create_dir_all(dir).expect("mkdir");
        std::fs::write(dir.join(META_PROPERTIES), UNREADABLE).expect("write");
    };
    let run = |first: &std::path::Path, second: &std::path::Path| {
        run_from_args([
            "krabka-format".to_owned(),
            "--log-dir".to_owned(),
            first.display().to_string(),
            "--log-dir".to_owned(),
            second.display().to_string(),
            "--node-id".to_owned(),
            NODE_ID.to_owned(),
        ])
    };

    unreadable(&data_dir);
    check!(run(&meta_dir, &data_dir).await == EXIT_OK);
    check!(meta_dir.join(META_PROPERTIES).is_file());
    check!(std::fs::read(data_dir.join(META_PROPERTIES)).expect("left alone") == UNREADABLE);

    let other = tmp.path().join("other");
    check!(run(&data_dir, &other).await == EXIT_DIRTY_LOG_DIR);
    check!(!other.exists());
}

/// The quorum flags of a standalone format.
const STANDALONE: &[&str] = &["--standalone", "--controller-listener", "c:9093"];

/// A `meta.properties` that does not read: its version is not a number.
const UNREADABLE: &[u8] = b"version=one\n";

/// The argv of a run under `root`: `--metadata-log-dir` when `metadata` names
/// one, a `--log-dir` for each of `log_dirs`, `--node-id 1`, and then `extra`.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct FormatArgvSetup<'a> {
    metadata: Option<&'a str>,
    #[default(&["a"])]
    log_dirs: &'a [&'a str],
    extra: &'a [&'a str],
}

#[bon::builder]
fn argv_under(
    root: &Path,
    metadata: Option<&'_ str>,
    #[builder(default = & ["a"])] log_dirs: &'_ [&'_ str],
    #[builder(default)] extra: &'_ [&'_ str],
) -> Vec<String> {
    let setup = FormatArgvSetup {
        metadata,
        log_dirs,
        extra,
    };
    let FormatArgvSetup {
        metadata,
        log_dirs,
        extra,
    } = setup;
    let mut argv = vec!["krabka-format".to_owned()];
    if let Some(dir) = metadata {
        argv.push("--metadata-log-dir".to_owned());
        argv.push(root.join(dir).display().to_string());
    }
    for dir in log_dirs {
        argv.push("--log-dir".to_owned());
        argv.push(root.join(dir).display().to_string());
    }
    argv.push("--node-id".to_owned());
    argv.push(NODE_ID.to_owned());
    argv.extend(extra.iter().map(|a| (*a).to_owned()));
    argv
}

/// The plan for `argv`.
fn plan_of(argv: Vec<String>) -> Result<Plan, Failure> {
    use clap::Parser as _;

    plan(Cli::try_parse_from(argv).expect("parse").args, Vec::new())
}

/// The run writes the metadata log directory first and then each `--log-dir`
/// in the order given, every path once. Only the metadata log directory is a
/// metadata directory, of the kind the quorum flags select. Each directory
/// gets its own id.
#[test]
fn the_metadata_log_directory_leads_the_directory_set() {
    use DirectoryKind::{Data, DynamicMetadata, DynamicMetadataVoter, StaticMetadata};

    // (what, --metadata-log-dir, --log-dir entries, quorum flags, the targets)
    let cases: &[DirectoryCase<DirectoryKind>] = &[
        DirectoryCase {
            what: "the first --log-dir by default",
            metadata: None,
            log_dirs: &["a", "b"],
            extra: &[],
            want: &[
                DirectoryWant {
                    dir: "a",
                    expected: StaticMetadata,
                },
                DirectoryWant {
                    dir: "b",
                    expected: Data,
                },
            ],
        },
        DirectoryCase {
            what: "a separate metadata log directory",
            metadata: Some("m"),
            log_dirs: &["a", "b"],
            extra: &[],
            want: &[
                DirectoryWant {
                    dir: "m",
                    expected: StaticMetadata,
                },
                DirectoryWant {
                    dir: "a",
                    expected: Data,
                },
                DirectoryWant {
                    dir: "b",
                    expected: Data,
                },
            ],
        },
        DirectoryCase {
            what: "a metadata log directory that is also a log directory",
            metadata: Some("b"),
            log_dirs: &["a", "b"],
            extra: &[],
            want: &[
                DirectoryWant {
                    dir: "b",
                    expected: StaticMetadata,
                },
                DirectoryWant {
                    dir: "a",
                    expected: Data,
                },
            ],
        },
        DirectoryCase {
            what: "a metadata log directory that is the only log directory",
            metadata: Some("a"),
            log_dirs: &["a"],
            extra: &[],
            want: &[DirectoryWant {
                dir: "a",
                expected: StaticMetadata,
            }],
        },
        DirectoryCase {
            what: "a log directory named twice",
            metadata: Some("m"),
            log_dirs: &["a", "a"],
            extra: &[],
            want: &[
                DirectoryWant {
                    dir: "m",
                    expected: StaticMetadata,
                },
                DirectoryWant {
                    dir: "a",
                    expected: Data,
                },
            ],
        },
        DirectoryCase {
            what: "a voter",
            metadata: Some("m"),
            log_dirs: &["a"],
            extra: STANDALONE,
            want: &[
                DirectoryWant {
                    dir: "m",
                    expected: DynamicMetadataVoter,
                },
                DirectoryWant {
                    dir: "a",
                    expected: Data,
                },
            ],
        },
        DirectoryCase {
            what: "a dynamic controller that is not a voter",
            metadata: Some("m"),
            log_dirs: &["a"],
            extra: &["--no-initial-controllers"],
            want: &[
                DirectoryWant {
                    dir: "m",
                    expected: DynamicMetadata,
                },
                DirectoryWant {
                    dir: "a",
                    expected: Data,
                },
            ],
        },
    ];
    for DirectoryCase {
        what,
        metadata,
        log_dirs,
        extra,
        want,
    } in cases
    {
        let tmp = tempfile::tempdir().expect("tempdir");
        let plan = plan_of(
            argv_under()
                .root(tmp.path())
                .maybe_metadata(*metadata)
                .log_dirs(log_dirs)
                .extra(extra)
                .call(),
        )
        .unwrap_or_else(|Failure { code, message }| panic!("{what}: exit {code}: {message}"));

        let got: Vec<DirectoryWant<PathBuf, DirectoryKind>> = plan
            .targets
            .iter()
            .map(|target| DirectoryWant {
                dir: target.dir.clone(),
                expected: target.kind,
            })
            .collect();
        let want: Vec<DirectoryWant<PathBuf, DirectoryKind>> = want
            .iter()
            .map(|wanted| DirectoryWant {
                dir: tmp.path().join(wanted.dir),
                expected: wanted.expected,
            })
            .collect();
        check!(got == want, "{what}");
        let ids: std::collections::HashSet<DirectoryId> = plan
            .targets
            .iter()
            .map(|target| target.directory_id)
            .collect();
        check!(ids.len() == plan.targets.len(), "{what}: {ids:?}");
    }
}

/// `--directory-id` and the local `--initial-controllers` entry name the id of
/// the metadata log directory, also when `--metadata-log-dir` puts it apart
/// from the log directories. A data directory gets another id.
#[test]
fn the_directory_id_belongs_to_the_metadata_log_directory() {
    /// What it is, the flags that name the id, and the metadata directory's kind.
    struct Case<'a> {
        what: &'a str,
        extra: Vec<&'a str>,
        kind: DirectoryKind,
    }
    const ID: &str = "00000000-0000-0000-0000-000000000064";
    let id = DirectoryId(uuid::Uuid::from_u128(0x64));
    let controllers = format!("1@controller-1:9093:{ID}");
    let cases = [
        Case {
            what: "--directory-id",
            extra: vec!["--directory-id", ID],
            kind: DirectoryKind::StaticMetadata,
        },
        Case {
            what: "--initial-controllers",
            extra: vec!["--initial-controllers", &controllers],
            kind: DirectoryKind::DynamicMetadataVoter,
        },
    ];
    for Case { what, extra, kind } in cases {
        let tmp = tempfile::tempdir().expect("tempdir");
        let plan = plan_of(
            argv_under()
                .root(tmp.path())
                .maybe_metadata(Some("m"))
                .extra(&extra)
                .call(),
        )
        .unwrap_or_else(|Failure { code, message }| panic!("{what}: exit {code}: {message}"));

        let [metadata, data] = plan.targets.as_slice() else {
            panic!("{what}: {:?}", plan.targets);
        };
        check!(
            *metadata
                == Target {
                    dir: tmp.path().join("m"),
                    kind,
                    directory_id: id,
                },
            "{what}"
        );
        check!(
            *data
                == Target {
                    dir: tmp.path().join("a"),
                    kind: DirectoryKind::Data,
                    directory_id: data.directory_id,
                },
            "{what}"
        );
        check!(data.directory_id != id, "{what}");
    }
}

krabka_macros::directory_tree_fixture!(
    files_under,
    String,
    { /// Every file under `dir`, as a sorted list of `/`-separated relative paths.
    },
    (|dir: &Path, path: &Path| {
                let relative = path.strip_prefix(dir).expect("under dir");
                let parts: Vec<String> = relative
                    .components()
                    .map(|part| part.as_os_str().to_string_lossy().into_owned())
                    .collect();
                parts.join("/")
    })
);

/// Only the metadata log directory gets the bootstrap files, and with a
/// dynamic format the offset-zero checkpoint in `__cluster_metadata-0`, as
/// Kafka trunk writes its bootstrap snapshot only into a metadata directory.
/// A data directory gets `meta.properties` and nothing else. The set shares
/// one cluster id and one node id, and each directory has its own id.
#[tokio::test]
async fn only_the_metadata_log_directory_gets_the_bootstrap_files() {
    const META: &[&str] = &[META_PROPERTIES];
    const STATIC: &[&str] = &["bootstrap.json", "bootstrap.records.bin", META_PROPERTIES];
    const DYNAMIC: &[&str] = &[
        "__cluster_metadata-0/00000000000000000000-0000000000.checkpoint",
        "bootstrap.json",
        "bootstrap.records.bin",
        META_PROPERTIES,
    ];

    // (what, --metadata-log-dir, --log-dir entries, quorum flags, the files
    // in each directory)
    let cases: &[DirectoryCase<&[&str]>] = &[
        DirectoryCase {
            what: "static, a separate metadata log directory",
            metadata: Some("m"),
            log_dirs: &["a", "b"],
            extra: &[],
            want: &[
                DirectoryWant {
                    dir: "m",
                    expected: STATIC,
                },
                DirectoryWant {
                    dir: "a",
                    expected: META,
                },
                DirectoryWant {
                    dir: "b",
                    expected: META,
                },
            ],
        },
        DirectoryCase {
            what: "standalone, a separate metadata log directory",
            metadata: Some("m"),
            log_dirs: &["a", "b"],
            extra: STANDALONE,
            want: &[
                DirectoryWant {
                    dir: "m",
                    expected: DYNAMIC,
                },
                DirectoryWant {
                    dir: "a",
                    expected: META,
                },
                DirectoryWant {
                    dir: "b",
                    expected: META,
                },
            ],
        },
        DirectoryCase {
            what: "standalone, the first --log-dir",
            metadata: None,
            log_dirs: &["a", "b"],
            extra: STANDALONE,
            want: &[
                DirectoryWant {
                    dir: "a",
                    expected: DYNAMIC,
                },
                DirectoryWant {
                    dir: "b",
                    expected: META,
                },
            ],
        },
        DirectoryCase {
            what: "standalone, a metadata log directory that is also a log directory",
            metadata: Some("b"),
            log_dirs: &["a", "b"],
            extra: STANDALONE,
            want: &[
                DirectoryWant {
                    dir: "a",
                    expected: META,
                },
                DirectoryWant {
                    dir: "b",
                    expected: DYNAMIC,
                },
            ],
        },
    ];
    for DirectoryCase {
        what,
        metadata,
        log_dirs,
        extra,
        want,
    } in cases
    {
        let tmp = tempfile::tempdir().expect("tempdir");
        let code = run_from_args(
            argv_under()
                .root(tmp.path())
                .maybe_metadata(*metadata)
                .log_dirs(log_dirs)
                .extra(extra)
                .call(),
        )
        .await;
        check!(code == EXIT_OK, "{what}");

        let got: Vec<DirectoryWant<String, Vec<String>>> = want
            .iter()
            .map(|wanted| DirectoryWant {
                dir: wanted.dir.to_owned(),
                expected: files_under(&tmp.path().join(wanted.dir)),
            })
            .collect();
        let want: Vec<DirectoryWant<String, Vec<String>>> = want
            .iter()
            .map(|wanted| DirectoryWant {
                dir: wanted.dir.to_owned(),
                expected: wanted
                    .expected
                    .iter()
                    .map(|file| (*file).to_owned())
                    .collect(),
            })
            .collect();
        check!(got == want, "{what}");

        let dirs: Vec<PathBuf> = got
            .iter()
            .map(|wanted| tmp.path().join(&wanted.dir))
            .collect();
        let ensemble = Ensemble::load(&dirs).expect("survey");
        check!(ensemble.formatted.len() == dirs.len(), "{what}");
        check!(
            ensemble.verify(None, 1).is_ok_and(|id| id.is_some()),
            "{what}: one cluster id, node 1, and distinct directory ids"
        );
    }
}

/// The run stops on an unreadable `meta.properties` in the metadata log
/// directory, and names that directory, wherever `--metadata-log-dir` puts
/// it. An unreadable data directory is skipped, and the rest are formatted.
#[test]
fn an_unreadable_metadata_log_directory_is_named() {
    /// The target and the error directories of a plan.
    #[derive(Debug, PartialEq, Eq)]
    struct PlannedDirs<Dir> {
        targets: Vec<Dir>,
        errors: Vec<Dir>,
    }
    /// The exit code of a refusal and the directory the message names.
    struct Refusal {
        code: i32,
        dir: &'static str,
    }
    type Want = Result<PlannedDirs<&'static str>, Refusal>;
    /// What, `--metadata-log-dir`, the `--log-dir` entries, the unreadable
    /// directory, and the outcome.
    struct Case {
        what: &'static str,
        metadata: Option<&'static str>,
        log_dirs: &'static [&'static str],
        unreadable: &'static str,
        want: Want,
    }
    let cases = [
        Case {
            what: "the first --log-dir",
            metadata: None,
            log_dirs: &["a", "b"],
            unreadable: "a",
            want: Err(Refusal {
                code: EXIT_DIRTY_LOG_DIR,
                dir: "a",
            }),
        },
        Case {
            what: "a separate metadata log directory",
            metadata: Some("m"),
            log_dirs: &["a"],
            unreadable: "m",
            want: Err(Refusal {
                code: EXIT_DIRTY_LOG_DIR,
                dir: "m",
            }),
        },
        Case {
            what: "a data directory",
            metadata: Some("m"),
            log_dirs: &["a", "b"],
            unreadable: "a",
            want: Ok(PlannedDirs {
                targets: vec!["m", "b"],
                errors: vec!["a"],
            }),
        },
    ];
    for Case {
        what,
        metadata,
        log_dirs,
        unreadable,
        want,
    } in cases
    {
        let tmp = tempfile::tempdir().expect("tempdir");
        let broken = tmp.path().join(unreadable);
        std::fs::create_dir_all(&broken).expect("mkdir");
        std::fs::write(broken.join(META_PROPERTIES), UNREADABLE).expect("write");

        let got = plan_of(
            argv_under()
                .root(tmp.path())
                .maybe_metadata(metadata)
                .log_dirs(log_dirs)
                .call(),
        )
        .map(|plan| PlannedDirs {
            targets: plan
                .targets
                .into_iter()
                .map(|target| target.dir)
                .collect::<Vec<_>>(),
            errors: plan.errors,
        });
        let at = |dirs: Vec<&str>| -> Vec<PathBuf> {
            dirs.into_iter().map(|dir| tmp.path().join(dir)).collect()
        };
        let want = want
            .map(|PlannedDirs { targets, errors }| PlannedDirs {
                targets: at(targets),
                errors: at(errors),
            })
            .map_err(|Refusal { code, dir }| Failure {
                code,
                message: format!(
                    "Encountered I/O error in metadata log directory {}. Cannot continue.",
                    tmp.path().join(dir).display()
                ),
            });
        check!(got == want, "{what}");
    }
}

/// A formatted directory of another node refuses the run, `--ignore-formatted`
/// or not, with Kafka's message: `Formatter.doFormat` verifies the set against
/// `node.id` before it looks at what is already formatted.
#[tokio::test]
async fn a_directory_of_another_node_is_refused() {
    let tmp = tempfile::tempdir().expect("tempdir");
    check!(crate::run_from_args(argv_under().root(tmp.path()).call()).await == EXIT_OK);
    let (a, b) = (tmp.path().join("a"), tmp.path().join("b"));
    let message = format!(
        "Stored node id 1 doesn't match previous node id 2 in {}. If you moved your data, make \
         sure your configured node id matches. If you intend to create a new node, you should \
         remove all data in your data directories.",
        a.join(META_PROPERTIES).display()
    );

    for extra in [&[][..], &["--ignore-formatted"][..]] {
        let mut argv = vec![
            "krabka-format".to_owned(),
            "--log-dir".to_owned(),
            format!("{},{}", a.display(), b.display()),
            "--node-id".to_owned(),
            "2".to_owned(),
        ];
        argv.extend(extra.iter().map(|flag| (*flag).to_owned()));
        check!(
            plan_of(argv).err()
                == Some(Failure {
                    code: EXIT_DIRTY_LOG_DIR,
                    message: message.clone(),
                }),
            "{extra:?}"
        );
    }
    check!(!b.exists(), "a refused run writes nothing");
}

/// A temporary root and the argv that formats a fresh directory under it.
struct FreshArgv {
    dir: tempfile::TempDir,
    argv: Vec<String>,
}

fn fresh_argv(extra: &[&str]) -> FreshArgv {
    let DataDirectory {
        root: dir,
        path: log_dir,
    } = fresh_data_directory();
    let argv = format_argv(&log_dir, NODE_ID, extra);
    FreshArgv { dir, argv }
}

krabka_macros::format_directories_fixture!(metadata_and_data_directories);
