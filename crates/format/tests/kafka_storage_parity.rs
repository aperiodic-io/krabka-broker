//! `krabka-format` against `kafka-storage format`, through the binary.
//!
//! Each case runs the binary as a subprocess and reads back what it wrote and
//! printed. The Kafka behaviour each case pins was observed with
//! `kafka-storage format` on `apache/kafka:4.3.1`; `docs/format-divergences.md`
//! lists the places krabka differs on purpose.

use {
    krabka_format::{ClusterId, DirectoryId, FAIL_AFTER_ENV, META_PROPERTIES, MetaProperties},
    krabka_metadata::MetadataRecord,
    std::{
        collections::BTreeSet,
        path::{Path, PathBuf},
        process::{Command, Output},
    },
};

/// The node id of every run, as `--node-id` takes it and as
/// `meta.properties` records it.
const NODE: &str = "1";
const NODE_ID: i32 = 1;

const STANDALONE: &[&str] = &[
    "--standalone",
    "--node-id",
    NODE,
    "--controller-listener",
    "controller-1:9093",
];

/// A cluster id in Kafka's form, and the same id in the hyphenated form.
const CLUSTER_ID: &str = "AQIDBAUGBwgJCgsMDQ4PEA";
const CLUSTER_ID_HYPHENATED: &str = "01020304-0506-0708-090a-0b0c0d0e0f10";

fn krabka_format(args: &[&str], fault: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_krabka-format"));
    command.args(args).env_remove(FAIL_AFTER_ENV);
    if let Some(name) = fault {
        command.env(FAIL_AFTER_ENV, name);
    }
    command.output().expect("run krabka-format")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("utf-8 stdout")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("utf-8 stderr")
}

fn path_str(path: &Path) -> &str { path.to_str().expect("utf-8 path") }

/// The ids in a directory's `meta.properties`, as written.
fn written_ids(dir: &Path) -> MetaProperties {
    MetaProperties::read(dir)
        .expect("meta.properties reads")
        .expect("meta.properties exists")
}

/// Kafka's form of a directory's cluster id.
fn written_cluster_id(dir: &Path) -> String { written_ids(dir).cluster_id.to_string() }

/// Kafka's form of a directory's own id.
fn written_directory_id(dir: &Path) -> String {
    written_ids(dir)
        .directory_id
        .expect("directory.id")
        .to_string()
}

fn manifest_cluster_id(dir: &Path) -> String {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("bootstrap.json")).expect("manifest"))
            .expect("manifest is json");
    manifest["cluster_id"]
        .as_str()
        .expect("cluster_id")
        .to_owned()
}

/// Kafka's bootstrap snapshot path, `Snapshots.BOOTSTRAP_SNAPSHOT_ID` in the
/// `__cluster_metadata-0` partition directory.
fn checkpoint(dir: &Path) -> PathBuf {
    dir.join("__cluster_metadata-0")
        .join("00000000000000000000-0000000000.checkpoint")
}

krabka_macros::directory_tree_fixture!(
    snapshot,
    (PathBuf, Vec<u8>),
    { /// Every file under `dir` with its bytes, for an unchanged-on-rerun check.
    },
    (|_dir: &Path, path: &Path| (path.to_path_buf(), std::fs::read(path).expect("read")))
);

/// The id form: whatever `--cluster-id` names, the directory, the manifest,
/// and stdout carry Kafka's 22-character form. The reserved ids parse, as
/// they do in `kafka-storage format -t AAAAAAAAAAAAAAAAAAAAAA`. clap refuses a
/// bad id with its usage error, exit 2, before anything is written.
#[test]
fn the_cluster_id_is_written_and_printed_in_kafka_form() {
    /// What it is, `--cluster-id`, and the id written, or `None` for a refused
    /// one.
    struct Case<'a> {
        what: &'a str,
        input: &'a str,
        written: Option<&'a str>,
    }
    let cases: &[Case] = &[
        Case {
            what: "Kafka form",
            input: CLUSTER_ID,
            written: Some(CLUSTER_ID),
        },
        Case {
            what: "hyphenated form",
            input: CLUSTER_ID_HYPHENATED,
            written: Some(CLUSTER_ID),
        },
        Case {
            what: "upper-case hyphenated form",
            input: "01020304-0506-0708-090A-0B0C0D0E0F10",
            written: Some(CLUSTER_ID),
        },
        Case {
            what: "Kafka form with canonical padding",
            input: "AQIDBAUGBwgJCgsMDQ4PEA==",
            written: Some(CLUSTER_ID),
        },
        Case {
            what: "unused low bits set in the last character",
            input: "AQIDBAUGBwgJCgsMDQ4PEB",
            written: Some(CLUSTER_ID),
        },
        Case {
            what: "the reserved zero id",
            input: "AAAAAAAAAAAAAAAAAAAAAA",
            written: Some("AAAAAAAAAAAAAAAAAAAAAA"),
        },
        Case {
            what: "the reserved one id",
            input: "AAAAAAAAAAAAAAAAAAAAAQ",
            written: Some("AAAAAAAAAAAAAAAAAAAAAQ"),
        },
        Case {
            what: "too short",
            input: "AQIDBAUGBwgJCgsMDQ4P",
            written: None,
        },
        Case {
            what: "too long",
            input: "AQIDBAUGBwgJCgsMDQ4PEBES",
            written: None,
        },
        Case {
            what: "outside the url-safe alphabet",
            input: "AQIDBAUGBwgJCgsMDQ4P+A",
            written: None,
        },
        Case {
            what: "hyphenated but not hex",
            input: "01020304-0506-0708-090a-0b0c0d0e0fzz",
            written: None,
        },
        Case {
            what: "unhyphenated hex",
            input: "0102030405060708090a0b0c0d0e0f10",
            written: None,
        },
    ];
    for Case {
        what,
        input,
        written,
    } in cases
    {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("data");
        let out = krabka_format(
            &[
                "--log-dir",
                path_str(&dir),
                "--cluster-id",
                input,
                "--node-id",
                NODE,
                "--no-initial-controllers",
            ],
            None,
        );
        if let Some(id) = written {
            assert2::assert!(out.status.code() == Some(0), "{what}: {}", stderr(&out));
            let got = (
                written_cluster_id(&dir),
                manifest_cluster_id(&dir),
                stdout(&out).contains(&format!("with cluster-id {id} ")),
            );
            assert2::assert!(got == ((*id).to_owned(), (*id).to_owned(), true), "{what}");
        } else {
            assert2::assert!(out.status.code() == Some(2), "{what}");
            assert2::assert!(!dir.exists(), "{what}: nothing is written");
        }
    }
}

/// `--directory-id` and the directory id of an `--initial-controllers` entry
/// take either form and are written in Kafka's.
#[test]
fn directory_ids_are_written_in_kafka_form() {
    const DIRECTORY_ID: &str = "AAAAAAAAAAAAAAAAAAAAZA";
    /// What it is, and the extra flags.
    struct Case<'a> {
        what: &'a str,
        extra: &'a [&'a str],
    }
    let cases: &[Case] = &[
        Case {
            what: "--directory-id, Kafka form",
            extra: &["--directory-id", DIRECTORY_ID],
        },
        Case {
            what: "--directory-id, hyphenated form",
            extra: &["--directory-id", "00000000-0000-0000-0000-000000000064"],
        },
        Case {
            what: "--initial-controllers, Kafka form",
            extra: &[
                "--initial-controllers",
                "1@controller-1:9093:AAAAAAAAAAAAAAAAAAAAZA",
            ],
        },
        Case {
            what: "--initial-controllers, hyphenated form",
            extra: &[
                "--initial-controllers",
                "1@controller-1:9093:00000000-0000-0000-0000-000000000064",
            ],
        },
    ];
    for Case { what, extra } in cases {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("data");
        let mut args = vec![
            "--log-dir",
            path_str(&dir),
            "--cluster-id",
            CLUSTER_ID,
            "--node-id",
            NODE,
        ];
        args.extend_from_slice(extra);
        format_successfully(&args, what);
        // The broker reads back exactly what was written.
        assert2::assert!(
            krabka_broker::bootstrap::read_meta_properties(&dir).ok()
                == Some(MetaProperties {
                    cluster_id: CLUSTER_ID.parse().expect("cluster id"),
                    node_id: NODE_ID,
                    directory_id: Some(DirectoryId(uuid::Uuid::from_u128(100))),
                }),
            "{what}"
        );
        assert2::assert!(written_directory_id(&dir) == DIRECTORY_ID, "{what}");
    }
}

/// What one directory of a multi-directory format holds.
#[derive(Debug, PartialEq, Eq)]
struct Formatted {
    cluster_id: String,
    has_checkpoint: bool,
    has_manifest: bool,
    bootstrap_records: usize,
}

fn formatted(dir: &Path) -> Formatted {
    Formatted {
        cluster_id: written_cluster_id(dir),
        has_checkpoint: checkpoint(dir).is_file(),
        has_manifest: dir.join("bootstrap.json").is_file(),
        bootstrap_records: krabka_broker::bootstrap::load_bootstrap_records(dir)
            .expect("bootstrap records")
            .len(),
    }
}

/// What a metadata directory holds after a format with `seed_records`
/// bootstrap records, and with the checkpoint of a dynamic format when
/// `has_checkpoint` is set.
fn metadata_directory(has_checkpoint: bool, seed_records: usize) -> Formatted {
    Formatted {
        cluster_id: CLUSTER_ID.to_owned(),
        has_checkpoint,
        has_manifest: true,
        bootstrap_records: seed_records,
    }
}

/// What a data directory holds: `meta.properties` and nothing else.
fn data_directory() -> Formatted {
    Formatted {
        cluster_id: CLUSTER_ID.to_owned(),
        has_checkpoint: false,
        has_manifest: false,
        bootstrap_records: 0,
    }
}

/// The number of bootstrap records a standalone format under [`CLUSTER_ID`]
/// seeds, read from a probe format in `root`.
fn standalone_seed_records(root: &Path) -> usize {
    let probe = root.join("probe");
    let mut args = vec!["--log-dir", path_str(&probe), "--cluster-id", CLUSTER_ID];
    args.extend_from_slice(STANDALONE);
    assert2::assert!(krabka_format(&args, None).status.success());
    krabka_broker::bootstrap::load_bootstrap_records(&probe)
        .expect("records")
        .len()
}

/// Every file under `dir`, as a sorted list of `/`-separated relative paths.
fn files_under(dir: &Path) -> Vec<String> {
    snapshot(dir)
        .into_iter()
        .map(|(path, _)| {
            path.strip_prefix(dir)
                .expect("under dir")
                .components()
                .map(|part| part.as_os_str().to_str().expect("utf-8 path"))
                .collect::<Vec<_>>()
                .join("/")
        })
        .collect()
}

/// Spells the `--log-dir` flags for two directories.
type Spelling = fn(&str, &str) -> Vec<String>;

/// What it is, and how it spells the `--log-dir` flags.
struct SpellingCase {
    what: &'static str,
    spell: Spelling,
}

/// Repeated and comma-separated `--log-dir` format every directory in one
/// run, as `kafka-storage format` formats every entry of `log.dirs`. The set
/// shares one cluster id, and each directory has its own id. Only the first
/// directory gets the bootstrap files and the `__cluster_metadata-0`
/// checkpoint. It is the metadata log directory, as `metadata.log.dir`
/// defaults to the first entry of `log.dirs`.
#[test]
fn every_log_dir_is_formatted_in_one_run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let seed_records = standalone_seed_records(tmp.path());

    let spellings = [
        SpellingCase {
            what: "repeated",
            spell: |a, b| vec!["--log-dir".into(), a.into(), "--log-dir".into(), b.into()],
        },
        SpellingCase {
            what: "comma-separated",
            spell: |a, b| vec!["--log-dir".into(), format!("{a},{b}")],
        },
    ];
    for SpellingCase { what, spell } in spellings {
        let run = tmp.path().join(what);
        let (meta_dir, data_dir) = (run.join("meta"), run.join("data"));
        let mut args = spell(path_str(&meta_dir), path_str(&data_dir));
        args.extend(["--cluster-id", CLUSTER_ID].map(String::from));
        args.extend(STANDALONE.iter().map(|a| (*a).to_owned()));
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = format_successfully(&args, what);

        assert2::assert!(
            [formatted(&meta_dir), formatted(&data_dir)]
                == [metadata_directory(true, seed_records), data_directory()],
            "{what}"
        );
        let directory_ids: BTreeSet<String> = [&meta_dir, &data_dir]
            .iter()
            .map(|dir| written_directory_id(dir))
            .collect();
        assert2::assert!(directory_ids.len() == 2, "{what}: {directory_ids:?}");
        assert2::assert!(
            stdout(&out).lines().take(2).collect::<Vec<_>>()
                == vec![
                    format!(
                        "Formatting dynamic metadata voter directory {} with metadata.version \
                         4.3-IV0.",
                        meta_dir.display()
                    ),
                    format!(
                        "Formatting data directory {} with metadata.version 4.3-IV0.",
                        data_dir.display()
                    ),
                ],
            "{what}"
        );
    }
}

/// `--metadata-log-dir` is `metadata.log.dir`: `kafka-storage format` adds it
/// to the `log.dirs` set, so one run formats it and every `--log-dir`. It is
/// the one metadata directory, and it is formatted once when it is also a
/// `--log-dir`. Only it gets the bootstrap files and the
/// `__cluster_metadata-0` checkpoint. Each other directory is a data
/// directory with `meta.properties` alone. The set shares one cluster id,
/// each directory has its own id, and the metadata directory is written first.
#[test]
fn a_metadata_log_dir_is_formatted_with_the_log_dirs() {
    const VOTER: &str = "dynamic metadata voter directory";
    const DATA: &str = "data directory";
    /// A directory and its Kafka description.
    struct Written<'a> {
        dir: &'a str,
        description: &'a str,
    }
    /// What it is, `--metadata-log-dir`, the `--log-dir` entries, and each
    /// directory in write order with its Kafka description.
    struct Case<'a> {
        what: &'a str,
        metadata: &'a str,
        log_dirs: &'a [&'a str],
        want: &'a [Written<'a>],
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let seed_records = standalone_seed_records(tmp.path());

    let cases: &[Case] = &[
        Case {
            what: "a separate metadata log directory",
            metadata: "m",
            log_dirs: &["a", "b"],
            want: &[
                Written {
                    dir: "m",
                    description: VOTER,
                },
                Written {
                    dir: "a",
                    description: DATA,
                },
                Written {
                    dir: "b",
                    description: DATA,
                },
            ],
        },
        Case {
            what: "a metadata log directory that is also a log directory",
            metadata: "b",
            log_dirs: &["a", "b"],
            want: &[
                Written {
                    dir: "b",
                    description: VOTER,
                },
                Written {
                    dir: "a",
                    description: DATA,
                },
            ],
        },
    ];
    for Case {
        what,
        metadata,
        log_dirs,
        want,
    } in cases
    {
        let run = tmp.path().join(what.replace(' ', "-"));
        let metadata_dir = run.join(metadata);
        let joined = log_dirs
            .iter()
            .map(|dir| run.join(dir).display().to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut args = vec![
            "--metadata-log-dir",
            path_str(&metadata_dir),
            "--log-dir",
            &joined,
            "--cluster-id",
            CLUSTER_ID,
        ];
        args.extend_from_slice(STANDALONE);
        let out = format_successfully(&args, what);

        let mut lines: Vec<String> = want
            .iter()
            .map(|Written { dir, description }| {
                format!(
                    "Formatting {description} {} with metadata.version 4.3-IV0.",
                    run.join(dir).display()
                )
            })
            .collect();
        lines.push(format!(
            "Formatted {} log directories with cluster-id {CLUSTER_ID} ({seed_records} seed \
             record(s))",
            want.len()
        ));
        assert2::assert!(stdout(&out).lines().collect::<Vec<_>>() == lines, "{what}");

        let dirs: Vec<PathBuf> = want.iter().map(|written| run.join(written.dir)).collect();
        let got: Vec<Formatted> = dirs.iter().map(|dir| formatted(dir)).collect();
        let expected: Vec<Formatted> = want
            .iter()
            .map(|Written { description, .. }| {
                if *description == VOTER {
                    metadata_directory(true, seed_records)
                } else {
                    data_directory()
                }
            })
            .collect();
        assert2::assert!(got == expected, "{what}");
        assert2::assert!(
            files_under(&metadata_dir)
                == vec![
                    "__cluster_metadata-0/00000000000000000000-0000000000.checkpoint",
                    "bootstrap.json",
                    "bootstrap.records.bin",
                    META_PROPERTIES,
                ],
            "{what}"
        );
        for dir in dirs.iter().filter(|dir| **dir != metadata_dir) {
            assert2::assert!(
                files_under(dir) == vec![META_PROPERTIES],
                "{what}: {}",
                dir.display()
            );
        }
        let directory_ids: BTreeSet<String> =
            dirs.iter().map(|dir| written_directory_id(dir)).collect();
        assert2::assert!(
            directory_ids.len() == dirs.len(),
            "{what}: {directory_ids:?}"
        );
    }
}

/// Without `--ignore-formatted`, one formatted directory refuses the whole
/// run with Kafka's message, and the others are left alone. With it, the
/// formatted directories are skipped, the rest are formatted under the same
/// cluster id, and a second run changes nothing.
#[test]
fn ignore_formatted_skips_the_formatted_and_formats_the_rest() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a, b) = (tmp.path().join("a"), tmp.path().join("b"));
    let both = format!("{},{}", a.display(), b.display());

    let first = krabka_format(
        &[
            "--log-dir",
            path_str(&a),
            "--cluster-id",
            CLUSTER_ID,
            "--node-id",
            NODE,
        ],
        None,
    );
    assert2::assert!(first.status.success());
    let a_ids = written_ids(&a);

    let refused = krabka_format(
        &[
            "--log-dir",
            &both,
            "--cluster-id",
            CLUSTER_ID,
            "--node-id",
            NODE,
        ],
        None,
    );
    assert2::assert!(
        (refused.status.code(), stderr(&refused))
            == (
                Some(3),
                format!(
                    "Log directory {} is already formatted. Use --ignore-formatted to ignore \
                     this directory and format the others.\n",
                    a.display()
                )
            )
    );
    assert2::assert!(!b.exists(), "a refused run writes nothing");

    let other_cluster = krabka_format(
        &[
            "--log-dir",
            &both,
            "--cluster-id",
            "BQIDBAUGBwgJCgsMDQ4PEA",
            "--node-id",
            NODE,
            "--ignore-formatted",
        ],
        None,
    );
    assert2::assert!(
        (other_cluster.status.code(), stderr(&other_cluster))
            == (
                Some(3),
                format!(
                    "Invalid cluster.id in: {}. Expected BQIDBAUGBwgJCgsMDQ4PEA, but read {CLUSTER_ID}\n",
                    a.join(META_PROPERTIES).display()
                )
            )
    );

    // No --cluster-id: the formatted directory supplies it.
    let mixed = krabka_format(
        &["--log-dir", &both, "--node-id", NODE, "--ignore-formatted"],
        None,
    );
    assert2::assert!(mixed.status.code() == Some(0), "{}", stderr(&mixed));
    assert2::assert!(written_ids(&a) == a_ids);
    let b_ids = written_ids(&b);
    assert2::assert!(
        MetaProperties {
            directory_id: None,
            ..b_ids
        } == MetaProperties {
            directory_id: None,
            ..a_ids
        }
    );
    assert2::assert!(b_ids.directory_id != a_ids.directory_id);
    assert2::assert!(stdout(&mixed).contains(&format!(
        "Formatting data directory {} with metadata.version 4.3-IV0.",
        b.display()
    )));

    let before = (snapshot(&a), snapshot(&b));
    let again = krabka_format(
        &[
            "--log-dir",
            &both,
            "--cluster-id",
            CLUSTER_ID,
            "--node-id",
            NODE,
            "--ignore-formatted",
        ],
        None,
    );
    assert2::assert!(again.status.code() == Some(0), "{}", stderr(&again));
    assert2::assert!(
        stdout(&again)
            .lines()
            .last()
            .is_some_and(|line| line == "All of the log directories are already formatted.")
    );
    assert2::assert!((snapshot(&a), snapshot(&b)) == before);
}

/// A run that stops after any file it writes leaves a directory the next run
/// formats without an `rm -rf`, because `meta.properties` is the last file
/// and is published by a rename. Once the marker has landed, the directory is
/// formatted, and only `--ignore-formatted` passes it.
#[test]
fn an_interrupted_run_can_be_run_again() {
    /// The file the run stops after, and the exit of a plain rerun.
    struct Case<'a> {
        fault: &'a str,
        rerun_exit: i32,
    }
    let cases: &[Case] = &[
        Case {
            fault: "00000000000000000000-0000000000.checkpoint",
            rerun_exit: 0,
        },
        Case {
            fault: "bootstrap.records.bin",
            rerun_exit: 0,
        },
        Case {
            fault: "bootstrap.json",
            rerun_exit: 0,
        },
        Case {
            fault: "meta.properties.tmp",
            rerun_exit: 0,
        },
        Case {
            fault: META_PROPERTIES,
            rerun_exit: 3,
        },
    ];
    for Case { fault, rerun_exit } in cases {
        let (_tmp, meta_dir, data_dir) = metadata_and_data_directories();
        let dirs = format!("{},{}", meta_dir.display(), data_dir.display());
        let mut args = vec!["--log-dir", &dirs, "--cluster-id", CLUSTER_ID];
        args.extend_from_slice(STANDALONE);

        let interrupted = krabka_format(&args, Some(fault));
        assert2::assert!(interrupted.status.code() == Some(4), "{fault}");
        assert2::assert!(
            stderr(&interrupted).contains("injected failure after"),
            "{fault}"
        );

        let rerun = krabka_format(&args, None);
        assert2::assert!(
            rerun.status.code() == Some(*rerun_exit),
            "{fault}: {}",
            stderr(&rerun)
        );
        if *rerun_exit != 0 {
            let mut ignoring = args.clone();
            ignoring.push("--ignore-formatted");
            let finished = krabka_format(&ignoring, None);
            assert2::assert!(
                finished.status.code() == Some(0),
                "{fault}: {}",
                stderr(&finished)
            );
        }

        for dir in [&meta_dir, &data_dir] {
            let meta = krabka_broker::bootstrap::read_meta_properties(dir)
                .unwrap_or_else(|e| panic!("{fault}: {} does not read: {e}", dir.display()));
            assert2::assert!(
                (meta.cluster_id, meta.node_id)
                    == (
                        ClusterId(uuid::Uuid::from_u128(
                            0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10
                        )),
                        NODE_ID
                    ),
                "{fault}"
            );
        }
        assert2::assert!(
            [files_under(&meta_dir), files_under(&data_dir)]
                == [
                    vec![
                        "__cluster_metadata-0/00000000000000000000-0000000000.checkpoint",
                        "bootstrap.json",
                        "bootstrap.records.bin",
                        META_PROPERTIES,
                    ],
                    vec![META_PROPERTIES],
                ],
            "{fault}"
        );
        let records = krabka_broker::bootstrap::load_bootstrap_records(&meta_dir).expect("records");
        assert2::assert!(
            records
                .iter()
                .all(|r| !matches!(r, MetadataRecord::V1Voters(_))),
            "{fault}: voters live in the checkpoint"
        );
    }
}

/// Whether `line` is the date comment of `Properties.store` on a UTC host:
/// `#` and `java.util.Date.toString`, `EEE MMM dd HH:mm:ss zzz yyyy`.
fn is_utc_date_comment(line: &str) -> bool {
    const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let digits = |field: &str, len: usize| {
        field.len() == len && field.bytes().all(|byte| byte.is_ascii_digit())
    };
    let Some(date) = line.strip_prefix('#') else {
        return false;
    };
    let fields: Vec<&str> = date.split(' ').collect();
    let [weekday, month, day, time, zone, year] = fields.as_slice() else {
        return false;
    };
    let clock: Vec<&str> = time.split(':').collect();
    WEEKDAYS.contains(weekday)
        && MONTHS.contains(month)
        && digits(day, 2)
        && clock.len() == 3
        && clock.iter().all(|field| digits(field, 2))
        && *zone == "UTC"
        && digits(year, 4)
}

/// Every `meta.properties` a format writes is the file Kafka's
/// `PropertiesUtils.writePropertiesFile` writes for the same ids, byte for
/// byte, except for the time on the date comment: an empty comment line, the
/// date, and `cluster.id`, `directory.id`, `node.id`, and `version=1` in key
/// order, as `Properties.store` writes them on Java 18 and later.
#[test]
fn meta_properties_is_the_file_kafka_writes() {
    const DIRECTORY_ID: &str = "AAAAAAAAAAAAAAAAAAAAZA";
    let (_tmp, meta_dir, data_dir) = metadata_and_data_directories();
    let out = krabka_format(
        &[
            "--metadata-log-dir",
            path_str(&meta_dir),
            "--log-dir",
            path_str(&data_dir),
            "--cluster-id",
            CLUSTER_ID,
            "--node-id",
            "2147483647",
            "--directory-id",
            DIRECTORY_ID,
        ],
        None,
    );
    assert2::assert!(out.status.code() == Some(0), "{}", stderr(&out));

    // The data directory's id is generated, so its line is read back.
    for (dir, directory_id) in [
        (&meta_dir, DIRECTORY_ID.to_owned()),
        (&data_dir, written_directory_id(&data_dir)),
    ] {
        let text = std::fs::read_to_string(dir.join(META_PROPERTIES)).expect("meta.properties");
        let mut lines: Vec<&str> = text.split_inclusive('\n').collect();
        let date = lines.get(1).map(|line| line.trim_end_matches('\n'));
        assert2::assert!(
            date.is_some_and(is_utc_date_comment),
            "{}: {date:?}",
            dir.display()
        );
        lines[1] = "#<date>\n";
        assert2::assert!(
            lines.concat()
                == format!(
                    "#\n#<date>\ncluster.id={CLUSTER_ID}\ndirectory.id={directory_id}\n\
                     node.id=2147483647\nversion=1\n"
                ),
            "{}",
            dir.display()
        );
    }
}

fn format_successfully(args: &[&str], what: &str) -> std::process::Output {
    let out = krabka_format(args, None);
    assert2::assert!(out.status.code() == Some(0), "{what}: {}", stderr(&out));
    out
}

krabka_macros::format_directories_fixture!(metadata_and_data_directories);
