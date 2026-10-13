//! Smoke tests for the `krabka format` binary.
//!
//! Each test runs the binary as a subprocess. It then asserts on the exit
//! code and on the on-disk output. These tests show that the clap surface and
//! the bootstrap-write path work end-to-end without a broker.

use {krabka_metadata::MetadataRecord, std::process::Command};

fn run_format(dir: &tempfile::TempDir, args: &[&str]) -> std::process::Output {
    // The monorepo spells this `krabka format`; here the command is its own
    // binary, so the `format` subcommand argument is gone and the rest is the
    // same. `CARGO_BIN_EXE_<bin>` is set because the binary is in this package.
    let bin = env!("CARGO_BIN_EXE_krabka-format");
    let mut command = Command::new(bin);
    command
        .args(["--log-dir", dir.path().to_str().unwrap()])
        .args(args)
        .output()
        .expect("run krabka-format")
}

fn bootstrap_records(dir: &tempfile::TempDir) -> Vec<MetadataRecord> {
    krabka_broker::bootstrap::load_bootstrap_records(dir.path()).expect("bootstrap records")
}

fn offset_zero_checkpoint(dir: &tempfile::TempDir) -> std::path::PathBuf {
    dir.path()
        .join("__cluster_metadata-0")
        .join("00000000000000000000-0000000000.checkpoint")
}

fn check_static_records(records: &[MetadataRecord]) {
    assert2::assert!(records.iter().all(|record| !matches!(
        record,
        MetadataRecord::V1KRaftVersion(_) | MetadataRecord::V1Voters(_)
    )));
}

fn check_dynamic_checkpoint(dir: &tempfile::TempDir, records: &[MetadataRecord]) {
    check_static_records(records);
    assert2::assert!(std::fs::metadata(offset_zero_checkpoint(dir)).is_ok_and(|m| m.len() > 0));
}

fn check_refused_cases(cases: &[Vec<&str>], expected: i32) {
    for args in cases {
        let dir = tempfile::tempdir().unwrap();
        let out = run_format(&dir, args);
        assert2::assert!(!out.status.success());
        assert2::assert!(out.status.code() == Some(expected));
    }
}

fn formatted_directory_id(dir: &tempfile::TempDir, args: &[&str]) -> uuid::Uuid {
    let out = run_format(dir, args);
    assert2::assert!(out.status.success());
    krabka_broker::bootstrap::read_directory_id(dir.path()).expect("formatted directory id")
}

#[test]
fn format_with_add_scram_writes_credential_record() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_format(&dir, &[
        "--node-id",
        "1",
        "--add-scram",
        "SCRAM-SHA-512=[name=admin,password=admin-secret,iterations=4096]",
    ]);
    assert2::assert!(out.status.success());
    let bin_meta = std::fs::metadata(dir.path().join("bootstrap.records.bin"))
        .expect("bootstrap.records.bin must exist");
    assert2::assert!(bin_meta.len() > 0);
    let records = bootstrap_records(&dir);
    // Static format seeds every registered feature whose default at Kafka
    // 4.3's latest production release, 4.3-IV0, is > 0 (features at 0 are
    // omitted per KIP-1022), plus the SCRAM credential.
    let features: std::collections::BTreeMap<&str, i16> = records
        .iter()
        .filter_map(|record| match record {
            MetadataRecord::V1FeatureLevel(f) => Some((f.name.as_str(), f.level)),
            _ => None,
        })
        .collect();
    assert2::assert!(
        features
            == std::collections::BTreeMap::from([
                ("eligible.leader.replicas.version", 1),
                ("group.version", 1),
                ("metadata.version", 30),
                ("share.version", 1),
                ("streams.version", 1),
                ("transaction.version", 2),
            ])
    );
    assert2::assert!(records.len() == features.len() + 1);
    check_static_records(&records);
    assert2::assert!(!offset_zero_checkpoint(&dir).exists());
}

/// `krabka.version` bootstraps at level 0, which writes no record, unless
/// `--feature krabka.version=N` overrides it.
#[test]
fn format_seeds_krabka_version_only_when_overridden() {
    let cases: [(&[&str], Option<i16>); 3] = [
        (&[], None),
        (&["--feature", "krabka.version=1"], Some(1)),
        (&["--feature", "krabka.version=0"], None),
    ];
    let actual: Vec<_> = cases
        .iter()
        .map(|(flags, _)| {
            let dir = tempfile::tempdir().unwrap();
            let mut args = vec!["--node-id", "1"];
            args.extend_from_slice(flags);
            let out = run_format(&dir, &args);
            assert2::assert!(out.status.success(), "{flags:?}: {out:?}");
            let seeded = bootstrap_records(&dir)
                .into_iter()
                .find_map(|record| match record {
                    MetadataRecord::V1FeatureLevel(f) if f.name == "krabka.version" => {
                        Some(f.level)
                    }
                    _ => None,
                });
            (*flags, seeded)
        })
        .collect();
    let expected: Vec<_> = cases.to_vec();
    assert2::assert!(actual == expected);
}

#[test]
fn format_low_iterations_fails() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_format(&dir, &[
        "--node-id",
        "1",
        "--add-scram",
        "SCRAM-SHA-512=[name=admin,password=p,iterations=1]",
    ]);
    assert2::assert!(!out.status.success());
    assert2::assert!(out.status.code() == Some(2));
}

#[test]
fn no_initial_controllers_writes_offset_zero_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_format(&dir, &["--node-id", "1", "--no-initial-controllers"]);
    assert2::assert!(out.status.success());

    let records = bootstrap_records(&dir);
    assert2::assert!(!records.is_empty());
    check_dynamic_checkpoint(&dir, &records);
}

#[test]
fn standalone_writes_offset_zero_checkpoint_for_local_voter() {
    let dir = tempfile::tempdir().unwrap();
    let directory_id = formatted_directory_id(&dir, &[
        "--standalone",
        "--node-id",
        "7",
        "--controller-listener",
        "controller.example:9093",
    ]);
    let records = bootstrap_records(&dir);
    assert2::assert!(!directory_id.is_nil());
    check_dynamic_checkpoint(&dir, &records);
}

#[test]
fn initial_controllers_persists_the_local_listed_directory_id() {
    let dir = tempfile::tempdir().unwrap();
    let local_directory_id = "00000000-0000-0000-0000-000000000003";
    let directory_id = formatted_directory_id(&dir, &[
        "--node-id",
        "3",
        "--initial-controllers",
        &format!(
            "2@two.example:9093:00000000-0000-0000-0000-000000000002,3@three.example:9093:{local_directory_id}"
        ),
    ]);
    assert2::assert!(directory_id.to_string() == local_directory_id);
    let records = bootstrap_records(&dir);
    check_dynamic_checkpoint(&dir, &records);
}

#[test]
fn initial_controllers_rejects_ambiguous_or_missing_local_identity() {
    check_refused_cases(
        &[
            vec![
                "--node-id",
                "3",
                "--initial-controllers",
                "2@two.example:9093:00000000-0000-0000-0000-000000000002",
            ],
            vec![
                "--node-id",
                "2",
                "--initial-controllers",
                "2@two.example:9093:00000000-0000-0000-0000-000000000002,2@other.example:9093:00000000-0000-0000-0000-000000000003",
            ],
            vec![
                "--node-id",
                "2",
                "--initial-controllers",
                "2@two.example:9093:00000000-0000-0000-0000-000000000002,3@three.example:9093:00000000-0000-0000-0000-000000000002",
            ],
        ],
        4,
    );
}

#[test]
fn dynamic_modes_are_mutually_exclusive() {
    check_refused_cases(
        &[
            vec!["--node-id", "1", "--standalone", "--no-initial-controllers"],
            vec![
                "--node-id",
                "1",
                "--initial-controllers",
                "1@one.example:9093:00000000-0000-0000-0000-000000000001",
                "--no-initial-controllers",
            ],
            vec![
                "--node-id",
                "1",
                "--standalone",
                "--initial-controllers",
                "1@one.example:9093:00000000-0000-0000-0000-000000000001",
            ],
        ],
        2,
    );
}

#[test]
fn kraft_version_must_match_the_selected_format_mode() {
    check_refused_cases(
        &[
            vec![
                "--node-id",
                "1",
                "--no-initial-controllers",
                "--feature",
                "kraft.version=0",
            ],
            vec!["--node-id", "1", "--feature", "kraft.version=1"],
            vec!["--node-id", "1", "--feature", "kraft.version=2"],
        ],
        5,
    );

    let static_dir = tempfile::tempdir().unwrap();
    let out = run_format(&static_dir, &[
        "--node-id",
        "1",
        "--feature",
        "kraft.version=0",
    ]);
    assert2::assert!(out.status.success());
    assert2::assert!(bootstrap_records(&static_dir).iter().all(|record| {
        !matches!(
            record,
            MetadataRecord::V1KRaftVersion(_) | MetadataRecord::V1Voters(_)
        )
    }));
}

/// `--node-id` is required, as Kafka's `node.id` is: without it clap refuses
/// the command line, exit 2, and nothing is written.
#[test]
fn node_id_is_required() {
    let parent = tempfile::tempdir().unwrap();
    let dir = parent.path().join("data");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_krabka-format"))
        .args([
            "--log-dir",
            dir.to_str().unwrap(),
            "--no-initial-controllers",
        ])
        .output()
        .expect("run krabka-format");
    assert2::assert!(out.status.code() == Some(2));
    assert2::assert!(String::from_utf8_lossy(&out.stderr).contains("--node-id <NODE_ID>"));
    assert2::assert!(!dir.exists());
}
