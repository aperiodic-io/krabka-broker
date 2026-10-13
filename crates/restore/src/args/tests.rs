//! Unit tests for the command line itself: what clap accepts, what it refuses,
//! and what the parsed `RestoreArgs` holds.

use {
    super::{
        test_support::{args_from, partition},
        *,
    },
    assert2::check,
    clap::Parser as _,
    krabka_ids::Offset,
};

#[test]
fn an_archive_source_is_required_and_exclusive() {
    check!(crate::Cli::try_parse_from(["krabka-restore", "--log-dir", "/target"]).is_err());
    check!(
        crate::Cli::try_parse_from([
            "krabka-restore",
            "--log-dir",
            "/target",
            "--archive-local",
            "/archive",
            "--archive-s3-bucket",
            "b",
        ])
        .is_err()
    );
    check!(
        crate::Cli::try_parse_from([
            "krabka-restore",
            "--log-dir",
            "/target",
            "--archive-s3-bucket",
            "b",
            "--archive-gcs-bucket",
            "g",
        ])
        .is_err()
    );
    check!(args_from(&[]).is_ok());
}

#[test]
fn the_three_controller_modes_conflict() {
    check!(args_from(&["--standalone", "--no-initial-controllers"]).is_err());
    check!(args_from(&["--standalone", "--initial-controllers", "1@h:1:u"]).is_err());
    check!(
        args_from(&[
            "--no-initial-controllers",
            "--initial-controllers",
            "1@h:1:u",
        ])
        .is_err()
    );
}

#[test]
fn repeatable_flags_accumulate() {
    let args = args_from(&[
        "--topic",
        "orders",
        "--topic",
        "payments",
        "--to-offset",
        "orders:0=10",
        "--to-offset",
        "payments:1=20",
        "--exclude-producer-id",
        "1",
        "--exclude-producer-id",
        "2",
        "--exclude-offset",
        "orders:0=5..7",
    ])
    .expect("args");
    check!(args.topic == vec!["orders".to_owned(), "payments".to_owned()]);
    check!(args.to_offset.len() == 2);
    check!(args.exclude_producer_id == vec![ProducerId(1), ProducerId(2)]);
    check!(
        args.exclude_offset
            == vec![OffsetRange {
                partition: partition("orders", 0),
                start: Offset(5),
                end_exclusive: Offset(7),
            }]
    );
}

#[test]
fn the_report_format_defaults_to_text() {
    check!(args_from(&[]).expect("args").report == ReportFormat::Text);
    check!(args_from(&["--report", "json"]).expect("args").report == ReportFormat::Json);
    check!(args_from(&["--report", "yaml"]).is_err());
}

#[test]
fn trusted_worm_keys_pair_by_position() {
    let args = args_from(&[
        "--worm-key-id",
        "old",
        "--worm-public-key",
        "/keys/old.pub",
        "--worm-key-id",
        "new",
        "--worm-public-key",
        "/keys/new.pub",
    ])
    .expect("args");
    check!(args.archive.worm_key_id == vec!["old", "new"]);
    check!(args.archive.worm_public_key.len() == 2);

    let unequal = args_from(&[
        "--worm-key-id",
        "old",
        "--worm-key-id",
        "new",
        "--worm-public-key",
        "/keys/old.pub",
    ])
    .expect("clap accepts repeated paired flags");
    check!(unequal.validate().is_err());
}

#[test]
fn an_empty_topic_list_selects_everything() {
    let args = args_from(&[]).expect("args");
    check!(args.selects_topic("anything"));

    let args = args_from(&["--topic", "orders"]).expect("args");
    check!(args.selects_topic("orders"));
    check!(!args.selects_topic("payments"));
}
