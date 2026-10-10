//! The flags `#[krabka_env]` gives a clap argument struct, read back from the
//! command clap builds and from what that command parses.

use std::path::PathBuf;

use assert2::assert;
use clap::{Args, Command};
use krabka_units::Time;

#[krabka_macros::krabka_env]
#[derive(Debug, Args)]
struct Flags {
    /// How long to wait.
    request_timeout: Option<Time>,
    attempts: Option<u32>,
    lag: Option<i64>,
    threads: Option<i32>,
    enabled: Option<bool>,
    name: Option<String>,
    #[arg(long = "explicit-flag", env = "SOMETHING_ELSE")]
    explicit: Option<u32>,
}

fn command() -> Command {
    Flags::augment_args(Command::new("flags"))
}

#[test]
fn every_field_without_arg_gets_a_long_flag_and_a_krabka_env() {
    let shapes = command()
        .get_arguments()
        .map(|arg| {
            (
                arg.get_long().map(str::to_owned),
                arg.get_env().map(|env| env.to_string_lossy().into_owned()),
                arg.get_help().map(ToString::to_string),
            )
        })
        .collect::<Vec<_>>();
    let expected = [
        (
            "request-timeout",
            "KRABKA_REQUEST_TIMEOUT",
            // clap drops the period that ends a one-sentence help.
            Some("How long to wait"),
        ),
        ("attempts", "KRABKA_ATTEMPTS", None),
        ("lag", "KRABKA_LAG", None),
        ("threads", "KRABKA_THREADS", None),
        ("enabled", "KRABKA_ENABLED", None),
        ("name", "KRABKA_NAME", None),
        ("explicit-flag", "SOMETHING_ELSE", None),
    ]
    .map(|(long, env, help)| {
        (
            Some(long.to_owned()),
            Some(env.to_owned()),
            help.map(str::to_owned),
        )
    });
    assert!(shapes == expected);
}

#[test]
fn the_value_parser_follows_the_field_type() {
    for (flag, accepted) in [
        ("--request-timeout=1ms", true),
        ("--request-timeout=0ms", false),
        ("--attempts=1", true),
        ("--attempts=0", false),
        ("--lag=0", true),
        ("--lag=-1", false),
        ("--threads=-1", true),
        ("--enabled=false", true),
        ("--enabled=maybe", false),
        ("--name=anything", true),
        ("--explicit-flag=0", true),
    ] {
        let parsed = command().try_get_matches_from(["flags", flag]).is_ok();
        assert!(parsed == accepted, "{flag}");
    }
}

#[derive(Debug, Args)]
struct Group {
    #[arg(long)]
    grouped: Option<u32>,
}

#[krabka_macros::krabka_env]
#[derive(Debug, Args)]
struct WithFlattened {
    #[command(flatten)]
    group: Group,
    outer: Option<u32>,
}

fn argument_names(command: &Command) -> Vec<(Option<String>, Option<String>)> {
    command
        .get_arguments()
        .map(|arg| {
            (
                arg.get_long().map(str::to_owned),
                arg.get_env().map(|env| env.to_string_lossy().into_owned()),
            )
        })
        .collect()
}

#[test]
fn a_field_with_its_own_command_attribute_is_left_alone() {
    let shapes = argument_names(&WithFlattened::augment_args(Command::new("flattened")));
    let expected = [
        (Some("grouped".to_owned()), None),
        (Some("outer".to_owned()), Some("KRABKA_OUTER".to_owned())),
    ];
    assert!(shapes == expected);
}

#[krabka_macros::krabka_env(prefix = "BENCH_")]
#[derive(Debug, Args)]
struct Prefixed {
    server_name: Option<String>,
    ca_path: Option<PathBuf>,
    #[arg(long, env = "BENCH_OUTPUT_PATH")]
    out: Option<PathBuf>,
}

#[test]
fn the_prefix_argument_replaces_krabka_in_every_variable() {
    let command = Prefixed::augment_args(Command::new("prefixed"));
    let shapes = argument_names(&command);
    let expected = [
        ("server-name", "BENCH_SERVER_NAME"),
        ("ca-path", "BENCH_CA_PATH"),
        ("out", "BENCH_OUTPUT_PATH"),
    ]
    .map(|(long, env)| (Some(long.to_owned()), Some(env.to_owned())));
    assert!(shapes == expected);

    let parsed = command
        .try_get_matches_from(["prefixed", "--ca-path=/tmp/ca.crt"])
        .expect("a path flag");
    assert!(parsed.get_one::<PathBuf>("ca_path") == Some(&PathBuf::from("/tmp/ca.crt")));
}
