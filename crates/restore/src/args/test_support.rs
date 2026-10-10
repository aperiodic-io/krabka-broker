//! Fixtures the argument tests share: the `PartitionRef` a bound names, and a
//! `RestoreArgs` parsed the way the binary parses it.

use clap::Parser as _;

use crate::args::{PartitionRef, RestoreArgs};

pub(crate) fn partition(topic: &str, index: i32) -> PartitionRef {
    PartitionRef {
        topic: topic.to_owned(),
        partition: index,
    }
}

/// Parses `RestoreArgs` through the real command line, with an archive source
/// and a target already given, so a test states only the flags it is about.
pub(crate) fn args_from(extra: &[&str]) -> Result<RestoreArgs, clap::Error> {
    let mut argv = vec![
        "krabka-restore",
        "--archive-local",
        "/archive",
        "--log-dir",
        "/target",
    ];
    argv.extend_from_slice(extra);
    crate::Cli::try_parse_from(argv).map(|cli| cli.args)
}
