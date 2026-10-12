//! Formats the log directories of a krabka broker node.
//!
//! A `KRaft` node will not boot against an unformatted directory: the broker
//! treats one as operator error and aborts startup. Formatting seeds Kafka's
//! `meta.properties`, the bootstrap records, and the singleton `VotersRecord`,
//! and can provision seed SCRAM credentials at the same time. One run formats
//! every directory of the node, as `kafka-storage format` does, and writes the
//! cluster and directory ids in Kafka's 22-character base64 form.
//! `docs/format-divergences.md` in the repository lists where the command
//! differs from `kafka-storage format`, and why.
//!
//! [`MetaProperties`] reads and writes `meta.properties` byte for byte as
//! Kafka does. The broker reads the file through it, and so can any tool that
//! has to find the identity of a formatted directory.
//!
//! [`run_with_records`] takes further [`MetadataRecord`]s from the caller and
//! seeds them into the same stream. A restore tool that rebuilds a cluster from
//! tiered-storage archives hands over the topic and partition records it
//! recovered that way, and the broker then boots with those topics present.
//!
//! This is the `krabka format` command from the monorepo's `krabka-cli`. That
//! crate also drives the gres layer, which is why it could not follow the broker
//! into this repository; the command itself needs only [`krabka_metadata`] and
//! [`krabka_security`]. It is a library as well as a binary so `krabka-cli` can
//! call it rather than carry a second copy.

use clap::Parser;

mod format;
mod ids;
mod meta_properties;

/// The seed record type [`run_with_records`] accepts, re-exported so a caller
/// building a bootstrap stream does not have to name [`krabka_metadata`]
/// itself.
pub use krabka_metadata::MetadataRecord;

pub use self::{
    format::{
        FAIL_AFTER_ENV, FormatArgs, LATEST_PRODUCTION_METADATA_VERSION, ScramSpec, parse_node_id,
        run, run_with_records,
    },
    ids::{ClusterId, DirectoryId, KafkaUuidError, random_uuid},
    meta_properties::{
        META_PROPERTIES, META_PROPERTIES_TMP, META_PROPERTIES_VERSION, MetaProperties,
        MetaPropertiesError, verify_ensemble,
    },
};

/// The formatter's command line.
///
/// Shared by the binary and by [`run_from_args`], so both accept exactly the
/// same flags.
#[derive(Parser)]
#[command(
    name = "krabka-format",
    version,
    about = "Format the log directories of a node, with optional seed SCRAM credentials",
    long_about = "Format the log directories of a node, with optional seed SCRAM credentials.\n\n\
                  This is the counterpart of `kafka-storage format`. It takes no --config \
                  file: krabka's broker does not read server.properties, so the directories \
                  come from --log-dir and --metadata-log-dir. Without --metadata-log-dir, \
                  the first --log-dir is the metadata log directory.\n\n\
                  Exit codes: 0 success; 2 a SCRAM iteration count below 4096 or an invalid \
                  command line; 3 a directory that is already formatted, holds foreign files, \
                  or names another cluster; 4 a write failure or an invalid voter set; 5 an \
                  invalid feature or quorum mode."
)]
pub struct Cli {
    /// The formatter's arguments, flattened so they are top-level flags.
    #[command(flatten)]
    pub args: FormatArgs,
}

/// Run the formatter from an argv-style iterator, returning its exit code.
///
/// Every broker test that boots a node needs a formatted log directory first.
/// Calling this beats spawning the binary: a subprocess needs a Cargo working
/// tree to build from, which a Bazel test sandbox does not have, and the
/// formatting is setup rather than the thing under test. The binary itself is
/// covered end to end by `tests/format_smoke.rs`.
///
/// Malformed arguments print a diagnostic and return 2. Help and version
/// requests print to stdout and return 0, without terminating the caller.
#[krabka_macros::argv_entrypoint]
pub async fn run_from_args() -> i32 {
    run_from_args_with_records(argv, Vec::new()).await
}

/// Run the formatter from an argv-style iterator, seeding `extra` alongside the
/// records the flags produce, and return its exit code.
///
/// This is the entry point for a tool that materializes a cluster and then has
/// to hand the formatter the metadata it recovered, such as the topic and
/// partition records behind a point-in-time restore. [`FormatArgs`] holds
/// private fields, so an argv is how such a caller states the rest of the
/// format; [`run_with_records`] documents where `extra` lands in the seed
/// stream.
///
/// Malformed arguments print a diagnostic and return 2. Help and version
/// requests print to stdout and return 0, without terminating the caller.
#[krabka_macros::argv_entrypoint]
pub async fn run_from_args_with_records(extra: Vec<MetadataRecord>) -> i32 {
    match Cli::try_parse_from(argv) {
        Ok(cli) => run_with_records(cli.args, extra).await,
        Err(error) => {
            let _ = error.print();
            if error.use_stderr() { 2 } else { 0 }
        }
    }
}
