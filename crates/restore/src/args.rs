//! The `krabka restore` command line and the parsers for its compound values.
//!
//! Every flag is long-form. The target-side flags carry the same names as
//! `krabka format`, because a restore formats the cluster it writes into and an
//! operator must not have to learn two spellings for one concept.
//!
//! The flags are grouped into three flattened structs, one per stage of the
//! command: where the archive is, what the target cluster is, and what the
//! restore keeps. `#[command(flatten)]` keeps every flag top-level, so the
//! grouping shows up in `--help` as headings and nowhere else.
//!
//! This file holds the clap definition itself. The values those flags take, and
//! the parsers clap calls to build them, live in `value` and `timestamp`, and
//! the cross-flag checks clap cannot express live in `validate`.

use std::path::PathBuf;

use clap::Args;
use krabka_ids::ProducerId;
use krabka_metadata::NodeId;
use regex::Regex;
use uuid::Uuid;

use self::{
    timestamp::parse_timestamp,
    value::{
        parse_cluster_id, parse_header_pattern, parse_node_id, parse_offset_bound,
        parse_offset_range, parse_producer_id, parse_regex, parse_topic_name,
    },
};
use crate::report::ReportFormat;

mod timestamp;
mod validate;
mod value;

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

pub use self::value::{HeaderPattern, OffsetBound, OffsetRange, PartitionRef};

/// Where the archive is, and the restore inputs that sit beside it.
///
/// The `--archive-*` flags are the ones `krabka backup` takes, from
/// [`krabka_object_store::ArchiveArgs`]; the rest are restore's own.
#[derive(Args, Debug)]
#[command(next_help_heading = "Archive source")]
pub struct ArchiveArgs {
    /// The backend and key prefix the archive is read from.
    #[command(flatten)]
    pub location: krabka_object_store::ArchiveArgs,

    /// A broker's `<log.dir>/remote-log-metadata/snapshot`.
    ///
    /// The snapshot is authoritative about segment lifecycle state for an
    /// unauthenticated restore. Authenticated restore uses WORM manifests for
    /// inventory and requires this snapshot's digest in the signed diskless
    /// capture before seeding its chain receipts into the restored broker.
    #[arg(long, value_name = "PATH")]
    pub rlmm_snapshot: Option<PathBuf>,

    /// A controller `<offset>-<epoch>.checkpoint` metadata snapshot.
    ///
    /// Topic configuration, ACLs, client quotas, SCRAM credentials, and
    /// finalized feature levels are recovered from it. Authenticated restore
    /// requires its digest to be bound into the signed diskless capture.
    #[arg(long, value_name = "PATH")]
    pub metadata_snapshot: Option<PathBuf>,

    /// Committed diskless-WAL projection captured by `krabka backup`.
    #[arg(long, value_name = "PATH")]
    pub diskless_wal_capture: Option<PathBuf>,

    /// Trusted WORM manifest signing-key id. Repeat with
    /// `--worm-public-key` to trust archives spanning a key rotation.
    #[arg(long, value_name = "ID", requires = "worm_public_key")]
    pub worm_key_id: Vec<String>,

    /// Raw 32-byte Ed25519 public key paired by position with
    /// `--worm-key-id`. Supplying a pair enables authenticated restore.
    #[arg(long, value_name = "PATH", requires = "worm_key_id")]
    pub worm_public_key: Vec<PathBuf>,
}

/// The cluster the restore writes.
///
/// Every flag here carries the name `krabka format` gives it, and is forwarded
/// to the formatter unchanged.
#[derive(Args, Debug)]
#[command(next_help_heading = "Target cluster")]
pub struct TargetArgs {
    /// Directory to restore into. Must be empty or absent.
    #[arg(long, value_name = "DIR")]
    pub log_dir: PathBuf,

    /// Cluster id of the restored cluster. Generated if not provided.
    /// Accepts Kafka's base64 `Uuid` form -- what `Metadata` and
    /// `DescribeCluster` report (#1042) -- or `java.util.UUID`'s hyphenated
    /// form.
    #[arg(long, value_parser = parse_cluster_id)]
    pub cluster_id: Option<Uuid>,

    /// This node's id, Kafka's `node.id`: an integer from 0 to 2147483647.
    /// Required: the formatter records it in `meta.properties`, and every
    /// restored partition names this node as its leader and sole replica.
    #[arg(long, value_parser = parse_node_id)]
    pub node_id: Option<NodeId>,

    /// Format the restored node as the sole initial controller voter.
    #[arg(long, conflicts_with_all = ["initial_controllers", "no_initial_controllers"])]
    pub standalone: bool,

    /// Explicit initial controllers: `id@host:port:directory-id`,
    /// comma-separated.
    #[arg(
        long,
        value_delimiter = ',',
        conflicts_with_all = ["standalone", "no_initial_controllers"]
    )]
    pub initial_controllers: Vec<String>,

    /// Format a dynamic controller that will join an existing quorum.
    #[arg(long, conflicts_with_all = ["standalone", "initial_controllers"])]
    pub no_initial_controllers: bool,

    /// This node's controller listener, as `host:port`.
    #[arg(long, value_name = "HOST:PORT")]
    pub controller_listener: Option<String>,
}

/// Arguments of an offline point-in-time restore.
///
/// The fields are public so a test or an embedding tool can build the struct
/// directly. A `clap::Args` struct with private fields is reachable only
/// through an argv, which forces every caller through string formatting.
#[derive(Args, Debug)]
pub struct RestoreArgs {
    /// Where the archive is.
    #[command(flatten)]
    pub archive: ArchiveArgs,

    /// Independently pinned WORM chain head, `PARTITION_DIR=64_HEX`.
    #[arg(long, value_name = "PARTITION_DIR=HEX", requires = "worm_key_id")]
    pub worm_expect_head: Vec<String>,

    /// The cluster the restore writes.
    #[command(flatten)]
    pub target: TargetArgs,

    /// Restore this topic. May be repeated. Every topic the archive holds is
    /// restored when the flag is absent.
    #[arg(
        long = "topic",
        value_name = "NAME",
        value_parser = parse_topic_name,
        help_heading = HEADING_BOUNDS
    )]
    pub topic: Vec<String>,

    /// Keep offsets at or below `N` in one partition: `topic:partition=N`.
    /// May be repeated.
    #[arg(
        long,
        value_name = "TOPIC:PARTITION=N",
        value_parser = parse_offset_bound,
        help_heading = HEADING_BOUNDS
    )]
    pub to_offset: Vec<OffsetBound>,

    /// Keep records whose timestamp is below this instant. Accepts RFC 3339
    /// with an explicit zone, or bare epoch milliseconds.
    #[arg(
        long,
        value_name = "RFC3339|EPOCH_MS",
        value_parser = parse_timestamp,
        help_heading = HEADING_BOUNDS
    )]
    pub to_timestamp: Option<i64>,

    /// Drop records whose key matches this pattern. May be repeated.
    #[arg(
        long,
        value_name = "REGEX",
        value_parser = parse_regex,
        help_heading = HEADING_BOUNDS
    )]
    pub exclude_key: Vec<Regex>,

    /// Drop records that carry a header matching `NAME=REGEX`. May be
    /// repeated.
    #[arg(
        long,
        value_name = "NAME=REGEX",
        value_parser = parse_header_pattern,
        help_heading = HEADING_BOUNDS
    )]
    pub exclude_header: Vec<HeaderPattern>,

    /// Drop records written by this producer id. May be repeated.
    #[arg(
        long,
        value_name = "ID",
        value_parser = parse_producer_id,
        help_heading = HEADING_BOUNDS
    )]
    pub exclude_producer_id: Vec<ProducerId>,

    /// Drop an offset range in one partition: `topic:partition=A..B`, with `B`
    /// exclusive. Write `A..=B` to include `B`. May be repeated.
    #[arg(
        long,
        value_name = "TOPIC:PARTITION=A..B",
        value_parser = parse_offset_range,
        help_heading = HEADING_BOUNDS
    )]
    pub exclude_offset: Vec<OffsetRange>,

    /// Verify, format cluster metadata, and report without writing partition data.
    #[arg(long, help_heading = HEADING_BEHAVIOUR)]
    pub dry_run: bool,

    /// Report format.
    #[arg(long, value_enum, default_value = "text", help_heading = HEADING_BEHAVIOUR)]
    pub report: ReportFormat,

    /// Skip a segment that fails verification instead of stopping. The report
    /// names every segment that was skipped.
    #[arg(long, help_heading = HEADING_BEHAVIOUR)]
    pub continue_on_corrupt: bool,
}

/// Help headings for the flags that are not in a flattened group.
const HEADING_BOUNDS: &str = "Selection and bounds";
const HEADING_BEHAVIOUR: &str = "Behaviour";

impl RestoreArgs {
    /// Whether `topic` is in the restore set.
    ///
    /// An empty `--topic` list selects every topic the archive holds.
    #[must_use]
    pub fn selects_topic(&self, topic: &str) -> bool {
        self.topic.is_empty() || self.topic.iter().any(|selected| selected == topic)
    }
}
