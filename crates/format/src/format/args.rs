//! The command line `krabka format` accepts, and the scalar value parsers that
//! belong to no larger concern.
//!
//! [`FormatArgs`] is the one description of that command line: the binary and
//! [`crate::run_from_args`] both parse into it, so a caller of either sees the
//! same flags. The structured values behind `--feature`, `--add-scram`, and
//! `--add-acl` parse in the module that owns each of those concerns, and this
//! module names their parsers in the `value_parser` attributes.

use std::path::PathBuf;

use clap::Args;
use krabka_metadata::AclEntry;
use krabka_security::SaslMechanism;

use super::{acl::parse_acl_spec, features::parse_feature_spec, scram::parse_scram_spec};
use crate::ids::{ClusterId, DirectoryId};

#[derive(Args, Debug)]
pub struct FormatArgs {
    /// A log directory to format, an entry of Kafka's `log.dirs`. Repeat the
    /// flag, or separate paths with commas, to format every directory of the
    /// node in one run. Without `--metadata-log-dir`, the first directory is
    /// also the metadata log directory, as Kafka's `metadata.log.dir` defaults
    /// to the first entry of `log.dirs`. `kafka-storage format` reads the same
    /// set from its `--config` file. Krabka takes no `server.properties`, so
    /// there is no `--config`.
    #[arg(
        long = "log-dir",
        value_name = "DIR",
        value_delimiter = ',',
        required = true
    )]
    pub(super) log_dirs: Vec<PathBuf>,
    /// The metadata log directory, Kafka's `metadata.log.dir`. The run formats
    /// it together with every `--log-dir`, as `kafka-storage format` adds
    /// `metadata.log.dir` to the `log.dirs` set. Only this directory gets the
    /// bootstrap files and the `__cluster_metadata-0` checkpoint. It can also
    /// be one of the `--log-dir` entries. Give the broker the same directory
    /// as its `--metadata-log-dir`. Defaults to the first `--log-dir`.
    #[arg(long, value_name = "DIR")]
    pub(super) metadata_log_dir: Option<PathBuf>,
    /// Cluster id. Written and printed in Kafka's 22-character base64 form.
    /// Accepts that form or the hyphenated form. When omitted, the id of an
    /// already formatted directory in the set is kept, and otherwise a new id
    /// is generated.
    #[arg(long, value_parser = ClusterId::parse_cli)]
    pub(super) cluster_id: Option<ClusterId>,
    /// Bootstrap `metadata.version` (KIP-778), e.g. `4.0` or `4.0-IV3`. A
    /// string with more than two dot-separated segments keeps the first two,
    /// as Kafka's `MetadataVersion.fromVersionString` does, so `4.3.1` is
    /// `4.3`. Defaults to Kafka 4.3's latest production level, `4.3-IV0`, when
    /// omitted.
    #[arg(long)]
    pub(super) release_version: Option<String>,
    /// Set an individual feature's finalized level at format time (KIP-1022),
    /// e.g. `--feature transaction.version=2`. May be repeated. Combines with
    /// `--release-version` (which sets the base release) for every feature
    /// except `metadata.version`, where the two conflict.
    #[arg(long = "feature", value_parser = parse_feature_spec)]
    pub(super) feature: Vec<(String, i16)>,
    /// Accept feature levels past the latest production ones: Kafka trunk's
    /// `metadata.version` `4.4-IV0` to `4.4-IV2`, which a stock Kafka 4.3.1
    /// does not know. It is Kafka's internal `unstable.feature.versions.enable`,
    /// which `kafka-storage format` reads from its `--config` file; without it
    /// such a release is refused as Kafka refuses it. A node formatted at one
    /// of those levels needs the same setting in its `server_properties`.
    #[arg(
        long,
        action = clap::ArgAction::SetTrue,
        value_parser = clap::builder::TypedValueParser::map(
            clap::builder::BoolValueParser::new(),
            krabka_raft::UnstableFeatureVersions::from,
        ),
    )]
    pub(super) unstable_feature_versions_enable: krabka_raft::UnstableFeatureVersions,
    /// Seed a SCRAM credential. May be repeated.
    /// Format: `SCRAM-SHA-256=[name=<u>,password=<p>,iterations=<n>]`
    /// or `SCRAM-SHA-512=[name=<u>,password=<p>,iterations=<n>]`
    /// (iterations defaults to 4096 when omitted)
    #[arg(long, value_parser = parse_scram_spec)]
    pub(super) add_scram: Vec<ScramSpec>,
    /// Seed an ACL entry. May be repeated.
    /// Format: `principal=User:<name>,host=<ip|*>,operation=<Op>,permission=<Allow|Deny>,resource=<Type>:<Name>[:<Pattern>]`
    /// Pattern defaults to `Literal`.
    #[arg(long, value_parser = parse_acl_spec)]
    pub(super) add_acl: Vec<AclEntry>,
    /// This node's id, Kafka's `node.id`: an integer from 0 to 2147483647.
    /// Every `meta.properties` records it, and the broker refuses to start on
    /// a directory that records another id. `kafka-storage format` reads it
    /// from its `--config` file. Give the broker the same id as its
    /// `--broker-id`.
    #[arg(long, value_parser = parse_node_id)]
    pub(super) node_id: krabka_metadata::NodeId,
    /// The metadata log directory's stable id: the node's KIP-853 voter
    /// identity. Intended for orchestrators that verify the exact node
    /// incarnation before they declare it ready. Accepts Kafka's base64 form
    /// or the hyphenated form. The other directories get generated ids.
    #[arg(long, value_parser = parse_directory_id)]
    pub(super) directory_id: Option<DirectoryId>,
    /// Format this node as the sole initial controller voter.
    #[arg(
        long,
        conflicts_with_all = ["initial_controllers", "no_initial_controllers"]
    )]
    pub(super) standalone: bool,
    /// Explicit initial controllers: `id@host:port:directory-id`,
    /// comma-separated. The directory id is in Kafka's base64 form or the
    /// hyphenated form.
    #[arg(
        long,
        value_delimiter = ',',
        conflicts_with_all = ["standalone", "no_initial_controllers"]
    )]
    pub(super) initial_controllers: Vec<String>,
    /// Format a dynamic controller that will join an existing quorum.
    #[arg(
        long,
        conflicts_with_all = ["standalone", "initial_controllers"]
    )]
    pub(super) no_initial_controllers: bool,
    /// This node's controller listener (`host:port`) — written into the
    /// `VotersRecord` when `--standalone`.
    #[arg(long)]
    pub(super) controller_listener: Option<String>,
    /// The name of the controller listener: the first entry of Kafka's
    /// `controller.listener.names`. Each voter endpoint that `--standalone` or
    /// `--initial-controllers` writes carries it, as `kafka-storage format`
    /// names them. A leader refuses an `AddRaftVoter` whose endpoints do not
    /// include its own listener name.
    #[arg(long, default_value = "CONTROLLER", value_parser = parse_listener_name)]
    pub(super) controller_listener_name: String,
    /// Skip an already formatted directory, instead of refusing the run, and
    /// format the others. Matches `kafka-storage format --ignore-formatted`.
    ///
    /// This is what makes the formatter safe to run unconditionally, which a
    /// Kubernetes init container has to: the image carries no shell, so there
    /// is nothing to test the directory with before the call, and a pod that
    /// restarts against its existing volume would otherwise fail every
    /// restart after the first.
    #[arg(long)]
    pub(super) ignore_formatted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramSpec {
    pub(super) mechanism: SaslMechanism,
    pub(super) name: String,
    pub(super) password: String,
    pub(super) iterations: u32,
}

/// Parses a node id as Kafka's `node.id`: an `int` that is not negative.
///
/// `meta.properties` holds the id as an `int`, which Kafka's tools read with
/// `Integer.parseInt`, so a larger id would make a file they cannot read. The
/// negative-id message is the one Kafka's `Formatter.run` throws.
///
/// # Errors
///
/// Returns a message for a value that is not an integer, is past the `int`
/// range, or is negative.
pub fn parse_node_id(s: &str) -> Result<krabka_metadata::NodeId, String> {
    let id: i32 = s.trim().parse().map_err(|e| format!("node id: {e}"))?;
    let id = u64::try_from(id)
        .map_err(|_| "You must specify a valid non-negative node ID.".to_owned())?;
    Ok(krabka_metadata::NodeId(id))
}

/// Parses `--directory-id`: Kafka's base64 form or the hyphenated form, and
/// never one of the 100 ids that Kafka reserves as directory-id sentinels.
///
/// The broker refuses a reserved id at startup, as Kafka's
/// `MetaPropertiesEnsemble.verify` does, so the format refuses it first and
/// writes nothing.
///
/// # Errors
///
/// Returns the message of [`DirectoryId::parse_cli`], or one for a reserved
/// id.
pub fn parse_directory_id(s: &str) -> Result<DirectoryId, String> {
    let id = DirectoryId::parse_cli(s).map_err(|e| e.to_string())?;
    if id.is_reserved() {
        return Err(format!(
            "Invalid reserved directory ID {id}: Kafka reserves the 100 lowest directory ids"
        ));
    }
    Ok(id)
}

/// Parse `--controller-listener-name` into Kafka's normalised form: upper
/// case, as `ListenerName.normalised` makes it.
///
/// # Errors
///
/// Returns a message for an empty name, or one with leading or trailing white
/// space, which Kafka's `RaftVoterEndpoint` refuses.
pub fn parse_listener_name(s: &str) -> Result<String, String> {
    if s.is_empty() || s.trim() != s {
        return Err(format!(
            "controller listener name {s:?} must be non-empty, without leading or trailing \
             white space"
        ));
    }
    Ok(s.to_ascii_uppercase())
}

#[cfg(test)]
mod tests {

    use assert2::check;
    use clap::{Parser as _, error::ErrorKind};

    use super::*;
    use crate::Cli;

    /// `--log-dir` takes repeated flags and comma-separated lists alike, and
    /// keeps the order they were given in.
    #[test]
    fn log_dir_is_repeatable_and_comma_separated() {
        let cases: [&[&str]; 3] = [
            &["--log-dir", "/a", "--log-dir", "/b"],
            &["--log-dir", "/a,/b"],
            &["--log-dir=/a", "--log-dir=/b"],
        ];
        for argv in cases {
            let cli = Cli::try_parse_from(
                ["krabka-format", "--node-id", "1"]
                    .into_iter()
                    .chain(argv.iter().copied()),
            )
            .expect("parse");
            check!(
                cli.args.log_dirs == vec![PathBuf::from("/a"), PathBuf::from("/b")],
                "{argv:?}"
            );
        }
        check!(crate::Cli::try_parse_from(["krabka-format", "--node-id", "1"]).is_err());
    }

    /// `--metadata-log-dir` takes one path, comma and all, as Kafka's
    /// `metadata.log.dir` is one path. It is optional, it does not replace the
    /// required `--log-dir`, and clap refuses it twice.
    #[test]
    fn metadata_log_dir_is_one_optional_path() {
        type Parsed = Result<(Option<PathBuf>, Vec<PathBuf>), ErrorKind>;
        let path = PathBuf::from;
        // (argv after the program name, the metadata log dir and the log dirs)
        let cases: [(&[&str], Parsed); 6] = [
            (&["--log-dir", "/a"], Ok((None, vec![path("/a")]))),
            (
                &["--metadata-log-dir", "/m", "--log-dir", "/a,/b"],
                Ok((Some(path("/m")), vec![path("/a"), path("/b")])),
            ),
            (
                &["--log-dir=/a", "--metadata-log-dir=/a"],
                Ok((Some(path("/a")), vec![path("/a")])),
            ),
            (
                &["--metadata-log-dir", "/m,/n", "--log-dir", "/a"],
                Ok((Some(path("/m,/n")), vec![path("/a")])),
            ),
            (
                &["--metadata-log-dir", "/m"],
                Err(ErrorKind::MissingRequiredArgument),
            ),
            (
                &[
                    "--metadata-log-dir",
                    "/m",
                    "--metadata-log-dir",
                    "/n",
                    "--log-dir",
                    "/a",
                ],
                Err(ErrorKind::ArgumentConflict),
            ),
        ];
        for (argv, want) in cases {
            let got: Parsed = Cli::try_parse_from(
                ["krabka-format", "--node-id", "1"]
                    .into_iter()
                    .chain(argv.iter().copied()),
            )
            .map(|cli| (cli.args.metadata_log_dir, cli.args.log_dirs))
            .map_err(|error| error.kind());
            check!(got == want, "{argv:?}");
        }
    }

    /// A node id is Kafka's `int` without a sign: 0 through 2147483647.
    /// Everything else is an error rather than a silent zero.
    #[test]
    fn parse_node_id_takes_kafkas_node_id_range() {
        // (input, the id, or the message)
        let cases: [(&str, Result<u64, &str>); 9] = [
            ("7", Ok(7)),
            ("  7  ", Ok(7)),
            ("0", Ok(0)),
            ("2147483647", Ok(2_147_483_647)),
            (
                "2147483648",
                Err("node id: number too large to fit in target type"),
            ),
            ("-1", Err("You must specify a valid non-negative node ID.")),
            ("", Err("node id: cannot parse integer from empty string")),
            ("1.0", Err("node id: invalid digit found in string")),
            ("0x7", Err("node id: invalid digit found in string")),
        ];
        for (input, want) in cases {
            check!(
                parse_node_id(input).map(|n| n.0) == want.map_err(str::to_owned),
                "{input:?}"
            );
        }
    }

    /// `--directory-id` takes either id form and refuses the ids that Kafka
    /// reserves, which the broker would refuse at startup.
    #[test]
    fn parse_directory_id_refuses_reserved_ids() {
        let reserved = |n: u128| {
            Err(format!(
                "Invalid reserved directory ID {}: Kafka reserves the 100 lowest directory ids",
                DirectoryId::from(uuid::Uuid::from_u128(n))
            ))
        };
        let usable = uuid::Uuid::from_u64_pair(1, 1);
        let cases: [(String, Result<DirectoryId, String>); 5] = [
            (usable.to_string(), Ok(DirectoryId::from(usable))),
            (
                DirectoryId::from(usable).to_string(),
                Ok(DirectoryId::from(usable)),
            ),
            (uuid::Uuid::from_u128(1).to_string(), reserved(1)),
            (uuid::Uuid::from_u128(99).to_string(), reserved(99)),
            (
                uuid::Uuid::from_u128(100).to_string(),
                Ok(DirectoryId::from(uuid::Uuid::from_u128(100))),
            ),
        ];
        for (input, want) in cases {
            check!(parse_directory_id(&input) == want, "{input:?}");
        }
    }

    /// `--node-id` is required, as Kafka's `node.id` is: every
    /// `meta.properties` records it.
    #[test]
    fn node_id_is_required() {
        let missing = Cli::try_parse_from(["krabka-format", "--log-dir", "/a"])
            .map(|cli| cli.args.node_id)
            .map_err(|error| error.kind());
        check!(missing == Err(ErrorKind::MissingRequiredArgument));
        let given = Cli::try_parse_from(["krabka-format", "--log-dir", "/a", "--node-id", "3"])
            .map(|cli| cli.args.node_id)
            .map_err(|error| error.kind());
        check!(given == Ok(krabka_metadata::NodeId(3)));
    }
}
