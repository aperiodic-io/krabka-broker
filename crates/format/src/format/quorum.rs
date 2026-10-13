//! The KIP-853 quorum decision: whether a format is dynamic, and which voters
//! its initial voter set holds.
//!
//! `--standalone`, `--initial-controllers`, and `--no-initial-controllers` are
//! one choice expressed three ways, and each answers both questions together
//! with the `kraft.version` feature. The rules that reconcile them, and the
//! `id@host:port:directory-id` parse the explicit form needs, live here rather
//! than in the run that writes their result to the checkpoint.

use {
    super::args::FormatArgs,
    crate::ids::DirectoryId,
    krabka_metadata::{
        FeatureLevelRecord, KRaftVersionRange, Voter, VoterEndpoint, VoterSet,
        metadata_version::KRAFT_VERSION_FEATURE,
    },
    std::collections::BTreeSet,
    uuid::Uuid,
};

/// Resolve the KIP-853 format mode and validate its kraft.version selection.
///
/// The three explicit quorum flags select dynamic membership and therefore
/// imply level 1. Omitting all three retains the static level-0 path.
pub(super) fn is_dynamic_format(args: &FormatArgs) -> Result<bool, String> {
    let dynamic =
        args.standalone || !args.initial_controllers.is_empty() || args.no_initial_controllers;
    let mut requested = None;
    for FeatureLevelRecord { name, level } in &args.feature {
        if name != KRAFT_VERSION_FEATURE {
            continue;
        }
        if requested.replace(*level).is_some() {
            return Err("feature kraft.version specified more than once".into());
        }
    }

    // As in `Formatter.effectiveKRaftFeatureLevel`, the level is reconciled
    // with the quorum flags before `Feature.fromFeatureLevel` checks that the
    // feature defines it, so a level above 1 without a quorum flag is a mode
    // conflict and not a `No feature` error.
    match (dynamic, requested) {
        (true, Some(0)) => Err(
            "--standalone, --initial-controllers, and --no-initial-controllers require kraft.version=1"
                .into(),
        ),
        (false, Some(level)) if level != 0 => Err(format!(
            "kraft.version={level} requires --standalone, --initial-controllers, or --no-initial-controllers"
        )),
        (_, Some(level)) if !(0..=1).contains(&level) => Err(format!(
            "No feature:kraft.version with feature level {level}"
        )),
        (dynamic, _) => Ok(dynamic),
    }
}

/// Parse one `--initial-controllers` entry: `id@host:port:directory-id`,
/// whose endpoint is named `listener_name`.
///
/// The directory uuid is the trailing colon-delimited field, so we split
/// it off the right first, then peel `host:port` off the remainder. It is in
/// Kafka's base64 form, as `kafka-storage format` takes it, or the hyphenated
/// form.
fn parse_initial_controller(spec: &str, listener_name: &str) -> Result<Voter, String> {
    let (id_part, rest) = spec.split_once('@').ok_or("missing '@'")?;
    let id = krabka_metadata::NodeId(id_part.parse::<u64>().map_err(|_| "bad id")?);
    let (host_port, dir_part) = rest.rsplit_once(':').ok_or("missing directory uuid")?;
    let dir: Uuid = DirectoryId::parse_cli(dir_part)
        .map_err(|_| "bad directory uuid")?
        .into();
    if dir.is_nil() {
        return Err("directory uuid must not be nil".into());
    }
    let (host, port) = host_port.rsplit_once(':').ok_or("missing host:port")?;
    if host.is_empty() {
        return Err("host must not be empty".into());
    }
    let port: u16 = port.parse().map_err(|_| "bad port")?;
    if port == 0 {
        return Err("port must not be zero".into());
    }
    Ok(Voter {
        id,
        directory_id: dir,
        endpoints: vec![VoterEndpoint {
            name: listener_name.to_owned(),
            host: host.to_string(),
            port,
        }],
        kraft_version: KRaftVersionRange::default(),
    })
}

/// Derive the initial controller voter set from the format args.
///
/// - `--standalone`: a singleton set holding just this node (requires
///   `--controller-listener`).
/// - `--initial-controllers`: the explicitly-listed voters, which must
///   include this node.
/// - `--no-initial-controllers` or static mode: an empty set.
pub(super) fn build_initial_voters(
    args: &FormatArgs,
    directory_id: DirectoryId,
) -> Result<VoterSet, String> {
    if args.standalone {
        let listener = args
            .controller_listener
            .as_deref()
            .ok_or("--standalone requires --controller-listener")?;
        let (host, port) = listener
            .rsplit_once(':')
            .ok_or("--controller-listener must be host:port")?;
        if host.is_empty() {
            return Err("--controller-listener host must not be empty".into());
        }
        let port: u16 = port.parse().map_err(|_| "bad --controller-listener port")?;
        if port == 0 {
            return Err("--controller-listener port must not be zero".into());
        }
        Ok(VoterSet::from_voters([Voter {
            id: args.node_id,
            // `Voter.directory_id` is a raw `Uuid` (owned by `krabka_voters`);
            // unwrap the newtype at this crate boundary.
            directory_id: directory_id.into(),
            endpoints: vec![VoterEndpoint {
                name: args.controller_listener_name.clone(),
                host: host.to_string(),
                port,
            }],
            kraft_version: KRaftVersionRange::default(),
        }]))
    } else if !args.initial_controllers.is_empty() {
        let voters: Vec<_> = args
            .initial_controllers
            .iter()
            .map(|s| parse_initial_controller(s, &args.controller_listener_name))
            .collect::<Result<_, _>>()?;
        let mut node_ids = BTreeSet::new();
        let mut directory_ids = BTreeSet::new();
        for voter in &voters {
            if !node_ids.insert(voter.id) {
                return Err(format!("duplicate initial controller id {}", voter.id));
            }
            if !directory_ids.insert(voter.directory_id) {
                return Err(format!(
                    "duplicate initial controller directory id {}",
                    voter.directory_id
                ));
            }
        }
        let voters = VoterSet::from_voters(voters);
        if !voters.contains(args.node_id) {
            return Err(format!(
                "--initial-controllers does not contain local --node-id {}",
                args.node_id
            ));
        }
        Ok(voters)
    } else {
        Ok(VoterSet::default())
    }
}

#[cfg(test)]
mod tests {

    use {super::*, crate::Cli};

    #[test]
    fn parses_initial_controller_spec() {
        let v = parse_initial_controller(
            "3@host:9093:00000000-0000-0000-0000-000000000003",
            "CONTROLLER",
        )
        .unwrap();
        assert2::assert!(
            v == Voter {
                id: krabka_metadata::NodeId(3),
                directory_id: Uuid::from_u128(3),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".to_string(),
                    host: "host".to_string(),
                    port: 9093,
                }],
                kraft_version: KRaftVersionRange { min: 0, max: 1 },
            }
        );
    }

    /// The directory id takes Kafka's base64 form too, which is what
    /// `kafka-storage format --initial-controllers` takes.
    #[test]
    fn parses_initial_controller_spec_with_a_kafka_directory_id() {
        let v =
            parse_initial_controller("3@host:9093:AAAAAAAAAAAAAAAAAAAAAw", "CONTROLLER").unwrap();
        assert2::assert!(v.directory_id == Uuid::from_u128(3));
    }

    #[test]
    fn rejects_initial_controller_without_at() {
        assert2::assert!(parse_initial_controller("3:host:9093:uuid", "CONTROLLER").is_err());
    }

    #[test]
    fn rejects_initial_controller_bad_uuid() {
        assert2::assert!(parse_initial_controller("3@host:9093:not-a-uuid", "CONTROLLER").is_err());
    }

    /// A `kraft.version` level the feature does not define gets Kafka's
    /// `Feature.fromFeatureLevel` text.
    #[test]
    fn a_kraft_version_level_out_of_range_is_kafkas_no_feature_error() {
        use clap::Parser as _;

        for level in ["2", "-1", "9"] {
            let cli = Cli::try_parse_from([
                "krabka-format",
                "--log-dir",
                "/data",
                "--node-id",
                "1",
                "--no-initial-controllers",
                "--feature",
                &format!("kraft.version={level}"),
            ])
            .expect("parse");
            assert2::assert!(
                is_dynamic_format(&cli.args)
                    == Err(format!(
                        "No feature:kraft.version with feature level {level}"
                    ))
            );
        }
    }

    /// The voter endpoints carry the controller listener name, as
    /// `kafka-storage format` takes it from `controller.listener.names`, in
    /// Kafka's upper-case form. Without the flag the name is `CONTROLLER`.
    #[test]
    fn the_voter_endpoints_carry_the_controller_listener_name() {
        /// The quorum flags, the listener name flag, and the endpoint name
        /// written.
        struct Case<'a> {
            quorum: &'a [&'a str],
            name: &'a [&'a str],
            written: &'a str,
        }
        use clap::Parser as _;

        let directory = DirectoryId(Uuid::from_u128(3));
        let controllers = "3@host:9093:00000000-0000-0000-0000-000000000003";
        let cases = [
            Case {
                quorum: &["--standalone", "--controller-listener", "host:9093"],
                name: &[],
                written: "CONTROLLER",
            },
            Case {
                quorum: &["--standalone", "--controller-listener", "host:9093"],
                name: &["--controller-listener-name", "CONTROLLER_PLAINTEXT"],
                written: "CONTROLLER_PLAINTEXT",
            },
            Case {
                quorum: &["--initial-controllers", controllers],
                name: &["--controller-listener-name", "controller_plaintext"],
                written: "CONTROLLER_PLAINTEXT",
            },
            Case {
                quorum: &["--initial-controllers", controllers],
                name: &[],
                written: "CONTROLLER",
            },
        ];
        let named: Vec<Vec<String>> = cases
            .iter()
            .map(|Case { quorum, name, .. }| {
                let cli = Cli::try_parse_from(
                    ["krabka-format", "--log-dir", "/data", "--node-id", "3"]
                        .into_iter()
                        .chain(quorum.iter().copied())
                        .chain(name.iter().copied()),
                )
                .expect("parse");
                build_initial_voters(&cli.args, directory)
                    .expect("voters")
                    .iter()
                    .flat_map(|voter| voter.endpoints.iter().map(|endpoint| endpoint.name.clone()))
                    .collect()
            })
            .collect();
        let expected: Vec<Vec<String>> = cases
            .iter()
            .map(|Case { written, .. }| vec![(*written).to_owned()])
            .collect();
        assert2::assert!(named == expected);
    }

    #[test]
    fn parse_initial_controller_error_branches() {
        for bad in [
            "notanum@host:9093:00000000-0000-0000-0000-000000000003", // bad id
            "3@host9093",                                             // missing directory uuid
            "3@host:notaport:00000000-0000-0000-0000-000000000003",   // bad port
            "3@hostonly:00000000-0000-0000-0000-000000000003",        // missing host:port
        ] {
            assert2::assert!(parse_initial_controller(bad, "CONTROLLER").is_err());
        }
    }
}
