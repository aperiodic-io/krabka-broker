//! `krabka format` subcommand.
//!
//! Formats every log directory of a node, as `kafka-storage format` does:
//! each directory gets the same cluster id and its own directory id. The set
//! is `--metadata-log-dir`, the counterpart of Kafka's `metadata.log.dir`,
//! and every `--log-dir`, the entries of Kafka's `log.dirs`. Without
//! `--metadata-log-dir`, the first `--log-dir` is the metadata log directory,
//! as `metadata.log.dir` defaults to the first entry of `log.dirs`.
//!
//! ## Output
//!
//! Non-Raft metadata is written as a bootstrap stream. The active controller
//! writes it to the metadata log when it activates on an empty log, and a
//! broker never writes it. Dynamic KIP-853 modes also write the offset-zero
//! bootstrap checkpoint, and a controller takes the records from that
//! checkpoint first, as Kafka's does. Only the metadata log directory
//! receives them. Kafka trunk also writes its bootstrap snapshot only into a
//! metadata directory, and the controller reads the bootstrap records only
//! from there:
//!
//! - `bootstrap.json` — a human-readable manifest with the cluster id and a
//!   base64'd `serde_wincode` blob per metadata record.
//! - `bootstrap.records.bin` — the same records concatenated as
//!   length-prefixed `serde_wincode<SerdeCompat<MetadataRecord>>` payloads, so
//!   the controller can stream them without touching JSON.
//! - `__cluster_metadata-0/00000000000000000000-0000000000.checkpoint` — the
//!   KIP-630/KIP-853 bootstrap snapshot for dynamic membership, for a dynamic
//!   format only. It holds the control state, then the same records in the
//!   same order, as Kafka's `Formatter.writeBoostrapSnapshot` writes them. The
//!   path is Kafka's.
//!
//! Every directory receives Kafka's `meta.properties`, with the cluster id,
//! the node id, and the directory's own id, written last. A data directory
//! receives nothing else.
//!
//! ## Exit codes
//!
//! `kafka-storage format` exits 1 for every failure. `krabka format` keeps a
//! code per cause, so an orchestrator can tell an operator error from an I/O
//! fault:
//!
//! | Code | Cause |
//! | :--- | :--- |
//! | 0 | Every directory is formatted, or was already and `--ignore-formatted` is set. |
//! | 2 | An `--add-scram` iteration count is below 4096, or clap refused the command line. |
//! | 3 | A directory is already formatted without `--ignore-formatted`, holds files `format` did not write, or disagrees with the others on the cluster id. |
//! | 4 | A write failed, or the quorum flags name an invalid voter set. |
//! | 5 | A `--feature`, `--release-version`, or quorum-mode combination is invalid. |

use {
    krabka_metadata::{
        KRaftVersionRecord, MetadataRecord, ScramCredentialRecord, VoterSet, VotersRecord,
    },
    krabka_security::scram::{MIN_SCRAM_ITERATIONS, hash_scram_password_with_salt},
    ring::rand::{SecureRandom, SystemRandom},
    std::path::{Path, PathBuf},
};

mod acl;
mod args;
mod ensemble;
mod features;
mod output;
mod quorum;
mod scram;
#[cfg(test)]
mod tests;

pub use self::{
    args::{FormatArgs, ScramSpec, parse_node_id},
    features::LATEST_PRODUCTION_METADATA_VERSION,
    output::FAIL_AFTER_ENV,
};
use {
    self::{
        ensemble::{Ensemble, SurveyError, remove_partial_output},
        features::resolve_format_features,
        output::{Fault, write_bootstrap_files, write_dynamic_checkpoint, write_meta_properties},
        quorum::{build_initial_voters, is_dynamic_format},
    },
    crate::{
        ids::{ClusterId, DirectoryId},
        meta_properties::MetaProperties,
    },
};

/// Exit codes. The table in the module documentation gives the cause of each.
const EXIT_OK: i32 = 0;
const EXIT_LOW_ITERATIONS: i32 = 2;
const EXIT_DIRTY_LOG_DIR: i32 = 3;
const EXIT_BOOTSTRAP_FAIL: i32 = 4;
const EXIT_INVALID_FEATURE: i32 = 5;

/// Formats the metadata log directory and `args.log_dirs`, returning the
/// process exit code.
///
/// Every failure a caller can cause -- an unwritable directory, a malformed
/// `--add-scram` spec, an unknown feature -- is reported on stderr and returned
/// as a non-zero code rather than raised.
///
/// # Panics
///
/// Panics if `--initial-controllers` was given without the node's own identity
/// appearing in it. `is_dynamic_format` rejects that combination before this
/// point, so reaching the panic means that validation and this branch have
/// drifted apart.
pub async fn run(args: FormatArgs) -> i32 { run_with_records(args, Vec::new()).await }

// `async` matches the entry point in `main.rs`; the body is sync today
// (purely fs + crypto) but a real raft-log bootstrap would await tokio I/O.
// The body yields an `i32` (not a future), so `#[instrument]` is safe here
// w.r.t. `clippy::async_yields_async`.
/// Formats the metadata log directory and `args.log_dirs` with `extra` seeded
/// alongside the records the flags produce, returning the process exit code.
///
/// A cluster restored from tiered-storage archives has to come up with its
/// topics already present, so the restore tool hands the topic and partition
/// records it recovered to the formatter instead of repeating the bootstrap
/// write itself. The extra records join the one seed stream, so they reach both
/// the offset-zero checkpoint and the bootstrap files.
///
/// `extra` lands directly after the feature records and ahead of the
/// `--add-scram` and `--add-acl` records. The finalized feature levels,
/// `metadata.version` first, decide how every later record is read, so they
/// lead the stream and a topic record can only come after them. Seeded topics
/// ahead of seeded ACL entries then match the order a live cluster writes them
/// in, where a topic exists before an entry names it as a resource. The
/// KIP-853 control state -- the `KRaft` version and the initial voters -- is a
/// separate stream that the checkpoint applies before any of these.
///
/// Ordering inside `extra` is the caller's. A `MetadataImage` derives a topic's
/// partition count from the partition records that apply after it, so each
/// `TopicRecord` must come before its own partitions.
///
/// # Panics
///
/// Panics if `--initial-controllers` was given without the node's own identity
/// appearing in it. `is_dynamic_format` rejects that combination before this
/// point, so reaching the panic means that validation and this branch have
/// drifted apart.
#[tracing::instrument(
    level = "info",
    name = "cli.format",
    skip_all,
    fields(
        log_dirs = ?args.log_dirs,
        metadata_log_dir = ?args.metadata_log_dir,
        standalone = args.standalone,
        extra_records = extra.len(),
    )
)]
pub async fn run_with_records(args: FormatArgs, extra: Vec<MetadataRecord>) -> i32 {
    match plan(args, extra).and_then(|plan| plan.execute(&Fault::from_env())) {
        Ok(()) => EXIT_OK,
        Err((code, message)) => {
            eprintln!("{message}");
            code
        }
    }
}

/// A failed run: its exit code and the line it prints on stderr.
type Failure = (i32, String);

/// Prefixes `message` with the command name, as every krabka-specific
/// message is. Kafka's own messages are printed bare.
fn krabka(code: i32, message: impl std::fmt::Display) -> Failure {
    (code, format!("krabka format: {message}"))
}

/// What a run writes, and where. [`plan`] builds it without touching the
/// disk beyond reading it, so every validation failure leaves the directories
/// as they were.
struct Plan {
    cluster_id: ClusterId,
    /// `--node-id`, as the `int` that `meta.properties` records.
    node_id: i32,
    metadata_version: String,
    /// The directories to write, the metadata log directory first when it is
    /// one of them.
    targets: Vec<Target>,
    /// The formatted directories `--ignore-formatted` skips.
    skipped: Vec<PathBuf>,
    /// The directories whose `meta.properties` does not read.
    errors: Vec<PathBuf>,
    raft_control_records: Vec<MetadataRecord>,
    records: Vec<MetadataRecord>,
}

/// One directory to format.
#[derive(Debug, PartialEq, Eq)]
struct Target {
    dir: PathBuf,
    kind: DirectoryKind,
    directory_id: DirectoryId,
}

/// Kafka's `Formatter.DirectoryType`: what a directory holds, which decides
/// whether it gets the bootstrap files and the checkpoint, and how the run
/// describes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectoryKind {
    Data,
    StaticMetadata,
    DynamicMetadata,
    DynamicMetadataVoter,
}

impl DirectoryKind {
    /// Kafka's `DirectoryType.description`.
    fn description(self) -> &'static str {
        match self {
            Self::Data => "data directory",
            Self::StaticMetadata => "metadata directory",
            Self::DynamicMetadata => "dynamic metadata directory",
            Self::DynamicMetadataVoter => "dynamic metadata voter directory",
        }
    }

    /// Kafka's `DirectoryType.isMetadataDirectory`: the directory gets the
    /// bootstrap files.
    fn is_metadata(self) -> bool { self != Self::Data }

    /// The directory also gets the offset-zero checkpoint.
    fn is_dynamic_metadata(self) -> bool {
        matches!(self, Self::DynamicMetadata | Self::DynamicMetadataVoter)
    }
}

/// Validates the command line, reads the directories, and decides what to
/// write.
fn plan(args: FormatArgs, extra: Vec<MetadataRecord>) -> Result<Plan, Failure> {
    // KIP-584 / KIP-778 / KIP-1022 bootstrap: finalize each registered feature
    // at its `--feature` override, else its per-release default for the
    // resolved bootstrap metadata.version (`--feature metadata.version` >
    // `--release-version` > latest stable). A 4.0 format thus seeds
    // metadata.version, group.version, etc. at their 4.0 defaults so a fresh
    // cluster engages each feature with no manual step; a level-0 feature is
    // omitted (absent = disabled), matching `kafka-storage format`.
    let (bootstrap_mv, feature_overrides) = resolve_format_features(
        args.release_version.as_deref(),
        &args.feature,
        args.unstable_feature_versions_enable,
    )
    .map_err(|e| krabka(EXIT_INVALID_FEATURE, e))?;
    let metadata_version = krabka_metadata::metadata_version::from_feature_level(bootstrap_mv)
        .map_or_else(|| bootstrap_mv.to_string(), |mv| mv.ivn().to_owned());

    // The `kraft.version` rules come after the release and the feature names,
    // as in `Formatter.run`: it resolves the release, then checks the names in
    // `calculateEffectiveFeatureLevels`, which is where `kraft.version` is
    // reconciled with the quorum flags.
    let dynamic_format = is_dynamic_format(&args).map_err(|e| krabka(EXIT_INVALID_FEATURE, e))?;

    // `meta.properties` holds the node id as Kafka's `int`. `--node-id`
    // parses only that range, so this refuses nothing that clap accepted.
    let node_id = i32::try_from(args.node_id.0).map_err(|_| {
        krabka(
            EXIT_BOOTSTRAP_FAIL,
            "You must specify a valid non-negative node ID.",
        )
    })?;

    // KIP-853: this node's stable directory id for the metadata log
    // directory. The broker reads it back from `meta.properties` on every
    // boot; it is the identity component of every `Voter` this node ever
    // appears as.
    let generated_directory_id = args.directory_id.unwrap_or_else(DirectoryId::random);
    let initial_voters = build_initial_voters(&args, generated_directory_id)
        .map_err(|e| krabka(EXIT_BOOTSTRAP_FAIL, e))?;
    let metadata_directory_id = if args.initial_controllers.is_empty() {
        generated_directory_id
    } else {
        DirectoryId(
            initial_voters
                .get(args.node_id)
                .expect("validated local initial controller")
                .directory_id,
        )
    };
    if args.directory_id.is_some() && metadata_directory_id != generated_directory_id {
        return Err(krabka(
            EXIT_BOOTSTRAP_FAIL,
            "--directory-id must match the local --initial-controllers entry",
        ));
    }
    let metadata_kind = metadata_directory_kind(dynamic_format, &args, &initial_voters);

    // KIP-853 control records live in the offset-zero metadata checkpoint,
    // separate from the non-Raft bootstrap record stream.
    let mut raft_control_records = Vec::new();
    if dynamic_format {
        raft_control_records.push(MetadataRecord::V1KRaftVersion(KRaftVersionRecord {
            kraft_version: 1,
        }));
        if !initial_voters.is_empty() {
            raft_control_records.push(MetadataRecord::V1Voters(VotersRecord {
                voters: initial_voters,
            }));
        }
    }

    let mut records: Vec<MetadataRecord> =
        krabka_metadata::bootstrap_feature_records_with_overrides(bootstrap_mv, &feature_overrides);
    // Caller-seeded records (a restore's recovered topics) follow the feature
    // levels that decide how they are read, and precede the credential and ACL
    // records so a seeded ACL names a topic the image already holds.
    records.extend(extra);
    records.extend(scram_records(&args)?);
    records.extend(
        args.add_acl
            .into_iter()
            .map(MetadataRecord::V1AccessControlEntry),
    );

    let metadata_log_dir = args
        .metadata_log_dir
        .unwrap_or_else(|| args.log_dirs[0].clone());
    let log_dirs = directory_set(&metadata_log_dir, args.log_dirs);
    let ensemble = Ensemble::load(&log_dirs).map_err(|error| match error {
        SurveyError::Foreign(dir) => krabka(
            EXIT_DIRTY_LOG_DIR,
            format_args!(
                "refusing to overwrite non-empty log directory {}: it holds files krabka format \
                 did not write",
                dir.display()
            ),
        ),
        SurveyError::Io(dir, e) => krabka(
            EXIT_BOOTSTRAP_FAIL,
            format_args!("cannot read log directory {}: {e}", dir.display()),
        ),
    })?;
    if ensemble.errors.contains(&metadata_log_dir) {
        return Err((
            EXIT_DIRTY_LOG_DIR,
            format!(
                "Encountered I/O error in metadata log directory {}. Cannot continue.",
                metadata_log_dir.display()
            ),
        ));
    }
    let cluster_id = ensemble
        .verify(args.cluster_id, node_id)
        .map_err(|message| (EXIT_DIRTY_LOG_DIR, message))?
        .unwrap_or_else(ClusterId::random);
    if !args.ignore_formatted
        && let Some((first, _)) = ensemble.formatted.first()
    {
        return Err((
            EXIT_DIRTY_LOG_DIR,
            format!(
                "Log directory {} is already formatted. Use --ignore-formatted to ignore this \
                 directory and format the others.",
                first.display()
            ),
        ));
    }

    let mut used: Vec<DirectoryId> = ensemble
        .formatted
        .iter()
        .filter_map(|(_, meta)| meta.directory_id)
        .collect();
    let mut targets = Vec::with_capacity(ensemble.empty.len());
    for dir in ensemble.empty {
        let (kind, directory_id) = if dir == metadata_log_dir {
            (metadata_kind, metadata_directory_id)
        } else {
            (DirectoryKind::Data, DirectoryId::random_unused(&used))
        };
        used.push(directory_id);
        targets.push(Target {
            dir,
            kind,
            directory_id,
        });
    }
    if targets.is_empty() && ensemble.formatted.is_empty() {
        return Err((
            EXIT_BOOTSTRAP_FAIL,
            "No available log directories to format.".to_owned(),
        ));
    }

    Ok(Plan {
        cluster_id,
        node_id,
        metadata_version,
        targets,
        skipped: ensemble.formatted.into_iter().map(|(dir, _)| dir).collect(),
        errors: ensemble.errors,
        raft_control_records,
        records,
    })
}

/// The directories one run formats: `metadata_log_dir` first, then every
/// `log_dirs` entry in the order given.
///
/// This is Kafka's `StorageTool.configToLogDirectories`, which adds
/// `metadata.log.dir` to the `log.dirs` set. Kafka keeps the set in a
/// `TreeSet`, so a path named twice is formatted once. The metadata log
/// directory can also be one of `log_dirs`, and then it is formatted once, as
/// the metadata log directory.
fn directory_set(metadata_log_dir: &Path, log_dirs: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut set = Vec::with_capacity(log_dirs.len() + 1);
    set.push(metadata_log_dir.to_path_buf());
    for dir in log_dirs {
        if !set.contains(&dir) {
            set.push(dir);
        }
    }
    set
}

/// Kafka's `DirectoryType.calculate` for the metadata log directory.
fn metadata_directory_kind(
    dynamic_format: bool,
    args: &FormatArgs,
    initial_voters: &VoterSet,
) -> DirectoryKind {
    if !dynamic_format {
        DirectoryKind::StaticMetadata
    } else if initial_voters.contains(args.node_id) {
        DirectoryKind::DynamicMetadataVoter
    } else {
        DirectoryKind::DynamicMetadata
    }
}

/// Hashes each `--add-scram` credential. The hashing happens here, on the
/// formatter's side, with `hash_scram_password_with_salt` from
/// `krabka-security`, so the record on disk carries the stretched keys and
/// never the plain password.
fn scram_records(args: &FormatArgs) -> Result<Vec<MetadataRecord>, Failure> {
    let min = u32::try_from(MIN_SCRAM_ITERATIONS).expect("SCRAM minimum is positive");
    let mut records = Vec::with_capacity(args.add_scram.len());
    for spec in &args.add_scram {
        if spec.iterations < min {
            return Err(krabka(
                EXIT_LOW_ITERATIONS,
                format_args!(
                    "iterations must be >= {MIN_SCRAM_ITERATIONS}, got {} for user {}",
                    spec.iterations, spec.name,
                ),
            ));
        }
        let mut salt = vec![0u8; 16];
        SystemRandom::new()
            .fill(&mut salt)
            .map_err(|e| krabka(EXIT_BOOTSTRAP_FAIL, format_args!("rng failure: {e}")))?;
        let cred = hash_scram_password_with_salt(
            spec.password.as_bytes(),
            spec.mechanism,
            spec.iterations,
            salt,
        );
        records.push(MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: spec.name.clone(),
            mechanism: spec.mechanism,
            salt: cred.salt,
            stored_key: cred.stored_key,
            server_key: cred.server_key,
            iterations: cred.iterations,
        }));
    }
    Ok(records)
}

impl Plan {
    /// Writes every target directory, printing Kafka's progress lines.
    fn execute(self, fault: &Fault) -> Result<(), Failure> {
        for dir in &self.errors {
            println!(
                "I/O error trying to read log directory {}. Ignoring...",
                dir.display()
            );
        }
        for dir in &self.skipped {
            println!(
                "krabka format: {} is already formatted; leaving it alone",
                dir.display()
            );
        }
        if self.targets.is_empty() {
            println!("All of the log directories are already formatted.");
            return Ok(());
        }
        for target in &self.targets {
            println!(
                "Formatting {} {} with metadata.version {}.",
                target.kind.description(),
                target.dir.display(),
                self.metadata_version,
            );
            self.write(target, fault)
                .map_err(|e| krabka(EXIT_BOOTSTRAP_FAIL, e))?;
        }
        println!(
            "Formatted {} log director{} with cluster-id {} ({} seed record(s))",
            self.targets.len(),
            if self.targets.len() == 1 { "y" } else { "ies" },
            self.cluster_id,
            self.records.len(),
        );
        Ok(())
    }

    /// Writes one directory.
    ///
    /// Whatever an interrupted run left is removed first, and
    /// `meta.properties` is written last and published by a rename. Its
    /// presence thus means "a format ran here to completion". A run that
    /// fails partway -- an unwritable checkpoint, a killed process -- leaves a
    /// directory without it, which the next run treats as empty and formats
    /// again. Written first, the marker would make the next unconditional
    /// init-container run exit 0 on a directory with no seed checkpoint, no
    /// voter set, and no bootstrap records.
    fn write(&self, target: &Target, fault: &Fault) -> Result<(), String> {
        let dir = &target.dir;
        remove_partial_output(dir)
            .map_err(|e| format!("cannot clear log directory {}: {e}", dir.display()))?;
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("cannot create log directory {}: {e}", dir.display()))?;
        if target.kind.is_dynamic_metadata() {
            write_dynamic_checkpoint(
                dir,
                self.cluster_id,
                &self.raft_control_records,
                &self.records,
                fault,
            )
            .map_err(|e| format!("checkpoint failed: {e}"))?;
        }
        if target.kind.is_metadata() {
            write_bootstrap_files(dir, self.cluster_id, &self.records, fault)
                .map_err(|e| format!("bootstrap failed: {e}"))?;
        }
        let meta = MetaProperties {
            cluster_id: self.cluster_id,
            node_id: self.node_id,
            directory_id: Some(target.directory_id),
        };
        write_meta_properties(dir, &meta, fault)
    }
}
