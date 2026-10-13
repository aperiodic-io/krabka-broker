//! `krabka-worm-verify` — verify a WORM archive with read-only credentials.
//!
//! The auditor runs this against a bucket or a directory with no broker and no
//! cluster: everything it checks is written into the archive. It reads the
//! per-segment manifests, recomputes the partition hash chain, checks every
//! signature against a key the auditor supplies, and confirms that every object
//! a manifest names is present with the recorded size. `--deep` additionally
//! re-hashes every object body.
//!
//! Credentials come from the ambient AWS chain. There is deliberately no
//! `--access-key` flag: an auditor should hold a read-only role, not a copy of
//! the writer's keys.

use {
    clap::{Args, Parser, Subcommand},
    hex::FromHex as _,
    krabka_object_store::{
        IncompleteMultipartUpload, ObjectStoreConfig, S3Config, build_object_store,
        list_s3_multipart_uploads,
    },
    krabka_remote_storage::{
        ArchiveVerifyReport, ChainHead, PartitionVerifyReport, TrustedManifestKeys, VerifyDepth,
        VerifyRequest, verify_archive,
    },
    std::{collections::BTreeSet, fmt::Write as _, path::PathBuf, process::ExitCode, sync::Arc},
};

/// Orphan keys listed before the output is truncated.
const ORPHANS_SHOWN: usize = 10;

#[derive(Parser)]
#[command(
    name = "krabka-worm-verify",
    about = "Verify a Krabka WORM archive with read-only credentials"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Verify an archive's manifest chain, signatures, and objects.
    Verify(VerifyArgs),
}

#[derive(Args)]
struct VerifyArgs {
    /// Directory holding the archive, for a local or mounted copy.
    #[arg(long, value_name = "PATH", conflicts_with = "bucket")]
    local_dir: Option<PathBuf>,
    /// S3-compatible bucket holding the archive. Credentials come from the
    /// ambient AWS chain, so run this under a read-only role.
    #[arg(long, value_name = "NAME", required_unless_present = "local_dir")]
    bucket: Option<String>,
    /// AWS region. Any value serves for an S3-compatible endpoint that
    /// ignores it, and `us-east-1` is the usual placeholder.
    #[arg(long, value_name = "REGION", default_value = "us-east-1")]
    region: String,
    /// Custom S3 endpoint, for example `http://minio:9000`.
    #[arg(long, value_name = "URL")]
    endpoint: Option<String>,
    /// Allow plaintext HTTP, for an S3-compatible endpoint served without TLS.
    #[arg(long)]
    allow_http: bool,
    /// Key prefix inside the bucket or directory. Verifies everything when
    /// absent.
    #[arg(long, value_name = "PREFIX")]
    prefix: Option<String>,
    /// Verify only the partitions of this topic.
    #[arg(long, value_name = "TOPIC")]
    topic: Option<String>,
    /// Verify only this partition index.
    #[arg(long, value_name = "INDEX")]
    partition: Option<i32>,
    /// The key id that the `--public-key` in the same position holds the key
    /// for. Repeat both flags to trust several keys in one run, which is what
    /// an archive written across a key rotation needs.
    #[arg(long, value_name = "ID", requires = "public_key")]
    key_id: Vec<String>,
    /// Path to a trusted Ed25519 public key, raw 32 bytes. Pairs by position
    /// with `--key-id`, so the two flags must be repeated the same number of
    /// times.
    #[arg(long, value_name = "PATH", requires = "key_id")]
    public_key: Vec<PathBuf>,
    /// Chain head the newest manifest must produce, obtained outside the
    /// archive. Without it, tail truncation is undetectable: removing the
    /// newest manifests leaves a shorter chain that verifies perfectly. A
    /// successful run prints each partition's tip, so use that value here on
    /// the next run.
    #[arg(long, value_name = "HEX", value_parser = parse_head)]
    expect_head: Option<ChainHead>,
    /// Download every object and recompute its SHA-256. The only check that
    /// catches a body replaced with different bytes of the same length.
    #[arg(long)]
    deep: bool,
    #[command(flatten)]
    grading: GradingArgs,
}

/// How the run grades what the walk found.
///
/// Separate from the rest because these two say nothing about *what* to read
/// and everything about which findings are worth a non-zero exit: both cover a
/// state that is expected in one deployment and unacceptable in another.
#[derive(Debug, Args)]
struct GradingArgs {
    /// Accept a chain restart instead of grading it as an attestation hole.
    #[arg(long)]
    allow_epoch_restarts: bool,
    /// Grade orphan objects as a failure. They are reported either way.
    #[arg(long)]
    strict_orphans: bool,
}

/// Parses a chain head written as 64 hex characters.
fn parse_head(text: &str) -> Result<ChainHead, String> {
    <[u8; 32]>::from_hex(text)
        .map(ChainHead)
        .map_err(|_| format!("expected 64 hex characters, got `{text}`"))
}

#[tokio::main]
async fn main() -> ExitCode {
    // The subscriber writes to stderr so the graded verdict on stdout stays
    // clean for a script that reads it.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Verify(args) => run_verify(args).await,
    }
}

/// Builds the store and the trusted key set, verifies, and grades the report.
async fn run_verify(args: VerifyArgs) -> ExitCode {
    let trusted = match trusted_keys(&args) {
        Ok(trusted) => trusted,
        Err(message) => {
            eprintln!("error: {message}");
            return ExitCode::FAILURE;
        }
    };
    let OpenedStore { store, s3 } = match open_store(&args) {
        Ok(store) => store,
        Err(message) => {
            eprintln!("error: {message}");
            return ExitCode::FAILURE;
        }
    };
    let request = VerifyRequest {
        prefix: args.prefix.clone(),
        topic: args.topic.clone(),
        partition: args.partition,
        depth: if args.deep {
            VerifyDepth::Deep
        } else {
            VerifyDepth::Shallow
        },
        // The tip comparison happens below, so a tip mismatch grades as its own
        // outcome and is never reported as tampering.
        expect_head: None,
        allow_epoch_restarts: args.grading.allow_epoch_restarts,
        externally_authenticated_objects: BTreeSet::default(),
    };
    match verify_archive(&store, &request, &trusted).await {
        Ok(report) => {
            let uploads = match s3 {
                Some(s3) => list_s3_multipart_uploads(&s3, args.prefix.as_deref())
                    .await
                    .map_err(|error| error.to_string()),
                None => Ok(Vec::new()),
            };
            grade(&report, &args, uploads)
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Reads every trusted public key the run was given.
///
/// `--key-id` and `--public-key` pair by position, so an unequal count is a
/// usage error rather than a silently dropped key: trusting one key fewer than
/// the auditor meant to turns valid manifests into untrusted ones and reads as
/// a finding.
fn trusted_keys(args: &VerifyArgs) -> Result<TrustedManifestKeys, String> {
    if args.key_id.len() != args.public_key.len() {
        return Err(format!(
            "--key-id was given {} time(s) and --public-key {} time(s); they pair by position",
            args.key_id.len(),
            args.public_key.len()
        ));
    }
    let mut pairs = Vec::with_capacity(args.key_id.len());
    for (key_id, path) in args.key_id.iter().zip(&args.public_key) {
        let public_key =
            std::fs::read(path).map_err(|e| format!("read public key {}: {e}", path.display()))?;
        pairs.push((key_id.clone(), public_key));
    }
    Ok(TrustedManifestKeys::from_pairs(pairs))
}

/// The opened archive and, for a bucket, the config to list its uploads with.
#[derive(Debug)]
struct OpenedStore {
    store: Arc<dyn object_store::ObjectStore>,
    s3: Option<S3Config>,
}

/// Opens the archive named by `--local-dir` or by `--bucket`.
fn open_store(args: &VerifyArgs) -> Result<OpenedStore, String> {
    let config = match (args.local_dir.as_ref(), args.bucket.as_ref()) {
        (Some(root), _) => ObjectStoreConfig::Local { root: root.clone() },
        (None, Some(bucket)) => ObjectStoreConfig::S3(S3Config {
            bucket: bucket.clone(),
            region: args.region.clone(),
            endpoint: args.endpoint.clone(),
            allow_http: args.allow_http,
            ..Default::default()
        }),
        // clap enforces that one of the two is present.
        (None, None) => return Err("give either --local-dir or --bucket".to_string()),
    };
    let s3 = match &config {
        ObjectStoreConfig::S3(s3) => Some(s3.clone()),
        _ => None,
    };
    build_object_store(&config)
        .map(|store| OpenedStore { store, s3 })
        .map_err(|e| e.to_string())
}

/// Grades the report, prints the verdict, and returns the exit code.
fn grade(
    report: &ArchiveVerifyReport,
    args: &VerifyArgs,
    uploads: Result<Vec<IncompleteMultipartUpload>, String>,
) -> ExitCode {
    if let Some(found) = report.first_break() {
        let seq = found
            .seq
            .map_or_else(|| "unknown".to_string(), |seq| seq.to_string());
        eprintln!(
            "TAMPER DETECTED at {} (seq {seq}): {}",
            found.manifest_key, found.reason
        );
        for line in summary(report) {
            eprintln!("{line}");
        }
        return ExitCode::FAILURE;
    }

    if let Some(expected) = args.expect_head {
        // An archive with nothing left in it is the extreme of the same
        // attack, so an empty report is a mismatch and not a pass.
        let mismatch = tip_mismatch(report, expected);
        if mismatch.is_some() || report.partitions.is_empty() {
            let (name, tip) = mismatch.map_or_else(
                || ("(none found)".to_string(), "none".to_string()),
                |partition| {
                    (
                        partition.partition_dir.clone(),
                        partition
                            .head
                            .map_or_else(|| "none".to_string(), |head| head.to_string()),
                    )
                },
            );
            eprintln!("HEAD MISMATCH: expected {expected}, archive tip {tip}");
            eprintln!(
                "  partition {name}: the chain it holds is internally perfect but stops short of \
                 the expected head. This is what tail truncation looks like: an attacker who \
                 deletes the newest manifests, and the objects they name, leaves a shorter \
                 archive that verifies.",
            );
            return ExitCode::FAILURE;
        }
    }

    let orphans: Vec<&String> = report
        .global_orphan_objects
        .iter()
        .chain(
            report
                .partitions
                .iter()
                .flat_map(|partition| partition.orphan_objects.iter()),
        )
        .collect();
    if !orphans.is_empty() {
        report_orphans(&orphans, args.grading.strict_orphans);
        if args.grading.strict_orphans {
            return ExitCode::FAILURE;
        }
    }

    let restarts: usize = report
        .partitions
        .iter()
        .map(|partition| partition.epochs.len().saturating_sub(1))
        .sum();
    if restarts > 0 && !args.grading.allow_epoch_restarts {
        eprintln!("INCOMPLETE ATTESTATION: chain restarted {restarts} time(s)");
        eprintln!(
            "  A restart happens when the broker could not read back its chain tip, so it began \
             a new epoch at genesis rather than silently continuing the old chain. Nothing binds \
             the manifests before a restart to the manifests after it, so the archive is \
             attested in pieces."
        );
        eprintln!(
            "  Expected with the non-durable in-memory RLMM. The fix is a topic-backed \
             `remote_log_metadata`, which survives a broker restart. Pass \
             --allow-epoch-restarts to accept the restarts instead."
        );
        return ExitCode::FAILURE;
    }

    let unsigned = total(report, |partition| partition.unsigned_manifests);
    let untrusted = total(report, |partition| partition.untrusted_manifests);
    if unsigned > 0 || untrusted > 0 {
        eprintln!(
            "INCOMPLETE ATTESTATION: {unsigned} manifest(s) unsigned, {untrusted} signed by an \
             untrusted key"
        );
        if untrusted > 0 && args.public_key.is_empty() {
            eprintln!(
                "  No trusted key was given. Pass --key-id and --public-key to check the \
                 signatures the archive carries."
            );
        }
        return ExitCode::FAILURE;
    }

    let mut uploads = match uploads {
        Ok(uploads) => uploads,
        Err(error) => {
            eprintln!("error: cannot list incomplete multipart uploads: {error}");
            return ExitCode::FAILURE;
        }
    };
    if args.topic.is_some() || args.partition.is_some() {
        let dirs: Vec<&str> = report
            .partitions
            .iter()
            .map(|partition| partition.partition_dir.as_str())
            .collect();
        uploads.retain(|upload| upload_in_partition(&upload.key, &dirs));
    }
    if !uploads.is_empty() {
        eprintln!(
            "INCOMPLETE MULTIPART UPLOADS: {} upload(s) still hold parts",
            uploads.len()
        );
        for upload in uploads.iter().take(ORPHANS_SHOWN) {
            eprintln!("  {} ({})", upload.key, upload.upload_id);
        }
        return ExitCode::FAILURE;
    }

    // Carried by both verdicts below: a directory holding nothing but orphans
    // reports no manifests, and calling that "empty" would contradict the
    // objects just listed on stderr.
    let noted = if orphans.is_empty() {
        String::new()
    } else {
        format!(" ({} orphan object(s), see stderr)", orphans.len())
    };

    if report.manifests() == 0 {
        println!("OK: empty archive{noted}");
        return ExitCode::SUCCESS;
    }
    println!(
        "OK: {} manifests over {} partition(s), chain continuous, all signatures valid{noted}",
        report.manifests(),
        report.partitions.len()
    );
    for line in summary(report) {
        println!("{line}");
    }
    ExitCode::SUCCESS
}

fn upload_in_partition(key: &str, partition_dirs: &[&str]) -> bool {
    partition_dirs.iter().any(|dir| {
        key.strip_prefix(dir)
            .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

/// Prints the orphan objects and says what they mean under the chosen grading.
///
/// An orphan is an object no manifest names. The archiver writes the manifest
/// **last**, as the commit point, so a copy that died before sealing one leaves
/// exactly this, and the retry ran under a fresh segment id -- the debris is
/// inert, and no reader ever reaches it.
///
/// It is not fatal by default, and that is a deliberate reversal. A WORM
/// archive refuses deletes, so orphans can never be cleared: grading them a
/// failure makes one interrupted copy condemn the archive on every run
/// thereafter, with no action any operator could take to fix it. A verdict
/// nobody can act on is a verdict they stop reading, which costs more than the
/// debris does. `--strict-orphans` restores the hard grade for a deployment
/// that wants the bucket to hold nothing else.
///
/// What is lost by that default is stated rather than buried: orphans are also
/// what a *removed* manifest leaves behind, and only `--expect-head` settles
/// tail truncation.
fn report_orphans(orphans: &[&String], strict: bool) {
    eprintln!(
        "ORPHAN OBJECTS: {} object(s) with no manifest",
        orphans.len()
    );
    for key in orphans.iter().take(ORPHANS_SHOWN) {
        eprintln!("  {key}");
    }
    if let Some(hidden) = orphans.len().checked_sub(ORPHANS_SHOWN).filter(|n| *n > 0) {
        eprintln!("  ... and {hidden} more");
    }
    eprintln!(
        "  An object no manifest names makes no integrity claim: nothing signed says what its \
         bytes should be."
    );
    if strict {
        eprintln!("  Graded as a failure because --strict-orphans was given.");
        return;
    }
    eprintln!(
        "  Not graded as a failure. A copy that died before sealing its manifest leaves exactly \
         this, the retry used a fresh segment id, and nothing reads the remains. A WORM archive \
         cannot delete them, so no run after this one could ever come back clean."
    );
    eprintln!(
        "  Worth a look all the same: this is also what an object placed in the bucket by hand \
         looks like, and what a removed manifest leaves behind. Only --expect-head settles \
         truncation. Pass --strict-orphans to grade orphans a failure."
    );
}

/// The first partition whose tip is not `expected`.
fn tip_mismatch(
    report: &ArchiveVerifyReport,
    expected: ChainHead,
) -> Option<&PartitionVerifyReport> {
    report
        .partitions
        .iter()
        .find(|partition| partition.head != Some(expected))
}

/// Sums one per-partition count over the whole report.
fn total(report: &ArchiveVerifyReport, count: impl Fn(&PartitionVerifyReport) -> u64) -> u64 {
    report
        .partitions
        .iter()
        .map(count)
        .fold(0u64, u64::saturating_add)
}

/// One line per partition, naming the tip an operator feeds to `--expect-head`.
fn summary(report: &ArchiveVerifyReport) -> Vec<String> {
    let mut lines = Vec::new();
    for partition in &report.partitions {
        let tip = partition
            .head
            .map_or_else(|| "none".to_string(), |head| head.to_string());
        let start = partition
            .epochs
            .iter()
            .map(|epoch| epoch.start_offset)
            .min();
        let end = partition.epochs.iter().map(|epoch| epoch.end_offset).max();
        let offsets = match (start, end) {
            (Some(start), Some(end)) => format!("offsets {start}..{end}"),
            _ => "no offsets".to_string(),
        };
        let mut line = format!(
            "  {}: tip {tip}, {} manifest(s), {} object(s), {offsets}, {} epoch(s)",
            partition.partition_dir,
            partition.manifests,
            partition.objects_checked,
            partition.epochs.len()
        );
        if !partition.offset_gaps.is_empty() {
            let _ = write!(line, ", {} offset gap(s)", partition.offset_gaps.len());
        }
        if !partition.orphan_objects.is_empty() {
            let _ = write!(
                line,
                ", {} orphan object(s)",
                partition.orphan_objects.len()
            );
        }
        lines.push(line);
        lines.push(format!(
            "    create precondition: {}",
            protection(&partition.create_precondition_objects)
        ));
        lines.push(format!(
            "    bucket retention: {}",
            protection(&partition.bucket_retention_objects)
        ));
    }
    lines
}

fn protection(report: &krabka_remote_storage::ObjectProtectionReport) -> String {
    if report.count == 0 {
        "none".to_string()
    } else {
        format!(
            "{} object(s), sample: {}",
            report.count,
            report.sample.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, krabka_remote_storage::VerifyBreak, std::process::ExitCode};

    fn args(topic: Option<&str>) -> VerifyArgs {
        VerifyArgs {
            local_dir: Some(".".into()),
            bucket: None,
            region: "us-east-1".into(),
            endpoint: None,
            allow_http: false,
            prefix: None,
            topic: topic.map(str::to_string),
            partition: None,
            key_id: Vec::new(),
            public_key: Vec::new(),
            expect_head: None,
            deep: false,
            grading: GradingArgs {
                allow_epoch_restarts: false,
                strict_orphans: false,
            },
        }
    }

    fn report(first_break: Option<VerifyBreak>) -> ArchiveVerifyReport {
        ArchiveVerifyReport {
            partitions: vec![PartitionVerifyReport {
                partition_dir: "archive/orders-0-id".into(),
                manifests: 0,
                objects_checked: 0,
                create_precondition_objects: krabka_remote_storage::ObjectProtectionReport::default(
                ),
                bucket_retention_objects: krabka_remote_storage::ObjectProtectionReport::default(),
                epochs: Vec::new(),
                unsigned_manifests: 0,
                untrusted_manifests: 0,
                orphan_objects: Vec::new(),
                offset_gaps: Vec::new(),
                head: None,
                ok: first_break.is_none(),
                first_break,
            }],
            global_orphan_objects: Vec::new(),
        }
    }

    fn upload(key: &str) -> IncompleteMultipartUpload {
        IncompleteMultipartUpload {
            key: key.into(),
            upload_id: "upload-id".into(),
        }
    }

    #[test]
    fn protection_summary_prints_only_the_bounded_sample() {
        let report = krabka_remote_storage::ObjectProtectionReport {
            count: 1_000_000,
            sample: (0..10).map(|index| format!("key-{index}")).collect(),
        };

        let rendered = protection(&report);

        assert!(rendered.contains("1000000 object(s)"));
        assert!(rendered.contains("key-0"));
        assert!(rendered.contains("key-9"));
    }

    #[test]
    fn multipart_upload_filter_matches_whole_partition_directory() {
        let dirs = ["archive/orders-0-id"];

        assert!(upload_in_partition(
            "archive/orders-0-id/segment.log",
            &dirs
        ));
        assert!(!upload_in_partition(
            "archive/orders-0-id-other/segment.log",
            &dirs
        ));
        assert!(!upload_in_partition(
            "archive/payments-0-id/segment.log",
            &dirs
        ));
    }

    #[test]
    fn multipart_findings_are_filtered_and_graded() {
        let report = report(None);

        assert!(
            grade(
                &report,
                &args(Some("orders")),
                Ok(vec![upload("archive/payments-0-id/segment.log")])
            ) == ExitCode::SUCCESS
        );
        assert!(
            grade(
                &report,
                &args(Some("orders")),
                Ok(vec![upload("archive/orders-0-id/segment.log")])
            ) == ExitCode::FAILURE
        );
        assert!(grade(&report, &args(None), Err("denied".into())) == ExitCode::FAILURE);
    }

    /// `--key-id` and `--public-key` pair by position. An unequal count has to
    /// be a usage error: silently zipping the shorter of the two would drop a
    /// key the auditor meant to trust, and the manifests it signed would then
    /// be reported as "signed by an untrusted key" -- a finding that reads as
    /// tampering when it is really a typo in the command line.
    #[test]
    fn unequal_key_flag_counts_are_a_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        let key_path = dir.path().join("worm.pub");
        std::fs::write(&key_path, [7u8; 32]).unwrap();

        let mut too_many_ids = args(None);
        too_many_ids.key_id = vec!["k1".into(), "k2".into()];
        too_many_ids.public_key = vec![key_path.clone()];
        let error = trusted_keys(&too_many_ids).expect_err("2 ids and 1 key is rejected");
        assert!(error.contains("pair by position"), "{error}");
        assert!(error.contains("--key-id was given 2 time(s)"), "{error}");

        let mut too_many_keys = args(None);
        too_many_keys.key_id = vec!["k1".into()];
        too_many_keys.public_key = vec![key_path.clone(), key_path];
        let error = trusted_keys(&too_many_keys).expect_err("1 id and 2 keys is rejected");
        assert!(error.contains("pair by position"), "{error}");
    }

    /// The pairing is positional, so key ids and key bytes must not drift
    /// apart: a swap here would trust the rotated key's bytes under the
    /// original key's id and reject every manifest in the archive.
    #[test]
    fn each_key_id_is_paired_with_the_public_key_in_the_same_position() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.pub");
        let second = dir.path().join("second.pub");
        std::fs::write(&first, [1u8; 32]).unwrap();
        std::fs::write(&second, [2u8; 32]).unwrap();

        let mut args = args(None);
        args.key_id = vec!["first-id".into(), "second-id".into()];
        args.public_key = vec![first, second];

        let trusted = trusted_keys(&args).expect("both keys are read");

        assert!(trusted.get("first-id") == Some(&[1u8; 32][..]));
        assert!(trusted.get("second-id") == Some(&[2u8; 32][..]));
        assert!(trusted.get("absent-id").is_none());
    }

    /// An unreadable key file must name the path. Falling through with an
    /// empty trust set would downgrade a hard stop into an "untrusted key"
    /// finding on every manifest, which hides the real cause.
    #[test]
    fn an_unreadable_public_key_names_the_path_it_could_not_read() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("absent.pub");
        let mut args = args(None);
        args.key_id = vec!["k1".into()];
        args.public_key = vec![missing.clone()];

        let error = trusted_keys(&args).expect_err("a missing key file is an error");

        assert!(error.contains("read public key"), "{error}");
        assert!(error.contains(&missing.display().to_string()), "{error}");
    }

    /// No `--key-id` at all is legal: the run then checks structure and
    /// objects and reports every manifest as unsigned or untrusted rather
    /// than refusing to start.
    #[test]
    fn no_key_flags_yields_an_empty_trust_set() {
        let trusted = trusted_keys(&args(None)).expect("no keys is not an error");

        assert!(trusted.get("any-id").is_none());
    }

    /// `--expect-head` is the only check that catches tail truncation, so the
    /// parser must reject anything that is not a full 32-byte head rather than
    /// silently accepting a short or non-hex value.
    #[test]
    fn expect_head_accepts_only_sixty_four_hex_characters() {
        let head = "ab".repeat(32);

        assert!(parse_head(&head).expect("64 hex characters parse") == ChainHead([0xab; 32]));

        for bad in ["", &"ab".repeat(31), &format!("{head}ab"), &"zz".repeat(32)] {
            let error = parse_head(bad).expect_err("not 64 hex characters");
            assert!(error.contains("expected 64 hex characters"), "{error}");
        }
    }

    /// `--local-dir` wins over `--bucket`, and the S3 side must hand back the
    /// `S3Config` the multipart-upload listing needs: losing it turns the
    /// incomplete-upload check into a silent no-op against a real bucket.
    #[test]
    fn the_store_source_follows_local_dir_then_bucket() {
        let dir = tempfile::tempdir().unwrap();

        let mut local = args(None);
        local.local_dir = Some(dir.path().to_path_buf());
        local.bucket = Some("ignored".into());
        let OpenedStore { s3, .. } = open_store(&local).expect("a local directory opens");
        assert!(s3.is_none(), "a local run has no bucket to list uploads in");

        let mut bucket = args(None);
        bucket.local_dir = None;
        bucket.bucket = Some("archive-bucket".into());
        bucket.region = "eu-west-1".into();
        bucket.endpoint = Some("http://minio:9000".into());
        bucket.allow_http = true;
        let OpenedStore { s3, .. } = open_store(&bucket).expect("an S3 bucket opens");
        let s3 = s3.expect("the S3 config is carried through for the upload listing");
        assert!(s3.bucket == "archive-bucket");
        assert!(s3.region == "eu-west-1");
        assert!(s3.endpoint.as_deref() == Some("http://minio:9000"));
        assert!(s3.allow_http);

        let mut neither = args(None);
        neither.local_dir = None;
        neither.bucket = None;
        let error = open_store(&neither).expect_err("neither source is an error");
        assert!(error.contains("--local-dir or --bucket"), "{error}");
    }

    #[test]
    fn tampering_is_graded_before_multipart_errors() {
        let report = report(Some(VerifyBreak {
            manifest_key: "archive/orders-0-id/manifest".into(),
            seq: None,
            reason: "digest mismatch".into(),
        }));

        assert!(grade(&report, &args(None), Err("denied".into())) == ExitCode::FAILURE);
    }
}
