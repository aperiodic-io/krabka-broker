//! End-to-end grading of the `krabka-worm-verify` binary.
//!
//! Each test builds a WORM archive on disk the way a backend writes one, runs
//! the real binary against it with `--local-dir`, and checks both the exit code
//! and that the message names the cause. An exit code alone would not catch a
//! verifier that fails for the wrong reason.

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use assert2::check;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use krabka_audit::signing::FileEd25519Signer;
use krabka_remote_storage::{
    ChainHead, ChainStamp, EpochId, MANIFEST_SUFFIX, ManifestSeq, ObjectEntry, Sha256Digest,
    WormArchiver, WormChainRecord, manifest_head,
};
use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
use tempfile::TempDir;
use uuid::Uuid;

const TOPIC: &str = "orders";
const PARTITION: i32 = 0;
const KEY_ID: &str = "worm-key-1";
const ROTATED_KEY_ID: &str = "worm-key-2";
const SEGMENTS: usize = 2;
const SEGMENT_SPAN: i64 = 100;

/// Kafka renders UUIDs in URL-safe, unpadded Base64 for remote-tier paths.
fn uuid_b64(uuid: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(uuid.as_bytes())
}

krabka_macros::worm_segment_fixture!(
    metadata,
    krabka_remote_storage,
    0x2000,
    TOPIC,
    PARTITION,
    SEGMENT_SPAN
);

#[derive(Clone, Copy, Default)]
enum ManifestSigning {
    #[default]
    Signed,
    Unsigned,
}

#[derive(Clone, Copy, Default)]
enum SigningKey {
    #[default]
    Original,
    RotatedAfterFirstSegment,
}

#[derive(Clone, Copy, Default)]
struct ArchiveSetup {
    signing: ManifestSigning,
    key: SigningKey,
}

/// An archive on disk, plus the paths a test needs to damage it.
///
/// The trusted public key lives outside the archive root. An auditor's key is
/// not archive content, and a file dropped into the root would show up in the
/// listing as an object no manifest names.
struct Fixture {
    root: TempDir,
    /// Held only so the key directory outlives the run.
    _keys: TempDir,
    partition_dir: String,
    public_key: PathBuf,
    /// The second signer's public key, used only by the rotated fixture.
    rotated_public_key: PathBuf,
    /// The signing key, so a test can rewrite a manifest the archive already
    /// holds without the rewrite failing on the signature instead.
    pkcs8: Vec<u8>,
    tip: ChainHead,
    manifest_keys: Vec<String>,
    log_keys: Vec<String>,
    entries: Vec<Vec<ObjectEntry>>,
}

impl Fixture {
    /// A signed archive whose second manifest uses the rotated key.
    fn rotated() -> Self {
        Self::build(ArchiveSetup {
            key: SigningKey::RotatedAfterFirstSegment,
            ..Default::default()
        })
    }

    /// Write a two-segment archive, signed with the original key by default.
    fn build(setup: ArchiveSetup) -> Self {
        let root = TempDir::new().unwrap();
        let keys = TempDir::new().unwrap();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer = FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), KEY_ID.to_string())
            .expect("ring mints a valid PKCS#8 Ed25519 key");
        let public_key = keys.path().join("worm.pub");
        std::fs::write(&public_key, signer.public_key()).unwrap();
        let archiver = if matches!(setup.signing, ManifestSigning::Signed) {
            WormArchiver::new(Some(Arc::new(signer)))
        } else {
            WormArchiver::new(None)
        };
        let rotated_pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let rotated_signer =
            FileEd25519Signer::from_pkcs8_bytes(rotated_pkcs8.as_ref(), ROTATED_KEY_ID.to_string())
                .expect("ring mints a valid PKCS#8 Ed25519 key");
        let rotated_public_key = keys.path().join("worm-rotated.pub");
        std::fs::write(&rotated_public_key, rotated_signer.public_key()).unwrap();
        let rotated_archiver = WormArchiver::new(Some(Arc::new(rotated_signer)));

        let partition_dir = format!("{TOPIC}-{PARTITION}-{}", uuid_b64(Uuid::from_u128(1)));
        std::fs::create_dir_all(root.path().join(&partition_dir)).unwrap();

        let epoch = EpochId(Uuid::from_u128(0x77));
        let mut prev_head = ChainHead::GENESIS;
        let mut manifest_keys = Vec::new();
        let mut log_keys = Vec::new();
        let mut all_entries = Vec::new();
        for index in 0..SEGMENTS {
            let md = metadata(index);
            let stem = format!(
                "{partition_dir}/{:020}-{}",
                md.start_offset(),
                uuid_b64(md.remote_log_segment_id().id)
            );
            let entries = vec![
                write_object(root.path(), &format!("{stem}.log"), ".log"),
                write_object(root.path(), &format!("{stem}.index"), ".index"),
            ];
            log_keys.push(format!("{stem}.log"));
            manifest_keys.push(format!("{stem}{MANIFEST_SUFFIX}"));
            all_entries.push(entries.clone());
            let stamped = md.with_custom_metadata(
                WormChainRecord::request(ChainStamp {
                    epoch_id: epoch,
                    seq: ManifestSeq(u64::try_from(index).unwrap()),
                    prev_head,
                })
                .to_custom_metadata(),
            );
            // The rotation lands part-way through: the chain is unbroken, so
            // only the trusted-key set separates a clean run from an
            // untrusted one.
            let sealed = if matches!(setup.key, SigningKey::RotatedAfterFirstSegment) && index > 0 {
                rotated_archiver.seal(&stamped, entries).unwrap()
            } else {
                archiver.seal(&stamped, entries).unwrap()
            };
            std::fs::write(
                root.path().join(format!("{stem}{MANIFEST_SUFFIX}")),
                &sealed.bytes,
            )
            .unwrap();
            prev_head = manifest_head(&sealed.manifest.body);
        }

        Self {
            root,
            _keys: keys,
            partition_dir,
            public_key,
            rotated_public_key,
            pkcs8: pkcs8.as_ref().to_vec(),
            tip: prev_head,
            manifest_keys,
            log_keys,
            entries: all_entries,
        }
    }

    fn add_orphan(&self) -> String {
        let stray = format!("{}/stray.bin", self.partition_dir);
        std::fs::write(self.root().join(&stray), b"nothing names me").unwrap();
        stray
    }

    /// Rewrites the newest manifest as the first manifest of a fresh chain run.
    ///
    /// This is what a broker writes when it cannot read back its chain tip: a
    /// new epoch at genesis, rather than a silent restart of the old chain.
    fn restart_chain(&self) {
        let index = SEGMENTS - 1;
        let signer = FileEd25519Signer::from_pkcs8_bytes(&self.pkcs8, KEY_ID.to_string()).unwrap();
        let stamped = metadata(index).with_custom_metadata(
            WormChainRecord::request(ChainStamp {
                epoch_id: EpochId(Uuid::from_u128(0x88)),
                seq: ManifestSeq(0),
                prev_head: ChainHead::GENESIS,
            })
            .to_custom_metadata(),
        );
        let sealed = WormArchiver::new(Some(Arc::new(signer)))
            .seal(&stamped, self.entries[index].clone())
            .unwrap();
        std::fs::write(self.root().join(&self.manifest_keys[index]), &sealed.bytes).unwrap();
    }

    fn root(&self) -> &Path {
        self.root.path()
    }

    /// The arguments every test shares: the archive and the trusted key.
    fn base_args(&self) -> Vec<String> {
        vec![
            "verify".to_string(),
            "--local-dir".to_string(),
            self.root().display().to_string(),
            "--key-id".to_string(),
            KEY_ID.to_string(),
            "--public-key".to_string(),
            self.public_key.display().to_string(),
        ]
    }

    /// The second `--key-id` / `--public-key` pair of a rotated archive.
    fn rotated_key_args(&self) -> Vec<String> {
        vec![
            "--key-id".to_string(),
            ROTATED_KEY_ID.to_string(),
            "--public-key".to_string(),
            self.rotated_public_key.display().to_string(),
        ]
    }
}

/// Writes one archived object and returns the manifest entry for it.
fn write_object(root: &Path, key: &str, suffix: &str) -> ObjectEntry {
    let body = format!("body of {key}").into_bytes();
    std::fs::write(root.join(key), &body).unwrap();
    ObjectEntry {
        suffix: suffix.to_string(),
        key: key.to_string(),
        size_bytes: u64::try_from(body.len()).unwrap(),
        sha256: Sha256Digest::of(&body),
        e_tag: None,
        version_id: None,
        create_precondition: true,
    }
}

/// What one run of the binary produced.
struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

fn run(args: &[String]) -> Run {
    let output = Command::new(env!("CARGO_BIN_EXE_krabka-worm-verify"))
        .args(args)
        .output()
        .expect("the verify binary runs");
    Run {
        code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

#[test]
fn a_clean_archive_verifies_and_prints_its_tip() {
    let fixture = Fixture::build(ArchiveSetup::default());

    let result = run(&fixture.base_args());

    check!(result.code == Some(0), "stderr: {}", result.stderr);
    check!(
        result
            .stdout
            .contains("OK: 2 manifests over 1 partition(s)")
    );
    check!(
        result
            .stdout
            .contains("chain continuous, all signatures valid")
    );
    // The tip is printed so an operator can feed it to `--expect-head`.
    check!(result.stdout.contains(&fixture.tip.to_string()));
    check!(result.stdout.contains(&fixture.partition_dir));
    check!(result.stdout.contains("create precondition:"));
    check!(result.stdout.contains(&fixture.log_keys[0]));
    check!(result.stdout.contains("bucket retention: none"));
}

#[test]
fn the_printed_tip_satisfies_expect_head_on_the_next_run() {
    let fixture = Fixture::build(ArchiveSetup::default());
    let mut args = fixture.base_args();
    args.push("--expect-head".to_string());
    args.push(fixture.tip.to_string());

    let result = run(&args);

    check!(result.code == Some(0), "stderr: {}", result.stderr);
}

#[test]
fn a_truncated_object_is_reported_as_tampering() {
    let fixture = Fixture::build(ArchiveSetup::default());
    let victim = fixture.root().join(&fixture.log_keys[1]);
    let body = std::fs::read(&victim).unwrap();
    std::fs::write(&victim, &body[..body.len() - 1]).unwrap();

    let result = run(&fixture.base_args());

    check!(result.code == Some(1), "stdout: {}", result.stdout);
    check!(result.stderr.contains("TAMPER DETECTED"));
    check!(result.stderr.contains(&fixture.log_keys[1]));
    check!(result.stderr.contains("the manifest records a size of"));
}

#[test]
fn a_same_length_body_edit_needs_deep_to_be_seen() {
    let fixture = Fixture::build(ArchiveSetup::default());
    let victim = fixture.root().join(&fixture.log_keys[1]);
    let mut body = std::fs::read(&victim).unwrap();
    body[0] ^= 0xff;
    std::fs::write(&victim, &body).unwrap();

    let shallow = run(&fixture.base_args());
    check!(shallow.code == Some(0), "stderr: {}", shallow.stderr);

    let mut args = fixture.base_args();
    args.push("--deep".to_string());
    let deep = run(&args);

    check!(deep.code == Some(1), "stdout: {}", deep.stdout);
    check!(deep.stderr.contains("TAMPER DETECTED"));
    check!(deep.stderr.contains("hashes to"));
}

#[test]
fn an_unsigned_archive_is_an_incomplete_attestation() {
    let fixture = Fixture::build(ArchiveSetup {
        signing: ManifestSigning::Unsigned,
        ..Default::default()
    });

    let result = run(&fixture.base_args());

    check!(result.code == Some(1), "stdout: {}", result.stdout);
    check!(result.stderr.contains("INCOMPLETE ATTESTATION"));
    check!(result.stderr.contains("2 manifest(s) unsigned"));
}

#[test]
fn a_tip_that_differs_from_expect_head_is_a_head_mismatch() {
    let fixture = Fixture::build(ArchiveSetup::default());
    let mut args = fixture.base_args();
    let expected = ChainHead([0x5a; 32]);
    args.push("--expect-head".to_string());
    args.push(expected.to_string());

    let result = run(&args);

    check!(result.code == Some(1), "stdout: {}", result.stdout);
    check!(result.stderr.contains("HEAD MISMATCH"));
    check!(result.stderr.contains(&expected.to_string()));
    check!(result.stderr.contains(&fixture.tip.to_string()));
    check!(result.stderr.contains("tail truncation"));
}

/// An orphan is reported in full and does not fail the run.
///
/// A WORM archive refuses deletes, so an orphan can never be cleared. Failing
/// on one would mean a single interrupted copy condemns the archive on every
/// run from then on, with no action anyone could take -- and a verdict nobody
/// can act on is one they stop reading.
#[test]
fn an_object_no_manifest_names_is_reported_but_does_not_fail_the_run() {
    let fixture = Fixture::build(ArchiveSetup::default());
    let stray = fixture.add_orphan();

    let result = run(&fixture.base_args());

    check!(result.code == Some(0), "stderr: {}", result.stderr);
    check!(
        result
            .stderr
            .contains("ORPHAN OBJECTS: 1 object(s) with no manifest")
    );
    check!(result.stderr.contains(&stray));
    check!(result.stderr.contains("Not graded as a failure"));
    // The verdict on stdout has to carry it too: a script that reads only the
    // exit code and the OK line must not be told the bucket is spotless.
    check!(
        result.stdout.contains("1 orphan object(s)"),
        "stdout: {}",
        result.stdout
    );
}

/// `--strict-orphans` restores the hard grade for a deployment that wants the
/// bucket to hold nothing but the archive.
#[test]
fn strict_orphans_grades_an_orphan_as_a_failure() {
    let fixture = Fixture::build(ArchiveSetup::default());
    fixture.add_orphan();

    let mut args = fixture.base_args();
    args.push("--strict-orphans".to_string());
    let result = run(&args);

    check!(result.code == Some(1), "stdout: {}", result.stdout);
    check!(
        result
            .stderr
            .contains("ORPHAN OBJECTS: 1 object(s) with no manifest")
    );
    check!(result.stderr.contains("--strict-orphans was given"));
}

#[test]
fn an_empty_archive_verifies() {
    let root = TempDir::new().unwrap();

    let result = run(&[
        "verify".to_string(),
        "--local-dir".to_string(),
        root.path().display().to_string(),
    ]);

    check!(result.code == Some(0), "stderr: {}", result.stderr);
    check!(result.stdout.contains("OK: empty archive"));
}

#[test]
fn naming_neither_a_bucket_nor_a_directory_is_a_usage_error() {
    let result = run(&["verify".to_string()]);

    check!(result.code == Some(2), "stdout: {}", result.stdout);
    check!(result.stderr.contains("--bucket"));
}

#[test]
fn a_chain_restart_is_an_incomplete_attestation_that_names_its_fix() {
    let fixture = Fixture::build(ArchiveSetup::default());
    fixture.restart_chain();

    let result = run(&fixture.base_args());

    check!(result.code == Some(1), "stdout: {}", result.stdout);
    check!(
        result
            .stderr
            .contains("INCOMPLETE ATTESTATION: chain restarted 1 time(s)")
    );
    // The message has to name the cause and the fix, not just the symptom.
    check!(result.stderr.contains("could not read back its chain tip"));
    check!(result.stderr.contains("remote_log_metadata"));
    check!(result.stderr.contains("--allow-epoch-restarts"));
}

#[test]
fn allow_epoch_restarts_accepts_a_restarted_chain() {
    let fixture = Fixture::build(ArchiveSetup::default());
    fixture.restart_chain();
    let mut args = fixture.base_args();
    args.push("--allow-epoch-restarts".to_string());

    let result = run(&args);

    check!(result.code == Some(0), "stderr: {}", result.stderr);
    check!(result.stdout.contains("2 epoch(s)"));
}

#[test]
fn an_emptied_archive_still_fails_against_an_expected_head() {
    let root = TempDir::new().unwrap();

    let result = run(&[
        "verify".to_string(),
        "--local-dir".to_string(),
        root.path().display().to_string(),
        "--expect-head".to_string(),
        ChainHead([0x5a; 32]).to_string(),
    ]);

    check!(result.code == Some(1), "stdout: {}", result.stdout);
    check!(result.stderr.contains("HEAD MISMATCH"));
    check!(result.stderr.contains("archive tip none"));
}

/// One run over an archive that spans a key rotation, with both keys trusted.
///
/// This is the case the single-pair command line could not express: every
/// manifest is signed, but under two different key ids, and a run naming one
/// of them grades the other half as untrusted.
#[test]
fn both_key_pairs_verify_an_archive_that_spans_a_rotation() {
    let fixture = Fixture::rotated();
    let mut args = fixture.base_args();
    args.extend(fixture.rotated_key_args());

    let result = run(&args);

    check!(result.code == Some(0), "stderr: {}", result.stderr);
    check!(!result.stderr.contains("INCOMPLETE ATTESTATION"));
    check!(
        result
            .stdout
            .contains("chain continuous, all signatures valid")
    );
}

#[test]
fn one_key_pair_leaves_the_rotated_half_untrusted() {
    let fixture = Fixture::rotated();

    let result = run(&fixture.base_args());

    check!(result.code == Some(1), "stdout: {}", result.stdout);
    check!(
        result.stderr.contains("INCOMPLETE ATTESTATION"),
        "stderr: {}",
        result.stderr
    );
    check!(result.stderr.contains("1 signed by an untrusted key"));
}

#[test]
fn an_unpaired_key_flag_is_rejected_rather_than_dropped() {
    let fixture = Fixture::rotated();
    let mut args = fixture.base_args();
    args.push("--key-id".to_string());
    args.push(ROTATED_KEY_ID.to_string());

    let result = run(&args);

    check!(result.code == Some(1), "stdout: {}", result.stdout);
    check!(
        result.stderr.contains("pair by position"),
        "stderr: {}",
        result.stderr
    );
}
