//! The WORM archive against a real Object-Lock bucket.
//!
//! Every other WORM test proves that Krabka never issues a delete. That is a
//! statement about Krabka, and an auditor does not have to believe it. This
//! suite proves the other half: the bucket itself refuses the delete, so the
//! archive survives an operator who does issue one. `MinIO` with Object Lock in
//! compliance mode is the S3 implementation under test.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.

mod jvm_acceptance;
mod support;

use std::process::Command;

use assert2::{assert, check};
use jvm_acceptance::*;
use krabka_audit::signing::FileEd25519Signer;
use krabka_log::LogConfig;
use ring::{rand::SystemRandom, signature::Ed25519KeyPair};

/// Object-Lock bucket for this suite. It is not [`MINIO_BUCKET`]: that bucket
/// is the ordinary mutable tier, and a locked bucket behaves differently enough
/// that sharing the name would mislead whoever reads a failure.
const BUCKET: &str = "krabka-worm-locked";

/// Key id the archive records in every signature, and the id the verifier
/// looks the trusted public key up under.
const KEY_ID: &str = "worm-itest-key";

const TOPIC: &str = "krabka-worm-lock-itest";

/// 200 records of about 30 bytes each, so the broker's `TIERED_SEGMENT_SIZE`
/// default rolls several sealed segments and the copy path runs more than
/// once. The same fixture the `jvm_acceptance_tiered` suites use.
const RECORDS: usize = 200;

// Same multi-thread caveat as the other container suites: blocking
// `Command::output()` calls would starve the broker accept loop on a
// single-threaded runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn object_lock_bucket_refuses_delete_of_an_archived_segment() {
    let minio_port = minio_port();
    let _minio = MinioContainer::start();
    minio_make_locked_bucket(BUCKET);

    // The signing key is a throwaway PKCS#8 Ed25519 key in a temp file, minted
    // the way `krabka_audit::signing`'s own tests mint one. The broker reads
    // the file; the test keeps the public half to verify with.
    let key_dir = tempfile::tempdir().expect("tempdir for the signing key");
    let key_path = key_dir.path().join("worm-signing.pk8");
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("generate PKCS#8 key");
    std::fs::write(&key_path, pkcs8.as_ref()).expect("write the signing key");
    let public_key = FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), KEY_ID.to_string())
        .expect("ring mints a valid PKCS#8 Ed25519 key")
        .public_key();

    let s3 = krabka_remote_storage::S3Config {
        bucket: BUCKET.to_string(),
        region: "us-east-1".to_string(),
        prefix: None,
        endpoint: Some(format!("http://127.0.0.1:{minio_port}")),
        access_key_id: Some(MINIO_ACCESS_KEY.to_string()),
        secret_access_key: Some(MINIO_SECRET_KEY.to_string()),
        allow_http: true,
        // A threshold well above the 2 KiB test segments keeps every object on
        // the single-PUT path, which is the only path `PutMode::Create` binds:
        // `object_store` 0.13's `PutMultipartOptions` has no mode field. The
        // multipart branch has its own coverage in `jvm_acceptance_tiered`.
        multipart_threshold: 8 * 1024 * 1024,
        multipart_chunk_size: 1024,
        // The two integrity knobs the ordinary tiered suites pin off. This is
        // the suite that covers them on.
        conditional_put: true,
        checksum_sha256: true,
        // The request bounds are not what this suite is about; take the
        // backend's own defaults rather than restating them here.
        ..krabka_remote_storage::S3Config::default()
    };

    let (broker, _dir) = start_worm_broker(s3.clone(), &key_path).await;
    nc_check_connectivity();

    create_tiered_topic(&broker, TOPIC).await;
    produce_records(TOPIC, RECORDS);

    // Wait for the copy pass to land at least two sealed segments, the same
    // gate the tiered suites use, and then for each copy to reach its commit
    // point.
    wait_for_minio_segments(BUCKET, 2).await;
    let listing = wait_for_sealed_manifests(BUCKET, 2).await;

    // A manifest beside every segment, under the segment's own key. Without
    // one the archive is only objects: nothing signed says what their bytes
    // should be.
    let log_keys = object_keys_with_extension(&listing, "log");
    assert!(
        log_keys.len() >= 2,
        "expected >=2 archived `.log` objects. Bucket listing:\n{listing}"
    );
    for key in &log_keys {
        let stem = key
            .strip_suffix(".log")
            .expect("the key was selected by its `.log` extension");
        let manifest = format!("{stem}.manifest");
        assert!(
            listing.contains(manifest.as_str()),
            "no `{manifest}` beside `{key}`. Bucket listing:\n{listing}"
        );
    }

    // `write_only` is false, so the archive is still a readable tier. Offsets
    // whose local segment the retention pass already evicted come back from
    // MinIO, which is what makes this a read through the WORM backend.
    let consumed = consume_records(TOPIC, RECORDS, 20_000, broker0_advertised());
    assert!(
        consumed >= RECORDS,
        "expected >={RECORDS} records back through the WORM archive, got {consumed}"
    );

    // The point of the suite. Delete every version of one archived segment
    // body, with credentials that have every right the broker has. Compliance
    // mode must refuse it.
    //
    // `--versions` matters: the bucket is versioned, and a plain `mc rm`
    // writes a delete marker instead of removing the locked version. A delete
    // marker succeeds and proves nothing.
    let victim = log_keys.first().expect("the listing holds a `.log` object");
    let removal = minio_rm_all_versions(BUCKET, victim);
    let stdout = String::from_utf8_lossy(&removal.stdout);
    let stderr = String::from_utf8_lossy(&removal.stderr);
    check!(
        !removal.status.success(),
        "MinIO accepted a delete of the locked object {victim}; Object Lock in compliance mode \
         must refuse it. stdout={stdout}, stderr={stderr}"
    );

    // Exit code alone would not settle it: a tool can fail after the server
    // already removed the bytes. The object still being listed is the
    // observable that matters.
    let after = minio_list_objects(BUCKET);
    assert!(
        after.contains(victim.as_str()),
        "{victim} is gone after the delete attempt; the bucket did not hold it. stdout={stdout}, \
         stderr={stderr}\nBucket listing:\n{after}"
    );

    // Stop the broker before the audit, so no copy is in flight and the
    // archive the verifier reads is the finished one.
    broker.shutdown().await;

    // The auditor's run, in process: recompute every chain head, check every
    // signature against the key the broker signed with, and re-hash every
    // object body. `Deep` is the only depth that catches a body replaced with
    // different bytes of the same length.
    let store =
        krabka_object_store::build_object_store(&krabka_object_store::ObjectStoreConfig::S3(s3))
            .expect("build the verifier's object store");
    let trusted =
        krabka_remote_storage::TrustedManifestKeys::single(KEY_ID.to_string(), public_key);
    let request = krabka_remote_storage::VerifyRequest {
        depth: krabka_remote_storage::VerifyDepth::Deep,
        ..krabka_remote_storage::VerifyRequest::default()
    };
    let report = krabka_remote_storage::verify_archive(&store, &request, &trusted)
        .await
        .expect("the archive is readable");
    check!(
        report.ok(),
        "the archive did not verify: {:?}",
        report.first_break()
    );
    check!(
        report.fully_attested(),
        "every manifest must be signed by the trusted key; report={report:?}"
    );
    assert!(
        report.manifests() >= 2,
        "expected the audit to cover the archived segments; report={report:?}"
    );

    // `_minio` is dropped here; the container is removed with `docker rm -f`.
}

/// Boot a broker whose S3 tier is a WORM archive signed by the key at
/// `key_path`.
///
/// This is [`start_host_broker_with_minio_tier`] plus
/// [`krabka_broker::BrokerConfig::remote_storage_worm`], which that helper does
/// not carry. The caller must keep the returned temp dir alive: it is the
/// broker's `log.dir`.
async fn start_worm_broker(
    s3: krabka_remote_storage::S3Config,
    key_path: &std::path::Path,
) -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    crate::support::init_tracing_with("krabka_broker=debug,info");
    let dir = tempfile::tempdir().expect("tempdir for log.dir");
    let listen_addr: std::net::SocketAddr = broker0_listen().parse().expect("allocated addr");
    let controller_addr: std::net::SocketAddr =
        controller_addr_0().parse().expect("allocated addr");
    let config = krabka_broker::BrokerConfig {
        // `create_tiered_topic` sends no segment override — the JVM
        // `TopicCommand` in the image it uses cannot name a sub-1-MiB one —
        // so the topic inherits this. See `TIERED_SEGMENT_SIZE`.
        log_config: LogConfig {
            segment_size: TIERED_SEGMENT_SIZE,
            ..LogConfig::default()
        },
        remote_storage_backend: Some(krabka_broker::RemoteStorageBackend::S3(s3)),
        remote_storage_worm: Some(krabka_remote_storage::WormConfig {
            signing_key_path: Some(key_path.to_path_buf()),
            signing_key_id: Some(KEY_ID.to_string()),
            // The archive stays readable, so the consume assertion above
            // exercises the WORM backend's fetch path rather than its refusal.
            write_only: false,
        }),
        // 1 s tick, so the sealed segments reach the archive and the
        // local-retention pass evicts them inside the test's wall clock.
        remote_log_manager_interval: krabka_units::secs(1),
        // One process, one run, so the in-memory manager holds the chain tip
        // for the whole test. A restart is what would start a new epoch, and
        // this suite never restarts.
        remote_log_metadata: krabka_broker::RlmmKind::InMemory,
        ..support::jvm_broker_config(
            dir.path().to_path_buf(),
            crate::support::JvmBrokerSetup {
                listen: listen_addr,
                controller: controller_addr,
                advertised: (broker0_advertised()).to_owned(),
                voters: crate::support::controller_voters(&[(1, controller_addr)]),
                ..Default::default()
            },
        )
    };
    let handle = krabka_broker::Broker::start(config)
        .await
        .expect("start the WORM broker");
    eprintln!(
        "KRABKA[test] WORM broker started listen={listen} advertised={advertised}",
        listen = broker0_listen(),
        advertised = broker0_advertised()
    );
    (handle, dir)
}

/// Poll the bucket until every archived `.log` has a `.manifest` beside it,
/// and at least `min_segments` segments are archived. Returns the listing.
///
/// A copy writes its manifest last, because the manifest is the commit point.
/// A listing taken the moment a `.log` appears can therefore be one manifest
/// short, and that is the copy still running rather than a missing manifest.
/// The poll runs at 500 ms intervals for up to 20 s.
///
/// # Panics
///
/// Panics when the archive never reaches that state inside the window.
async fn wait_for_sealed_manifests(bucket: &str, min_segments: usize) -> String {
    let mut listing = String::new();
    for _ in 0..40 {
        listing = minio_list_objects(bucket);
        let logs = object_keys_with_extension(&listing, "log").len();
        let manifests = object_keys_with_extension(&listing, "manifest").len();
        if logs >= min_segments && manifests >= logs {
            return listing;
        }
        // intentional: bounded poll of an external process, MinIO under
        // `mc ls`. No krabka metric reflects object arrival in the bucket.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!(
        "the archive never sealed a manifest for every one of >={min_segments} segments. Bucket \
         listing:\n{listing}"
    )
}

/// The object keys in an `mc ls --recursive` listing that end in
/// `.{extension}`, in listing order.
fn object_keys_with_extension(listing: &str, extension: &str) -> Vec<String> {
    listing
        .lines()
        .filter(|line| has_extension(line, extension))
        .filter_map(object_key)
        .collect()
}

/// Whether an `mc ls` line names an object with this extension.
///
/// `mc ls --recursive` writes the key last on the line, so treating the whole
/// line as a path reads the extension off the key. `wait_for_minio_segments`
/// matches `.log` the same way.
fn has_extension(line: &str, extension: &str) -> bool {
    std::path::Path::new(line)
        .extension()
        .is_some_and(|found| found == extension)
}

/// The object key an `mc ls --recursive` line ends with.
fn object_key(line: &str) -> Option<String> {
    line.split_whitespace().next_back().map(str::to_string)
}

/// `mc rm --versions --force local/<bucket>/<key>`, returning the raw output so
/// the caller can assert on the refusal.
///
/// `--versions` removes every version of the object rather than write a delete
/// marker over the newest one. On an Object-Lock bucket that is the request the
/// retention rule refuses; a delete marker is not. `--force` is what `mc`
/// needs before it acts on versions.
///
/// This lives in the suite and not in the shared harness because it is the one
/// thing this suite tests. A helper that removes objects is not something the
/// other tiered suites should reach for.
fn minio_rm_all_versions(bucket: &str, key: &str) -> std::process::Output {
    let minio_port = minio_port();
    let script = format!(
        "mc alias set local http://host.docker.internal:{minio_port} {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null && \
         mc rm --versions --force local/{bucket}/{key}"
    );
    Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "--entrypoint",
            "/bin/sh",
            MINIO_CLIENT_IMAGE,
            "-c",
            &script,
        ])
        .output()
        .expect("spawn mc rm")
}
