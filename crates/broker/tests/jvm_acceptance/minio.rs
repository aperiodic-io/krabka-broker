//! `MinIO` container lifecycle and bucket setup for the tiered-storage suites.
//!
//! The container is owned by a guard that removes it on drop, so an aborted
//! test does not leave one squatting on the published port.

use {super::ports::minio_port, assert2::assert};

krabka_macros::minio_fixture!(minio_process);

// ---------------------------------------------------------------------------
// MinIO-backed tiered-storage acceptance test (KIP-405 S3 backend).
//
// Spins up a real `MinIO` container, points the broker at it via the
// S3-compatible `S3RemoteStorage` backend, then drives a JVM producer +
// consumer against a topic with `remote.storage.enable=true` and an
// aggressive `local.retention.bytes` override, on a broker whose default
// segment size is small (`TIERED_SEGMENT_SIZE`). We assert both that
// segment objects materialise in the MinIO bucket and that the JVM consumer
// reads back every record — including offsets whose local segments have
// already been evicted by `local_retention_pass`, forcing the read to come
// from the remote tier through `RemoteReader`.
// ---------------------------------------------------------------------------

// MinIO discontinued distribution of its own images, quay.io included;
// `//bazel/images:minio`/`:minio_mc` now build both from a Wolfi apko lock and
// load them under these tags rather than pull them from quay.io.
pub(crate) const MINIO_IMAGE: &str = "docker.io/krabka-io/minio:RELEASE.2025-09-07T16-13-09Z";

pub(crate) const MINIO_CLIENT_IMAGE: &str =
    "docker.io/krabka-io/minio-mc:RELEASE.2025-08-13T08-35-41Z";

pub(crate) const MINIO_ACCESS_KEY: &str = "minioadmin";

pub(crate) const MINIO_SECRET_KEY: &str = "minioadmin";

pub(crate) const MINIO_BUCKET: &str = "krabka-tiered";

/// Owns a `docker run -d` `MinIO` container and tears it down on drop.
pub(crate) struct MinioContainer {
    name: String,
}

impl MinioContainer {
    pub(crate) fn start() -> Self {
        // Unique name per test invocation so back-to-back runs don't see a
        // stale container squatting on port 9000.
        let minio_port = minio_port();
        let name = format!("krabka-minio-test-{}", uuid::Uuid::new_v4().simple());
        // Best-effort orphan reap from a prior aborted run.
        minio_process::remove(&name);
        minio_process::start(
            &name,
            minio_port,
            MINIO_IMAGE,
            MINIO_ACCESS_KEY,
            MINIO_SECRET_KEY,
        );
        Self { name }
    }
}

/// Poll the published host port until `MinIO`'s HTTP listener answers. This
/// avoids a race with the first health check of the fast-starting image.
pub(crate) fn wait_for_minio_ready() { minio_process::wait(minio_port()); }

pub(crate) fn minio_make_bucket(bucket: &str) {
    // `mc mb -p` is idempotent and creates parent prefixes; the inner
    // loop retries the `alias set` so a slow MinIO startup doesn't fail
    // the test on the first probe.
    let minio_port = minio_port();
    let script = format!(
        "for i in 1 2 3 4 5 6 7 8 9 10; do \
           mc alias set local http://host.docker.internal:{minio_port} {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && break; \
           sleep 1; \
         done && mc mb -p local/{bucket}"
    );
    minio_process::make_bucket(MINIO_CLIENT_IMAGE, &script);
}

/// Create a bucket with S3 Object Lock on and a compliance-mode default
/// retention, for the WORM archive suite.
///
/// `mc mb --with-lock` also turns on bucket versioning, which Object Lock
/// needs: a lock protects one object *version*, and an unversioned bucket has
/// none. `mc retention set --default COMPLIANCE 1d` is the part that makes
/// `MinIO` refuse a delete. Without it the bucket only *can* hold locked
/// objects, and no object is locked.
///
/// `1d` is the shortest retention the suite can use and still prove the point.
/// The bucket lives inside a container the test removes on drop, so nothing
/// outlives the run.
pub(crate) fn minio_make_locked_bucket(bucket: &str) {
    // Same `alias set` retry as `minio_make_bucket`, so a slow MinIO startup
    // doesn't fail the test on the first probe.
    let minio_port = minio_port();
    let script = format!(
        "for i in 1 2 3 4 5 6 7 8 9 10; do \
           mc alias set local http://host.docker.internal:{minio_port} {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && break; \
           sleep 1; \
         done && mc mb --with-lock local/{bucket} \
         && mc retention set --default COMPLIANCE 1d local/{bucket}"
    );
    let out = minio_process::mc(MINIO_CLIENT_IMAGE, &script, "spawn mc mb --with-lock");
    assert!(
        out.status.success(),
        "mc mb --with-lock failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// `mc ls --recursive local/<bucket>` for assertion-side bucket inspection.
pub(crate) fn minio_list_objects(bucket: &str) -> String {
    let minio_port = minio_port();
    let script = format!(
        "mc alias set local http://host.docker.internal:{minio_port} {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null && \
         mc ls --recursive local/{bucket}"
    );
    let out = minio_process::mc(MINIO_CLIENT_IMAGE, &script, "spawn mc ls");
    assert!(
        out.status.success(),
        "mc ls failed: stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

impl Drop for MinioContainer {
    fn drop(&mut self) { minio_process::remove(&self.name); }
}
