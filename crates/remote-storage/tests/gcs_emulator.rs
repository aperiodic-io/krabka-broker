//! The native GCS backend against a Cloud Storage emulator.
//!
//! `docs/config-reference.md` calls keyless Workload Identity "the primary
//! production path", and `[remote_storage.gcs]` reaches it through
//! [`S3RemoteStorage::from_gcs_config`]. Until this suite existed the only GCS
//! coverage in the tree was builder-constructs-ok unit tests and an
//! `InMemory`-backed round trip, so no byte of the GCS wire path -- the XML
//! object API `object_store`'s GCS client actually speaks, and the
//! `Buckets.get` control plane that gates archive startup -- was ever executed
//! against a server.
//!
//! The container lifecycle follows the pattern `crates/restore/tests/
//! minio_roundtrip.rs` uses: a `docker run -d` guard that removes the
//! container on drop, and a per-process published port so two container suites
//! can run at once.
//!
//! ## What this suite cannot reach: the object write
//!
//! `object_store` 0.14 writes a GCS object over the Cloud Storage **XML** API.
//! Its `GoogleCloudStorageClient::put` sends `PUT {endpoint}/{bucket}/{object}`
//! with the body inline and no query string at all, the object key
//! percent-encoded into one path segment. `fsouza/fake-gcs-server` does route
//! that path, but routes it to `insertObject` -- the **JSON** API's upload
//! handler -- which requires an `uploadType` query parameter and answers
//! anything else `400 Bad Request: invalid uploadType`. Its one escape hatch
//! is a signed-URL upload, taken when the query carries `X-Goog-Algorithm`,
//! which `object_store` never sends; every emulator release from 1.40 to the
//! current 1.56.1 has the same handler. Google's own
//! `googleapis/storage-testbench` does serve the XML `PUT`, but serves no XML
//! `DELETE`, so it would trade this suite's delete coverage for the copy leg
//! and put the two WORM cases on a server they have never run against.
//!
//! Reads and deletes travel the same XML object path and *are* routed:
//! `GET`/`HEAD` reach the emulator's download handler, which honours `Range`
//! and returns the `ETag`, `Last-Modified` and generation headers
//! `object_store` requires of a response, and `DELETE` reaches its delete
//! handler. So this suite seeds a segment's objects through the emulator's
//! JSON media upload -- the same control plane it creates buckets with, and
//! not a `krabka` code path -- and then drives fetch, delete and the WORM
//! startup gate through the backend, keyed exactly as the engine keys them.
//!
//! The copy path -- the single PUT, the multipart threshold, the generation
//! precondition of a conditional create and the sealed WORM manifest --
//! therefore stays covered where it can be executed against something: the
//! `InMemory`-backed suites under `crates/remote-storage/src/s3`,
//! `crates/object-store`'s `put_from_path` tests, and the MinIO-backed
//! S3 suites. No GCS emulator can execute it.

use std::{
    process::{Command, Stdio},
    time::Duration,
};

use assert2::{assert, check};
use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    GcsConfig, IndexType, LOG_FILE_SUFFIX, RemoteLogSegmentDetails, RemoteLogSegmentId,
    RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError, RemoteStorageManager as _,
    S3RemoteStorage, TopicIdPartition, WormConfig, partition_dir_name, segment_file_name,
};
use uuid::Uuid;

/// The emulator image, pinned by the same digest table the broker's container
/// suites load from (`//bazel/images`).
const FAKE_GCS_IMAGE: &str = "mirror.gcr.io/fsouza/fake-gcs-server:1.56.1";

/// The port the emulator listens on inside the container.
const CONTAINER_PORT: u16 = 4443;

/// A service-account key that turns `object_store`'s OAuth exchange off, so
/// the client signs nothing and the emulator authorises everything. The GKE
/// path this backend exists for has no key at all: the metadata server hands
/// out bearer tokens, and there is no metadata server in a test.
const DISABLE_OAUTH_KEY: &str = r#"{"private_key":"unused","private_key_id":"unused","client_email":"unused","disable_oauth":true}"#;

/// The archive sits under a prefix, so the operator-visible prefix is
/// exercised against a real server rather than only against `InMemory`.
const PREFIX: &str = "tier";

const TOPIC: &str = "orders";

const LOG_BODY: &[u8] = b"0123456789abcdef";
const OFFSET_INDEX_BODY: &[u8] = b"OFFSET-IDX";
const TIME_INDEX_BODY: &[u8] = b"TIME-IDX";
const SNAPSHOT_BODY: &[u8] = b"SNAP";
const EPOCH_BODY: &[u8] = b"EPOCH-BYTES";

/// A free ephemeral port on the loopback interface, so two container suites
/// running at once do not fight over a fixed 4443.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener
        .local_addr()
        .expect("the bound listener has an address")
        .port()
}

/// A `docker run -d` emulator, removed on drop so an aborted test leaves
/// nothing behind.
struct FakeGcs {
    name: String,
    port: u16,
}

impl FakeGcs {
    async fn start() -> Self {
        let port = free_port();
        let name = format!("krabka-fake-gcs-{}", Uuid::new_v4().simple());
        let status = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &name,
                "-p",
                &format!("{port}:{CONTAINER_PORT}"),
                FAKE_GCS_IMAGE,
                "-scheme",
                "http",
                "-port",
                &CONTAINER_PORT.to_string(),
                "-backend",
                "memory",
                // `object_store` addresses objects as `{endpoint}/{bucket}/
                // {object}`, which the emulator only routes when the request's
                // host matches its public host. The published port is
                // per-process, so name the host alone and let the emulator
                // compare host parts.
                "-public-host",
                "127.0.0.1",
                "-external-url",
                &format!("http://127.0.0.1:{port}"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .expect("spawn docker run fake-gcs-server");
        assert!(status.success(), "docker run fake-gcs-server failed");
        let server = Self { name, port };
        server.wait_until_ready().await;
        server
    }

    /// The base URL `GcsConfig::endpoint` points at.
    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Poll the control plane until the emulator answers a real Cloud Storage
    /// request, so the first backend call does not race container startup.
    async fn wait_until_ready(&self) {
        let client = reqwest::Client::new();
        let url = self.buckets_url();
        for _ in 0..60 {
            let served = match client.get(&url).send().await {
                Ok(response) => response.status().is_success(),
                Err(_) => false,
            };
            if served {
                return;
            }
            // Intentional: a bounded readiness poll of an external process. No
            // krabka metric reflects its listener coming up.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!(
            "fake-gcs-server never served a bucket listing on 127.0.0.1:{}",
            self.port,
        );
    }

    /// The control-plane collection buckets are listed from and created in.
    fn buckets_url(&self) -> String {
        format!("{}/storage/v1/b?project=krabka", self.endpoint())
    }

    /// Create a bucket through the emulator's JSON control plane, which is the
    /// one operation no `object_store` API reaches. Returns the bucket name.
    async fn create_bucket(&self, versioning: bool) -> String {
        let bucket = format!("krabka-gcs-{}", Uuid::new_v4().simple());
        let response = reqwest::Client::new()
            .post(self.buckets_url())
            .header("content-type", "application/json")
            .body(
                serde_json::to_vec(&serde_json::json!({
                    "name": bucket,
                    "versioning": {"enabled": versioning},
                }))
                .expect("serialise the bucket request"),
            )
            .send()
            .await
            .expect("create a bucket");
        assert!(
            response.status().is_success(),
            "create bucket {bucket}: {}",
            response.status(),
        );
        bucket
    }

    /// Write one object through the emulator's JSON media upload, the write
    /// the emulator serves. The module docs say why the backend's own write
    /// cannot stand in here.
    ///
    /// The key goes into the query string unescaped, which is exact for the
    /// keys this suite uses and no others: a Kafka archive key is made of the
    /// prefix, a topic name, digits, `-`, `_`, `.` and the `/` separator, and
    /// a query component may carry every one of those literally. The
    /// assertion below is what keeps that true.
    async fn put_object(&self, bucket: &str, key: &str, body: &'static [u8]) {
        assert!(
            key.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._/".contains(&b)),
            "key {key} needs percent-encoding to survive a query string",
        );
        let response = reqwest::Client::new()
            .post(format!(
                "{}/upload/storage/v1/b/{bucket}/o?uploadType=media&name={key}",
                self.endpoint(),
            ))
            .header("content-type", "application/octet-stream")
            .body(body)
            .send()
            .await
            .expect("seed an object");
        assert!(
            response.status().is_success(),
            "seed {key}: {}",
            response.status(),
        );
    }

    /// Seed every artifact of one segment at the keys the engine derives, so
    /// a backend read has to agree with the engine's own key layout to find
    /// anything.
    async fn seed_segment(&self, bucket: &str, metadata: &RemoteLogSegmentMetadata) {
        for (suffix, body) in segment_artifacts() {
            self.put_object(bucket, &object_key(metadata, suffix), body)
                .await;
        }
    }

    /// The names of every object currently in `bucket`.
    async fn object_names(&self, bucket: &str) -> Vec<String> {
        let body = reqwest::Client::new()
            .get(format!("{}/storage/v1/b/{bucket}/o", self.endpoint()))
            .send()
            .await
            .expect("list the bucket")
            .bytes()
            .await
            .expect("read the object listing");
        let listing: serde_json::Value =
            serde_json::from_slice(&body).expect("parse the object listing");
        listing["items"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .map(|item| item["name"].as_str().unwrap_or_default().to_owned())
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl Drop for FakeGcs {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// The backend configuration every case builds on: the emulator's endpoint,
/// plaintext HTTP, and the credential that turns OAuth off.
fn gcs_config(server: &FakeGcs, bucket: String) -> GcsConfig {
    GcsConfig {
        bucket,
        prefix: Some(PREFIX.to_owned()),
        endpoint: Some(server.endpoint()),
        service_account_key: Some(DISABLE_OAUTH_KEY.to_owned()),
        allow_http: true,
        ..GcsConfig::default()
    }
}

/// Every artifact a copy of the fixture segment would have written, by key
/// suffix and body. The transaction index is absent, as it is for a segment
/// with no aborted transactions.
fn segment_artifacts() -> [(&'static str, &'static [u8]); 5] {
    [
        (LOG_FILE_SUFFIX, LOG_BODY),
        (IndexType::Offset.suffix(), OFFSET_INDEX_BODY),
        (IndexType::Timestamp.suffix(), TIME_INDEX_BODY),
        (IndexType::ProducerSnapshot.suffix(), SNAPSHOT_BODY),
        (IndexType::LeaderEpoch.suffix(), EPOCH_BODY),
    ]
}

/// The object key one artifact of `metadata` lives at, prefix included.
fn object_key(metadata: &RemoteLogSegmentMetadata, suffix: &str) -> String {
    format!(
        "{PREFIX}/{}/{}",
        partition_dir_name(metadata),
        segment_file_name(metadata, suffix),
    )
}

fn sample_metadata(topic_id: Uuid, partition: i32) -> RemoteLogSegmentMetadata {
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(
            TopicIdPartition::new(topic_id, TOPIC, partition),
            Uuid::new_v4(),
        ),
        0,
        99,
        123,
        1,
        456,
        RemoteLogSegmentDetails::new(
            i32::try_from(LOG_BODY.len()).expect("fixture segment fits i32"),
            RemoteLogSegmentState::CopySegmentFinished,
            maplit::btreemap! {LeaderEpoch(0) => 0},
        ),
    )
    .expect("valid remote metadata")
}

async fn seeded_segment(
    index: i32,
) -> (FakeGcs, String, S3RemoteStorage, RemoteLogSegmentMetadata) {
    let server = FakeGcs::start().await;
    let bucket = server.create_bucket(false).await;
    let config = gcs_config(&server, bucket.clone());
    let storage = S3RemoteStorage::from_gcs_config(&config).expect("open the emulator bucket");
    let metadata = sample_metadata(Uuid::new_v4(), index);
    server.seed_segment(&bucket, &metadata).await;
    (server, bucket, storage, metadata)
}

/// Every artifact of a segment in a real bucket reads back byte for byte
/// through the backend, whole and by range, and each index reads back as
/// itself rather than as a neighbour.
///
/// Multi-thread on purpose: `S3RemoteStorage`'s sync trait methods bridge to
/// the async store with `block_in_place`, which a current-thread runtime
/// cannot do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_segment_seeded_in_the_emulator_fetches_back_byte_for_byte() {
    let (_server, _bucket, storage, metadata) = seeded_segment(0).await;

    tokio::task::spawn_blocking(move || {
        check!(
            storage
                .fetch_log_segment(&metadata, 0, None)
                .expect("fetch the whole segment")
                == LOG_BODY
        );
        check!(
            storage
                .fetch_log_segment(&metadata, 4, Some(7))
                .expect("fetch a byte range of the segment")
                == &LOG_BODY[4..=7]
        );
        for (index_type, expected) in [
            (IndexType::Offset, OFFSET_INDEX_BODY),
            (IndexType::Timestamp, TIME_INDEX_BODY),
            (IndexType::ProducerSnapshot, SNAPSHOT_BODY),
            (IndexType::LeaderEpoch, EPOCH_BODY),
        ] {
            check!(
                storage
                    .fetch_index(&metadata, index_type)
                    .expect("fetch an index")
                    == expected,
                "{index_type:?}",
            );
        }
    })
    .await
    .expect("the blocking half of the test");
}

/// A delete removes every artifact of the segment from the bucket, so a later
/// fetch reports the segment as absent rather than serving a stale body, and
/// the bucket itself is left empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_deleted_segment_is_gone_from_the_bucket() {
    let (server, bucket, storage, metadata) = seeded_segment(3).await;
    let seeded = server.object_names(&bucket).await;
    check!(seeded.len() == segment_artifacts().len());

    let deleted = metadata.clone();
    tokio::task::spawn_blocking(move || {
        storage
            .delete_log_segment_data(&deleted)
            .expect("delete the segment from the emulator");

        assert!(let Err(error) = storage.fetch_log_segment(&deleted, 0, None));
        check!(matches!(error, RemoteStorageError::SegmentNotFound(_)));
        assert!(let Err(error) = storage.fetch_index(&deleted, IndexType::Offset));
        check!(matches!(error, RemoteStorageError::SegmentNotFound(_)));

        // Kafka's SPI requires an idempotent delete: a retried expiry of a
        // segment already gone must not fail the retention pass.
        storage
            .delete_log_segment_data(&deleted)
            .expect("a second delete is a no-op");
    })
    .await
    .expect("the blocking half of the test");

    let left = server.object_names(&bucket).await;
    check!(left == Vec::<String>::new());
}

/// `with_worm` gates archive startup on `Buckets.get`, and a bucket with
/// neither versioning nor a retention policy fails that gate against a real
/// server rather than only against a stubbed response body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worm_startup_refuses_an_unversioned_emulator_bucket() {
    let server = FakeGcs::start().await;
    let config = gcs_config(&server, server.create_bucket(false).await);

    let storage = S3RemoteStorage::from_gcs_config(&config).expect("open the emulator bucket");
    assert!(let Err(error) = storage.with_worm(&WormConfig::default()));
    check!(
        error
            .to_string()
            .contains("requires GCS versioning enabled")
    );
}

/// The next rung of the same gate: versioning alone is not a WORM bucket. GCS
/// protects a completed object with a *locked* retention policy, and a bucket
/// carrying none is refused with that named as the reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worm_startup_refuses_a_versioned_bucket_with_no_retention_policy() {
    let server = FakeGcs::start().await;
    let config = gcs_config(&server, server.create_bucket(true).await);

    let storage = S3RemoteStorage::from_gcs_config(&config).expect("open the emulator bucket");
    assert!(let Err(error) = storage.with_worm(&WormConfig::default()));
    check!(error.to_string().contains("no GCS retention policy"));
}
