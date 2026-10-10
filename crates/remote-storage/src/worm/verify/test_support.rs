//! Fixtures shared by the verifier's unit tests: a WORM archive built object by
//! object the way a backend writes one, an in-memory store that keeps object
//! versions, and the edits an attacker with write access to the bucket could
//! make to what the fixture wrote.

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use krabka_audit::signing::FileEd25519Signer;
use krabka_object_store::{ObjectOps, ObjectStoreClient, PutRequest};
use object_store::{GetOptions, ObjectStore, ObjectStoreExt as _, memory::InMemory, path::Path};
use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
use uuid::Uuid;

use crate::{
    metadata::RemoteLogSegmentMetadata,
    storage_manager::{partition_dir_name, segment_file_name},
    worm::{
        archiver::WormArchiver,
        chain::WormChainRecord,
        manifest::{
            ChainHead, ChainStamp, EpochId, MANIFEST_FORMAT_VERSION, MANIFEST_SUFFIX, ManifestSeq,
            ObjectEntry, SegmentManifest, Sha256Digest, manifest_head,
        },
        verify::{ArchiveVerifyReport, TrustedManifestKeys, VerifyRequest, verify_archive},
    },
};

pub(super) const TOPIC: &str = "orders";
pub(super) const PARTITION: i32 = 0;
const KEY_ID: &str = "worm-key-1";
pub(super) const PREFIX: &str = "archive";
pub(super) const STRAY: &str = "stray.bin";
/// Offsets one fixture segment covers.
pub(super) const SEGMENT_SPAN: i64 = 100;

/// A throwaway Ed25519 signer, and the raw public key that verifies it.
fn signer(key_id: &str) -> (Arc<FileEd25519Signer>, Vec<u8>) {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let signer = FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), key_id.to_string())
        .expect("ring mints a valid PKCS#8 Ed25519 key");
    let public_key = signer.public_key();
    (Arc::new(signer), public_key)
}

krabka_macros::worm_segment_fixture!(metadata, crate, 0x1000, TOPIC, PARTITION, SEGMENT_SPAN);

/// One archived segment, and what the fixture needs to tamper with it.
pub(super) struct Segment {
    pub(super) metadata: RemoteLogSegmentMetadata,
    pub(super) manifest_key: String,
    pub(super) log_key: String,
    pub(super) log_body: Vec<u8>,
    pub(super) entries: Vec<ObjectEntry>,
    pub(super) manifest: SegmentManifest,
}

pub(super) fn first_break_reason(report: &ArchiveVerifyReport) -> String {
    report
        .first_break()
        .map(|found| found.reason.clone())
        .unwrap_or_default()
}

/// A WORM archive built object by object, the way a backend writes one.
pub(super) struct Archive {
    pub(super) store: Arc<dyn ObjectStore>,
    pub(super) ops: ObjectStoreClient,
    pub(super) dir: String,
    pub(super) segments: Vec<Segment>,
    public_key: Vec<u8>,
    pub(super) signer: Arc<FileEd25519Signer>,
}

impl Archive {
    /// Builds an archive whose chain runs hold `runs[i]` segments each.
    ///
    /// The objects go in through raw [`ObjectOps`] puts and the manifests
    /// through [`WormArchiver`], so the fixture never borrows the backend's
    /// idea of what a correct archive looks like.
    pub(super) async fn build(runs: &[usize]) -> Self {
        Self::build_on(Arc::new(InMemory::new()), runs).await
    }

    /// The same archive, on a store the caller chose.
    pub(super) async fn build_on(store: Arc<dyn ObjectStore>, runs: &[usize]) -> Self {
        let ops = ObjectStoreClient::new(Arc::clone(&store));
        let (signer, public_key) = signer(KEY_ID);
        let archiver = WormArchiver::new(Some(Arc::clone(&signer)));
        let dir = format!("{PREFIX}/{}", partition_dir_name(&metadata(0)));

        let mut segments = Vec::new();
        let mut index = 0usize;
        for run in runs {
            let epoch = EpochId(Uuid::new_v4());
            let mut prev_head = ChainHead::GENESIS;
            for seq in 0..*run {
                let md = metadata(index);
                let log_body = format!("segment-{index}-log-body").into_bytes();
                let index_body = format!("segment-{index}-index").into_bytes();
                let log_key = format!("{dir}/{}", segment_file_name(&md, ".log"));
                let index_key = format!("{dir}/{}", segment_file_name(&md, ".index"));
                let entries = vec![
                    put_entry(&ops, ".log", &log_key, &log_body).await,
                    put_entry(&ops, ".index", &index_key, &index_body).await,
                ];
                let stamped = md.clone().with_custom_metadata(
                    WormChainRecord::request(ChainStamp {
                        epoch_id: epoch,
                        seq: ManifestSeq(u64::try_from(seq).unwrap()),
                        prev_head,
                    })
                    .to_custom_metadata(),
                );
                let sealed = archiver.seal(&stamped, entries.clone()).unwrap();
                let manifest_key = format!("{dir}/{}", segment_file_name(&md, MANIFEST_SUFFIX));
                put_raw(&ops, &manifest_key, sealed.bytes.clone()).await;
                prev_head = manifest_head(&sealed.manifest.body);
                segments.push(Segment {
                    metadata: md,
                    manifest_key,
                    log_key,
                    log_body,
                    entries,
                    manifest: sealed.manifest,
                });
                index += 1;
            }
        }
        Self {
            store,
            ops,
            dir,
            segments,
            public_key,
            signer,
        }
    }

    pub(super) fn trusted(&self) -> TrustedManifestKeys {
        TrustedManifestKeys::single(KEY_ID.to_string(), self.public_key.clone())
    }

    /// A default [`verify_archive`] run over this archive, trusting its key.
    pub(super) async fn verify(&self) -> ArchiveVerifyReport {
        self.verify_with(&VerifyRequest::default()).await
    }

    pub(super) async fn verify_with(&self, request: &VerifyRequest) -> ArchiveVerifyReport {
        verify_archive(&self.store, request, &self.trusted())
            .await
            .unwrap()
    }

    /// Chain head of the newest manifest, before any tampering.
    pub(super) fn tip(&self) -> ChainHead {
        manifest_head(
            &self
                .segments
                .last()
                .expect("the fixture always builds at least one segment")
                .manifest
                .body,
        )
    }

    /// Re-seals one manifest in place, so the fixture can change the key
    /// that signed it or the head it claims to follow.
    async fn reseal(
        &self,
        index: usize,
        signer: Option<Arc<FileEd25519Signer>>,
        prev_head: Option<ChainHead>,
        entries: Option<Vec<ObjectEntry>>,
    ) {
        let segment = &self.segments[index];
        let mut stamp = segment.manifest.body.chain;
        if let Some(head) = prev_head {
            stamp.prev_head = head;
        }
        let stamped = segment
            .metadata
            .clone()
            .with_custom_metadata(WormChainRecord::request(stamp).to_custom_metadata());
        let sealed = WormArchiver::new(signer)
            .seal(&stamped, entries.unwrap_or_else(|| segment.entries.clone()))
            .unwrap();
        put_raw(&self.ops, &segment.manifest_key, sealed.bytes).await;
    }

    pub(super) async fn delete(&self, key: &str) {
        self.ops.delete(&Path::from(key)).await.unwrap();
    }
}

/// An in-memory store that actually keeps versions.
///
/// [`InMemory`] reports `version: None` on every put and ignores a version
/// asked for on get, so a test built on it would be asserting that fake's
/// indifference rather than the verifier's handling of a versioned bucket.
/// This keeps each put's bytes to one side and serves them back when a get
/// pins that version, which is what an Object Lock bucket does and the
/// whole reason [`pinned_version_note`] exists.
///
/// The versions live outside the inner store, so they never appear in a
/// listing: the verifier walks the same archive it would on S3, and the
/// history is reachable only by asking for it.
#[derive(Debug)]
pub(super) struct VersionedStore {
    inner: InMemory,
    history: std::sync::Mutex<HashMap<(String, String), Bytes>>,
    next: std::sync::atomic::AtomicU64,
}

impl VersionedStore {
    pub(super) fn new() -> Self {
        Self {
            inner: InMemory::new(),
            history: std::sync::Mutex::new(HashMap::new()),
            next: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl std::fmt::Display for VersionedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VersionedStore({})", self.inner)
    }
}

#[krabka_macros::object_store_delegate(
    put_multipart_opts,
    delete_stream,
    list,
    list_with_delimiter,
    copy_opts
)]
#[async_trait::async_trait]
impl ObjectStore for VersionedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let version = format!(
            "v{}",
            self.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        );
        let bytes = Bytes::from(payload.iter().flatten().copied().collect::<Vec<u8>>());
        let mut result = self.inner.put_opts(location, payload, opts).await?;
        self.history
            .lock()
            .expect("no test panics while holding this")
            .insert((location.to_string(), version.clone()), bytes);
        result.version = Some(version);
        Ok(result)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let Some(version) = options.version.clone() else {
            return self.inner.get_opts(location, options).await;
        };
        let bytes = self
            .history
            .lock()
            .expect("no test panics while holding this")
            .get(&(location.to_string(), version.clone()))
            .cloned();
        let Some(bytes) = bytes else {
            return Err(object_store::Error::NotFound {
                path: format!("{location}?versionId={version}"),
                source: "no such version".into(),
            });
        };
        let mut meta = self.inner.head(location).await?;
        meta.size = bytes.len() as u64;
        meta.version = Some(version);
        Ok(object_store::GetResult {
            range: 0..meta.size,
            payload: object_store::GetResultPayload::Stream(Box::pin(futures_util::stream::once(
                async move { Ok(bytes) },
            ))),
            meta,
            attributes: object_store::Attributes::default(),
            extensions: object_store::Extensions::default(),
        })
    }
}

/// Puts one object and returns the manifest entry that records it.
pub(super) async fn put_entry(
    ops: &ObjectStoreClient,
    suffix: &str,
    key: &str,
    body: &[u8],
) -> ObjectEntry {
    let outcome = ops
        .put(
            &Path::from(key),
            Bytes::copy_from_slice(body),
            PutRequest {
                digest: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    ObjectEntry {
        suffix: suffix.to_string(),
        key: key.to_string(),
        size_bytes: outcome.size_bytes,
        sha256: Sha256Digest(outcome.sha256.expect("the put was asked for a digest")),
        e_tag: outcome.e_tag,
        version_id: outcome.version_id,
        create_precondition: outcome.create_precondition,
    }
}

pub(super) async fn put_raw(ops: &ObjectStoreClient, key: &str, body: Bytes) {
    ops.put(&Path::from(key), body, PutRequest::default())
        .await
        .unwrap();
}

/// One edit an attacker with write access to the bucket could make.
#[derive(Clone, Copy)]
pub(super) enum Tamper {
    None,
    FlipLogByte(usize),
    TruncateLog(usize),
    DeleteLog(usize),
    RewritePrevHead(usize),
    DeleteManifest(usize),
    SignWithAnotherKey(usize),
    SignWithUnknownKeyId(usize),
    Unsign(usize),
    RewritePublicKey(usize),
    DuplicateObject(usize),
    WrongObjectCoordinates(usize),
    DropObjectEntry(usize),
    StrayObject,
    BumpFormatVersion(usize),
}

impl Tamper {
    pub(super) async fn apply(self, archive: &Archive) {
        match self {
            Tamper::None => {}
            Tamper::FlipLogByte(i) => {
                let segment = &archive.segments[i];
                let mut body = segment.log_body.clone();
                body[0] ^= 0xff;
                put_raw(&archive.ops, &segment.log_key, Bytes::from(body)).await;
            }
            Tamper::TruncateLog(i) => {
                let segment = &archive.segments[i];
                let body = segment.log_body[..segment.log_body.len() - 1].to_vec();
                put_raw(&archive.ops, &segment.log_key, Bytes::from(body)).await;
            }
            Tamper::DeleteLog(i) => archive.delete(&archive.segments[i].log_key).await,
            Tamper::RewritePrevHead(i) => {
                archive
                    .reseal(
                        i,
                        Some(Arc::clone(&archive.signer)),
                        Some(ChainHead([0xaa; 32])),
                        None,
                    )
                    .await;
            }
            Tamper::DeleteManifest(i) => {
                archive.delete(&archive.segments[i].manifest_key).await;
            }
            Tamper::SignWithAnotherKey(i) => {
                // Same `key_id`, different key material: the verifier must
                // check against the key it trusts and not the key the
                // manifest carries.
                let (other, _) = signer(KEY_ID);
                archive.reseal(i, Some(other), None, None).await;
            }
            Tamper::SignWithUnknownKeyId(i) => {
                let (rogue, _) = signer("rogue-key");
                archive.reseal(i, Some(rogue), None, None).await;
            }
            Tamper::Unsign(i) => archive.reseal(i, None, None, None).await,
            Tamper::RewritePublicKey(i) => {
                let segment = &archive.segments[i];
                let mut manifest = segment.manifest.clone();
                manifest.signature.as_mut().unwrap().public_key.0[0] ^= 0xff;
                put_raw(
                    &archive.ops,
                    &segment.manifest_key,
                    Bytes::from(serde_json::to_vec(&manifest).unwrap()),
                )
                .await;
            }
            Tamper::DuplicateObject(i) => {
                let mut entries = archive.segments[i].entries.clone();
                entries.push(entries[0].clone());
                archive
                    .reseal(i, Some(Arc::clone(&archive.signer)), None, Some(entries))
                    .await;
            }
            Tamper::WrongObjectCoordinates(i) => {
                let mut entries = archive.segments[i].entries.clone();
                entries[0].suffix = ".timeindex".to_string();
                archive
                    .reseal(i, Some(Arc::clone(&archive.signer)), None, Some(entries))
                    .await;
            }
            Tamper::DropObjectEntry(i) => {
                let mut entries = archive.segments[i].entries.clone();
                entries.pop();
                archive
                    .reseal(i, Some(Arc::clone(&archive.signer)), None, Some(entries))
                    .await;
            }
            Tamper::StrayObject => {
                put_raw(
                    &archive.ops,
                    &format!("{}/{STRAY}", archive.dir),
                    Bytes::from_static(b"nothing names me"),
                )
                .await;
            }
            Tamper::BumpFormatVersion(i) => {
                let segment = &archive.segments[i];
                let mut value: serde_json::Value = serde_json::to_value(&segment.manifest).unwrap();
                value["body"]["format_version"] =
                    serde_json::Value::from(MANIFEST_FORMAT_VERSION + 1);
                let bytes = serde_json::to_vec(&value).unwrap();
                put_raw(&archive.ops, &segment.manifest_key, Bytes::from(bytes)).await;
            }
        }
    }
}
