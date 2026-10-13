use {
    super::*,
    crate::ops::PutMode,
    assert2::{assert, check},
    object_store::{GetRange, path::Path},
    sha2::Sha256,
    std::{io::Write, sync::Arc},
};

fn client() -> ObjectStoreClient {
    ObjectStoreClient::new(Arc::new(object_store::memory::InMemory::new()))
}

fn local_file(contents: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(contents).unwrap();
    file
}

/// A counting store, a client over it, and a local file to upload.
struct CountingFile {
    store: Arc<CountingStore>,
    client: ObjectStoreClient,
    file: tempfile::NamedTempFile,
}

fn counting_file(contents: &[u8]) -> CountingFile {
    let store = Arc::new(CountingStore::new());
    let client = ObjectStoreClient::new(store.clone());
    CountingFile {
        store,
        client,
        file: local_file(contents),
    }
}

fn check_upload_counts(store: &CountingStore, puts: usize, multiparts: usize) {
    assert!(store.puts.load(std::sync::atomic::Ordering::SeqCst) == puts);
    assert!(store.multiparts.load(std::sync::atomic::Ordering::SeqCst) == multiparts);
}

/// SHA-256 of `bytes`, computed independently of the upload path under
/// test.
fn sha256_of(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

#[tokio::test]
async fn put_get_round_trips() {
    let c = client();
    let key = Path::from("a/b");
    c.put(
        &key,
        bytes::Bytes::from_static(b"hello"),
        PutRequest::default(),
    )
    .await
    .unwrap();
    let got = c.get(&key).await.unwrap();
    assert!(&got[..] == b"hello");
}

#[tokio::test]
async fn get_range_returns_slice() {
    let c = client();
    let key = Path::from("a/b");
    c.put(
        &key,
        bytes::Bytes::from_static(b"hello world"),
        PutRequest::default(),
    )
    .await
    .unwrap();
    let got = c.get_range(&key, GetRange::Bounded(0..5)).await.unwrap();
    assert!(&got[..] == b"hello");
}

#[tokio::test]
async fn get_missing_maps_to_not_found() {
    let c = client();
    let err = c.get(&Path::from("nope")).await.unwrap_err();
    assert!(matches!(err, ObjectStoreError::NotFound(_)));
}

#[tokio::test]
async fn capped_get_accepts_the_limit_and_rejects_one_byte_over() {
    let c = client();
    let key = Path::from("capped");
    c.put(&key, Bytes::from_static(b"123"), PutRequest::default())
        .await
        .unwrap();
    assert!(c.get_capped(&key, 3).await.unwrap().as_ref() == b"123");
    assert!(matches!(
        c.get_capped(&key, 2).await.unwrap_err(),
        ObjectStoreError::TooLarge { .. }
    ));
}

#[tokio::test]
async fn head_and_list_and_delete() {
    let c = client();
    let key = Path::from("p/x");
    c.put(
        &key,
        bytes::Bytes::from_static(b"1234"),
        PutRequest::default(),
    )
    .await
    .unwrap();
    assert!(c.head(&key).await.unwrap().size == 4);
    let listed = c.list(Some(Path::from("p"))).await.unwrap();
    assert!(listed.iter().any(|m| m.location == key));
    c.delete(&key).await.unwrap();
    assert!(matches!(
        c.get(&key).await.unwrap_err(),
        ObjectStoreError::NotFound(_)
    ));
}

/// `list` and `list_stream` describe the same prefix: the stream yields every
/// object the collecting call returns, in the same order, so a caller can move
/// from one to the other without changing what it sees.
#[tokio::test]
async fn list_stream_yields_the_same_objects_as_list() {
    use futures_util::stream::TryStreamExt as _;

    let c = client();
    for name in ["p/a", "p/b", "p/c", "q/d"] {
        c.put(
            &Path::from(name),
            bytes::Bytes::from_static(b"1234"),
            PutRequest::default(),
        )
        .await
        .unwrap();
    }

    let collected: Vec<Path> = c
        .list(Some(Path::from("p")))
        .await
        .unwrap()
        .into_iter()
        .map(|meta| meta.location)
        .collect();
    let streamed: Vec<Path> = c
        .list_stream(Some(Path::from("p")))
        .map_ok(|meta| meta.location)
        .try_collect()
        .await
        .unwrap();

    assert!(collected == vec![Path::from("p/a"), Path::from("p/b"), Path::from("p/c")]);
    assert!(streamed == collected);
}

/// A listing failure surfaces as an [`ObjectStoreError`] item rather than as
/// a panic or a silently truncated stream.
#[tokio::test]
async fn list_stream_surfaces_a_backend_failure_as_an_error_item() {
    use futures_util::stream::TryStreamExt as _;

    let c = ObjectStoreClient::new(Arc::new(CountingStore::failing_list()));
    let err = c.list_stream(None).try_next().await.unwrap_err();
    assert!(matches!(err, ObjectStoreError::Backend(_)));
}

/// The default request asks for no digest, and the outcome reports the
/// payload size plus whatever identifiers the backend returned. `InMemory`
/// hands out sequential etags from `0`, so the whole outcome is
/// predictable.
#[tokio::test]
async fn put_outcome_reports_size_and_backend_identifiers() {
    let c = client();

    let got = c
        .put(
            &Path::from("a/b"),
            bytes::Bytes::from_static(b"hello"),
            PutRequest::default(),
        )
        .await
        .unwrap();

    assert!(
        got == PutOutcome {
            size_bytes: 5,
            sha256: None,
            e_tag: Some("\"0\"".to_owned()),
            version_id: None,
            create_precondition: false,
        }
    );
}

/// `PutRequest::digest` turns on whole-object hashing, and the digest must
/// be of the bytes actually stored.
#[tokio::test]
async fn put_with_digest_returns_payload_sha256() {
    let c = client();
    let payload = bytes::Bytes::from_static(b"hello");

    let got = c
        .put(&Path::from("a/b"), payload.clone(), PutRequest {
            digest: true,
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(
        got == PutOutcome {
            size_bytes: 5,
            sha256: Some(sha256_of(&payload)),
            e_tag: Some("\"0\"".to_owned()),
            version_id: None,
            create_precondition: false,
        }
    );
}

/// `PutMode::Create` is a precondition, not a hint: the second write to a
/// key must fail rather than clobber the first.
#[tokio::test]
async fn put_create_mode_rejects_an_existing_key() {
    let c = client();
    let key = Path::from("worm/manifest");
    let req = PutRequest {
        mode: PutMode::Create,
        digest: false,
    };
    c.put(&key, bytes::Bytes::from_static(b"first"), req.clone())
        .await
        .unwrap();

    let err = c
        .put(&key, bytes::Bytes::from_static(b"second"), req)
        .await
        .unwrap_err();

    assert!(matches!(err, ObjectStoreError::AlreadyExists(p) if p == key));
    assert!(&c.get(&key).await.unwrap()[..] == b"first");
}

#[tokio::test]
async fn put_from_path_single_put_below_threshold() {
    let c = client();
    let f = local_file(b"tiny");
    let key = Path::from("seg/small");
    c.put_from_path(&key, f.path(), 8, 4, PutRequest::default())
        .await
        .unwrap();
    assert!(&c.get(&key).await.unwrap()[..] == b"tiny");
}

#[tokio::test]
async fn put_from_path_multipart_above_threshold() {
    let c = client();
    let payload = vec![7u8; 20]; // 20 bytes, threshold 8, chunk 4 -> multipart
    let f = local_file(&payload);
    let key = Path::from("seg/big");
    c.put_from_path(&key, f.path(), 8, 4, PutRequest::default())
        .await
        .unwrap();
    assert!(c.get(&key).await.unwrap()[..] == payload[..]);
}

/// Both upload paths must produce the same digest over the same file, and
/// the multipart path must fold every chunk in — a partial fold would give
/// a different hash. The cases differ only in file length, which selects
/// the path: below `threshold` is single-PUT, at or above it is multipart
/// across several `chunk_size` reads.
#[tokio::test]
async fn put_from_path_digests_the_whole_file_on_both_paths() {
    for (case, len) in [("single put", 7usize), ("multipart", 21usize)] {
        let c = client();
        let payload: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&payload).unwrap();

        let got = c
            .put_from_path(&Path::from("seg/x"), f.path(), 8, 4, PutRequest {
                digest: true,
                ..Default::default()
            })
            .await
            .unwrap();

        check!(
            got == PutOutcome {
                size_bytes: len as u64,
                sha256: Some(sha256_of(&payload)),
                e_tag: Some("\"0\"".to_owned()),
                version_id: None,
                create_precondition: false,
            },
            "{case}"
        );
    }
}

/// Without `digest`, neither path pays for hashing, so the outcome carries
/// `None` while still reporting the byte count.
#[tokio::test]
async fn put_from_path_omits_digest_when_not_requested() {
    for (case, len) in [("single put", 7usize), ("multipart", 21usize)] {
        let c = client();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&vec![3u8; len]).unwrap();

        let got = c
            .put_from_path(&Path::from("seg/x"), f.path(), 8, 4, PutRequest::default())
            .await
            .unwrap();

        check!(
            got == PutOutcome {
                size_bytes: len as u64,
                sha256: None,
                e_tag: Some("\"0\"".to_owned()),
                version_id: None,
                create_precondition: false,
            },
            "{case}"
        );
    }
}

/// A delegating spy that counts which upload path `put_from_path` takes.
/// Both paths produce identical object bytes. Only the call counts can thus
/// pin the threshold comparison `len < threshold` at its boundary.
#[derive(Debug)]
struct CountingStore {
    inner: object_store::memory::InMemory,
    puts: std::sync::atomic::AtomicUsize,
    multiparts: std::sync::atomic::AtomicUsize,
    failure_pending: Option<bool>,
    /// Whether a listing fails on its first item instead of reading `inner`.
    failing_list: bool,
    parts: Arc<std::sync::atomic::AtomicUsize>,
    aborts: Arc<std::sync::atomic::AtomicUsize>,
}

impl CountingStore {
    fn new() -> Self {
        Self {
            inner: object_store::memory::InMemory::new(),
            puts: std::sync::atomic::AtomicUsize::new(0),
            multiparts: std::sync::atomic::AtomicUsize::new(0),
            failure_pending: None,
            failing_list: false,
            parts: Arc::default(),
            aborts: Arc::default(),
        }
    }

    fn failing(pending: bool) -> Self {
        Self {
            failure_pending: Some(pending),
            ..Self::new()
        }
    }

    /// A store whose listing fails on its first item, so the streaming
    /// listing has an error to surface.
    fn failing_list() -> Self {
        Self {
            failing_list: true,
            ..Self::new()
        }
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[krabka_macros::object_store_delegate(get_opts, delete_stream, list_with_delimiter, copy_opts)]
#[async_trait::async_trait]
impl object_store::ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.multiparts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(pending) = self.failure_pending {
            return Ok(Box::new(FailingUpload {
                pending,
                parts: self.parts.clone(),
                aborts: self.aborts.clone(),
                abort_release: None,
            }));
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        if self.failing_list {
            use futures_util::stream::StreamExt as _;
            return futures_util::stream::once(async {
                Err(object_store::Error::Generic {
                    store: "CountingStore",
                    source: "listing is broken".into(),
                })
            })
            .boxed();
        }
        self.inner.list(prefix)
    }
}

/// Pins the `len < threshold` boundary. One byte under the threshold must
/// take the single-PUT path and must never take multipart.
#[tokio::test]
async fn put_from_path_at_threshold_minus_one_takes_single_put() {
    let CountingFile {
        store,
        client: c,
        file: f,
    } = counting_file(&[1u8; 7]); // len 7, threshold 8
    c.put_from_path(
        &Path::from("b/under"),
        f.path(),
        8,
        4,
        PutRequest::default(),
    )
    .await
    .unwrap();
    check_upload_counts(&store, 1, 0);
}

/// Pins the other side of the boundary. Exactly the threshold must take
/// the multipart path, because the comparison is a strict `<`, not a
/// `<=`.
#[tokio::test]
async fn put_from_path_at_exact_threshold_takes_multipart() {
    let CountingFile {
        store,
        client: c,
        file: f,
    } = counting_file(&[2u8; 8]); // len 8 == threshold 8
    c.put_from_path(&Path::from("b/at"), f.path(), 8, 4, PutRequest::default())
        .await
        .unwrap();
    check_upload_counts(&store, 0, 1);
}

#[tokio::test]
async fn multipart_create_atomically_reserves_the_key() {
    let CountingFile {
        store,
        client: c,
        file: f,
    } = counting_file(&[3u8; 8]);

    c.put_from_path(&Path::from("worm/segment"), f.path(), 8, 4, PutRequest {
        mode: PutMode::Create,
        digest: true,
    })
    .await
    .unwrap();

    check_upload_counts(&store, 1, 1);
}

#[tokio::test]
async fn put_from_path_rejects_zero_chunk_size() {
    let c = client();
    let f = local_file(b"tiny");
    let key = Path::from("seg/bad");

    let err = c
        .put_from_path(&key, f.path(), 8, 0, PutRequest::default())
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        ObjectStoreError::Io(e) if e.kind() == std::io::ErrorKind::InvalidInput
    ));
}

/// A multipart `Create` cannot attach the precondition to completion, but its
/// atomic reservation still refuses a replay before starting the upload.
#[tokio::test]
async fn put_from_path_create_mode_refuses_replay_on_both_paths() {
    for (case, len) in [("single put", 7usize), ("multipart", 8usize)] {
        let c = client();
        let key = Path::from("seg/once");
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&vec![9u8; len]).unwrap();
        let req = PutRequest {
            mode: PutMode::Create,
            digest: false,
        };
        c.put_from_path(&key, f.path(), 8, 4, req.clone())
            .await
            .unwrap();

        let second = c.put_from_path(&key, f.path(), 8, 4, req).await;

        check!(
            matches!(second, Err(ObjectStoreError::AlreadyExists(_))),
            "{case}"
        );
    }
}

#[derive(Debug)]
struct FailingUpload {
    pending: bool,
    parts: Arc<std::sync::atomic::AtomicUsize>,
    aborts: Arc<std::sync::atomic::AtomicUsize>,
    abort_release: Option<Arc<tokio::sync::Notify>>,
}

#[async_trait::async_trait]
impl object_store::MultipartUpload for FailingUpload {
    fn put_part(&mut self, _data: object_store::PutPayload) -> object_store::UploadPart {
        self.parts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.pending {
            Box::pin(std::future::pending())
        } else {
            Box::pin(std::future::ready(Err(object_store::Error::Generic {
                store: "test",
                source: "part failed".into(),
            })))
        }
    }

    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        unreachable!("a failed part cannot complete")
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.aborts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(release) = &self.abort_release {
            release.notified().await;
        }
        self.parts.store(0, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

/// A client over a failing store, and the store.
struct FailingClient {
    client: ObjectStoreClient,
    store: Arc<CountingStore>,
}

fn failing_client(pending: bool) -> FailingClient {
    let store = Arc::new(CountingStore::failing(pending));
    FailingClient {
        client: ObjectStoreClient::new(store.clone()),
        store,
    }
}

async fn wait_for_abort(store: &CountingStore) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while store.aborts.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("multipart upload was not aborted");
}

async fn check_aborted_upload(store: &CountingStore) {
    wait_for_abort(store).await;
    assert!(store.parts.load(std::sync::atomic::Ordering::SeqCst) == 0);
}

async fn cancel_after_progress<T: std::fmt::Debug>(
    task: tokio::task::JoinHandle<T>,
    progress: &std::sync::atomic::AtomicUsize,
) {
    while progress.load(std::sync::atomic::Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    task.await.unwrap_err();
}

#[tokio::test]
async fn put_from_path_aborts_after_a_part_failure() {
    let FailingClient { client, store } = failing_client(false);
    let file = local_file(&[1; 12]);

    client
        .put_from_path(
            &Path::from("seg/fails"),
            file.path(),
            8,
            4,
            PutRequest::default(),
        )
        .await
        .unwrap_err();

    check_aborted_upload(&store).await;
}

#[tokio::test]
async fn put_from_path_aborts_when_cancelled() {
    let FailingClient { client, store } = failing_client(true);
    let file = local_file(&[1; 12]);
    let task = tokio::spawn(async move {
        client
            .put_from_path(
                &Path::from("seg/cancelled"),
                file.path(),
                8,
                4,
                PutRequest::default(),
            )
            .await
    });
    cancel_after_progress(task, &store.parts).await;
    check_aborted_upload(&store).await;
}

#[tokio::test]
async fn multipart_backpressure_bounds_pending_parts_and_cancellation_aborts() {
    let FailingClient { client, store } = failing_client(true);
    let file = local_file(&[1; 400]);
    let task = tokio::spawn(async move {
        client
            .put_from_path(
                &Path::from("seg/bounded"),
                file.path(),
                8,
                4,
                PutRequest::default(),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let parts = store.parts.load(std::sync::atomic::Ordering::SeqCst);
            assert!(parts <= MULTIPART_MAX_CONCURRENCY);
            if parts == MULTIPART_MAX_CONCURRENCY {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("upload reached its capacity");
    for _ in 0..100 {
        tokio::task::yield_now().await;
        assert!(store.parts.load(std::sync::atomic::Ordering::SeqCst) == MULTIPART_MAX_CONCURRENCY);
    }
    task.abort();
    let _ = task.await;
    check_aborted_upload(&store).await;
}

#[tokio::test]
async fn explicit_abort_survives_caller_cancellation() {
    let parts = Arc::new(std::sync::atomic::AtomicUsize::new(1));
    let aborts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let mut guard = AbortOnDrop::new(
        Box::new(FailingUpload {
            pending: false,
            parts: parts.clone(),
            aborts: aborts.clone(),
            abort_release: Some(release.clone()),
        }),
        &Path::from("seg/abort-cancelled"),
    );
    let task = tokio::spawn(async move { guard.abort().await });
    cancel_after_progress(task, &aborts).await;
    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while parts.load(std::sync::atomic::Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("independent abort task did not finish");
}
