//! Capped buffered reads.
//!
//! The module does a `head()` on an object, rejects the object if it exceeds a
//! byte cap, then does a conditional, streaming GET. It centralises the OOM guard that runs before
//! the code buffers a whole object, such as an index snapshot, into memory.

use {
    super::{error::ObjectStoreError, object_store_api as object_store},
    bytes::{Bytes, BytesMut},
    futures_util::TryStreamExt as _,
    object_store::{GetOptions, ObjectStoreExt as _, path::Path},
    std::sync::Arc,
};

/// Read a whole object, and reject it if its size exceeds `max_bytes`.
///
/// The function does a `head()` first, so it refuses an oversized object
/// *before* it buffers any bytes. This is the guard against OOM on a corrupt or
/// malicious object.
/// The GET uses the HEAD version and etag when available. Its response size
/// and streamed chunks are also checked, so an overwrite cannot bypass the cap.
///
/// # Errors
///
/// - [`ObjectStoreError::TooLarge`] if the object is larger than `max_bytes`.
/// - [`ObjectStoreError::NotFound`] if the object does not exist.
/// - [`ObjectStoreError::Precondition`] if its etag changed after the HEAD.
/// - [`ObjectStoreError::Backend`] for any other backend failure.
pub async fn read_capped(
    store: &Arc<dyn object_store::ObjectStore>,
    key: &Path,
    max_bytes: u64,
) -> Result<Bytes, ObjectStoreError> {
    let meta = store.head(key).await?;
    if meta.size > max_bytes {
        return Err(ObjectStoreError::TooLarge {
            key: key.clone(),
            size: meta.size,
            max_bytes,
        });
    }
    let result = store
        .get_opts(key, GetOptions {
            version: meta.version,
            if_match: meta.e_tag,
            ..GetOptions::default()
        })
        .await?;
    if result.meta.size > max_bytes {
        return Err(ObjectStoreError::TooLarge {
            key: key.clone(),
            size: result.meta.size,
            max_bytes,
        });
    }
    let mut stream = result.into_stream();
    let mut bytes = BytesMut::new();
    while let Some(chunk) = stream.try_next().await? {
        let size = u64::try_from(bytes.len())
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        if size > max_bytes {
            return Err(ObjectStoreError::TooLarge {
                key: key.clone(),
                size,
                max_bytes,
            });
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes.freeze())
}

#[cfg(test)]
mod tests {
    use {
        super::{object_store, *},
        assert2::assert,
        futures_util::StreamExt as _,
        object_store::{PutPayload, path::Path},
        std::sync::{Arc, atomic::Ordering::SeqCst},
    };

    async fn store_with(key: &str, bytes: &'static [u8]) -> Arc<dyn object_store::ObjectStore> {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        store
            .put(&Path::from(key), PutPayload::from(bytes))
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn under_cap_returns_bytes() {
        let store = store_with("k", b"hello").await;
        let got = read_capped(&store, &Path::from("k"), 1024).await.unwrap();
        assert!(&got[..] == b"hello");
    }

    #[tokio::test]
    async fn over_cap_returns_too_large() {
        let store = store_with("k", b"hello world").await;
        let err = read_capped(&store, &Path::from("k"), 4).await.unwrap_err();
        assert!(matches!(err, ObjectStoreError::TooLarge {
            size: 11,
            max_bytes: 4,
            ..
        }));
    }

    #[tokio::test]
    async fn missing_object_maps_to_not_found() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let err = read_capped(&store, &Path::from("absent"), 1024)
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotFound(_)));
    }

    #[derive(Debug, Clone, Copy)]
    enum Race {
        Etag,
        Version,
        Metadata,
        Body,
    }

    #[derive(Debug)]
    struct ChangingStore {
        inner: object_store::memory::InMemory,
        race: Race,
        chunks: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl std::fmt::Display for ChangingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ChangingStore")
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
    impl object_store::ObjectStore for ChangingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if options.head {
                let mut result = self.inner.get_opts(location, options).await?;
                if matches!(self.race, Race::Version) {
                    result.meta.version = Some("original".into());
                } else if !matches!(self.race, Race::Etag) {
                    result.meta.e_tag = None;
                }
                return Ok(result);
            }
            if matches!(self.race, Race::Version) {
                assert!(options.version.as_deref() == Some("original"));
                let original = self
                    .inner
                    .get_opts(location, GetOptions {
                        version: None,
                        ..options
                    })
                    .await?;
                self.inner
                    .put(location, PutPayload::from_static(b"123456789"))
                    .await?;
                return Ok(original);
            }
            self.inner
                .put(location, PutPayload::from_static(b"123456789"))
                .await?;
            let mut result = self.inner.get_opts(location, options).await?;
            if matches!(self.race, Race::Body) {
                result.meta.size = 3;
                let chunks = self.chunks.clone();
                result.payload = object_store::GetResultPayload::Stream(
                    futures_util::stream::iter((0..3).map(|_| Ok(Bytes::from_static(b"123"))))
                        .inspect(move |_| {
                            chunks.fetch_add(1, SeqCst);
                        })
                        .boxed(),
                );
            }
            Ok(result)
        }
    }

    #[yare::parameterized(
        etag = { Race::Etag },
        metadata = { Race::Metadata },
        body = { Race::Body },
    )]
    #[test_macro(tokio::test)]
    async fn overwrite_between_head_and_get_cannot_bypass_the_cap(race: Race) {
        let changing = Arc::new(ChangingStore {
            inner: object_store::memory::InMemory::new(),
            race,
            chunks: Arc::default(),
        });
        let key = Path::from("changing");
        changing
            .put(&key, PutPayload::from_static(b"123"))
            .await
            .unwrap();
        let store: Arc<dyn object_store::ObjectStore> = changing.clone();
        let error = read_capped(&store, &key, 5).await.unwrap_err();
        match race {
            Race::Etag => assert2::assert!(matches!(error, ObjectStoreError::Precondition { .. })),
            Race::Version => unreachable!("the versioned case returns the original bytes"),
            Race::Metadata => {
                assert2::assert!(matches!(error, ObjectStoreError::TooLarge { size: 9, .. }));
            }
            Race::Body => {
                assert2::assert!(matches!(error, ObjectStoreError::TooLarge { size: 6, .. }));
                assert2::assert!(changing.chunks.load(SeqCst) == 2);
            }
        }
    }

    #[tokio::test]
    async fn versioned_get_reads_the_head_version_after_an_overwrite() {
        let changing = Arc::new(ChangingStore {
            inner: object_store::memory::InMemory::new(),
            race: Race::Version,
            chunks: Arc::default(),
        });
        let key = Path::from("versioned");
        changing
            .put(&key, PutPayload::from_static(b"123"))
            .await
            .unwrap();
        let store: Arc<dyn object_store::ObjectStore> = changing.clone();
        assert!(read_capped(&store, &key, 5).await.unwrap().as_ref() == b"123");
        assert!(changing.inner.head(&key).await.unwrap().size == 9);
    }
}
