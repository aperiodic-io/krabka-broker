//! The archive the capture is written to: the `--archive-*` flags turned into
//! an object store, plus the operations this tool needs from it.
//!
//! The flags carry the names `krabka restore` gives them, because the capture
//! is written into the archive the restore reads and an operator must not have
//! to learn two spellings for one bucket.

use std::{collections::BTreeSet, sync::Arc};

pub use krabka_object_store::ArchiveArgs;
use krabka_object_store::{ObjectOps, ObjectStoreClient, PutMode, PutRequest, build_object_store};
use object_store::path::Path;

use crate::error::BackupError;

/// A read and write handle on the archive, with the operator's key prefix
/// applied.
pub struct Archive {
    client: ObjectStoreClient,
    prefix: Option<String>,
}

impl Archive {
    /// Build the handle the `--archive-*` flags describe.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::InvalidArgument`] when the flags contradict each
    /// other, and [`BackupError::ObjectStore`] when the backend builder rejects
    /// the bucket, region, endpoint, or credentials.
    pub fn open(args: &ArchiveArgs) -> Result<Self, BackupError> {
        args.validate()?;
        let store: Arc<dyn object_store::ObjectStore> = build_object_store(&args.to_config()?)?;
        Ok(Self {
            client: ObjectStoreClient::new(store),
            prefix: args.normalized_prefix(),
        })
    }

    /// The absolute object key for a path relative to the archive root.
    #[must_use]
    pub fn key(&self, relative: &str) -> Path {
        krabka_object_store::prefixed_key(self.prefix.as_deref(), relative)
    }

    /// Write one object, overwriting whatever was there.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::ObjectStore`] when the write fails.
    pub async fn put(&self, relative: &str, bytes: Vec<u8>) -> Result<(), BackupError> {
        self.client
            .put(&self.key(relative), bytes.into(), PutRequest::default())
            .await?;
        Ok(())
    }

    /// Create a capture object, refusing to replace an existing object.
    ///
    /// # Errors
    /// Returns [`BackupError::ObjectStore`] when the key already exists or the write fails.
    pub async fn put_new(&self, relative: &str, bytes: Vec<u8>) -> Result<(), BackupError> {
        self.client
            .put(
                &self.key(relative),
                bytes.into(),
                PutRequest {
                    mode: PutMode::Create,
                    ..PutRequest::default()
                },
            )
            .await?;
        Ok(())
    }

    /// Read one object whole.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::ObjectStore`] when the object is absent or the
    /// read fails.
    pub async fn get(&self, relative: &str) -> Result<Vec<u8>, BackupError> {
        Ok(self.client.get(&self.key(relative)).await?.to_vec())
    }

    /// Read an archive-relative object within an independent byte limit.
    ///
    /// # Errors
    /// Returns [`BackupError::ObjectStore`] when the object is absent, exceeds `max_bytes`, changes during the read, or cannot be read.
    pub async fn get_capped(&self, relative: &str, max_bytes: u64) -> Result<Vec<u8>, BackupError> {
        Ok(self
            .client
            .get_capped(&self.key(relative), max_bytes)
            .await?
            .to_vec())
    }

    /// Read an exact object key, refusing oversized bodies before buffering.
    ///
    /// # Errors
    /// Returns [`BackupError::ObjectStore`] when the object is absent, exceeds
    /// `max_bytes`, or cannot be read.
    pub async fn get_absolute_capped(
        &self,
        key: &str,
        max_bytes: u64,
    ) -> Result<Vec<u8>, BackupError> {
        Ok(self
            .client
            .get_capped(&Path::from(key), max_bytes)
            .await?
            .to_vec())
    }

    /// The distinct directory names one level below `relative`.
    ///
    /// Object stores have no directories, so this lists the keys under the
    /// prefix and keeps the segment that follows it. An empty listing is an
    /// empty set rather than an error: a bucket that has never been captured
    /// into is not a failure, and the caller says what that means.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::ObjectStore`] when the listing fails.
    pub async fn child_directories(&self, relative: &str) -> Result<BTreeSet<String>, BackupError> {
        let prefix = self.key(relative);
        let listing = self.client.list(Some(prefix.clone())).await?;
        let under = format!("{prefix}/");
        Ok(listing
            .into_iter()
            .filter_map(|meta| {
                let key = meta.location.as_ref().to_owned();
                let rest = key.strip_prefix(&under)?;
                let (head, _) = rest.split_once('/')?;
                (!head.is_empty()).then(|| head.to_owned())
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use bytes::Bytes;
    use krabka_object_store::{ObjectStoreClient, ObjectStoreError, fault::FaultInjectingStore};
    use object_store::{ObjectStoreExt as _, PutPayload, path::Path};

    use super::{Archive, ArchiveArgs, BTreeSet};
    use crate::error::{BackupError, EXIT_BAD_ARGUMENT};

    fn local_args(root: &std::path::Path) -> ArchiveArgs {
        ArchiveArgs {
            local: Some(root.to_path_buf()),
            ..ArchiveArgs::default()
        }
    }

    #[test]
    fn a_key_carries_the_normalized_prefix_when_there_is_one() {
        let root = tempfile::tempdir().expect("archive root");
        let archive = Archive::open(&ArchiveArgs {
            prefix: Some(" /prod/tier/ ".to_owned()),
            ..local_args(root.path())
        })
        .expect("open a local archive");
        check!(
            archive.key("restore-inputs/1/manifest.json").as_ref()
                == "prod/tier/restore-inputs/1/manifest.json"
        );

        let bare = Archive::open(&local_args(root.path())).expect("open a local archive");
        check!(
            bare.key("restore-inputs/1/manifest.json").as_ref() == "restore-inputs/1/manifest.json"
        );
    }

    /// The shared flag checks reach the operator as a bad argument, exit 2.
    #[test]
    fn a_flag_set_the_archive_cannot_mean_is_a_bad_argument() {
        let root = tempfile::tempdir().expect("archive root");
        let cases = [
            (
                ArchiveArgs {
                    s3_region: Some("eu-west-1".to_owned()),
                    ..local_args(root.path())
                },
                "--archive-s3-region needs --archive-s3-bucket",
            ),
            (
                ArchiveArgs::default(),
                "no archive backend selected: pass one of --archive-local, \
                 --archive-s3-bucket, or --archive-gcs-bucket",
            ),
        ];
        for (args, expected) in cases {
            let Err(error) = Archive::open(&args) else {
                panic!("{expected}: the archive opened")
            };
            check!(matches!(&error, BackupError::InvalidArgument(_)));
            check!(error.exit_code() == EXIT_BAD_ARGUMENT);
            check!(error.to_string() == expected);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_archive_lists_the_directories_one_level_below_a_prefix() {
        let root = tempfile::tempdir().expect("archive root");
        let archive = Archive::open(&local_args(root.path())).expect("open the archive");
        check!(
            archive
                .child_directories("restore-inputs")
                .await
                .expect("list an archive that has never been captured into")
                .is_empty()
        );

        for key in [
            "restore-inputs/0000000000000001/manifest.json",
            "restore-inputs/0000000000000001/rlmm-snapshot",
            "restore-inputs/0000000000000002/manifest.json",
            // A key directly under the root names no capture directory.
            "restore-inputs/stray",
        ] {
            archive
                .put(key, b"bytes".to_vec())
                .await
                .expect("write an object");
        }

        check!(
            archive
                .child_directories("restore-inputs")
                .await
                .expect("list the captures")
                == BTreeSet::from(["0000000000000001".to_owned(), "0000000000000002".to_owned()])
        );
        check!(
            archive
                .get("restore-inputs/stray")
                .await
                .expect("read an object back")
                == b"bytes".to_vec()
        );
    }

    #[tokio::test]
    async fn capture_objects_cannot_replace_a_completed_object() {
        let root = tempfile::tempdir().unwrap();
        let archive = Archive::open(&local_args(root.path())).unwrap();
        archive
            .put_new("capture/manifest.json", b"first".to_vec())
            .await
            .unwrap();
        check!(
            archive
                .put_new("capture/manifest.json", b"second".to_vec())
                .await
                .is_err()
        );
        check!(archive.get("capture/manifest.json").await.unwrap() == b"first");
    }

    async fn archive_that_grows_during_get(key: &str, prefix: Option<&str>) -> Archive {
        let store = Arc::new(
            FaultInjectingStore::memory().with_get_overwrite(Bytes::from_static(b"123456")),
        );
        store
            .put(&Path::from(key), PutPayload::from_static(b"123"))
            .await
            .unwrap();
        Archive {
            client: ObjectStoreClient::new(store),
            prefix: prefix.map(str::to_owned),
        }
    }

    #[tokio::test]
    async fn capped_relative_reads_apply_the_prefix_and_detect_growth() {
        let archive = archive_that_grows_during_get("prod/document", Some("prod")).await;
        check!(matches!(
            archive.get_capped("document", 5).await,
            Err(BackupError::ObjectStore(
                ObjectStoreError::Precondition { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn an_absolute_read_rejects_an_oversized_object_before_fetching_it() {
        let root = tempfile::tempdir().expect("archive root");
        std::fs::write(root.path().join("wal.ckwl"), b"oversized").expect("write object");
        let archive = Archive::open(&local_args(root.path())).expect("open the archive");

        let error = archive
            .get_absolute_capped("wal.ckwl", 4)
            .await
            .expect_err("object exceeds the cap");

        check!(error.to_string().contains("exceeds cap of 4 bytes"));
    }

    #[tokio::test]
    async fn an_absolute_read_rejects_growth_between_head_and_get() {
        let archive = archive_that_grows_during_get("wal.ckwl", None).await;
        let error = archive
            .get_absolute_capped("wal.ckwl", 5)
            .await
            .unwrap_err();
        check!(matches!(
            error,
            BackupError::ObjectStore(ObjectStoreError::Precondition { .. })
        ));
    }
}
