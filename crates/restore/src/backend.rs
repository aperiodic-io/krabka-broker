//! The archive handle: the `--archive-*` flags turned into an object store.
//!
//! The broker holds the same backend mapping behind its own TOML config, but a
//! recovery tool must run when the broker does not, so this builds straight on
//! the object-store layer instead.

use std::sync::Arc;

use krabka_object_store::{
    ObjectOps, ObjectStoreClient, ObjectStoreConfig, build_object_store, normalize_prefix,
    prefixed_key,
};
use object_store::path::Path;

use crate::{args::RestoreArgs, error::RestoreError};

/// A read handle on the archive, with the operator's key prefix applied.
///
/// The client is fully async. `S3RemoteStorage` would also serve the reads,
/// but it uses `block_in_place`, which fails without a current multi-thread
/// Tokio runtime handle.
#[derive(Clone)]
pub struct ArchiveStore {
    store: Arc<dyn object_store::ObjectStore>,
    client: ObjectStoreClient,
    prefix: Option<String>,
}

impl ArchiveStore {
    /// The object operations to read the archive with.
    #[must_use]
    pub fn ops(&self) -> &dyn ObjectOps {
        &self.client
    }

    /// The object-store handle used by the WORM verifier.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn object_store::ObjectStore> {
        &self.store
    }

    /// The key prefix every archive key carries, absent when the archive is
    /// the whole bucket.
    #[must_use]
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// The absolute object key for a path relative to the archive root.
    #[must_use]
    pub fn key(&self, relative: &str) -> Path {
        prefixed_key(self.prefix.as_deref(), relative)
    }

    /// The prefix to list the archive root under.
    #[must_use]
    pub fn root(&self) -> Option<Path> {
        self.prefix.as_deref().map(Path::from)
    }

    /// A handle onto an already-built object store, under `prefix`.
    ///
    /// [`open_archive`] is what the binary calls; this is the seam a test
    /// drives the scan through a store of its own with, such as one that
    /// synthesises a listing no fixture could write to disk.
    #[must_use]
    pub fn with_store(store: Arc<dyn object_store::ObjectStore>, prefix: Option<&str>) -> Self {
        Self {
            client: ObjectStoreClient::new(store.clone()),
            store,
            prefix: normalize_prefix(prefix),
        }
    }
}

/// Build the archive handle the `--archive-*` flags describe.
///
/// # Errors
///
/// Returns [`RestoreError::ObjectStore`] when the backend builder rejects the
/// bucket, region, endpoint, or credentials, and
/// [`RestoreError::InvalidArgument`] when no backend was selected.
pub fn open_archive(args: &RestoreArgs) -> Result<ArchiveStore, RestoreError> {
    let config = object_store_config(args)?;
    let store: Arc<dyn object_store::ObjectStore> = build_object_store(&config)?;
    Ok(ArchiveStore {
        client: ObjectStoreClient::new(store.clone()),
        store,
        prefix: args.archive.location.normalized_prefix(),
    })
}

/// Map the `--archive-*` flags onto an object-store configuration.
///
/// The prefix is not set here. It stays on [`ArchiveStore`], so the same
/// handle can address a key inside the archive and list the archive root.
///
/// # Errors
///
/// Returns [`RestoreError::InvalidArgument`] when no backend was selected.
/// The argument parser makes exactly one of them required, so this reports a
/// caller that built [`RestoreArgs`] by hand.
pub fn object_store_config(args: &RestoreArgs) -> Result<ObjectStoreConfig, RestoreError> {
    args.archive
        .location
        .to_config()
        .map_err(|error| RestoreError::InvalidArgument(error.to_string()))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use clap::Parser as _;

    use super::*;

    fn args_from(extra: &[&str]) -> RestoreArgs {
        let mut argv = vec!["krabka-restore", "--log-dir", "/target"];
        argv.extend_from_slice(extra);
        crate::Cli::parse_from(argv).args
    }

    krabka_macros::s3_archive_config_fixture!(expected_s3_config, krabka_object_store::S3Config);

    /// The mapping itself is pinned in `krabka-object-store`; this checks the
    /// flags reach it through restore's own command line.
    #[test]
    fn s3_flags_map_onto_the_s3_config() {
        let config = object_store_config(&args_from(&[
            "--archive-s3-bucket",
            "backups",
            "--archive-s3-region",
            "eu-west-1",
            "--archive-s3-endpoint",
            "http://minio:9000",
            "--archive-s3-access-key-id",
            "key",
            "--archive-s3-secret-access-key",
            "secret",
            "--archive-s3-allow-http",
        ]))
        .expect("config");
        let ObjectStoreConfig::S3(s3) = config else {
            panic!("expected an S3 config");
        };
        check!(s3 == expected_s3_config());
    }

    #[test]
    fn a_hand_built_args_without_a_backend_is_rejected() {
        let mut args = args_from(&["--archive-local", "/archive"]);
        args.archive.location.local = None;
        check!(matches!(
            object_store_config(&args),
            Err(RestoreError::InvalidArgument(_))
        ));
    }

    #[test]
    fn keys_are_built_under_the_prefix() {
        // `LocalFileSystem` canonicalizes its root, so the archive must exist.
        let archive = tempfile::tempdir().expect("temp dir");
        let store = open_archive(&args_from(&[
            "--archive-local",
            &archive.path().display().to_string(),
            "--archive-prefix",
            "/tier/",
        ]))
        .expect("store");
        check!(store.prefix() == Some("tier"));
        check!(store.key("orders-0-abc/000.log") == Path::from("tier/orders-0-abc/000.log"));
        check!(store.root() == Some(Path::from("tier")));
    }

    /// The seam a test drives the scan through: a store built elsewhere, with
    /// the same prefix handling the flags get, and reads that reach it.
    #[tokio::test]
    async fn a_handle_on_a_store_built_elsewhere_reads_it_under_the_prefix() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ArchiveStore::with_store(inner.clone(), Some("/tier/"));

        check!(store.prefix() == Some("tier"));
        check!(store.root() == Some(Path::from("tier")));
        check!(store.key("orders-0-abc/000.log") == Path::from("tier/orders-0-abc/000.log"));

        object_store::ObjectStoreExt::put(
            inner.as_ref(),
            &store.key("orders-0-abc/000.log"),
            object_store::PutPayload::from_static(b"segment bytes"),
        )
        .await
        .expect("write an object");
        let listed = store.ops().list(store.root()).await.expect("list");
        check!(
            listed
                .iter()
                .map(|meta| meta.location.clone())
                .collect::<Vec<_>>()
                == vec![Path::from("tier/orders-0-abc/000.log")]
        );

        // And with no prefix the same store is the whole archive.
        let bare = ArchiveStore::with_store(inner, None);
        check!(bare.prefix().is_none());
        check!(bare.root().is_none());
        check!(bare.key("orders-0-abc/000.log") == Path::from("orders-0-abc/000.log"));
    }

    #[test]
    fn keys_are_bare_without_a_prefix() {
        let archive = tempfile::tempdir().expect("temp dir");
        let store = open_archive(&args_from(&[
            "--archive-local",
            &archive.path().display().to_string(),
        ]))
        .expect("store");
        check!(store.prefix().is_none());
        check!(store.key("orders-0-abc/000.log") == Path::from("orders-0-abc/000.log"));
        check!(store.root().is_none());
    }
}
