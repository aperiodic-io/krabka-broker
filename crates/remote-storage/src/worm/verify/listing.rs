//! What the object store actually holds, grouped the way a partition sits in
//! it.
//!
//! The verifier never trusts a manifest about the archive's shape, so it lists
//! the store once and compares every later claim against that listing. This
//! module produces the listing and answers the one question the listing alone
//! settles: which objects no manifest accounts for.

use {
    super::manifest_read::KeyedManifest,
    crate::worm::{error::WormError, manifest::MANIFEST_SUFFIX},
    futures_util::TryStreamExt as _,
    object_store::{ObjectStore, path::Path},
    std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    },
};

/// One directory's objects: full object-store key to size in bytes.
pub(super) type DirListing = BTreeMap<String, u64>;

/// Lists the archive and groups it by the directory each key sits in.
pub(super) async fn list_archive(
    store: &Arc<dyn ObjectStore>,
    prefix: Option<&str>,
) -> Result<BTreeMap<String, DirListing>, WormError> {
    let root = prefix.map(Path::from);
    let mut stream = store.list(root.as_ref());
    let mut dirs: BTreeMap<String, DirListing> = BTreeMap::new();
    loop {
        let next = stream.try_next().await.map_err(|e| {
            WormError::Archive(format!(
                "cannot list the archive under `{}`: {e}",
                prefix.unwrap_or("")
            ))
        })?;
        let Some(meta) = next else { break };
        let key = meta.location.to_string();
        let dir = key
            .rsplit_once('/')
            .map_or_else(String::new, |(dir, _)| dir.to_string());
        dirs.entry(dir).or_default().insert(key, meta.size);
    }
    Ok(dirs)
}

/// Objects in the directory that no manifest names, sorted by key.
pub(super) fn orphans(listing: &DirListing, decoded: &[KeyedManifest]) -> Vec<String> {
    let referenced: BTreeSet<&str> = decoded
        .iter()
        .flat_map(|KeyedManifest { manifest, .. }| {
            manifest
                .body
                .objects
                .iter()
                .map(|object| object.key.as_str())
        })
        .collect();
    listing
        .keys()
        .filter(|key| !key.ends_with(MANIFEST_SUFFIX) && !referenced.contains(key.as_str()))
        .cloned()
        .collect()
}
