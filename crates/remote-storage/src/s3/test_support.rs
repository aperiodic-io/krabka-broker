//! Fixtures shared by the S3 backend's unit tests: an `InMemory`-backed
//! [`S3RemoteStorage`], the segment metadata and on-disk segment files a copy
//! needs, and the throwaway signing key and chain stamp a WORM archive needs.

use std::sync::Arc;

use krabka_object_store::fault::{FaultInjectingStore, FaultPolicy};
use object_store::{ObjectStore, memory::InMemory};
use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
use tempfile::TempDir;
use uuid::Uuid;

use super::S3RemoteStorage;
pub(super) use crate::test_support::{sample_data, sample_metadata, write_file};
use crate::{
    metadata::RemoteLogSegmentMetadata,
    storage_manager::RemoteStorageManager,
    worm::{ChainHead, ChainStamp, EpochId, ManifestSeq, WormChainRecord, WormConfig},
};

pub(super) const WORM_KEY_ID: &str = "s3-worm-key";

pub(super) fn rsm(prefix: Option<&str>) -> S3RemoteStorage {
    S3RemoteStorage::with_store(Arc::new(InMemory::new()), prefix.map(str::to_string))
}

/// [`rsm`] over a store that counts the requests reaching it, so a test can
/// assert how many an operation issues rather than only what it leaves behind.
/// The policy injects no faults; only the counters are of interest.
pub(super) fn counting_rsm() -> (S3RemoteStorage, Arc<FaultInjectingStore>) {
    let counter = Arc::new(FaultInjectingStore::new(
        Arc::new(InMemory::new()),
        FaultPolicy::none(),
    ));
    (S3RemoteStorage::with_store(counter.clone(), None), counter)
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum WormAccess {
    #[default]
    ReadWrite,
    WriteOnly,
}

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct SeededCopySetup {
    #[default(sample_metadata(Uuid::from_u128(10)))]
    pub(super) metadata: RemoteLogSegmentMetadata,
    pub(super) transaction_index: crate::test_support::TransactionIndex,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct StampedMetadataSetup {
    #[default(Uuid::from_u128(52))]
    pub(super) segment_id: Uuid,
    #[default(ManifestSeq(0))]
    pub(super) sequence: ManifestSeq,
    #[default(ChainHead::GENESIS)]
    pub(super) previous_head: ChainHead,
}

impl SeededCopySetup {
    pub(super) fn without_transaction_index() -> Self {
        Self {
            transaction_index: crate::test_support::TransactionIndex::Omitted,
            ..Default::default()
        }
    }
}

/// A readable memory archive with a signing key whose directory stays alive with the fixture.
pub(super) fn memory_worm_archive() -> (TempDir, S3RemoteStorage) {
    let keys = TempDir::new().unwrap();
    let store = worm_rsm(Arc::new(InMemory::new()), &keys, WormAccess::ReadWrite);
    (keys, store)
}

/// Copy one [`sample_data`] segment into `store` as `md` on the blocking
/// pool, where the store's synchronous API may block, and then run `then`
/// there with both.
pub(super) async fn seeded_blocking(
    store: S3RemoteStorage,
    setup: SeededCopySetup,
    then: impl FnOnce(S3RemoteStorage, RemoteLogSegmentMetadata) + Send + 'static,
) {
    let SeededCopySetup {
        metadata: md,
        transaction_index,
    } = setup;
    tokio::task::spawn_blocking(move || {
        let src = TempDir::new().unwrap();
        store
            .copy_log_segment_data(&md, &sample_data(src.path(), transaction_index))
            .unwrap();
        then(store, md);
    })
    .await
    .unwrap();
}

/// The chain epoch every stamped fixture belongs to.
pub(super) fn worm_epoch() -> EpochId {
    EpochId(Uuid::from_u128(0x5eed))
}

/// A [`WormConfig`] naming a throwaway PKCS#8 Ed25519 key written into
/// `dir`. `ring` mints it because `krabka-audit` exposes no key generator.
pub(super) fn worm_config(dir: &std::path::Path, access: WormAccess) -> WormConfig {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let path = dir.join("worm.pk8");
    std::fs::write(&path, pkcs8.as_ref()).unwrap();
    WormConfig {
        signing_key_path: Some(path),
        signing_key_id: Some(WORM_KEY_ID.to_string()),
        write_only: access == WormAccess::WriteOnly,
    }
}

/// An archive backed by `store`, signing with a key under `keys`.
pub(super) fn worm_rsm(
    store: Arc<dyn ObjectStore>,
    keys: &TempDir,
    access: WormAccess,
) -> S3RemoteStorage {
    S3RemoteStorage::with_store(store, None)
        .with_worm_unchecked(&worm_config(keys.path(), access))
        .unwrap()
}

/// [`sample_metadata`] plus the chain stamp the broker leaves on a segment
/// before it asks for the copy.
pub(super) fn stamped_metadata(setup: StampedMetadataSetup) -> RemoteLogSegmentMetadata {
    let StampedMetadataSetup {
        segment_id,
        sequence,
        previous_head,
    } = setup;
    sample_metadata(segment_id).with_custom_metadata(
        WormChainRecord::request(ChainStamp {
            epoch_id: worm_epoch(),
            seq: sequence,
            prev_head: previous_head,
        })
        .to_custom_metadata(),
    )
}
