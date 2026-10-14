//! Sealing a segment copy into a signed, chained manifest.
//!
//! The archiver is the cryptographic half of a WORM copy. The backend does
//! the IO and reports what the store gave back for each object; the archiver
//! turns those observations into the manifest bytes and the chain receipt.

use {
    crate::{
        metadata::RemoteLogSegmentMetadata,
        worm::{
            chain::WormChainRecord,
            config::WormConfig,
            error::WormError,
            manifest::{
                ChainStamp, HexBytes, MANIFEST_FORMAT_VERSION, ManifestBody, ManifestSignature,
                ObjectEntry, SegmentIdentity, SegmentManifest, manifest_head,
                manifest_signing_bytes,
            },
        },
    },
    bytes::Bytes,
    krabka_audit::signing::FileEd25519Signer,
    std::sync::Arc,
};

/// Builds and signs the per-segment integrity manifest that backs WORM mode.
///
/// The archiver owns no store handle: the backend uploads the objects, hands
/// the archiver what it observed for each, and gets back manifest bytes plus a
/// receipt. Keeping the IO in the backend and the cryptography here makes the
/// manifest logic testable without an object store.
///
/// `Debug` shows the key id and never the key. The archiver holds live
/// private-key material, and a `Debug` that walked into it would put that key
/// in every log line that formats a backend.
#[derive(derive_more::Debug)]
pub struct WormArchiver {
    #[debug("{:?}", signer.as_ref().map(|signer| signer.key_id()))]
    signer: Option<Arc<FileEd25519Signer>>,
}

/// One sealed manifest: the value, the bytes to write, and the receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedManifest {
    /// The manifest, body and signature together.
    pub manifest: SegmentManifest,
    /// The exact bytes to PUT at the `.manifest` key.
    pub bytes: Bytes,
    /// Receipt for `CopySegmentFinished`; `manifest_version_id` is `None`
    /// until the PUT reports one.
    pub receipt: WormChainRecord,
}

impl WormArchiver {
    /// An archiver that signs with `signer`, or leaves manifests unsigned when
    /// `signer` is `None`.
    #[must_use]
    pub fn new(signer: Option<Arc<FileEd25519Signer>>) -> Self { Self { signer } }

    /// Loads the signing key that `cfg` names, if it names one.
    ///
    /// # Errors
    ///
    /// [`WormError::SigningKey`] when a key path is set without a key id (or
    /// vice versa), or the file is not a PKCS#8 Ed25519 key.
    pub fn from_config(cfg: &WormConfig) -> Result<Self, WormError> {
        let signer = match (cfg.signing_key_path.as_ref(), cfg.signing_key_id.as_ref()) {
            (None, None) => None,
            (Some(path), Some(key_id)) => {
                let signer = FileEd25519Signer::from_pkcs8_file(path, key_id.clone())
                    .map_err(|e| WormError::SigningKey(e.to_string()))?;
                Some(Arc::new(signer))
            }
            // A half-configured key is a misconfiguration and not a request
            // for an unsigned archive. Unsigned is what an empty config asks
            // for, and it must stay hard to reach by accident.
            (Some(_), None) => {
                return Err(WormError::SigningKey(
                    "signing key path is set without a key id".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(WormError::SigningKey(
                    "signing key id is set without a key path".to_string(),
                ));
            }
        };
        Ok(Self::new(signer))
    }

    /// Seals one segment copy: builds the manifest, chains it, and signs it.
    ///
    /// `objects` must list every object the copy wrote, in the order it wrote
    /// them.
    ///
    /// # Errors
    ///
    /// [`WormError::MissingChainStamp`] when the broker did not stamp a chain
    /// position on `metadata` — a WORM backend refuses to write an unchained,
    /// and therefore unverifiable, manifest.
    ///
    /// [`WormError::MalformedChainRecord`] when the stamp is present but does
    /// not decode, and [`WormError::Codec`] when the manifest does not
    /// serialise.
    pub fn seal(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        objects: Vec<ObjectEntry>,
    ) -> Result<SealedManifest, WormError> {
        let stamped = metadata
            .custom_metadata()
            .ok_or(WormError::MissingChainStamp)
            .and_then(WormChainRecord::from_custom_metadata)?;
        // Only the position is taken from the incoming record. Any head it
        // already carries belongs to an earlier manifest, never to this one.
        let chain = ChainStamp {
            epoch_id: stamped.epoch_id,
            seq: stamped.seq,
            prev_head: stamped.prev_head,
        };

        let body = ManifestBody {
            format_version: MANIFEST_FORMAT_VERSION,
            segment: SegmentIdentity::from_metadata(metadata),
            objects,
            chain,
        };
        let head = manifest_head(&body);
        let signature = self.signer.as_ref().map(|signer| {
            let message = manifest_signing_bytes(signer.key_id(), chain.epoch_id, chain.seq, head);
            ManifestSignature {
                key_id: signer.key_id().to_string(),
                public_key: HexBytes(signer.public_key()),
                signature: HexBytes(signer.sign(&message)),
            }
        });

        let manifest = SegmentManifest { body, signature };
        // The chain covers `canonical_manifest_bytes`, not this JSON, so the
        // JSON layout is free to be whatever reads best. Compact keeps the
        // object small; a reader that wants it indented can re-serialise it.
        let bytes = serde_json::to_vec(&manifest).map_err(|e| WormError::Codec(e.to_string()))?;
        Ok(SealedManifest {
            manifest,
            bytes: Bytes::from(bytes),
            receipt: WormChainRecord::request(chain).with_head(head),
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            metadata::{
                CustomMetadata, RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentState,
                TopicIdPartition,
            },
            worm::manifest::{
                ChainHead, EpochId, ManifestSeq, Sha256Digest, verify_manifest_signature,
            },
        },
        assert2::{assert, check},
        krabka_ids::LeaderEpoch,
        ring::{rand::SystemRandom, signature::Ed25519KeyPair},
        tempfile::TempDir,
        uuid::Uuid,
    };

    const KEY_ID: &str = "worm-archiver-key";

    fn epoch() -> EpochId { EpochId(Uuid::from_u128(0xabcd)) }

    fn metadata(segment_id: u128, stamp: Option<ChainStamp>) -> RemoteLogSegmentMetadata {
        let md = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(1), "orders", 0),
                Uuid::from_u128(segment_id),
            ),
            0,
            99,
            1_713_000_000_000,
            1,
            1_713_000_001_000,
            RemoteLogSegmentDetails::new(
                4096,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(0) => 0},
            ),
        )
        .unwrap();
        match stamp {
            Some(stamp) => {
                md.with_custom_metadata(WormChainRecord::request(stamp).to_custom_metadata())
            }
            None => md,
        }
    }

    fn objects() -> Vec<ObjectEntry> {
        vec![ObjectEntry {
            suffix: ".log".to_string(),
            key: "orders-0/00000000000000000000.log".to_string(),
            size_bytes: 10,
            sha256: Sha256Digest::of(b"0123456789"),
            e_tag: Some("0".to_string()),
            version_id: None,
            create_precondition: true,
        }]
    }

    fn signer() -> Arc<FileEd25519Signer> {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let signer =
            FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), KEY_ID.to_string()).unwrap();
        Arc::new(signer)
    }

    /// Writes a throwaway PKCS#8 Ed25519 key, for the `from_config` path that
    /// only takes a path.
    fn key_file(dir: &TempDir) -> std::path::PathBuf {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let path = dir.path().join("worm.pk8");
        std::fs::write(&path, pkcs8.as_ref()).unwrap();
        path
    }

    #[test]
    fn seal_head_chains_from_the_stamp() {
        let stamp = ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(7),
            prev_head: ChainHead([3u8; 32]),
        };
        let sealed = WormArchiver::new(Some(signer()))
            .seal(&metadata(10, Some(stamp)), objects())
            .unwrap();

        check!(sealed.manifest.body.chain == stamp);
        check!(sealed.manifest.body.objects == objects());
        check!(sealed.manifest.body.format_version == MANIFEST_FORMAT_VERSION);
        // The receipt's head is the head of the manifest it accompanies, and
        // the receipt repeats the position it was sealed at.
        check!(
            sealed.receipt
                == WormChainRecord {
                    epoch_id: stamp.epoch_id,
                    seq: stamp.seq,
                    prev_head: stamp.prev_head,
                    head: Some(manifest_head(&sealed.manifest.body)),
                    manifest_version_id: None,
                }
        );
        // The bytes are the manifest, byte for byte.
        let decoded: SegmentManifest = serde_json::from_slice(&sealed.bytes).unwrap();
        check!(decoded == sealed.manifest);
    }

    #[test]
    fn seal_without_signer_leaves_manifest_unsigned_but_chained() {
        let stamp = ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(0),
            prev_head: ChainHead::GENESIS,
        };
        let sealed = WormArchiver::new(None)
            .seal(&metadata(11, Some(stamp)), objects())
            .unwrap();

        check!(sealed.manifest.signature.is_none());
        check!(sealed.manifest.body.chain == stamp);
        check!(sealed.receipt.head == Some(manifest_head(&sealed.manifest.body)));
        // An unsigned manifest still fails signature verification: it proves
        // nothing about who wrote it.
        check!(!verify_manifest_signature(&sealed.manifest, &[]));
    }

    #[test]
    fn seal_refuses_metadata_with_no_chain_stamp() {
        let err = WormArchiver::new(Some(signer()))
            .seal(&metadata(12, None), objects())
            .unwrap_err();

        check!(matches!(err, WormError::MissingChainStamp));
    }

    #[test]
    fn seal_refuses_metadata_with_an_undecodable_chain_stamp() {
        let md = metadata(13, None).with_custom_metadata(CustomMetadata(b"not json".to_vec()));

        let err = WormArchiver::new(None).seal(&md, objects()).unwrap_err();

        check!(matches!(err, WormError::MalformedChainRecord(_)));
    }

    #[test]
    fn two_sealed_manifests_chain() {
        let archiver = WormArchiver::new(Some(signer()));
        let first = ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(0),
            prev_head: ChainHead::GENESIS,
        };

        let a = archiver
            .seal(&metadata(20, Some(first)), objects())
            .unwrap();
        assert!(let Some(second) = a.receipt.next_stamp());
        let b = archiver
            .seal(&metadata(21, Some(second)), objects())
            .unwrap();

        let a_head = manifest_head(&a.manifest.body);
        check!(b.manifest.body.chain.prev_head == a_head);
        check!(b.manifest.body.chain.seq == ManifestSeq(1));
        check!(b.manifest.body.chain.epoch_id == first.epoch_id);
        // Two distinct segments at distinct positions must not produce the
        // same head, or the chain would say nothing.
        check!(manifest_head(&b.manifest.body) != a_head);
    }

    #[test]
    fn sealed_manifest_signature_verifies_with_the_signing_key() {
        let signer = signer();
        let public_key = signer.public_key();
        let sealed = WormArchiver::new(Some(signer))
            .seal(
                &metadata(
                    30,
                    Some(ChainStamp {
                        epoch_id: epoch(),
                        seq: ManifestSeq(2),
                        prev_head: ChainHead([9u8; 32]),
                    }),
                ),
                objects(),
            )
            .unwrap();

        assert!(let Some(signature) = sealed.manifest.signature.as_ref());
        check!(signature.key_id == KEY_ID);
        check!(signature.public_key == HexBytes(public_key.clone()));
        check!(verify_manifest_signature(&sealed.manifest, &public_key));

        // The signature covers the body: an edited object list breaks it.
        let mut tampered = sealed.manifest.clone();
        tampered.body.objects[0].size_bytes += 1;
        check!(!verify_manifest_signature(&tampered, &public_key));
    }

    #[test]
    fn from_config_loads_a_key_only_when_both_halves_are_set() {
        let dir = TempDir::new().unwrap();
        let path = key_file(&dir);

        check!(
            WormArchiver::from_config(&WormConfig::default())
                .unwrap()
                .signer
                .is_none(),
            "empty config"
        );
        let both = WormConfig {
            signing_key_path: Some(path.clone()),
            signing_key_id: Some(KEY_ID.to_string()),
            write_only: false,
        };
        assert!(let Ok(archiver) = WormArchiver::from_config(&both));
        assert!(let Some(signer) = archiver.signer.as_ref());
        check!(signer.key_id() == KEY_ID);
    }

    #[yare::parameterized(
        path_without_id = { true, false, true },
        id_without_path = { false, true, true },
        missing_key_file = { true, true, false },
    )]
    fn from_config_rejects_incomplete_or_missing_signing_keys(
        has_path: bool,
        has_id: bool,
        key_file_exists: bool,
    ) {
        let dir = TempDir::new().unwrap();
        let path = if key_file_exists {
            key_file(&dir)
        } else {
            dir.path().join("absent.pk8")
        };
        let cfg = WormConfig {
            signing_key_path: has_path.then_some(path),
            signing_key_id: has_id.then(|| KEY_ID.to_string()),
            write_only: false,
        };
        check!(matches!(
            WormArchiver::from_config(&cfg),
            Err(WormError::SigningKey(_))
        ));
    }

    #[test]
    fn archiver_debug_names_the_key_and_shows_no_key_material() {
        let signer = signer();
        let public_key = hex::encode(signer.public_key());
        let rendered = format!("{:?}", WormArchiver::new(Some(signer)));

        check!(rendered.contains(KEY_ID));
        check!(!rendered.contains(&public_key));
        check!(format!("{:?}", WormArchiver::new(None)).contains("None"));
    }
}
