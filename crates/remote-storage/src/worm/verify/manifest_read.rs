//! Getting one manifest out of an attacker-controlled object.
//!
//! Every manifest the verifier reads comes from the archive it is auditing, so
//! this module is the boundary that decides whether an object is a manifest at
//! all. It caps the read, refuses a body it cannot decode, and refuses a format
//! version it does not implement, and it reports each refusal as archive
//! content rather than as a failure to look.

use {
    crate::worm::{
        error::WormError,
        manifest::{MANIFEST_FORMAT_VERSION, SegmentManifest},
    },
    krabka_object_store::{ObjectStoreError, read_capped},
    object_store::{ObjectStore, path::Path},
    std::sync::Arc,
};

/// Byte cap on one manifest object.
///
/// A manifest describes a single segment copy, so a real one is a few
/// kilobytes. The cap is what stops a hostile archive from making the verifier
/// buffer an arbitrary object: [`read_capped`] issues a `HEAD` first and
/// refuses an oversized object before it reads a byte of it.
pub const MAX_MANIFEST_BYTES: u64 = 1024 * 1024;

/// One manifest object, as far as the verifier could take it.
pub(super) enum ManifestRead {
    /// The object decoded into a manifest of a supported format version.
    Decoded(Box<SegmentManifest>),
    /// The object is not a manifest this verifier accepts, and why.
    Rejected(String),
}

/// Reads and decodes one manifest object.
pub(super) async fn read_manifest(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<ManifestRead, WormError> {
    let bytes = match read_capped(store, &Path::from(key), MAX_MANIFEST_BYTES).await {
        Ok(bytes) => bytes,
        // An oversized object is archive content and not a failure to look, so
        // it grades as a break rather than as an error.
        Err(ObjectStoreError::TooLarge {
            size, max_bytes, ..
        }) => {
            return Ok(ManifestRead::Rejected(format!(
                "manifest object is {size} bytes, above the {max_bytes} byte cap"
            )));
        }
        Err(e) => {
            return Err(WormError::Archive(format!(
                "cannot read manifest `{key}`: {e}"
            )));
        }
    };
    let manifest: SegmentManifest = match serde_json::from_slice(&bytes) {
        Ok(manifest) => manifest,
        Err(e) => {
            return Ok(ManifestRead::Rejected(format!(
                "manifest does not decode: {e}"
            )));
        }
    };
    if let Some(reason) = format_version_problem(manifest.body.format_version) {
        return Ok(ManifestRead::Rejected(reason));
    }
    Ok(ManifestRead::Decoded(Box::new(manifest)))
}

/// Why a manifest of `version` is refused, or `None` for
/// [`MANIFEST_FORMAT_VERSION`], the only version a 1.x verifier reads.
fn format_version_problem(version: u32) -> Option<String> {
    match version {
        MANIFEST_FORMAT_VERSION => None,
        1 => Some(format!(
            "manifest format version 1 predates krabka 1.0, whose verifier reads only version \
             {MANIFEST_FORMAT_VERSION}"
        )),
        other => Some(format!(
            "unsupported manifest format version {other}: this build reads only version \
             {MANIFEST_FORMAT_VERSION}"
        )),
    }
}

/// One manifest object and the key it was read from.
pub(super) struct KeyedManifest {
    pub key: String,
    pub manifest: SegmentManifest,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::worm::verify::test_support::{Archive, put_raw},
        assert2::check,
        bytes::Bytes,
    };

    #[tokio::test]
    async fn a_manifest_of_any_version_but_the_current_one_is_a_break() {
        for (version, expected) in [
            (
                1,
                "manifest format version 1 predates krabka 1.0, whose verifier reads only \
                 version 2",
            ),
            (
                0,
                "unsupported manifest format version 0: this build reads only version 2",
            ),
            (
                3,
                "unsupported manifest format version 3: this build reads only version 2",
            ),
        ] {
            let archive = Archive::build(&[1]).await;
            let segment = &archive.segments[0];
            let mut value = serde_json::to_value(&segment.manifest).unwrap();
            value["body"]["format_version"] = serde_json::json!(version);
            put_raw(
                &archive.ops,
                &segment.manifest_key,
                Bytes::from(serde_json::to_vec(&value).unwrap()),
            )
            .await;

            let report = archive.verify().await;

            check!(
                report.first_break().map(|found| found.reason.as_str()) == Some(expected),
                "version {version}"
            );
        }
    }

    #[test]
    fn only_the_current_manifest_version_is_read() {
        check!(MANIFEST_FORMAT_VERSION == 2);
        check!(format_version_problem(MANIFEST_FORMAT_VERSION) == None);
    }

    #[tokio::test]
    async fn an_oversized_manifest_object_is_a_break_and_not_an_error() {
        let archive = Archive::build(&[1]).await;
        let huge = vec![b'{'; usize::try_from(MAX_MANIFEST_BYTES).unwrap() + 1];
        put_raw(
            &archive.ops,
            &archive.segments[0].manifest_key,
            Bytes::from(huge),
        )
        .await;

        let report = archive.verify().await;

        check!(!report.ok());
        match report.first_break() {
            Some(found) => {
                check!(found.reason.contains("above the"));
            }
            None => {
                check!(false, "an oversized manifest must record a break");
            }
        }
    }
}
