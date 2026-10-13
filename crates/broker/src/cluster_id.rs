//! The cluster id string forms this broker reports and accepts.
//!
//! Kafka's `Uuid.toString()` is URL-safe unpadded base64 of the 16 raw bytes,
//! e.g. `AQIDBAUGBwgJCgsMDQ4PEA`, not `java.util.UUID`'s hyphenated form, e.g.
//! `01020304-0506-0708-090a-0b0c0d0e0f10`. `Metadata` and `DescribeCluster`
//! report the cluster id in that base64 form, so a KIP-1242 `ApiVersions`
//! request that echoes it back, a `krabka-guard` freeze signature that covers
//! it, and every other place this broker reports or verifies its own cluster
//! id, encode with [`encode`] to match. [`matches`] additionally accepts the
//! hyphenated form, for a caller-supplied id such as `AddRaftVoter.ClusterId`
//! that a JVM tool may still send in that form.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

/// Kafka's `Uuid.toString()`: URL-safe unpadded base64 of the 16 raw bytes.
#[must_use]
pub(crate) fn encode(cluster_id: uuid::Uuid) -> String {
    URL_SAFE_NO_PAD.encode(cluster_id.as_bytes())
}

/// Whether `request` names `cluster_id`, in either Kafka's base64 `Uuid` form
/// or `java.util.UUID`'s hyphenated form.
#[must_use]
pub(crate) fn matches(request: &str, cluster_id: uuid::Uuid) -> bool {
    request == encode(cluster_id) || request == cluster_id.to_string()
}

#[cfg(test)]
mod tests {
    use {
        super::{encode, matches},
        assert2::assert,
        uuid::Uuid,
    };

    #[test]
    fn encodes_kafkas_base64_uuid_form() {
        let id = Uuid::from_bytes([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ]);
        assert!(encode(id) == "AQIDBAUGBwgJCgsMDQ4PEA");
    }

    #[test]
    fn matches_accepts_both_kafka_base64_and_hyphenated_forms() {
        let id = Uuid::from_u128(0x00000000_0000_0000_0000_0000000000ff);
        assert!(matches(&encode(id), id));
        assert!(matches(&id.to_string(), id));
        assert!(!matches("different", id));
    }
}
