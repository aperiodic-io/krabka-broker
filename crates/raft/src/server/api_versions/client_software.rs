//! The KIP-511 `client_software_name` and `client_software_version` check that
//! `ApiVersions` v3 and above applies to a handshake.
//!
//! The broker listener and both controller-listener paths (before and after
//! SASL authentication) run the same check. So it lives here, and the broker
//! re-exports it.

/// KIP-511 client-information validity check. Matches the JVM
/// `ApiVersionsRequest.isValid` regex
/// `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?`:
///
/// - non-empty
/// - first and last chars are `[a-zA-Z0-9]`
/// - interior chars are `[a-zA-Z0-9\-.]`
///
/// A single alphanumeric char is valid, because the optional middle group lets
/// the first and last char coincide.
///
/// The check is a byte scan instead of a `regex` dependency. Every Kafka-client
/// name in use stays within ASCII, so full UTF-8 char-class semantics are not
/// needed.
#[must_use]
pub fn is_valid_client_info(s: &str) -> bool {
    let bytes = s.as_bytes();
    let is_alnum = |b: u8| b.is_ascii_alphanumeric();
    let is_interior = |b: u8| b.is_ascii_alphanumeric() || b == b'-' || b == b'.';
    match bytes.len() {
        0 => false,
        1 => is_alnum(bytes[0]),
        n => {
            is_alnum(bytes[0])
                && is_alnum(bytes[n - 1])
                && bytes[1..n - 1].iter().all(|&b| is_interior(b))
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    // ── KIP-511 client-info validation ─────────────────────────────────────

    #[test]
    fn valid_client_info_accepts_typical_names() {
        for s in [
            "apache-kafka-java",
            "krabka-client-core",
            "librdkafka",
            "kafka-python",
            "node-rdkafka",
            "Sarama",
            "3.6.2",
            "0.0.0",
            "1.0.0-SNAPSHOT",
            "a", // single alnum char — allowed
            "1.2.3.4",
        ] {
            assert!(is_valid_client_info(s), "{s:?} should be valid");
        }
    }

    #[test]
    fn valid_client_info_rejects_empty() {
        assert!(!is_valid_client_info(""));
    }

    #[test]
    fn valid_client_info_rejects_leading_or_trailing_special() {
        for s in ["-leading", "trailing-", ".dotstart", "dotend.", "-only-"] {
            assert!(!is_valid_client_info(s), "{s:?} should be rejected");
        }
    }

    #[test]
    fn valid_client_info_rejects_disallowed_interior_chars() {
        for s in [
            "has space",
            "has/slash",
            "has\\backslash",
            "has;semi",
            "has@at",
            "has(paren)",
            "has\"quote",
            "café", // non-ASCII alphanumeric — KIP-511 regex is ASCII-only
        ] {
            assert!(!is_valid_client_info(s), "{s:?} should be rejected");
        }
    }
}
