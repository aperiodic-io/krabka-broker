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

    #[yare::parameterized(
        apache_java = { "apache-kafka-java" },
        krabka_client = { "krabka-client-core" },
        librdkafka = { "librdkafka" },
        kafka_python = { "kafka-python" },
        node_rdkafka = { "node-rdkafka" },
        sarama = { "Sarama" },
        version = { "3.6.2" },
        zero_version = { "0.0.0" },
        snapshot_version = { "1.0.0-SNAPSHOT" },
        single_alnum = { "a" },
        four_part_version = { "1.2.3.4" },
    )]
    fn valid_client_info_accepts_typical_names(s: &str) {
        assert2::assert!(is_valid_client_info(s), "{s:?} should be valid");
    }

    #[test]
    fn valid_client_info_rejects_empty() {
        assert!(!is_valid_client_info(""));
    }

    #[yare::parameterized(
        leading_hyphen = { "-leading" },
        trailing_hyphen = { "trailing-" },
        leading_dot = { ".dotstart" },
        trailing_dot = { "dotend." },
        only_hyphens = { "-only-" },
    )]
    fn valid_client_info_rejects_leading_or_trailing_special(s: &str) {
        assert2::assert!(!is_valid_client_info(s), "{s:?} should be rejected");
    }

    #[yare::parameterized(
        space = { "has space" },
        slash = { "has/slash" },
        backslash = { "has\\backslash" },
        semicolon = { "has;semi" },
        at = { "has@at" },
        parentheses = { "has(paren)" },
        quote = { "has\"quote" },
        non_ascii = { "café" },
    )]
    fn valid_client_info_rejects_disallowed_interior_chars(s: &str) {
        // KIP-511's alphanumeric class is ASCII-only.
        assert2::assert!(!is_valid_client_info(s), "{s:?} should be rejected");
    }
}
