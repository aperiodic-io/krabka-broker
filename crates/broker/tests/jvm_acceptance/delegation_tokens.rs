//! KIP-48 delegation-token cluster and the parser for what the token CLI
//! prints.
//!
//! The token tool emits its token id and HMAC in several layouts across Kafka
//! versions, so the cluster that mints a token and the reader that recovers it
//! stay together.

use krabka_broker::BrokerConfig;

/// Like [`start_three_broker_sasl_plaintext_jvm_cluster_with_users`] but
/// also enables `SCRAM-SHA-256` on the listener and installs the given
/// `secret_key` as the HMAC master for KIP-48 delegation tokens on every
/// broker. The admin user is provisioned as PLAIN, so the JVM CLI's
/// `kafka-delegation-tokens --create/--describe/--expire` calls can
/// authenticate over PLAIN. The *token consumer* needs the SCRAM-SHA-256
/// mechanism: `kafka-console-producer` authenticates as the new token with
/// SCRAM-SHA-256, and the broker satisfies that on the token-fallback path,
/// where `TokenID` becomes the username and the HMAC becomes the password.
///
/// Returns `(h1, h2, h3, cfg1, cfg2, cfg3, dir1, dir2, dir3)`.
pub(crate) async fn start_three_broker_sasl_plaintext_jvm_cluster_with_delegation_tokens(
    admin: &str,
    admin_pass: &str,
    secret_key: &[u8],
) -> (
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    BrokerConfig,
    BrokerConfig,
    BrokerConfig,
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    use krabka_security::{SaslMechanism, SecretBytes};

    super::three_broker_cluster::start_three_broker_sasl_plaintext_jvm_cluster_configured(
        crate::jvm_acceptance::SaslClusterSetup {
            admin,
            admin_pass,
            ..Default::default()
        },
        |config| {
            config
                .enabled_sasl_mechanisms
                .push(SaslMechanism::ScramSha256);
            config.delegation_token_secret_key = Some(SecretBytes::new(secret_key.to_vec()));
        },
    )
    .await
}

/// Parse the JVM `kafka-delegation-tokens --create` stdout for a line
/// matching `<key>\t<value>` or `<key>=<value>` and return `<value>`.
/// The tool prints both a header row and a data row separated by tabs. This
/// function scans every line and returns the first match on the key.
pub(crate) fn extract_jvm_kv(stdout: &str, key: &str) -> String {
    // The kafka-delegation-tokens tool prints output in three forms
    // across versions and code paths:
    //   1. `key = value` lines, or
    //   2. `key : value` lines (used by the "Created delegation token
    //      with tokenId : <id>" preamble), or
    //   3. a space-aligned column table:
    //         TOKENID                              HMAC      OWNER ...
    //                                                                 <- blank
    //         <id>                                 <hmac>    User:admin ...
    // Try each in order.
    for line in stdout.lines() {
        if let Some(rest) = line.strip_prefix(&format!("{key} = ")) {
            return rest.trim().to_string();
        }
        if let Some(rest) = line.strip_prefix(&format!("{key}=")) {
            return rest.trim().to_string();
        }
    }
    // `Created delegation token with tokenId : <id>` is the canonical
    // single-line output for TOKENID after a successful --create.
    if key.eq_ignore_ascii_case("tokenid") {
        for line in stdout.lines() {
            if let Some(rest) = line.split_once("tokenId :") {
                return rest.1.trim().to_string();
            }
        }
    }
    // Column table — split on runs of whitespace.
    let mut header_cols: Option<Vec<String>> = None;
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let cols: Vec<String> = trimmed.split_whitespace().map(str::to_string).collect();
        if header_cols.is_none() {
            if cols.iter().any(|c| c.eq_ignore_ascii_case(key)) {
                header_cols = Some(cols);
            }
            continue;
        }
        let idx = header_cols
            .as_ref()
            .unwrap()
            .iter()
            .position(|c| c.eq_ignore_ascii_case(key));
        if let Some(i) = idx
            && i < cols.len()
        {
            return cols[i].clone();
        }
    }
    panic!("could not extract key={key} from stdout: {stdout}");
}
