//! Generate the three broker reference pages.
//!
//! The pages are the server config, from the `FileConfig` JSON schema, the
//! topic configs whitelist, and the protocol API catalog.

use {
    crate::schema_md::render_sectioned_field_table,
    krabka_broker::api_catalog::{ClientMetricsReceiver, ListenerKind},
    std::fmt::Write,
};

/// Server-config reference, rendered from the `FileConfig` JSON schema.
///
/// The schema comes from [`krabka_broker::file_config::config_schema`], which
/// is the same value `krabka-broker --print-config-schema` prints and the same
/// value `docs/config-schema.json` holds.
///
/// This function uses [`render_sectioned_field_table`], so each top-level TOML
/// key or table becomes its own captioned section with a horizontal-rule
/// separator. The result is not one dense flat table.
#[must_use]
pub fn server_config_md() -> String {
    let value = krabka_broker::file_config::config_schema();
    let mut out = String::from(
        "Krabka brokers are configured from a TOML file. Each section below maps \
         to a top-level key or `[table]` of that file.\n\n",
    );
    out.push_str(&render_sectioned_field_table(&value));
    out
}

/// Topic-config reference: the `AlterConfigs` whitelist.
#[must_use]
pub fn topic_configs_md() -> String {
    let mut out = String::from(
        "These dynamic per-topic configs are accepted by `AlterConfigs` / \
         `IncrementalAlterConfigs`. Unknown keys are rejected with `INVALID_CONFIG`.\n\n\
         | Key | Type | Default | KIP | Description |\n\
         |-----|------|---------|-----|-------------|\n",
    );
    for d in krabka_broker::topic_config_docs() {
        let default = d.default.map_or_default(|x| format!("`{x}`"));
        let kip = d.kip.unwrap_or("—");
        let _ = writeln!(
            out,
            "| `{}` | {} | {} | {} | {} |",
            d.key, d.value_type, default, kip, d.description
        );
    }
    out
}

/// The KIP, or the protocol-guide section, that defines an API key. It renders
/// into a markdown link.
struct ApiSpec {
    label: &'static str,
    url: &'static str,
}

/// Canonical specification for an API key.
///
/// This is the KIP, or the protocol-guide section, that defines the API key.
///
/// The Kafka wire protocol is not an IETF RFC standard. The
/// [Apache Kafka protocol guide](https://kafka.apache.org/protocol) specifies
/// it, together with the per-feature KIP that introduced each RPC. This
/// function maps each API key to the KIP that defines it where we know that KIP
/// confidently. For the other API keys, it falls back to the API-keys section
/// of the protocol guide.
fn api_spec(api_key: i16) -> ApiSpec {
    // The general protocol-guide fallback for APIs without a single defining
    // KIP (the original Kafka 0.x core RPCs and a handful of admin RPCs).
    const PROTOCOL_GUIDE: ApiSpec = ApiSpec {
        label: "Protocol guide",
        url: "https://kafka.apache.org/43/design/protocol/",
    };
    const KIP_848: ApiSpec = ApiSpec {
        label: "KIP-848",
        url: "https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol",
    };
    const KIP_932: ApiSpec = ApiSpec {
        label: "KIP-932",
        url: "https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka",
    };
    const KIP_1071: ApiSpec = ApiSpec {
        label: "KIP-1071",
        url: "https://cwiki.apache.org/confluence/display/KAFKA/KIP-1071%3A+Streams+Rebalance+Protocol",
    };
    const KIP_595: ApiSpec = ApiSpec {
        label: "KIP-595",
        url: "https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum",
    };
    const KIP_630: ApiSpec = ApiSpec {
        label: "KIP-630",
        url: "https://cwiki.apache.org/confluence/display/KAFKA/KIP-630%3A+Kafka+Raft+Snapshot",
    };
    const KIP_853: ApiSpec = ApiSpec {
        label: "KIP-853",
        url: "https://cwiki.apache.org/confluence/display/KAFKA/KIP-853%3A+KRaft+Controller+Membership+Changes",
    };
    const KIP_98: ApiSpec = ApiSpec {
        label: "KIP-98",
        url: "https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging",
    };
    const KIP_500: ApiSpec = ApiSpec {
        label: "KIP-500",
        url: "https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum",
    };
    const KIP_631: ApiSpec = ApiSpec {
        label: "KIP-631",
        url: "https://cwiki.apache.org/confluence/display/KAFKA/KIP-631%3A+The+Quorum-based+Kafka+Controller",
    };
    const KIP_714: ApiSpec = ApiSpec {
        label: "KIP-714",
        url: "https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability",
    };

    // Only the KIP-specific arms are enumerated; every API key without a single
    // confidently-known defining KIP (the core 0.x RPCs, delegation tokens,
    // SCRAM/quota admin, UpdateFeatures, Envelope, transactions admin, etc.)
    // falls through to the protocol-guide permalink.
    match api_key {
        // KIP-98: idempotent producer + transactions
        // (InitProducerId, Add*ToTxn, EndTxn, WriteTxnMarkers, TxnOffsetCommit).
        22 | 24..=28 => KIP_98,
        // KIP-595: the KRaft Raft protocol (Vote/Begin/EndQuorumEpoch,
        // DescribeQuorum) and AlterPartition.
        52..=56 => KIP_595,
        // KIP-630: Raft snapshot fetch.
        59 => KIP_630,
        // KIP-500/631: KRaft broker & controller registration / heartbeat.
        62..=64 | 70 => KIP_631,
        // KIP-848: next-gen consumer group protocol.
        68 | 69 => KIP_848,
        // KIP-714: client metrics / observability telemetry.
        71 | 72 => KIP_714,
        // KIP-932: share groups (queues) and their state RPCs.
        76..=79 | 83..=87 | 90..=92 => KIP_932,
        // KIP-853: KRaft voter membership changes.
        80..=82 => KIP_853,
        // KIP-1071: Kafka Streams rebalance protocol.
        88 | 89 => KIP_1071,
        // GetReplicaLogInfo — krabka-private (1020) KRaft tooling RPC; KIP-500
        // family.
        1020 => KIP_500,
        _ => PROTOCOL_GUIDE,
    }
}

/// Protocol API catalog: every Kafka API key the broker advertises to a
/// client.
///
/// The set is the one a client listener advertises with a client-metrics
/// receiver configured. That is the widest client-facing surface: it leaves
/// out the inter-broker-only keys, which no Kafka client sends, and it keeps
/// the two KIP-714 telemetry keys, which a broker withholds only when an
/// operator configures no receiver.
#[must_use]
pub fn protocol_apis_md() -> String {
    use krabka_protocol::api_key::ApiKey;
    let mut out = String::from(
        "The Kafka protocol APIs this broker advertises to a client in its \
         `ApiVersions` (key 18) response, with the supported version range for \
         each. The wire protocol is specified by the [Apache Kafka protocol \
         guide](https://kafka.apache.org/protocol); the **Spec** column links \
         each API to the KIP (or protocol-guide section) that defines it.\n\n\
         | API Key | Name | Min Version | Max Version | Spec |\n\
         |---------|------|-------------|-------------|------|\n",
    );
    let mut apis = krabka_broker::api_catalog::supported_apis(
        ListenerKind::Client,
        ClientMetricsReceiver::Configured,
        krabka_broker::api_catalog::VersionGates::default(),
    );
    apis.sort_by_key(|a| a.api_key);
    for a in apis {
        let name: &'static str = ApiKey::from_i16(a.api_key).map_or("?", <&'static str>::from);
        let ApiSpec { label, url } = api_spec(a.api_key);
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | [{}]({}) |",
            a.api_key, name, a.min_version, a.max_version, label, url
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};
    #[test]
    fn server_config_page_has_sectioned_field_tables() {
        let md = server_config_md();
        // Sectioned layout: per-key `## ` headings + horizontal-rule separators,
        // and a known top-level table (`tls_config`) renders as its own section.
        for needle in [
            "| Field | Type | Required | Default | Description |",
            "## ",
            "\n---\n",
            "## tls_config",
        ] {
            assert2::assert!(md.contains(needle));
        }
    }
    #[test]
    fn topic_configs_page_lists_retention_ms() {
        let md = topic_configs_md();
        assert2::assert!(md.contains("`retention.ms`"));
        assert2::assert!(md.contains("| Key | Type | Default | KIP | Description |"));
    }
    #[test]
    fn protocol_apis_page_lists_named_apis() {
        let md = protocol_apis_md();
        check!(md.contains("| API Key | Name | Min Version | Max Version | Spec |"));
        check!(md.contains("| 18 | ApiVersions |"));
        check!(!md.contains("| ? |"));
    }
    #[test]
    fn protocol_apis_page_links_canonical_spec_and_kips() {
        let md = protocol_apis_md();
        for needle in [
            // Canonical protocol-guide permalink in the header.
            "https://kafka.apache.org/protocol",
            // At least one real KIP link in the Spec column.
            "[KIP-",
            "cwiki.apache.org/confluence/display/KAFKA/KIP-",
        ] {
            assert2::assert!(md.contains(needle));
        }
        // Consumer-group heartbeat (68) maps to KIP-848.
        let row = md
            .lines()
            .find(|l| l.contains("| 68 |"))
            .expect("ConsumerGroupHeartbeat row");
        assert2::assert!(row.contains("[KIP-848]"));
        // Share-group fetch (78) maps to KIP-932.
        let share = md
            .lines()
            .find(|l| l.contains("| 78 |"))
            .expect("ShareFetch row");
        assert2::assert!(share.contains("[KIP-932]"));
    }
}
