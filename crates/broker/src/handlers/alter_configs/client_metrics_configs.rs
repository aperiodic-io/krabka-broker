//! The authoritative `V1ClientMetricsConfig` record an `AlterConfigs`
//! `CLIENT_METRICS` resource becomes.
//!
//! `AlterConfigs` replaces a subscription's whole override map, so unlike
//! `IncrementalAlterConfigs`'
//! [`super::super::incremental_alter_configs::client_metrics_scope`] (which
//! merges per-key SET/DELETE operations onto the current map), this builder
//! never reads the subscription's stored overrides: the request carries the
//! complete set of non-default values. Every value is validated per KIP-714.

use {
    crate::client_metrics,
    krabka_metadata::{ClientMetricsConfigRecord, MetadataRecord},
    krabka_protocol::owned::alter_configs_request::AlterConfigsResource,
};

/// Build the authoritative `V1ClientMetricsConfig` record for a
/// `CLIENT_METRICS` resource. The request carries the *complete* set of
/// non-default values, so the map this builds is the whole override map.
pub(super) fn client_metrics_config_record(
    resource: &AlterConfigsResource,
) -> Result<MetadataRecord, (i16, String)> {
    // Kafka's `legacyAlterConfigResource` turns a null value into a
    // deletion, so it is left out of the map `ClientMetricsConfigs.validate`
    // checks.
    let overrides: std::collections::BTreeMap<String, String> = resource
        .configs
        .iter()
        .filter_map(|cfg| Some((cfg.name.clone(), cfg.value.clone()?)))
        .collect();
    client_metrics::config::validate(&resource.resource_name, &overrides)
        .map_err(|error| (error.code(), error.message().to_string()))?;
    Ok(MetadataRecord::V1ClientMetricsConfig(
        ClientMetricsConfigRecord {
            name: resource.resource_name.clone(),
            configs: overrides,
        },
    ))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{codes, handlers::alter_configs::test_support::client_metrics_resource},
        assert2::assert,
    };

    #[test]
    fn client_metrics_replacement_builds_authoritative_override_map() {
        let record = client_metrics_config_record(&client_metrics_resource("sub-a", &[
            ("interval.ms", "60000"),
            ("metrics", "org.apache.kafka.consumer."),
        ]))
        .expect("valid client-metrics replacement");

        let expected = MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
            name: "sub-a".into(),
            configs: maplit::btreemap! {
                "interval.ms".to_string() => "60000".to_string(),
                "metrics".to_string() => "org.apache.kafka.consumer.".to_string(),
            },
        });
        assert!(record == expected);
    }

    /// `ClientMetricsConfigs.validate`: an unknown key, an out-of-range
    /// interval and an empty subscription name are `InvalidRequestException`
    /// (42), a value `ConfigDef` cannot parse is `ConfigException` (40).
    #[test]
    fn client_metrics_replacement_refusals_match_kafka() {
        for (name, subscription, configs, expected) in [
            (
                "interval below the minimum",
                "sub-a",
                &[("interval.ms", "5")][..],
                (
                    codes::INVALID_REQUEST,
                    "Invalid value 5 for interval.ms, interval must be between 100 and 3600000 \
                     (1 hour)",
                ),
            ),
            (
                "unknown key",
                "sub-a",
                &[("bogus.key", "x")][..],
                (
                    codes::INVALID_REQUEST,
                    "Unknown client metrics configuration: bogus.key",
                ),
            ),
            (
                "interval not an int",
                "sub-a",
                &[("interval.ms", "abc")][..],
                (
                    codes::INVALID_CONFIG,
                    "Invalid value abc for configuration interval.ms: Not a number of type INT",
                ),
            ),
            (
                "empty subscription name",
                "",
                &[][..],
                (codes::INVALID_REQUEST, "Subscription name can't be empty"),
            ),
        ] {
            let error =
                client_metrics_config_record(&client_metrics_resource(subscription, configs))
                    .expect_err(name);
            assert2::check!(error == (expected.0, expected.1.to_string()), "case {name}");
        }
    }
}
