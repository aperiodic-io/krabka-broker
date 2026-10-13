//! The authoritative `V1GroupConfig` record an `AlterConfigs` GROUP resource
//! becomes.
//!
//! `AlterConfigs` replaces a group's whole override map, so unlike
//! `IncrementalAlterConfigs`' [`super::super::incremental_alter_configs::group_scope`]
//! (which merges per-key SET/DELETE operations onto the current map), this
//! builder never reads the group's stored overrides: the request carries the
//! complete set of non-default values.

use {
    crate::{
        api_catalog::UnstableApiVersions,
        codes,
        config_keys::group::{GroupBounds, validate_group_configs},
    },
    krabka_metadata::{GroupConfigRecord, MetadataRecord},
    krabka_protocol::owned::alter_configs_request::AlterConfigsResource,
};

/// Build the authoritative `V1GroupConfig` record for a GROUP resource. The
/// request carries the *complete* set of non-default values, so the map this
/// builds is the whole override map.
pub(super) fn group_config_record(
    resource: &AlterConfigsResource,
    bounds: &GroupBounds,
    unstable: UnstableApiVersions,
) -> Result<MetadataRecord, (i16, String)> {
    if resource.resource_name.is_empty() {
        return Err((
            codes::INVALID_REQUEST,
            "Default group resources are not allowed.".into(),
        ));
    }
    let overrides: std::collections::BTreeMap<String, String> = resource
        .configs
        .iter()
        .map(|cfg| (cfg.name.clone(), cfg.value.clone().unwrap_or_default()))
        .collect();
    validate_group_configs(&overrides, bounds, unstable)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    Ok(MetadataRecord::V1GroupConfig(GroupConfigRecord {
        group_id: resource.resource_name.clone(),
        configs: overrides,
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            api_catalog::UnstableApiVersions,
            coordinator::unified::streams::config::{
                KEY_NUM_STANDBY_REPLICAS, KEY_SESSION_TIMEOUT_MS,
            },
            handlers::alter_configs::test_support::group_resource,
        },
        assert2::assert,
    };

    #[test]
    fn group_replacement_builds_authoritative_override_map() {
        let record = group_config_record(
            &group_resource("streams-app", &[(KEY_NUM_STANDBY_REPLICAS, "1")]),
            &GroupBounds::default(),
            UnstableApiVersions::Enabled,
        )
        .expect("valid group replacement");

        let expected = MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: "streams-app".into(),
            configs: maplit::btreemap! {
                crate::coordinator::unified::streams::config::KEY_NUM_STANDBY_REPLICAS.to_string()
                    => "1".to_string()
            },
        });
        assert!(record == expected);
    }

    #[test]
    fn group_replacement_rejects_unknown_key() {
        let error = group_config_record(
            &group_resource("streams-app", &[("bogus.key", "1")]),
            &GroupBounds::default(),
            UnstableApiVersions::Enabled,
        )
        .expect_err("unknown group config key must be rejected");
        assert!(error.0 == codes::INVALID_CONFIG);
    }

    #[test]
    fn group_replacement_rejects_out_of_bounds_value() {
        let error = group_config_record(
            &group_resource("streams-app", &[(KEY_SESSION_TIMEOUT_MS, "1000")]),
            &GroupBounds::default(),
            UnstableApiVersions::Enabled,
        )
        .expect_err("out-of-bounds session timeout must be rejected");
        assert!(error.0 == codes::INVALID_CONFIG);
    }

    #[test]
    fn group_replacement_rejects_empty_group_id() {
        let error = group_config_record(
            &group_resource("", &[]),
            &GroupBounds::default(),
            UnstableApiVersions::Enabled,
        )
        .expect_err("empty group id must be rejected");
        assert!(error.0 == codes::INVALID_REQUEST);
    }
}
