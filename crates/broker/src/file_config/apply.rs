//! The `FileConfig::apply_to` pipeline.
//!
//! This module holds the merge itself: the top-level scalars, then the
//! listener settings, OAUTHBEARER, delegation tokens, super users, remote
//! storage, and the config tail, in the order each depends on. Present file
//! values replace the current broker value, absent ones retain it, and a few
//! keys fill only while the target still holds its default so an explicit CLI
//! or environment value keeps precedence.

use super::{
    FileConfig, FileConfigError,
    delegation_token::apply_delegation_tokens,
    listener_settings::{ListenerSettings, apply_listener_settings},
    oauthbearer_apply::apply_oauthbearer,
    privileged_actions::apply_privileged_action_policy,
    remote_storage::apply_remote_storage,
    sasl_plain::apply_sasl_plain,
    tail::{FileConfigTail, apply_config_tail},
    topic_policy::apply_topic_policy,
    validate::positive_time,
};
use crate::{
    config::{BrokerConfig, DEFAULT_BROKER_ID},
    replica_selector::ReplicaSelectorKind,
};

impl FileConfig {
    /// The node id of a broker that holds `current` before this file applies.
    ///
    /// The file's `broker_id` replaces [`DEFAULT_BROKER_ID`], and any other id
    /// stays, so a `--broker-id` flag wins over the file. The broker binary
    /// resolves its id with this before it derives the raft node id and the
    /// seeded self-voter from it, and [`Self::apply_to`] applies the same rule.
    ///
    /// [`DEFAULT_BROKER_ID`]: crate::config::DEFAULT_BROKER_ID
    #[must_use]
    pub fn resolved_broker_id(&self, current: i32) -> i32 {
        match self.broker_id {
            Some(id) if current == DEFAULT_BROKER_ID => id,
            _ => current,
        }
    }

    /// Apply this file-config to a `BrokerConfig`. Present `[runtime]` values
    /// replace current runtime values; other file sections retain their
    /// established fill-or-replace semantics.
    ///
    /// The broker binary uses [`Self::apply_before_runtime_overlay`] and then
    /// applies explicit CLI/environment values so those inputs win.
    ///
    /// **Caller contract:** when `--config-file` is used, the caller
    /// must NOT pass `--listen-addr` or `--advertised-listener`. The
    /// binary entrypoint enforces this (see `bin/broker.rs`); this
    /// method just merges what it's given.
    // Linear config-load pipeline; each arm is its own validator construction —
    // extraction obscures the dispatch shape.
    //
    // # Errors
    //
    // * [`FileConfigError::MissingSection`] when `[authorization] type = "opa"`
    //   is set without the required `[authorization.opa]` subtable.
    // * [`FileConfigError::OpaConfig`] when [`crate::authorizer::opa::OpaAuthorizer::new`]
    //   rejects the resolved knobs (zero cache size, no tokio runtime, etc.).
    // * [`FileConfigError::SchemaRegistryConfig`] when
    //   [`crate::schema_validation::SchemaValidator::new`] rejects the resolved
    //   `[schema_registry]` knobs (zero cache size).
    // * [`FileConfigError::OperatorKeys`] when an `[[operator_keys]]` entry is
    //   unloadable, or when `[freeze]` / `[break_glass]` demands a signature
    //   that no configured key can verify.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn apply_to(self, cfg: &mut BrokerConfig) -> Result<(), FileConfigError> {
        self.apply_to_inner(cfg, true)
    }

    /// Apply file values before a higher-precedence runtime overlay.
    ///
    /// Runtime relational validation is deferred until the caller applies the
    /// final overlay and validates the resolved [`crate::config::BrokerConfig`].
    ///
    /// # Errors
    ///
    /// Returns an error when any individual file value is invalid.
    pub fn apply_before_runtime_overlay(
        self,
        cfg: &mut BrokerConfig,
    ) -> Result<(), FileConfigError> {
        self.apply_to_inner(cfg, false)
    }

    fn apply_to_inner(
        self,
        cfg: &mut BrokerConfig,
        validate_runtime: bool,
    ) -> Result<(), FileConfigError> {
        let defaults = BrokerConfig::default();
        cfg.broker_id = self.resolved_broker_id(cfg.broker_id);
        let has_runtime = self.runtime.is_some();
        if let Some(runtime) = self.runtime {
            runtime.apply_to(cfg)?;
        }
        if let Some(rack) = self.rack {
            cfg.rack = Some(rack);
        }
        if let Some(stretch) = self.stretch {
            cfg.stretch = Some(stretch.into_profile()?);
        }
        if let Some(sel) = self.replica_selector {
            cfg.replica_selector = ReplicaSelectorKind::from_config_str(&sel).map_err(|bad| {
                FileConfigError::InvalidConfig(format!("unknown replica_selector: {bad}"))
            })?;
        }
        if let Some(value) = self.heartbeat_interval
            && cfg.heartbeat_interval == defaults.heartbeat_interval
        {
            cfg.heartbeat_interval = positive_time("heartbeat_interval", value)?;
        }
        if let Some(value) = self.heartbeat_timeout
            && cfg.heartbeat_timeout == defaults.heartbeat_timeout
        {
            cfg.heartbeat_timeout = positive_time("heartbeat_timeout", value)?;
        }
        if let Some(value) = self.replica_lag_time_max
            && cfg.replica_lag_time_max == defaults.replica_lag_time_max
        {
            cfg.replica_lag_time_max = positive_time("replica_lag_time_max", value)?;
        }
        if let Some(value) = self.controller_election_timeout
            && cfg.controller_election_timeout == defaults.controller_election_timeout
        {
            cfg.controller_election_timeout = positive_time("controller_election_timeout", value)?;
        }
        if let Some(value) = self.controller_heartbeat_interval
            && cfg.controller_heartbeat_interval == defaults.controller_heartbeat_interval
        {
            cfg.controller_heartbeat_interval =
                positive_time("controller_heartbeat_interval", value)?;
            cfg.controller_heartbeat_interval_explicit = true;
        }
        if let Some(ld) = self.log_dir
            && cfg.log_dir == defaults.log_dir
        {
            cfg.log_dir = std::path::PathBuf::from(ld);
        }
        if !self.extra_log_dirs.is_empty() && cfg.extra_log_dirs.is_empty() {
            cfg.extra_log_dirs = self
                .extra_log_dirs
                .into_iter()
                .map(std::path::PathBuf::from)
                .collect();
        }
        if let Some(dir) = self.metadata_log_dir
            && cfg.metadata_log_dir.is_none()
        {
            cfg.metadata_log_dir = Some(std::path::PathBuf::from(dir));
        }
        apply_listener_settings(
            ListenerSettings {
                listeners: self.listeners,
                inter_broker_listener_name: self.inter_broker_listener_name,
                max_connections: self.max_connections,
                max_connections_per_ip: self.max_connections_per_ip,
                connections_max_idle: self.connections_max_idle,
                connections_max_reauth: self.connections_max_reauth,
                server_properties: self.server_properties,
                controller_listener_protocol: self.controller_listener_protocol,
                tls_config: self.tls_config,
            },
            cfg,
            &defaults,
        )?;
        apply_oauthbearer(self.oauthbearer, cfg)?;

        if let Some(bindings) = self.inter_broker_principal_node_ids {
            cfg.inter_broker_principal_node_ids = bindings
                .into_iter()
                .map(|(principal, node_id)| (principal, krabka_raft::NodeId(node_id)))
                .collect();
        }

        apply_delegation_tokens(self.delegation_token.as_ref(), cfg)?;

        apply_sasl_plain(self.sasl_plain.as_ref(), cfg)?;

        // Merge the TOML super-user list into the broker's
        // set (initially empty). `extend` over `clone_from` because a
        // future CLI/programmatic source may pre-populate entries that
        // we should preserve. The `[authorization]` block
        // below may overwrite this with its own super-user list.
        if let Some(vec) = self.super_users {
            cfg.super_users.extend(
                vec.iter()
                    .map(|entry| super::authorization::super_user_name(entry)),
            );
        }

        // `[remote_storage]` enables tiered storage broker-
        // wide. Exactly one of `storage_dir` (local filesystem),
        // `[remote_storage.s3]` (S3-compatible object store), or
        // `[remote_storage.gcs]` (native Google Cloud Storage) selects the
        // backend. More than one set → error.
        apply_remote_storage(self.remote_storage.as_ref(), cfg)?;

        // Pluggable cluster authorizer. When `[authorization]`
        // is present, its `super_users` list becomes the broker's
        // authoritative super-user set (overwriting whatever the
        // top-level list contributed above — operator O2
        // emits exactly one of the two sources). When absent, fall
        // through to the default [`AllowAllAuthorizer`] and leave
        // `cfg.super_users` as whatever the earlier extend produced.
        apply_config_tail(
            FileConfigTail {
                authorization: self.authorization,
                process: self.process,
                gssapi: self.gssapi,
                inter_broker_credentials: self.inter_broker_credentials,
                controller_quorum_voters: self.controller_quorum_voters,
                bootstrap_servers: self.bootstrap_servers,
                auto_join: self.auto_join,
                controller_server_name: self.controller_server_name,
                audit: self.audit,
                schema_registry: self.schema_registry,
            },
            cfg,
        )?;

        // `[[operator_keys]]`, `[freeze]` and `[break_glass]`: one trust set
        // shared by the freeze signature path and the break-glass approval
        // path, plus the two rules that cross those sections.
        apply_privileged_action_policy(&self.operator_keys, self.freeze, self.break_glass, cfg)?;

        // `[topic_policy]`: the KIP-108 / KIP-133 rule set.
        apply_topic_policy(self.topic_policy, cfg)?;

        if has_runtime && validate_runtime {
            cfg.validate()
                .map_err(|error| FileConfigError::InvalidConfig(error.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_security::ListenerProtocol;
    use krabka_units::{millis, secs};

    use super::*;
    use crate::{
        config::BrokerConfig,
        file_config::{FileListener, test_support::configured_unwrap_parse},
    };

    #[test]
    fn empty_toml_round_trips() {
        let cfg: FileConfig = toml::from_str("").unwrap();
        assert!(cfg == FileConfig::default());
    }

    #[test]
    fn applies_inter_broker_principal_node_ids() {
        let cfg = configured_unwrap_parse(
            r"
[inter_broker_principal_node_ids]
admin = 1
",
        )
        .unwrap();

        assert!(cfg.inter_broker_principal_node_ids.get("admin") == Some(&krabka_raft::NodeId(1)));
    }

    #[test]
    fn full_toml_round_trips() {
        let src = r#"
broker_id = 0
log_dir = "/var/lib/krabka/data"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "demo-0:9092"
protocol = "Plaintext"

[[listeners]]
name = "EXTERNAL"
bind_addr = "0.0.0.0:9094"
advertised = "10.0.1.5:32100"
protocol = "Plaintext"

[server_properties]
"log.retention.hours" = "24"
"#;
        let cfg: FileConfig = toml::from_str(src).unwrap();
        let expected = FileConfig {
            schema_registry: None,
            operator_keys: vec![],
            freeze: None,
            break_glass: None,
            topic_policy: None,
            runtime: None,
            broker_id: Some(0),
            log_dir: Some("/var/lib/krabka/data".to_string()),
            extra_log_dirs: vec![],
            metadata_log_dir: None,
            rack: None,
            replica_selector: None,
            stretch: None,
            heartbeat_interval: None,
            heartbeat_timeout: None,
            replica_lag_time_max: None,
            controller_election_timeout: None,
            controller_heartbeat_interval: None,
            inter_broker_listener_name: Some("PLAIN".to_string()),
            max_connections: None,
            max_connections_per_ip: None,
            connections_max_idle: None,
            connections_max_reauth: None,
            controller_quorum_voters: vec![],
            bootstrap_servers: vec![],
            auto_join: None,
            controller_server_name: None,
            listeners: vec![
                FileListener {
                    name: "PLAIN".to_string(),
                    bind_addr: "0.0.0.0:9092".parse().unwrap(),
                    advertised: "demo-0:9092".to_string(),
                    protocol: ListenerProtocol::Plaintext,
                    tls_config: None,
                    sasl_config: None,
                    connections_max_idle: None,
                    connections_max_reauth: None,
                },
                FileListener {
                    name: "EXTERNAL".to_string(),
                    bind_addr: "0.0.0.0:9094".parse().unwrap(),
                    advertised: "10.0.1.5:32100".to_string(),
                    protocol: ListenerProtocol::Plaintext,
                    tls_config: None,
                    sasl_config: None,
                    connections_max_idle: None,
                    connections_max_reauth: None,
                },
            ],
            server_properties: maplit::btreemap! {"log.retention.hours".to_string() => "24".to_string()},
            controller_listener_protocol: None,
            tls_config: None,
            oauthbearer: None,
            delegation_token: None,
            super_users: None,
            remote_storage: None,
            authorization: None,
            process: None,
            sasl_plain: None,
            gssapi: None,
            inter_broker_credentials: None,
            inter_broker_principal_node_ids: None,
            audit: None,
        };
        assert!(cfg == expected);
    }
    #[test]
    fn apply_to_log_dir_fills_default_but_preserves_existing() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str(r#"log_dir = "/var/lib/krabka/file""#).unwrap();

        let mut default_cfg = BrokerConfig::default();
        file.clone().apply_to(&mut default_cfg).unwrap();
        assert!(default_cfg.log_dir == std::path::PathBuf::from("/var/lib/krabka/file"));

        let mut existing_cfg = BrokerConfig {
            log_dir: std::path::PathBuf::from("/var/lib/krabka/cli"),
            ..BrokerConfig::default()
        };
        file.apply_to(&mut existing_cfg).unwrap();
        assert!(existing_cfg.log_dir == std::path::PathBuf::from("/var/lib/krabka/cli"));
    }
    /// `metadata_log_dir` is Kafka's `metadata.log.dir`: the file sets it
    /// unless `--metadata-log-dir` already did, and unset it leaves the
    /// metadata log in `log_dir`, the first entry of `log.dirs`. A separate
    /// metadata directory is not a data directory.
    #[test]
    fn metadata_log_dir_resolves_like_kafkas_metadata_log_dir() {
        use std::path::PathBuf;

        use crate::config::BrokerConfig;

        // (what, file, flag, metadata dir, data dirs)
        let cases = [
            (
                "unset",
                "log_dir = \"/data/a\"\nextra_log_dirs = [\"/data/b\"]",
                None,
                "/data/a",
                vec!["/data/a", "/data/b"],
            ),
            (
                "a separate directory",
                "log_dir = \"/data/a\"\nmetadata_log_dir = \"/meta\"",
                None,
                "/meta",
                vec!["/data/a"],
            ),
            (
                "one of the data directories",
                "log_dir = \"/data/a\"\nextra_log_dirs = [\"/data/b\"]\nmetadata_log_dir = \"/data/b\"",
                None,
                "/data/b",
                vec!["/data/a", "/data/b"],
            ),
            (
                "the flag wins over the file",
                "log_dir = \"/data/a\"\nmetadata_log_dir = \"/meta\"",
                Some("/flag"),
                "/flag",
                vec!["/data/a"],
            ),
        ];
        for (what, toml, flag, metadata, data) in cases {
            let file: FileConfig = toml::from_str(toml).unwrap();
            let mut cfg = BrokerConfig {
                metadata_log_dir: flag.map(PathBuf::from),
                ..BrokerConfig::default()
            };
            file.apply_to(&mut cfg).unwrap();
            assert!(
                (cfg.metadata_dir().to_path_buf(), cfg.all_log_dirs())
                    == (
                        PathBuf::from(metadata),
                        data.into_iter().map(PathBuf::from).collect::<Vec<_>>()
                    ),
                "{what}"
            );
        }
    }

    #[test]
    fn apply_to_extra_log_dirs_fills_empty_but_preserves_existing() {
        use crate::config::BrokerConfig;

        let file: FileConfig = toml::from_str(r#"extra_log_dirs = ["/mnt/a", "/mnt/b"]"#).unwrap();

        let mut default_cfg = BrokerConfig::default();
        file.clone().apply_to(&mut default_cfg).unwrap();
        assert!(
            default_cfg.extra_log_dirs
                == vec![
                    std::path::PathBuf::from("/mnt/a"),
                    std::path::PathBuf::from("/mnt/b"),
                ]
        );

        let mut existing_cfg = BrokerConfig {
            extra_log_dirs: vec![std::path::PathBuf::from("/mnt/cli")],
            ..BrokerConfig::default()
        };
        file.apply_to(&mut existing_cfg).unwrap();
        assert!(existing_cfg.extra_log_dirs == vec![std::path::PathBuf::from("/mnt/cli")]);

        let mut empty_file_existing_cfg = BrokerConfig {
            extra_log_dirs: vec![std::path::PathBuf::from("/mnt/cli")],
            ..BrokerConfig::default()
        };
        FileConfig::default()
            .apply_to(&mut empty_file_existing_cfg)
            .unwrap();
        assert!(
            empty_file_existing_cfg.extra_log_dirs == vec![std::path::PathBuf::from("/mnt/cli")]
        );
    }
    #[test]
    fn apply_to_does_not_clobber_non_default_broker_id() {
        use crate::config::BrokerConfig;

        let src = r"broker_id = 42";
        let file: FileConfig = toml::from_str(src).unwrap();
        // simulate CLI --broker-id 7 already applied
        let mut cfg = BrokerConfig {
            broker_id: 7,
            ..BrokerConfig::default()
        };

        file.apply_to(&mut cfg).unwrap();

        // CLI value wins because it differs from default.
        assert!(cfg.broker_id == 7);
    }
    #[test]
    fn apply_to_fills_in_default_broker_id() {
        use crate::config::BrokerConfig;

        let src = r"broker_id = 42";
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = BrokerConfig::default(); // broker_id == default (1)

        file.apply_to(&mut cfg).unwrap();

        assert!(cfg.broker_id == 42);
    }
    #[test]
    fn apply_to_fills_heartbeat_and_lag_tunables() {
        let src = r#"
heartbeat_interval = "500ms"
heartbeat_timeout = "1500ms"
replica_lag_time_max = "2s"
controller_election_timeout = "500ms"
controller_heartbeat_interval = "100ms"
"#;
        let cfg = configured_unwrap_parse(src).unwrap();

        check!(cfg.heartbeat_interval == millis(500));
        check!(cfg.heartbeat_timeout == millis(1500));
        check!(cfg.replica_lag_time_max == secs(2));
        check!(cfg.controller_election_timeout == millis(500));
        check!(cfg.controller_heartbeat_interval == millis(100));
    }
    #[test]
    fn apply_to_parses_rack_and_replica_selector() {
        use crate::replica_selector::ReplicaSelectorKind;
        let src = r#"
broker_id = 0
rack = "az-1"
replica_selector = "rack-aware"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse");
        let mut broker = BrokerConfig::default();
        cfg.apply_to(&mut broker).expect("apply");
        assert!(broker.rack.as_deref() == Some("az-1"));
        assert!(broker.replica_selector == ReplicaSelectorKind::RackAware);
    }
    #[test]
    fn apply_to_rejects_unknown_replica_selector() {
        let src = r#"
broker_id = 0
replica_selector = "nonsense"
"#;
        let cfg: FileConfig = toml::from_str(src).expect("parse");
        let mut broker = BrokerConfig::default();
        assert!(cfg.apply_to(&mut broker).is_err());
    }
    #[test]
    fn file_config_schema_generates() {
        let schema = schemars::schema_for!(FileConfig);
        let value = serde_json::to_value(&schema).expect("schema serializes");
        assert!(
            value.get("properties").is_some(),
            "FileConfig schema has properties"
        );
    }
}
