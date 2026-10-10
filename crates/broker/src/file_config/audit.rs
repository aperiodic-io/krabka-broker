//! The `[audit]` TOML shapes for the `FedRAMP` 20x MLA audit subsystem.
//!
//! [`FileAuditConfig`] has three subtables: signing, checkpoint cadence, and
//! the durable spool for the AU-5 degraded path. An absent `[audit]` table
//! or subtable keeps the broker's current settings, as other absent tables do.
//! The broker's own defaults are the secure settings, so a broker with no
//! `[audit]` block still audits to the standard internal topic.

use std::num::NonZeroU64;

use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};
use schemars::JsonSchema;
use serde::Deserialize;

/// `[audit]` section of `broker.toml` (`FedRAMP` 20x MLA).
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditConfig {
    /// Whether the audit subsystem is active.
    #[serde(default = "default_audit_enabled")]
    pub enabled: bool,
    /// Whether privileged operations continue when audit processing fails.
    #[serde(default = "default_audit_failure_mode")]
    #[schemars(with = "String")]
    pub failure_mode: krabka_audit::AuditMode,
    /// Internal topic name for audit records. It has to start with `__`: the
    /// broker reports it internal on `Metadata` and refuses to freeze it by
    /// that prefix, and a name outside the convention would satisfy only the
    /// first of the two.
    #[serde(default = "default_audit_topic")]
    pub topic: String,
    /// Ed25519 checkpoint signing key. An absent table keeps the current key.
    /// The broker has none by default: chaining only, no checkpoints.
    pub signing: Option<FileAuditSigningConfig>,
    /// Checkpoint emission cadence. An absent table keeps the current
    /// cadence, which starts at the defaults.
    pub checkpoint: Option<FileAuditCheckpointConfig>,
    /// Durable spool for the AU-5 degraded path. An absent table keeps the
    /// current spool settings, which start at the defaults.
    pub spool: Option<FileAuditSpoolConfig>,
}

impl Default for FileAuditConfig {
    fn default() -> Self {
        Self {
            enabled: default_audit_enabled(),
            failure_mode: krabka_audit::AuditMode::FailOpen,
            topic: default_audit_topic(),
            signing: None,
            checkpoint: None,
            spool: None,
        }
    }
}

/// `[audit.spool]` — durable spool for the AU-5 degraded path.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditSpoolConfig {
    /// Directory that holds the spool files. A relative path resolves under
    /// the broker's log directory.
    #[serde(default = "default_spool_dir")]
    pub dir: String,
    /// Cap on the total size of the spool on disk.
    #[serde(default = "default_spool_max_bytes")]
    pub max_bytes: u64,
    /// Number of appended records between durable file syncs.
    #[serde(default = "default_spool_sync_every_n")]
    pub sync_every_n: NonZeroU64,
}

impl Default for FileAuditSpoolConfig {
    fn default() -> Self {
        Self {
            dir: default_spool_dir(),
            max_bytes: default_spool_max_bytes(),
            sync_every_n: default_spool_sync_every_n(),
        }
    }
}

fn default_spool_dir() -> String {
    crate::config::DEFAULT_AUDIT_SPOOL_DIR.to_string()
}

fn default_spool_max_bytes() -> u64 {
    crate::config::DEFAULT_AUDIT_SPOOL_MAX.bytes_u64()
}

fn default_spool_sync_every_n() -> NonZeroU64 {
    crate::config::DEFAULT_AUDIT_SPOOL_SYNC_EVERY_N
}

/// `[audit.signing]` — Ed25519 checkpoint signing key.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditSigningConfig {
    /// Path to the PKCS#8 Ed25519 private key that signs audit checkpoints.
    pub key_path: String,
    /// Key id recorded on each checkpoint, so a verifier can follow a key
    /// rotation.
    pub key_id: String,
}

/// `[audit.checkpoint]` — checkpoint cadence.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuditCheckpointConfig {
    /// Emit a checkpoint after this many audit records.
    #[serde(default = "default_checkpoint_every_n")]
    pub every_n: u64,
    /// Emit a checkpoint at least this often, in seconds.
    #[serde(default = "default_checkpoint_every_secs")]
    pub every_secs: u64,
}

impl Default for FileAuditCheckpointConfig {
    fn default() -> Self {
        Self {
            every_n: default_checkpoint_every_n(),
            every_secs: default_checkpoint_every_secs(),
        }
    }
}

fn default_checkpoint_every_n() -> u64 {
    crate::config::DEFAULT_AUDIT_CHECKPOINT_EVERY_N
}

fn default_checkpoint_every_secs() -> u64 {
    crate::config::DEFAULT_AUDIT_CHECKPOINT_EVERY
        .secs_i64()
        .cast_unsigned()
}

fn default_audit_enabled() -> bool {
    true
}

fn default_audit_failure_mode() -> krabka_audit::AuditMode {
    krabka_audit::AuditMode::FailOpen
}

fn default_audit_topic() -> String {
    crate::config::DEFAULT_AUDIT_TOPIC.to_string()
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, path::PathBuf};

    use krabka_units::{ByteSize, Time, kibibytes, secs};

    use crate::{config::BrokerConfig, file_config::FileConfig};

    // Every audit setting of a `BrokerConfig`, so a test compares them as one
    // value.
    #[derive(Debug, Clone, PartialEq)]
    struct AuditSettings {
        enabled: bool,
        failure_mode: krabka_audit::AuditMode,
        topic: String,
        signing_key_path: Option<PathBuf>,
        signing_key_id: Option<String>,
        checkpoint_every_n: u64,
        checkpoint_every: Time,
        spool_dir: PathBuf,
        spool_max: ByteSize,
        spool_sync_every_n: NonZeroU64,
    }

    impl AuditSettings {
        fn of(cfg: &BrokerConfig) -> Self {
            Self {
                enabled: cfg.audit_enabled,
                failure_mode: cfg.audit_failure_mode,
                topic: cfg.audit_topic.clone(),
                signing_key_path: cfg.audit_signing_key_path.clone(),
                signing_key_id: cfg.audit_signing_key_id.clone(),
                checkpoint_every_n: cfg.audit_checkpoint_every_n,
                checkpoint_every: cfg.audit_checkpoint_every,
                spool_dir: cfg.audit_spool_dir.clone(),
                spool_max: cfg.audit_spool_max,
                spool_sync_every_n: cfg.audit_spool_sync_every_n,
            }
        }

        fn broker_config(&self) -> BrokerConfig {
            BrokerConfig {
                audit_enabled: self.enabled,
                audit_failure_mode: self.failure_mode,
                audit_topic: self.topic.clone(),
                audit_signing_key_path: self.signing_key_path.clone(),
                audit_signing_key_id: self.signing_key_id.clone(),
                audit_checkpoint_every_n: self.checkpoint_every_n,
                audit_checkpoint_every: self.checkpoint_every,
                audit_spool_dir: self.spool_dir.clone(),
                audit_spool_max: self.spool_max,
                audit_spool_sync_every_n: self.spool_sync_every_n,
                ..BrokerConfig::for_tests(PathBuf::from("/tmp/x"))
            }
        }
    }

    // An absent `[audit]` table, like any other absent table, keeps what the
    // broker config already holds, and so does an absent subtable of a
    // present one. A present table and its subtables still apply.
    fn check_default_audit(cfg: &crate::config::BrokerConfig) {
        assert2::check!(cfg.audit_enabled);
        assert2::check!(cfg.audit_failure_mode == krabka_audit::AuditMode::FailOpen);
        assert2::check!(cfg.audit_topic == "__krabka_audit");
    }

    #[test]
    fn apply_keeps_the_audit_settings_the_file_leaves_out() {
        let embedded = AuditSettings {
            enabled: false,
            failure_mode: krabka_audit::AuditMode::FailClosed,
            topic: "__lab_audit".into(),
            signing_key_path: Some(PathBuf::from("/lab/audit.pk8")),
            signing_key_id: Some("lab-key".into()),
            checkpoint_every_n: 7,
            checkpoint_every: secs(9),
            spool_dir: PathBuf::from("/lab/spool"),
            spool_max: kibibytes(64),
            spool_sync_every_n: NonZeroU64::new(3).unwrap(),
        };
        let cases = [
            ("no [audit] table", "", embedded.clone()),
            (
                "an [audit] table with no subtables",
                "[audit]\nenabled = true\n",
                AuditSettings {
                    enabled: true,
                    failure_mode: krabka_audit::AuditMode::FailOpen,
                    topic: "__krabka_audit".into(),
                    ..embedded.clone()
                },
            ),
            (
                "an [audit] table with every subtable",
                r#"
                    [audit]
                    enabled = true
                    failure_mode = "fail-open"
                    topic = "__file_audit"
                    [audit.signing]
                    key_path = "/etc/krabka/audit.pk8"
                    key_id = "audit-2026"
                    [audit.checkpoint]
                    every_n = 500
                    every_secs = 30
                    [audit.spool]
                    dir = "/var/lib/krabka/audit-spool"
                    max_bytes = 2048
                    sync_every_n = 5
                "#,
                AuditSettings {
                    enabled: true,
                    failure_mode: krabka_audit::AuditMode::FailOpen,
                    topic: "__file_audit".into(),
                    signing_key_path: Some(PathBuf::from("/etc/krabka/audit.pk8")),
                    signing_key_id: Some("audit-2026".into()),
                    checkpoint_every_n: 500,
                    checkpoint_every: secs(30),
                    spool_dir: PathBuf::from("/var/lib/krabka/audit-spool"),
                    spool_max: kibibytes(2),
                    spool_sync_every_n: NonZeroU64::new(5).unwrap(),
                },
            ),
        ];
        let mut applied_rows = Vec::new();
        let mut expected_rows = Vec::new();
        for (name, toml, expected) in cases {
            let file: FileConfig = toml::from_str(toml).expect("parse");
            let mut cfg = embedded.broker_config();
            file.apply_to(&mut cfg).expect("apply");
            applied_rows.push((name, AuditSettings::of(&cfg)));
            expected_rows.push((name, expected));
        }
        assert2::assert!(applied_rows == expected_rows);
    }

    fn applied_for_tests(file: FileConfig) -> crate::config::BrokerConfig {
        let mut cfg = crate::config::BrokerConfig::for_tests(std::path::PathBuf::from("/tmp/x"));
        file.apply_to(&mut cfg).expect("apply");
        cfg
    }

    #[test]
    fn audit_section_parses_and_applies() {
        let toml = r#"
            [audit]
            enabled = true
            topic = "__krabka_audit"
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse audit section");
        let audit = fc.audit.clone().expect("audit present");
        assert2::check!(audit.enabled);
        assert2::check!(audit.topic == "__krabka_audit");

        let cfg = applied_for_tests(fc);
        check_default_audit(&cfg);
    }

    #[test]
    fn audit_defaults_to_enabled_with_internal_topic() {
        // Absent [audit] section → secure default (enabled, standard topic name).
        let fc: FileConfig = toml::from_str("").expect("parse empty");
        let cfg = applied_for_tests(fc);
        check_default_audit(&cfg);
    }

    #[test]
    fn audit_signing_and_checkpoint_parse_and_apply() {
        let toml = r#"
            [audit]
            enabled = true

            [audit.signing]
            key_path = "/etc/krabka/audit.pk8"
            key_id = "audit-2026"

            [audit.checkpoint]
            every_n = 500
            every_secs = 30
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let cfg = applied_for_tests(fc);
        assert2::check!(
            cfg.audit_signing_key_path == Some(std::path::PathBuf::from("/etc/krabka/audit.pk8"))
        );
        assert2::check!(cfg.audit_signing_key_id.as_deref() == Some("audit-2026"));
        assert2::check!(cfg.audit_checkpoint_every_n == 500);
        assert2::check!(cfg.audit_checkpoint_every == secs(30));
    }

    #[test]
    fn audit_checkpoint_has_sane_defaults_when_absent() {
        let fc: FileConfig = toml::from_str("[audit]\nenabled = true\n").expect("parse");
        let cfg = applied_for_tests(fc);
        assert2::check!(cfg.audit_signing_key_path == None);
        assert2::check!(cfg.audit_signing_key_id == None);
        assert2::check!(cfg.audit_checkpoint_every_n == 1000);
        assert2::check!(cfg.audit_checkpoint_every == secs(60));
    }

    #[test]
    fn audit_spool_parses_and_defaults() {
        let toml = r#"
            [audit]
            enabled = true
            failure_mode = "fail-closed"
            [audit.spool]
            dir = "/var/lib/krabka/audit-spool"
            max_bytes = 2048
            sync_every_n = 7
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let cfg = applied_for_tests(fc);
        assert2::check!(
            cfg.audit_spool_dir == std::path::PathBuf::from("/var/lib/krabka/audit-spool")
        );
        assert2::check!(cfg.audit_spool_max == krabka_units::kibibytes(2));
        assert2::check!(cfg.audit_failure_mode == krabka_audit::AuditMode::FailClosed);
        assert2::check!(cfg.audit_spool_sync_every_n.get() == 7);

        let fc2: FileConfig = toml::from_str("[audit]\nenabled = true\n").expect("parse");
        let cfg2 = applied_for_tests(fc2);
        assert2::check!(cfg2.audit_spool_dir == std::path::PathBuf::from("audit-spool"));
        assert2::check!(cfg2.audit_spool_max == krabka_units::gibibytes(1));
        assert2::check!(cfg2.audit_spool_sync_every_n.get() == 1);
    }

    #[test]
    fn audit_spool_rejects_zero_sync_cadence() {
        assert2::check!(toml::from_str::<FileConfig>("[audit.spool]\nsync_every_n = 0\n").is_err());
    }
}
