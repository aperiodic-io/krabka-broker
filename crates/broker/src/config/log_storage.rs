//! The broker's log directories and the per-log IO policy: where partition
//! data lands, and the read budgets every hosted log inherits.

use std::path::PathBuf;

use crate::{BrokerError, config::BrokerConfig, cordoned_log_dirs::resolve};

impl BrokerConfig {
    pub(super) fn validate_log_io_policy(&self) -> Result<(), BrokerError> {
        if self.log_config.read_buffer_cap <= krabka_units::bytes(0) {
            return Err(BrokerError::InvalidRuntimeConfig(
                "log_read_buffer_cap must be positive".into(),
            ));
        }
        if self.log_config.timestamp_scan_window <= krabka_units::bytes(0) {
            return Err(BrokerError::InvalidRuntimeConfig(
                "log_timestamp_scan_window must be positive".into(),
            ));
        }
        self.validate_cordoned_log_dirs()
    }

    /// Kafka's `KafkaConfig.validateCordonedLogDirs`, which a node with the
    /// broker role runs at startup: every entry of `cordoned.log.dirs` names a
    /// configured log directory, and `*` stands alone.
    fn validate_cordoned_log_dirs(&self) -> Result<(), BrokerError> {
        match (&self.cordoned_log_dirs, self.is_broker()) {
            (Some(value), true) => resolve(value, &self.all_log_dirs())
                .map(drop)
                .map_err(BrokerError::InvalidRuntimeConfig),
            _ => Ok(()),
        }
    }

    /// The log directories a `cordoned.log.dirs` value on this node must name:
    /// [`all_log_dirs`][Self::all_log_dirs] on a node with the broker role, and
    /// none on a controller-only node, which Kafka does not check the key on.
    #[must_use]
    pub(crate) fn broker_log_dirs(&self) -> Vec<PathBuf> {
        if self.is_broker() {
            self.all_log_dirs()
        } else {
            Vec::new()
        }
    }

    /// The metadata log directory, Kafka's `metadata.log.dir`:
    /// [`metadata_log_dir`][Self::metadata_log_dir] when it is set, and
    /// otherwise [`log_dir`][Self::log_dir], the first entry of `log.dirs`.
    #[must_use]
    pub fn metadata_dir(&self) -> &std::path::Path {
        self.metadata_log_dir.as_deref().unwrap_or(&self.log_dir)
    }

    /// All log directories this broker stores partition data in, primary
    /// first and de-duplicated: Kafka's `log.dirs`. This is the placement and
    /// `DescribeLogDirs` surface (KIP-113). A separate
    /// [`metadata_log_dir`][Self::metadata_log_dir] is not in it, as Kafka's
    /// `metadata.log.dir` is not in `log.dirs`.
    #[must_use]
    pub fn all_log_dirs(&self) -> Vec<PathBuf> {
        let mut out = vec![self.log_dir.clone()];
        for d in &self.extra_log_dirs {
            if !out.contains(d) {
                out.push(d.clone());
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn all_log_dirs_keeps_primary_first_and_deduplicates_extras() {
        let primary = std::path::PathBuf::from("/data/primary");
        let extra = std::path::PathBuf::from("/data/extra");
        let mut c = BrokerConfig::for_tests(primary.clone());
        c.extra_log_dirs = vec![extra.clone(), primary.clone(), extra.clone()];

        assert!(c.all_log_dirs() == vec![primary, extra]);
    }

    /// A broker refuses at startup a `cordoned.log.dirs` Kafka refuses, with
    /// Kafka's message. A node without the broker role does not check it.
    #[test]
    fn a_static_cordoned_value_is_checked_on_a_broker_only() {
        let primary = std::path::PathBuf::from("/data/primary");
        let cases = [
            (None, true, Ok(())),
            (Some("/data/primary"), true, Ok(())),
            (Some("*"), true, Ok(())),
            (
                Some("/elsewhere"),
                true,
                Err(
                    "invalid runtime configuration: requirement failed: All entries in \
                     cordoned.log.dirs must be present in log.dirs or log.dir. Missing entries \
                     : /elsewhere",
                ),
            ),
            (Some("/elsewhere"), false, Ok(())),
        ];
        for (value, broker, want) in cases {
            let mut c = BrokerConfig::for_tests(primary.clone());
            c.cordoned_log_dirs = value.map(str::to_owned);
            if !broker {
                c.roles = vec![crate::config::NodeRole::Controller];
            }
            let got = c
                .validate_cordoned_log_dirs()
                .map_err(|error| error.to_string());
            assert!(
                got == want.map_err(str::to_owned),
                "{value:?} broker={broker}"
            );
        }
    }
}
