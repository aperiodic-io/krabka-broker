//! KIP-460 automatic leader rebalancing: the cadence the ticker reads, and
//! the bounds it must stay inside.
//!
//! There is no imbalance-ratio threshold. Kafka's
//! `leader.imbalance.per.broker.percentage` belongs to the `ZooKeeper`
//! controller; the `KRaft` controller ignores it and restores every partition
//! whose preferred replica is available, bounded only by how many elections
//! it will submit in one pass. krabka is `KRaft`-only, so it follows the
//! `KRaft` controller.

use {
    crate::{BrokerError, config::BrokerConfig},
    krabka_units::{Time, convert::TimeExt},
};

impl BrokerConfig {
    pub(super) fn validate_leader_rebalance(&self) -> Result<(), BrokerError> {
        if self.leader_imbalance_check_interval <= <Time as TimeExt>::ZERO {
            return Err(BrokerError::InvalidLeaderRebalanceInterval { value: 0 });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        krabka_units::minutes,
    };

    #[test]
    fn auto_leader_rebalance_defaults_to_true_in_default() {
        let c = BrokerConfig::default();
        check!(c.features.auto_leader_rebalance_enable);
        check!(c.leader_imbalance_check_interval == minutes(5));
    }

    #[test]
    fn auto_leader_rebalance_defaults_to_false_in_for_tests() {
        let c = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(!c.features.auto_leader_rebalance_enable);
    }

    #[test]
    fn rebalance_zero_interval_rejected_by_validate() {
        let c = BrokerConfig {
            leader_imbalance_check_interval: <Time as TimeExt>::ZERO,
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::InvalidLeaderRebalanceInterval { value: 0 })
        ));
    }
}
