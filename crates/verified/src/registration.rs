//! Broker heartbeat admission decisions.

use creusot_std::prelude::*;

/// How one broker heartbeat relates to the broker's registration.
///
/// Fencing and shutdown are not decided here: the controller's heartbeat
/// state machine derives them from the current broker state, as Kafka's
/// `BrokerHeartbeatManager.calculateNextBrokerState` does.
#[krabka_macros::model_type]
pub enum BrokerHeartbeatDecision {
    /// No registration exists for the broker id.
    Missing,
    /// The heartbeat carries another broker epoch.
    Stale,
    /// The heartbeat carries the registered epoch. The broker has caught up
    /// once its metadata offset reaches its registration record, whose offset
    /// is the broker epoch.
    Current { caught_up: bool },
}

/// Fence an absent or stale registration, as Kafka's
/// `ClusterControlManager.checkBrokerEpoch` does, and otherwise report whether
/// the broker has replayed its own registration record.
#[ensures(match result {
    BrokerHeartbeatDecision::Missing => registered_epoch == None,
    BrokerHeartbeatDecision::Stale => exists<epoch: i64>
        registered_epoch == Some(epoch) && request_epoch != epoch,
    BrokerHeartbeatDecision::Current { caught_up } => exists<epoch: i64>
        registered_epoch == Some(epoch)
            && request_epoch == epoch
            && caught_up == (metadata_offset@ >= epoch@),
})]
#[must_use]
pub fn broker_heartbeat_decision(
    registered_epoch: Option<i64>,
    request_epoch: i64,
    metadata_offset: i64,
) -> BrokerHeartbeatDecision {
    match registered_epoch {
        None => BrokerHeartbeatDecision::Missing,
        Some(epoch) if request_epoch != epoch => BrokerHeartbeatDecision::Stale,
        Some(epoch) => BrokerHeartbeatDecision::Current {
            caught_up: metadata_offset >= epoch,
        },
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{BrokerHeartbeatDecision as D, broker_heartbeat_decision},
        assert2::assert,
    };

    #[test]
    fn heartbeat_fences_exact_epochs_and_reports_catch_up() {
        let cases = [
            ("unregistered broker", None, 7, 7, D::Missing),
            ("older epoch", Some(7), 6, i64::MAX, D::Stale),
            ("newer epoch", Some(7), 8, i64::MAX, D::Stale),
            ("behind its registration", Some(7), 7, 6, D::Current {
                caught_up: false,
            }),
            ("at its registration", Some(7), 7, 7, D::Current {
                caught_up: true,
            }),
            ("past its registration", Some(7), 7, 9, D::Current {
                caught_up: true,
            }),
        ];
        for (case, registered_epoch, request_epoch, metadata_offset, expected) in cases {
            assert!(
                broker_heartbeat_decision(registered_epoch, request_epoch, metadata_offset)
                    == expected,
                "{case}"
            );
        }
    }
}
