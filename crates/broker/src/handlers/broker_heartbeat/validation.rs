//! The registration and epoch check a `BrokerHeartbeat` passes before the
//! controller acts on it.
//!
//! It is a pure function over the decoded request and the current metadata
//! image, so the handler stays a straight line of decisions.

use krabka_metadata::MetadataImage;
use krabka_protocol::owned::broker_heartbeat_request::BrokerHeartbeatRequest;
use krabka_raft::NodeId;

use crate::codes;

/// A heartbeat whose broker epoch matches the broker's registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CurrentRegistration {
    /// The broker has replayed its own registration record. The controller's
    /// heartbeat state machine decides fencing and shutdown from it.
    pub(super) caught_up: bool,
}

pub(super) fn validate_registration(
    image: &MetadataImage,
    req: &BrokerHeartbeatRequest,
) -> Result<(u64, CurrentRegistration), i16> {
    // Kafka's `ClusterControlManager.checkBrokerEpoch` answers a broker id
    // with no registration as a stale epoch, as it answers a mismatched one.
    let broker_id = u64::try_from(req.broker_id).map_err(|_| codes::STALE_BROKER_EPOCH)?;
    match krabka_verified::broker_heartbeat_decision(
        image
            .broker(NodeId(broker_id))
            .map(|registration| registration.broker_epoch),
        req.broker_epoch,
        req.current_metadata_offset,
    ) {
        krabka_verified::BrokerHeartbeatDecision::Missing
        | krabka_verified::BrokerHeartbeatDecision::Stale => Err(codes::STALE_BROKER_EPOCH),
        krabka_verified::BrokerHeartbeatDecision::Current { caught_up } => {
            Ok((broker_id, CurrentRegistration { caught_up }))
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};
    use uuid::Uuid;

    use super::*;

    fn registered_image() -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                broker_epoch: 42,
                host: "localhost".into(),
                ..crate::test_support::broker_registration(krabka_raft::NodeId(7))
            },
        ));
        image
    }

    #[test]
    fn registration_validation_rejects_unknown_and_stale_brokers() {
        let image = registered_image();
        let mut req = BrokerHeartbeatRequest {
            broker_id: -1,
            broker_epoch: 42,
            current_metadata_offset: 42,
            ..Default::default()
        };

        assert!(validate_registration(&image, &req) == Err(codes::STALE_BROKER_EPOCH));
        req.broker_id = 8;
        assert!(validate_registration(&image, &req) == Err(codes::STALE_BROKER_EPOCH));
        req.broker_id = 7;
        req.broker_epoch = 41;
        assert!(validate_registration(&image, &req) == Err(codes::STALE_BROKER_EPOCH));
    }

    #[test]
    fn registration_validation_reports_catch_up_at_registration_offset() {
        let image = registered_image();
        let mut req = BrokerHeartbeatRequest {
            broker_id: 7,
            broker_epoch: 42,
            current_metadata_offset: 41,
            ..Default::default()
        };

        assert!(
            validate_registration(&image, &req)
                == Ok((7, CurrentRegistration { caught_up: false }))
        );
        req.current_metadata_offset = 42;
        assert!(
            validate_registration(&image, &req) == Ok((7, CurrentRegistration { caught_up: true }))
        );
    }
}
