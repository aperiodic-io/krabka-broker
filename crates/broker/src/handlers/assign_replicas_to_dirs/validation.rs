//! The gates `AssignReplicasToDirs` applies before the controller plans an
//! assignment: broker registration and broker-epoch fencing.
//!
//! This mirrors Kafka's `ClusterControlManager.checkBrokerEpoch`, which
//! `ReplicationControlManager.handleAssignReplicasToDirs` calls before it
//! looks at the reported rows. A pure function over the decoded request and
//! the current metadata image, so the handler stays a straight line of
//! decisions.

use krabka_metadata::{MetadataImage, NodeId};

use crate::codes;

/// Checks the reporting broker's registration and epoch. A `broker_id` that
/// names no registration -- a negative wire value included, since none is
/// ever registered -- answers `BROKER_ID_NOT_REGISTERED`. A registered broker
/// reporting an epoch other than `-1` that disagrees with its registration
/// answers `STALE_BROKER_EPOCH`, fencing out a stale or restarted broker
/// before the controller applies its reported assignment.
///
/// `-1` means "not provided" and matches any registration, the same KIP-903
/// convention `AlterPartition`'s ISR eligibility check
/// (`alter_partition::isr_update`) uses, and the one
/// `crate::assign_dirs::build_request` relies on for this broker's own
/// self-reported directory assignments.
pub(super) fn check_broker_epoch(
    image: &MetadataImage,
    broker_id: i32,
    broker_epoch: i64,
) -> Result<u64, i16> {
    let broker_id = u64::try_from(broker_id).map_err(|_| codes::BROKER_ID_NOT_REGISTERED)?;
    let registration = image
        .broker(NodeId(broker_id))
        .ok_or(codes::BROKER_ID_NOT_REGISTERED)?;
    if broker_epoch != -1 && broker_epoch != registration.broker_epoch {
        return Err(codes::STALE_BROKER_EPOCH);
    }
    Ok(broker_id)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};
    use uuid::Uuid;

    use super::*;

    fn image_with_broker(node_id: u64, broker_epoch: i64) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                broker_epoch,
                host: "localhost".into(),
                ..crate::test_support::broker_registration(krabka_raft::NodeId(node_id))
            },
        ));
        image
    }

    #[test]
    fn check_broker_epoch_rejects_unregistered_and_stale_brokers() {
        let image = image_with_broker(7, 42);

        let cases = [
            ("current epoch of a registered broker", 7, 42, Ok(7)),
            ("-1 (not provided) matches any registration", 7, -1, Ok(7)),
            (
                "stale epoch of a registered broker",
                7,
                41,
                Err(codes::STALE_BROKER_EPOCH),
            ),
            (
                "an id that names no registration",
                8,
                42,
                Err(codes::BROKER_ID_NOT_REGISTERED),
            ),
            (
                "a negative id, which can never be registered",
                -1,
                42,
                Err(codes::BROKER_ID_NOT_REGISTERED),
            ),
        ];
        let mut actual = Vec::with_capacity(cases.len());
        let mut expected = Vec::with_capacity(cases.len());
        for (label, broker_id, broker_epoch, want) in cases {
            actual.push((label, check_broker_epoch(&image, broker_id, broker_epoch)));
            expected.push((label, want));
        }
        assert!(actual == expected);
    }
}
