//! A broker's fencing and controlled-shutdown state in the metadata log.
//!
//! Kafka keeps both on the broker registration. `RegisterBrokerRecord` writes
//! a new registration fenced (the schema default), and every later change is
//! a `BrokerRegistrationChangeRecord` for the same broker epoch:
//! `ReplicationControlManager.handleBrokerFenced`, `handleBrokerUnfenced` and
//! `handleBrokerInControlledShutdown` write one, and
//! `ClusterControlManager.replay` applies it to the registration.
//! `KRaftMetadataCache.isReplicaOffline` then reads `fenced()` back on
//! whichever broker serves the request.
//!
//! Krabka writes the same records. [`registration_change`] builds the
//! `BrokerRegistrationChangeRecord` of one transition for the broker's current
//! epoch. It applies only while the broker is still registered at that epoch:
//! the controller drops a change whose broker registered again after it was
//! built, and the image ignores one it replays at another epoch, so a stale
//! change never touches the new registration. A new registration carries
//! broker epoch -1, and the controller stamps it.
//!
//! The heartbeat handler writes the fence, unfence and controlled-shutdown
//! transitions of Kafka's heartbeat state machine. The liveness ticker writes
//! the fence of a broker whose session expired, as
//! `ReplicationControlManager.maybeFenceOneStaleBroker` does:
//! [`publish_fencing_changes`] runs on the controller leader at every tick and
//! is level-triggered, so a broker that stays dead costs one record, not one
//! per tick. Only a heartbeat unfences, as in Kafka.

use std::{collections::HashSet, sync::Arc, time::Duration};

use krabka_metadata::{
    BrokerRegistrationChangeRecord, FencingChange, MetadataImage, MetadataRecord, NodeId,
};

use crate::{
    heartbeat::controller_state::ControllerLivenessState, metadata_source::MetadataSource,
};

/// Upper bound on the fencing commit, matching the failover submit the same
/// tick makes. A stalled raft commit must not wedge the liveness ticker; the
/// next tick recomputes the same difference and retries.
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(10);

/// What one `BrokerRegistrationChangeRecord` changes: `fenced` is `Some(true)`
/// to fence, `Some(false)` to unfence and `None` to leave the fence, and
/// `in_controlled_shutdown` enters controlled shutdown. Nothing but a new
/// registration leaves controlled shutdown, as in Kafka.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RegistrationChange {
    pub(crate) fenced: Option<bool>,
    pub(crate) in_controlled_shutdown: bool,
}

impl RegistrationChange {
    pub(crate) const FENCE: Self = Self {
        fenced: Some(true),
        in_controlled_shutdown: false,
    };
    pub(crate) const UNFENCE: Self = Self {
        fenced: Some(false),
        in_controlled_shutdown: false,
    };
    pub(crate) const CONTROLLED_SHUTDOWN: Self = Self {
        fenced: None,
        in_controlled_shutdown: true,
    };
}

/// The `BrokerRegistrationChangeRecord` that applies `change` to `node_id`'s
/// current registration, at its broker epoch, or `None` when the broker is not
/// registered or the registration already says so.
///
/// Kafka's `handleBrokerFenced` and `handleBrokerUnfenced` set only `Fenced`
/// and `handleBrokerInControlledShutdown` sets only `InControlledShutdown`,
/// and each names the epoch of the registration it changes.
pub(crate) fn registration_change(
    image: &MetadataImage,
    node_id: NodeId,
    change: RegistrationChange,
) -> Option<MetadataRecord> {
    let current = image.broker(node_id)?;
    let fenced = match change.fenced {
        Some(true) if !current.fenced => FencingChange::Fence,
        Some(false) if current.fenced => FencingChange::Unfence,
        _ => FencingChange::None,
    };
    let in_controlled_shutdown = change.in_controlled_shutdown && !current.in_controlled_shutdown;
    (fenced != FencingChange::None || in_controlled_shutdown).then(|| {
        MetadataRecord::V1BrokerRegistrationChange(BrokerRegistrationChangeRecord {
            fenced,
            in_controlled_shutdown,
            ..BrokerRegistrationChangeRecord::no_change(node_id, current.broker_epoch)
        })
    })
}

/// Whether the registration of `node_id` is fenced. A broker with no
/// registration is not fenced; it is offline by the registration rule.
pub(crate) fn is_fenced(image: &MetadataImage, node_id: NodeId) -> bool {
    image.broker(node_id).is_some_and(|broker| broker.fenced)
}

/// Every registered broker whose registration is fenced: the replicated half
/// of the offline-replica projection, the same set on every node that holds
/// the image.
pub(crate) fn fenced_node_ids(image: &MetadataImage) -> HashSet<u64> {
    image
        .brokers()
        .filter(|broker| broker.fenced)
        .map(|broker| broker.node_id.0)
        .collect()
}

/// The records that fence every registered broker in `unavailable` whose
/// registration is not fenced yet.
///
/// `unavailable` is [`ControllerLivenessState::unavailable_snapshot`]: the
/// brokers that are fenced or past their heartbeat deadline. Everything else
/// yields nothing, which is what makes a repeated tick silent.
fn fencing_changes(image: &MetadataImage, unavailable: &HashSet<u64>) -> Vec<MetadataRecord> {
    image
        .brokers()
        .filter(|broker| unavailable.contains(&broker.node_id.0))
        .filter_map(|broker| {
            let record = registration_change(image, broker.node_id, RegistrationChange::FENCE)?;
            tracing::info!(
                broker = broker.node_id.0,
                broker_epoch = broker.broker_epoch,
                "fencing broker in the metadata log",
            );
            Some(record)
        })
        .collect()
}

/// Publish the fence of every broker the controller's registry holds
/// unavailable, if the image does not already carry it. The caller gates on
/// controller leadership, as only the leader holds a populated registry and
/// only the leader can submit. A submit failure is logged and does not
/// propagate: the next tick retries.
pub(crate) async fn publish_fencing_changes(
    controller: &Arc<dyn MetadataSource>,
    liveness: &ControllerLivenessState,
) {
    let image = controller.current_image();
    let changes = fencing_changes(&image, &liveness.unavailable_snapshot().await);
    if changes.is_empty() {
        return;
    }
    match tokio::time::timeout(SUBMIT_TIMEOUT, controller.submit_change(changes)).await {
        Ok(Err(error)) => tracing::warn!(%error, "fencing-state submit_change failed"),
        Err(_elapsed) => tracing::warn!(
            timeout = ?SUBMIT_TIMEOUT,
            "fencing-state submit_change did not commit in time",
        ),
        Ok(Ok(_)) => {}
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::BrokerRegistrationRecord;

    use super::*;

    fn registration(node: u64, fenced: bool) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord {
            fenced,
            broker_epoch: i64::try_from(node).expect("small id") * 10,
            incarnation_id: uuid::Uuid::from_u128(u128::from(node)),
            log_dirs: vec![uuid::Uuid::from_u128(0x600d)],
            ..crate::test_support::broker_registration(krabka_raft::NodeId(node))
        }
    }

    fn image_with(brokers: &[u64], fenced: &[u64]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for &node in brokers {
            image.apply(&MetadataRecord::V1BrokerRegistration(registration(
                node,
                fenced.contains(&node),
            )));
        }
        image
    }

    fn unavailable(ids: &[u64]) -> HashSet<u64> {
        ids.iter().copied().collect()
    }

    /// The `BrokerRegistrationChangeRecord` Kafka writes for `node` at its
    /// registered epoch, with `fenced` and `in_controlled_shutdown` set.
    fn change(node: u64, fenced: FencingChange, in_controlled_shutdown: bool) -> MetadataRecord {
        MetadataRecord::V1BrokerRegistrationChange(BrokerRegistrationChangeRecord {
            node_id: NodeId(node),
            broker_epoch: i64::try_from(node).expect("small id") * 10,
            fenced,
            in_controlled_shutdown,
            log_dirs: vec![],
            cordoned_log_dirs: None,
        })
    }

    /// Each transition writes the `BrokerRegistrationChangeRecord` Kafka's
    /// `handleBrokerFenced`, `handleBrokerUnfenced` or
    /// `handleBrokerInControlledShutdown` writes, at the registration's epoch
    /// and with only its own field set, and nothing when the registration
    /// already agrees. Replaying the record moves only that flag.
    #[test]
    fn a_registration_change_is_kafkas_change_record() {
        let shutting_down = BrokerRegistrationRecord {
            in_controlled_shutdown: true,
            ..registration(1, false)
        };
        let cases = [
            (
                "fence an unfenced broker",
                registration(1, false),
                RegistrationChange::FENCE,
                Some(change(1, FencingChange::Fence, false)),
                registration(1, true),
            ),
            (
                "fence a fenced broker",
                registration(1, true),
                RegistrationChange::FENCE,
                None,
                registration(1, true),
            ),
            (
                "unfence a fenced broker",
                registration(1, true),
                RegistrationChange::UNFENCE,
                Some(change(1, FencingChange::Unfence, false)),
                registration(1, false),
            ),
            (
                "unfence an unfenced broker",
                registration(1, false),
                RegistrationChange::UNFENCE,
                None,
                registration(1, false),
            ),
            (
                "enter controlled shutdown",
                registration(1, false),
                RegistrationChange::CONTROLLED_SHUTDOWN,
                Some(change(1, FencingChange::None, true)),
                shutting_down.clone(),
            ),
            (
                "enter controlled shutdown twice",
                shutting_down.clone(),
                RegistrationChange::CONTROLLED_SHUTDOWN,
                None,
                shutting_down.clone(),
            ),
            (
                "fencing keeps controlled shutdown",
                shutting_down.clone(),
                RegistrationChange::FENCE,
                Some(change(1, FencingChange::Fence, false)),
                BrokerRegistrationRecord {
                    fenced: true,
                    ..shutting_down.clone()
                },
            ),
        ];
        for (name, current, transition, want, after) in cases {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            image.apply(&MetadataRecord::V1BrokerRegistration(current));
            let record = registration_change(&image, NodeId(1), transition);
            assert!(record == want, "{name}");
            if let Some(record) = &record {
                image.apply(record);
            }
            assert!(image.broker(NodeId(1)) == Some(&after), "{name}");
        }
    }

    /// Kafka's `ClusterControlManager.replayRegistrationChange` refuses a
    /// change for any epoch but the registration's: a change built before the
    /// broker registered again changes nothing, and the leader's validation
    /// refuses it.
    #[test]
    fn a_change_at_another_epoch_is_a_no_op() {
        let mut image = image_with(&[1], &[]);
        let stale = registration_change(&image, NodeId(1), RegistrationChange::FENCE)
            .expect("broker 1 is unfenced");
        let reregistered = BrokerRegistrationRecord {
            broker_epoch: 11,
            ..registration(1, false)
        };
        image.apply(&MetadataRecord::V1BrokerRegistration(reregistered.clone()));

        assert!(image.validate(&stale).is_err());
        image.apply(&stale);
        assert!(image.broker(NodeId(1)) == Some(&reregistered));
    }

    #[test]
    fn an_unregistered_broker_has_no_registration_change() {
        let image = image_with(&[1], &[]);

        assert!(registration_change(&image, NodeId(7), RegistrationChange::FENCE) == None);
        assert!(!is_fenced(&image, NodeId(7)));
    }

    #[test]
    fn the_fenced_set_is_read_from_the_registrations() {
        let image = image_with(&[1, 2, 3], &[2, 3]);

        assert!(fenced_node_ids(&image) == unavailable(&[2, 3]));
        assert!(is_fenced(&image, NodeId(2)));
        assert!(!is_fenced(&image, NodeId(1)));
    }

    /// The tick fences an unavailable broker once, never unfences one, and
    /// ignores a broker the image does not register.
    #[test]
    fn the_tick_only_fences() {
        let cases = [
            (
                "a dead broker is fenced",
                image_with(&[1, 2], &[]),
                unavailable(&[2]),
                vec![change(2, FencingChange::Fence, false)],
            ),
            (
                "a fenced broker is not fenced again",
                image_with(&[1, 2, 3], &[3]),
                unavailable(&[3]),
                vec![],
            ),
            (
                "a recovered broker waits for its heartbeat to unfence",
                image_with(&[1, 2], &[2]),
                unavailable(&[]),
                vec![],
            ),
            (
                "an unregistered broker is not published",
                image_with(&[1], &[]),
                unavailable(&[7]),
                vec![],
            ),
        ];
        for (name, image, unavailable, want) in cases {
            assert!(fencing_changes(&image, &unavailable) == want, "{name}");
        }
    }
}
