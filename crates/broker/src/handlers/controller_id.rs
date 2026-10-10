//! The `controllerId` that `Metadata` and `DescribeCluster` advertise.
//!
//! In `KRaft` the field does not name the controller. `apache/kafka:4.3.1`
//! answers both APIs with `metadataCache.getRandomAliveBrokerId().orElse(-1)`
//! -- `KafkaApis.handleTopicMetadataRequest` and
//! `KafkaApis.$anonfun$handleDescribeCluster$3` in `kafka_2.13-4.3.1.jar` both
//! reach that one method -- and `KRaftMetadataCache.getRandomAliveBrokerId`
//! streams `cluster().brokers().values()`, drops the registrations whose
//! `fenced()` is true, maps the survivors to `id()`, and returns a uniformly
//! chosen element, or `Optional.empty()` when nothing survives.
//!
//! So the value is *a registered, unfenced broker*, never the quorum leader.
//! A client resolves it against the `brokers` array of the same response:
//! `MetadataResponse.controller()` is a lookup in that array, and the
//! `AdminClient` routes controller-bound requests at whatever it finds. In a
//! role-separated cluster the quorum leader is a controller-only node that
//! never registers as a broker, so advertising the leader's id hands every
//! client an id that is absent from the array it has to resolve against --
//! the same dead end as the `-1` that Kafka sends when no broker is alive.
//!
//! Kafka spreads the choice so that the forwarded controller traffic does not
//! all land on one broker. Krabka rotates instead of drawing at random: it
//! spreads at least as evenly, and it needs no random source.

use std::{
    collections::HashSet,
    sync::atomic::{AtomicUsize, Ordering},
};

use krabka_metadata::{BrokerRegistrationRecord, MetadataImage};

/// `MetadataResponse.NO_CONTROLLER_ID`, which `DescribeCluster` shares.
pub(crate) const NO_CONTROLLER_ID: i32 = -1;

/// The rotation cursor behind [`advertised_controller_id`].
static NEXT: AtomicUsize = AtomicUsize::new(0);

/// The `controllerId` to advertise, given the fenced set from
/// [`crate::handlers::offline_replicas::unavailable_brokers`].
///
/// [`NO_CONTROLLER_ID`] when no registered broker is unfenced, which is what a
/// caller sees while a cluster is wholly fenced.
pub(crate) fn advertised_controller_id(image: &MetadataImage, unavailable: &HashSet<u64>) -> i32 {
    advertised_controller_id_among(image, unavailable, |_| true)
}

/// [`advertised_controller_id`] drawn only from the unfenced brokers that
/// `eligible` accepts.
///
/// `DescribeCluster` lists only the brokers with an endpoint on the request's
/// listener, and Kafka's `AuthHelper.computeDescribeClusterResponse` answers
/// [`NO_CONTROLLER_ID`] for a drawn id that the list does not hold. Drawing
/// from the listed brokers alone gives one of the answers Kafka's draw can
/// give, and never a needless `-1`.
pub(crate) fn advertised_controller_id_among(
    image: &MetadataImage,
    unavailable: &HashSet<u64>,
    eligible: impl Fn(&BrokerRegistrationRecord) -> bool,
) -> i32 {
    let mut alive: Vec<i32> = image
        .brokers()
        .filter(|broker| !unavailable.contains(&broker.node_id.0) && eligible(broker))
        .filter_map(|broker| i32::try_from(broker.node_id.0).ok())
        .collect();
    // The image's iteration order is not part of its contract, and the
    // rotation only spreads evenly over a stable order.
    alive.sort_unstable();
    rotate(&alive, NEXT.fetch_add(1, Ordering::Relaxed))
}

/// The `nth` broker of `alive` by rotation, or [`NO_CONTROLLER_ID`] when
/// `alive` is empty.
fn rotate(alive: &[i32], nth: usize) -> i32 {
    if alive.is_empty() {
        return NO_CONTROLLER_ID;
    }
    alive[nth % alive.len()]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord, NodeId};

    use super::*;

    fn broker_record(node_id: NodeId) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord {
            host: "broker-host".into(),
            ..crate::test_support::broker_registration(node_id)
        }
    }

    #[test]
    fn rotate_walks_the_alive_brokers_and_answers_no_controller_when_empty() {
        let cases: [(&[i32], usize, i32); 6] = [
            (&[], 0, NO_CONTROLLER_ID),
            (&[], 7, NO_CONTROLLER_ID),
            (&[4], 0, 4),
            (&[4], 3, 4),
            (&[2, 3], 0, 2),
            (&[2, 3], 3, 3),
        ];
        for (alive, nth, want) in cases {
            assert!(rotate(alive, nth) == want, "{alive:?} at {nth}");
        }
    }

    fn image_with_brokers(ids: &[u64]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for &id in ids {
            image.apply(&MetadataRecord::V1BrokerRegistration(broker_record(
                NodeId(id),
            )));
        }
        image
    }

    /// A role-separated cluster is the case that matters: the quorum leader is
    /// node 1 and it is not in the image's broker set at all, so the id has to
    /// come from the brokers that are.
    #[test]
    fn advertised_controller_id_names_only_unfenced_registered_brokers() {
        let image = image_with_brokers(&[2, 3]);
        let unfenced = HashSet::new();

        // Every call names a registered broker, and the rotation reaches both
        // of them rather than pinning one.
        let seen: BTreeSet<i32> = (0..8)
            .map(|_| advertised_controller_id(&image, &unfenced))
            .collect();
        assert!(seen == BTreeSet::from([2, 3]));

        // A fenced broker drops out.
        let fenced_three = HashSet::from([3]);
        let seen: BTreeSet<i32> = (0..8)
            .map(|_| advertised_controller_id(&image, &fenced_three))
            .collect();
        assert!(seen == BTreeSet::from([2]));

        // Wholly fenced, and unregistered, both answer NO_CONTROLLER_ID.
        let all_fenced = HashSet::from([2, 3]);
        assert!(advertised_controller_id(&image, &all_fenced) == NO_CONTROLLER_ID);
        assert!(
            advertised_controller_id(&image_with_brokers(&[]), &HashSet::new()) == NO_CONTROLLER_ID
        );
    }
}
