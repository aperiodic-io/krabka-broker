//! Translation from a member's server-side target assignment to the classic
//! `ConsumerProtocolAssignment` wire blob.
//!
//! Both migration directions and every classic RPC an upgraded group serves
//! read the same translation, so it lives in one place.

use {
    crate::coordinator::unified::reconciler::ReconcileInput,
    bytes::{BufMut, Bytes, BytesMut},
    krabka_protocol::{
        Encode,
        owned::consumer_protocol_assignment::{
            ConsumerProtocolAssignment, MAX_VERSION as ASSIGNMENT_MAX_VERSION, TopicPartition,
        },
        primitives::uuid::Uuid,
    },
    std::collections::HashMap,
};

/// Translates a member's server-side target, which maps topic ID to
/// partitions, into a classic `ConsumerProtocolAssignment` wire blob of
/// version 0, which maps topic name to partitions.
///
/// See [`consumer_assignment_blob`] for the translation.
pub(crate) fn target_to_consumer_assignment(
    target: &HashMap<Uuid, Vec<i32>>,
    image: &ReconcileInput,
) -> Bytes {
    consumer_assignment_blob(target, image, 0)
}

/// Kafka's `ConsumerProtocol.serializeAssignment(toConsumerProtocolAssignment(
/// partitions, image), version)`: the partitions, by topic ID, as a classic
/// `ConsumerProtocolAssignment` wire blob, which maps topic name to
/// partitions, with no user data.
///
/// The blob starts with the `i16` version prefix that a classic client reads
/// first. A version above the highest the schema knows is written as the
/// highest, as Kafka's `checkAssignmentVersion` does. The caller rejects a
/// negative version, which Kafka refuses with a `SchemaException`.
///
/// A topic ID that the metadata image does not hold, because the topic was
/// deleted, is dropped. Topics are ordered by name and partitions ascending,
/// so the bytes do not depend on map order.
pub(crate) fn consumer_assignment_blob(
    partitions: &HashMap<Uuid, Vec<i32>>,
    image: &ReconcileInput,
    version: i16,
) -> Bytes {
    let version = version.clamp(0, ASSIGNMENT_MAX_VERSION);
    let id_to_name: HashMap<Uuid, &str> = image
        .topic_id_by_name
        .iter()
        .map(|(name, id)| (*id, name.as_str()))
        .collect();
    let mut assigned: Vec<TopicPartition> = partitions
        .iter()
        .filter_map(|(tid, parts)| {
            id_to_name.get(tid).map(|name| {
                let mut p = parts.clone();
                p.sort_unstable();
                TopicPartition {
                    topic: (*name).to_string(),
                    partitions: p,
                    ..Default::default()
                }
            })
        })
        .collect();
    assigned.sort_by(|a, b| a.topic.cmp(&b.topic));
    let assignment = ConsumerProtocolAssignment {
        assigned_partitions: assigned,
        ..Default::default()
    };
    let mut out = BytesMut::new();
    out.put_i16(version);
    assignment
        .encode(&mut out, version)
        .expect("ConsumerProtocolAssignment encode is infallible into BytesMut");
    out.freeze()
}

/// Kafka's `ConsumerProtocol.deserializeVersion`: the `i16` version prefix of
/// a consumer embedded-protocol blob, or `None` when the blob is too short to
/// hold one.
pub(crate) fn embedded_protocol_version(blob: &[u8]) -> Option<i16> {
    let prefix: [u8; 2] = blob.get(..2)?.try_into().ok()?;
    Some(i16::from_be_bytes(prefix))
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, bytes::Buf, krabka_protocol::Decode};

    #[test]
    fn target_translates_to_consumer_assignment_blob() {
        let t1 = Uuid([1; 16]);
        let t2 = Uuid([2; 16]);
        let image = ReconcileInput {
            topic_id_by_name: [("orders".to_string(), t1), ("events".to_string(), t2)].into(),
            ..Default::default()
        };
        let target: std::collections::HashMap<Uuid, Vec<i32>> =
            [(t1, vec![2, 0, 1]), (t2, vec![5])].into();

        let blob = target_to_consumer_assignment(&target, &image);
        // Strip the version prefix and decode back.
        let mut cur = &blob[..];
        let version = cur.get_i16();
        assert!(version == 0);
        let decoded = ConsumerProtocolAssignment::decode(&mut cur, version).unwrap();
        // Deterministic order by topic name: events, orders.
        let names: Vec<&str> = decoded
            .assigned_partitions
            .iter()
            .map(|tp| tp.topic.as_str())
            .collect();
        assert!(names == vec!["events", "orders"]);
        let orders = decoded
            .assigned_partitions
            .iter()
            .find(|tp| tp.topic == "orders")
            .unwrap();
        // Partitions sorted.
        assert!(orders.partitions == vec![0, 1, 2]);
    }

    #[test]
    fn target_drops_unknown_topic_ids() {
        let known = Uuid([1; 16]);
        let ghost = Uuid([9; 16]);
        let image = ReconcileInput {
            topic_id_by_name: [("orders".to_string(), known)].into(),
            ..Default::default()
        };
        let target: std::collections::HashMap<Uuid, Vec<i32>> =
            [(known, vec![0]), (ghost, vec![0])].into();
        let blob = target_to_consumer_assignment(&target, &image);
        let mut cur = &blob[..];
        let _ = cur.get_i16();
        let decoded = ConsumerProtocolAssignment::decode(&mut cur, 0).unwrap();
        assert!(decoded.assigned_partitions.len() == 1);
        assert!(decoded.assigned_partitions[0].topic == "orders");
    }
}
