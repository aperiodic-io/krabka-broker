//! Conversions from the wire `StreamsGroupHeartbeat` request payloads into the
//! actor's in-memory member state.
//!
//! The heartbeat request reports tasks as a flat list of `(subtopology,
//! partitions)` entries and offsets as a flat list of `(subtopology,
//! partition, offset)` entries, while the actor keeps both as maps. These
//! functions do that translation, and they build the [`StreamsMemberState`] a
//! first-join heartbeat implies.

use {
    crate::coordinator::unified::streams::state::StreamsMemberState,
    krabka_log::Offset,
    krabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    std::{collections::BTreeMap, time::Instant},
};

/// Converts request `TaskIds`, which hold a subtopology and its partitions,
/// into the in-memory `subtopology -> partitions` task map.
pub(super) fn task_ids_to_map(
    tasks: &[krabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds],
) -> BTreeMap<String, Vec<i32>> {
    let mut map: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for t in tasks {
        let entry = map.entry(t.subtopology_id.clone()).or_default();
        entry.extend_from_slice(&t.partitions);
    }
    for parts in map.values_mut() {
        parts.sort_unstable();
        parts.dedup();
    }
    map
}

/// Converts request `TaskOffset` entries into a
/// `(subtopology, partition) -> offset` map. The wire `o.offset` field stays a
/// raw `i64`, and this function wraps it as an `Offset` for the in-memory
/// changelog-position map.
pub(super) fn task_offsets_to_map(
    offsets: &[krabka_protocol::owned::common::streams_group_heartbeat_request::task_offset::TaskOffset],
) -> BTreeMap<(String, i32), Offset> {
    offsets
        .iter()
        .map(|o| ((o.subtopology_id.clone(), o.partition), Offset(o.offset)))
        .collect()
}

/// Replace present client tags in wire order, preserving the stored tags when absent.
pub(super) fn update_client_tags(
    member: &mut StreamsMemberState,
    request: &StreamsGroupHeartbeatRequest,
) {
    if let Some(tags) = &request.client_tags {
        member.client_tags = tags
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
    }
}

pub(super) fn build_member(
    member_id: &str,
    req: &StreamsGroupHeartbeatRequest,
    client_id: &str,
    host: &str,
    now: Instant,
) -> StreamsMemberState {
    let mut m = StreamsMemberState::joining(member_id, client_id, host);
    if let Some(pid) = &req.process_id
        && !pid.is_empty()
    {
        m.process_id.clone_from(pid);
    }
    m.rack_id.clone_from(&req.rack_id);
    m.instance_id.clone_from(&req.instance_id);
    m.user_endpoint = req
        .user_endpoint
        .as_ref()
        .map(|ep| (ep.host.clone(), ep.port));
    update_client_tags(&mut m, req);
    m.rebalance_timeout_ms = req.rebalance_timeout_ms;
    if let Some(topo) = &req.topology {
        m.topology_epoch = topo.epoch;
    }
    m.last_seen = now;
    m
}

#[cfg(test)]
mod tests {
    use {
        super::*, assert2::check,
        krabka_protocol::owned::common::streams_group_heartbeat_request::task_offset::TaskOffset,
    };

    #[test]
    fn task_offsets_to_map_wraps_each_wire_entry() {
        let wire = vec![
            TaskOffset {
                subtopology_id: "sub-a".to_string(),
                partition: 0,
                offset: 42,
                ..Default::default()
            },
            TaskOffset {
                subtopology_id: "sub-a".to_string(),
                partition: 1,
                offset: 7,
                ..Default::default()
            },
        ];
        let map = task_offsets_to_map(&wire);
        check!(
            map == maplit::btreemap! {
            ("sub-a".to_string(), 0) => Offset(42),
            ("sub-a".to_string(), 1) => Offset(7)}
        );
    }
}
