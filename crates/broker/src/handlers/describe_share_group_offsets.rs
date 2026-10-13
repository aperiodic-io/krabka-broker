//! `DescribeShareGroupOffsets` (`api_key` 90), from KIP-932. It returns the
//! share-partition start offset (SPSO), leader epoch and lag of each
//! requested `(group, topic, partition)`: the first two from the share-state
//! persister, the lag from the end offset the partition leader reports.
//!
//! `network::dispatch` intercepts it inline, so the handler receives the
//! per-connection principal and peer `SocketAddr` for the per-group `Describe`
//! ACL gate.
//!
//! This file holds only the wire entry point and the broker-wide feature gate.
//! `group` resolves one requested group, from the ACL check to the persister
//! lookup; `topics` decides which topics that group reports when the request
//! names none; `rows` builds the topic and partition rows themselves; and
//! `end_offsets` asks each partition leader for the end offset of the lag.

use krabka_protocol::owned::{
    describe_share_group_offsets_request::DescribeShareGroupOffsetsRequest,
    describe_share_group_offsets_response::{
        DescribeShareGroupOffsetsResponse, DescribeShareGroupOffsetsResponseGroup,
    },
};

mod end_offsets;
mod group;
mod rows;
mod topics;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {self::group::describe_group, crate::codes};

context_handler! {
    // cargo-mutants: share-coordinator response projection; integration-tested.
    #[cfg_attr(test, mutants::skip)]
    DescribeShareGroupOffsetsRequest => DescribeShareGroupOffsetsResponse,
    (broker, req, _version, ctx),
    {
        // Feature gate: share groups are on from a finalized `share.version` of 1,
        // and below it the RPC is unsupported. The response has no top-level error
        // code, so mark every requested group with UNSUPPORTED_VERSION.
        let image = broker.controller.current_image();
        if !crate::features::share_groups_enabled(&image) {
            let groups = req
                .groups
                .iter()
                .map(|g| DescribeShareGroupOffsetsResponseGroup {
                    group_id: g.group_id.clone(),
                    error_code: codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                })
                .collect();
            let resp = DescribeShareGroupOffsetsResponse {
                groups,
                ..Default::default()
            };
            return Ok(resp);
        }

        let ng_opt = Some(broker.group_coordinator.clone());

        let mut groups: Vec<DescribeShareGroupOffsetsResponseGroup> =
            Vec::with_capacity(req.groups.len());

        for group in req.groups {
            groups.push(describe_group(broker, ng_opt.as_deref(), &image, ctx, group).await);
        }

        let resp = DescribeShareGroupOffsetsResponse {
            groups,
            throttle_time_ms: 0,
            ..Default::default()
        };
        Ok(resp)
    }
}
