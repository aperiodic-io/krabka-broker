//! `OffsetFetch` (`api_key=9`). Reads from `Group.committed_offsets`.
//!
//! For v0 to v7, the request carries the legacy single-group fields:
//! `group_id` and `topics: Option<Vec<OffsetFetchRequestTopic>>`. From v8,
//! KIP-516 moves to a per-group `groups[]` array, and at v10 it keys topics by
//! `topic_id`. `handle_groups` serves that path.
//!
//! The internal offset storage stays keyed by name, so the handler resolves
//! topic ids to names at the wire boundary and echoes the ids back on the
//! response.
//!
//! From v7 the request also carries KIP-447's `require_stable`. It is a
//! top-level field, so it applies to every group the request names, and it
//! turns any partition an unresolved transaction has written into an
//! `UNSTABLE_OFFSET_COMMIT` row instead of the older stable offset. `unstable`
//! builds that row for both response shapes.
//!
//! The two request shapes are byte-exact contracts with different clients, so
//! each keeps its own module: `legacy` for v0 to v7 and `groups` for v8 and
//! above. They share the group-level gate in `authz` and the coordinator read
//! in `committed`. This file holds only the entry point that picks the shape.

use {
    crate::{
        broker::Broker,
        error::BrokerError,
        handlers::{ApiVersion, RequestContext},
    },
    krabka_protocol::owned::{
        offset_fetch_request::OffsetFetchRequest, offset_fetch_response::OffsetFetchResponse,
    },
};

mod authz;
mod committed;
mod groups;
mod legacy;
#[cfg(test)]
mod tests;
mod unstable;

use self::{groups::handle_groups, legacy::handle_legacy};

// cargo-mutants: coordinator-backed response projection; integration-tested.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn handle(
    broker: &Broker,
    req: OffsetFetchRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<OffsetFetchResponse, BrokerError> {
    // ── KIP-516 (v8+): per-group `groups[]` request/response shape ──
    // v8 moved from a single (group_id, topics) pair to an array of
    // groups, and v10 keys topics by `topic_id`. Internal offset storage
    // stays name-keyed, so resolve id→name at the boundary and echo the
    // id back. The legacy v0–v7 single-group path is preserved below.
    if version >= 8 {
        return Ok(handle_groups(broker, version, &req, ctx).await);
    }

    Ok(handle_legacy(broker, &req, ctx).await)
}
