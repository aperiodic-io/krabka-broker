//! Registration table for the krabka-private apis, whose wire codes sit above
//! the Kafka range and so name their `api_key` and `flexible_min` directly.

use {
    super::{ContextHandler, DispatchEntry, DispatchRegistry},
    crate::{
        broker::Broker,
        error::BrokerError,
        handlers::{ApiKeyCode, ApiVersion, RequestContext},
    },
    bytes::Bytes,
    futures_util::future::BoxFuture,
};

// `KRABKA_PRIVATE_API_KEY_FLOOR` for why.
// `flexible_min` comes from the codec of each message, so the framing the
// registry reports and the framing the codec writes cannot drift apart.
krabka_private_context_dispatches!(register_krabka_private_context_dispatches;
    (
        alter_barrier_groups_adapter,
        crate::handlers::ALTER_BARRIER_GROUPS_API_KEY,
        krabka_protocol::krabka::barrier::alter_barrier_groups::FLEXIBLE_MIN,
        crate::barrier::handlers::alter_groups::handle
    ),
    (
        describe_barrier_groups_adapter,
        crate::handlers::DESCRIBE_BARRIER_GROUPS_API_KEY,
        krabka_protocol::krabka::barrier::describe_barrier_groups::FLEXIBLE_MIN,
        crate::barrier::handlers::describe_groups::handle
    ),
    (
        trigger_barrier_adapter,
        crate::handlers::TRIGGER_BARRIER_API_KEY,
        krabka_protocol::krabka::barrier::trigger_barrier::FLEXIBLE_MIN,
        crate::barrier::handlers::trigger::handle
    ),
    (
        list_barrier_cuts_adapter,
        crate::handlers::LIST_BARRIER_CUTS_API_KEY,
        krabka_protocol::krabka::barrier::list_barrier_cuts::FLEXIBLE_MIN,
        crate::barrier::handlers::list_cuts::handle
    ),
    (
        write_barrier_markers_adapter,
        crate::handlers::WRITE_BARRIER_MARKERS_API_KEY,
        krabka_protocol::krabka::barrier::write_barrier_markers::FLEXIBLE_MIN,
        crate::barrier::handlers::write_markers::handle
    ),
    (
        set_topic_freeze_adapter,
        crate::handlers::SET_TOPIC_FREEZE_API_KEY,
        krabka_protocol::krabka::freeze::set_topic_freeze::FLEXIBLE_MIN,
        crate::freeze::handlers::set_freeze::handle
    ),
    (
        describe_topic_freezes_adapter,
        crate::handlers::DESCRIBE_TOPIC_FREEZES_API_KEY,
        krabka_protocol::krabka::freeze::describe_topic_freezes::FLEXIBLE_MIN,
        crate::freeze::handlers::describe_freezes::handle
    ),
    (
        propose_break_glass_adapter,
        crate::handlers::PROPOSE_BREAK_GLASS_API_KEY,
        krabka_protocol::krabka::break_glass::propose::FLEXIBLE_MIN,
        crate::break_glass::handlers::propose::handle
    ),
    (
        approve_break_glass_adapter,
        crate::handlers::APPROVE_BREAK_GLASS_API_KEY,
        krabka_protocol::krabka::break_glass::approve::FLEXIBLE_MIN,
        crate::break_glass::handlers::approve::handle
    ),
    (
        describe_break_glass_adapter,
        crate::handlers::DESCRIBE_BREAK_GLASS_API_KEY,
        krabka_protocol::krabka::break_glass::describe::FLEXIBLE_MIN,
        crate::break_glass::handlers::describe::handle
    ),
);

#[cfg(test)]
mod tests;
