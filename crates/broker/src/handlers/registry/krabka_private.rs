//! Registration table for the krabka-private apis, whose wire codes sit above
//! the Kafka range and so name their `api_key` and `flexible_min` directly.
//!
//! Registers krabka-private context dispatches by raw wire `api_key`.
//!
//! A krabka-private api key sits at or above
//! [`KRABKA_PRIVATE_API_KEY_FLOOR`][crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR],
//! and `ApiKey::from_i16` returns `None` for every key in that range. So each
//! entry names the wire code and its `flexible_min` directly, where a Kafka
//! entry reads both from the generated schema constants. The registry entry is
//! then the only place the framing layer can learn that the body is flexible.
//!
//! Every krabka-private api gets [`super::DispatchKind::Context`], so the handler
//! receives the [`RequestContext`] and can authorize on the principal.

use {
    super::{ContextHandler, DispatchEntry, DispatchRegistry},
    crate::{
        barrier::handlers as barrier_handlers,
        break_glass::handlers as break_glass_handlers,
        broker::Broker,
        error::BrokerError,
        freeze::handlers as freeze_handlers,
        handlers::{self as private_apis, ApiKeyCode, ApiVersion, RequestContext},
    },
    bytes::Bytes,
    futures_util::future::BoxFuture,
    krabka_protocol::krabka::{
        barrier as barrier_wire, break_glass as break_glass_wire, freeze as freeze_wire,
    },
};

// `KRABKA_PRIVATE_API_KEY_FLOOR` for why.
// `flexible_min` comes from the codec of each message, so the framing the
// registry reports and the framing the codec writes cannot drift apart.
krabka_macros::private_dispatch_table! { register register_krabka_private_context_dispatches;
    alter_barrier_groups_adapter { api = private_apis::ALTER_BARRIER_GROUPS_API_KEY, flexible = barrier_wire::alter_barrier_groups::FLEXIBLE_MIN, handler = barrier_handlers::alter_groups::handle }
    describe_barrier_groups_adapter { api = private_apis::DESCRIBE_BARRIER_GROUPS_API_KEY, flexible = barrier_wire::describe_barrier_groups::FLEXIBLE_MIN, handler = barrier_handlers::describe_groups::handle }
    trigger_barrier_adapter { api = private_apis::TRIGGER_BARRIER_API_KEY, flexible = barrier_wire::trigger_barrier::FLEXIBLE_MIN, handler = barrier_handlers::trigger::handle }
    list_barrier_cuts_adapter { api = private_apis::LIST_BARRIER_CUTS_API_KEY, flexible = barrier_wire::list_barrier_cuts::FLEXIBLE_MIN, handler = barrier_handlers::list_cuts::handle }
    write_barrier_markers_adapter { api = private_apis::WRITE_BARRIER_MARKERS_API_KEY, flexible = barrier_wire::write_barrier_markers::FLEXIBLE_MIN, handler = barrier_handlers::write_markers::handle }
    set_topic_freeze_adapter { api = private_apis::SET_TOPIC_FREEZE_API_KEY, flexible = freeze_wire::set_topic_freeze::FLEXIBLE_MIN, handler = freeze_handlers::set_freeze::handle }
    describe_topic_freezes_adapter { api = private_apis::DESCRIBE_TOPIC_FREEZES_API_KEY, flexible = freeze_wire::describe_topic_freezes::FLEXIBLE_MIN, handler = freeze_handlers::describe_freezes::handle }
    propose_break_glass_adapter { api = private_apis::PROPOSE_BREAK_GLASS_API_KEY, flexible = break_glass_wire::propose::FLEXIBLE_MIN, handler = break_glass_handlers::propose::handle }
    approve_break_glass_adapter { api = private_apis::APPROVE_BREAK_GLASS_API_KEY, flexible = break_glass_wire::approve::FLEXIBLE_MIN, handler = break_glass_handlers::approve::handle }
    describe_break_glass_adapter { api = private_apis::DESCRIBE_BREAK_GLASS_API_KEY, flexible = break_glass_wire::describe::FLEXIBLE_MIN, handler = break_glass_handlers::describe::handle }
}

#[cfg(test)]
mod tests;
