//! The [`BrokerConfig`] struct: every knob the broker reads at construction
//! time.
//!
//! A struct is one item, so it cannot be cut across modules the way a set of
//! functions can. Each child module instead holds one contiguous run of the
//! fields inside a `macro_rules!` that appends its run to the fields
//! collected so far and hands them on; the last child hands the whole set to
//! `define_broker_config`, which emits the struct here. The order of the
//! chain is the declaration order of the struct, so every field keeps the
//! position it has always had.

use {
    crate::{
        config::{
            BreakGlassConfig, BrokerFeatureFlags, FreezeConfig, InterBrokerCredentials,
            ListenerSpec, NodeRole, RemoteStorageBackend, ReplicationRuntimeConfig, RlmmKind,
            StretchProfile,
        },
        operator_keys::OperatorKeys,
    },
    krabka_log::LogConfig,
    krabka_raft::{
        BootstrapMode, ControllerFetchMissLimit, MetadataRaftCommandQueueCapacity,
        MetadataRaftFetchMax, NodeId,
    },
    krabka_security::{SaslMechanism, TlsConfig},
    krabka_units::{ByteSize, Ratio, Time},
    std::{
        collections::{BTreeMap, HashMap},
        net::SocketAddr,
        path::PathBuf,
        sync::Arc,
    },
};

// The end of the field chain: it emits the struct from the fields the chain
// collected. `macro_rules!` scope is textual, so it is defined ahead of the
// group modules, the last of which expands it.
macro_rules! define_broker_config {
    ($($field:tt)*) => {
        #[derive(Debug, Clone)]
        // a broad config struct; flags are independent knobs
        pub struct BrokerConfig {
            $($field)*
        }
    };
}

// The field groups, declared `#[macro_use]` and in reverse chain order, so
// that each link is already in scope where the link before it names it.
#[macro_use]
mod audit;
#[macro_use]
mod remote_storage;
#[macro_use]
mod delegation_tokens;
#[macro_use]
mod operations;
#[macro_use]
mod coordinators;
#[macro_use]
mod security;
#[macro_use]
mod quorum;
#[macro_use]
mod identity;
#[macro_use]
mod tuning;

// The head of the chain. It expands one group after another and ends in
// `define_broker_config`.
tuning_fields! {}

pub use self::security::PlainCredentials;
