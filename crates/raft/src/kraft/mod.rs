//! Hand-rolled `KRaft` consensus engine (KIP-595 and KIP-996).
//!
//! The engine is a pure, deterministic, sans-IO `on_event` state machine
//! ([`core`]) over the `QuorumState` and `Role` model. The async
//! [`controller::KraftController`] drives that state machine over the
//! [`log::KraftLog`] and the real KIP-595 [`transport`] wire. This is Krabka's
//! live metadata consensus engine.
#![allow(dead_code, unused_imports)]

// The pure, deterministic, sans-IO consensus core (the `on_event` state
// machine, its event/action/role/type model, and the snapshot reassembler)
// lives in the wasm-friendly leaf crate `krabka-kraft-core`. Re-export its
// modules here so the async engine, real wire, and on-disk log below keep
// referencing `crate::kraft::{core, types, event, ...}` unchanged.
pub use krabka_kraft_core::{action, core, event, role, snapshot_fetch, types};

pub mod controller;
pub mod log;
pub mod transport;

pub use {
    action::Action,
    controller::{Activation, KraftConfig, KraftController},
    core::QuorumStateMachine,
    event::Event,
    log::KraftLog,
    role::Role,
    transport::{
        Command, Inbound, MetadataFetchSlice, NullPeerSender, PeerSender, QuorumStateSnapshot,
        TimerTick,
    },
    types::{Epoch, LogOffsetMetadata, LogView, NodeId, QuorumState, ReplicaKey, SimInstant},
};
