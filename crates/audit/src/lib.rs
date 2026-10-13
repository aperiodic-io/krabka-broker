//! Krabka audit subsystem: event model, OCSF serialization, and write pipeline.
//!
//! See `docs/superpowers/specs/2026-06-18-krabka-fedramp-mla-audit-design.md`.

pub mod chain;
pub mod checkpoint;
pub mod event;
pub mod ids;
pub mod log;
pub mod ocsf;
pub mod signing;
pub mod sink;
pub mod spool;
pub mod stats;
pub mod verify;

#[cfg(test)]
mod spool_model;
#[cfg(test)]
mod test_support;

pub use {
    chain::{ChainState, GENESIS_HEAD, chain_hash},
    checkpoint::{Checkpoint, EVENT_CLASS_CHECKPOINT},
    event::{
        AuditEndpoint, AuditEvent, AuditEventClass, AuditOutcome, AuditPrincipal, AuditResource,
        LifecycleKind, PrivilegedPhase,
    },
    ids::{CheckpointCount, EpochMs, MaxSpoolBytes, RecordCount, Seq, SpoolBytes},
    krabka_ids::NodeId,
    log::{AuditLog, AuditMode, AuditReceiver, AuditWriter, AuditWriterParams},
    ocsf::{ProductInfo, to_ocsf},
    signing::{FileEd25519Signer, checkpoint_signing_bytes, verify_signature},
    sink::{AuditError, AuditRecord, AuditSink, HEADER_PREV_HASH, HEADER_SEQ, MemorySink},
    spool::Spool,
    stats::AuditStats,
    verify::{TrustedKeys, VerifyBreak, VerifyReport, verify_partition_dir},
};
