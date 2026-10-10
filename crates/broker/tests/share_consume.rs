//! End-to-end integration tests for KIP-932 share-partition consumption
//! (`ShareFetch`, `api_key` 78) and acknowledge (`ShareAcknowledge`, `api_key`
//! 79), driven against an in-process Krabka broker through `krabka-client-core`.
//!
//! The typed client works because `ApiVersions` advertises `api_keys` 78/79.
//! Both `ShareFetchRequest` / `ShareAcknowledgeRequest` impl `ProtocolRequest`,
//! so `client.send(req)` returns the typed response and exercises the real wire
//! path. The client negotiates the version through `ApiVersions`. Both RPCs are
//! MIN=1 MAX=2, so the client negotiates v2.
//!
//! These tests prove the full acquire/ack loop:
//! - acquire under a lock and read the verbatim record bytes;
//! - Accept advances the SPSO, and the advance survives a broker restart, which
//!   shows the broker persisted it to the share coordinator;
//! - Release re-delivers with an incremented `delivery_count`;
//! - Reject archives and advances the SPSO past the poison record;
//! - the background lock-timeout sweep re-delivers an unacknowledged lock that
//!   expires;
//! - the broker archives a record that exhausts `max_delivery_attempts` (poison
//!   pill);
//! - the share-session epoch state machine rejects stale / unknown epochs.
//!
//! The binary root carries only the error-code and ack-type constants the
//! whole suite shares, plus the module tree below. `harness` starts the broker
//! and prepares the topic, the group, and the share state; `share_rpc` drives
//! the two RPCs; and each remaining child covers one consume behaviour --
//! acknowledgement outcomes, the record bytes a fetch returns, the
//! acquisition-lock lifetime, the share-session state machine, and
//! `read_committed` isolation.

mod support;

#[path = "share_consume/acknowledgements.rs"]
mod acknowledgements;
#[path = "share_consume/dead_letter_queue.rs"]
mod dead_letter_queue;
#[path = "share_consume/harness.rs"]
mod harness;
#[path = "share_consume/isolation_level.rs"]
mod isolation_level;
#[path = "share_consume/lock_lifetime.rs"]
mod lock_lifetime;
#[path = "share_consume/record_bytes.rs"]
mod record_bytes;
#[path = "share_consume/session_epoch.rs"]
mod session_epoch;
#[path = "share_consume/share_rpc.rs"]
mod share_rpc;

const NONE: i16 = 0;
const INVALID_SHARE_SESSION_EPOCH: i16 = 123;
const SHARE_SESSION_NOT_FOUND: i16 = 122;

// Ack types (KIP-932): one i8 per offset.
const REJECT: i8 = 3;
const RENEW: i8 = 4;

const ONE_MB: i32 = 1 << 20;
