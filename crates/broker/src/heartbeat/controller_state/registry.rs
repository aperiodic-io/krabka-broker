//! The liveness registry itself: the per-broker entry, the states it moves
//! between, and the constructors that build the registry.
//!
//! The behaviour over these types is split by concern into the sibling
//! modules, which reach the fields from here.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use krabka_units::{Time, convert::TimeExt as _};
use tokio::sync::Mutex;

use super::clock::Clock;
#[cfg(test)]
use super::clock::TestClock;

/// Per-broker liveness state as seen by the controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrokerLivenessState {
    /// Broker has sent a heartbeat within the timeout window.
    Alive,
    /// No heartbeat received within the timeout window.
    Dead,
}

/// An edge transition emitted by [`ControllerLivenessState::tick`] or
/// [`ControllerLivenessState::record_fenced_heartbeat`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LivenessTransition {
    /// Broker was `Dead`; this heartbeat revived it.
    DeadToAlive(u64),
    /// Broker crossed the deadline; marked `Dead`.
    AliveToDead(u64),
}

pub(super) struct BrokerEntry {
    pub(super) last_heartbeat: Instant,
    pub(super) state: BrokerLivenessState,
    pub(super) fenced: bool,
    /// Whether `last_heartbeat` is contact from the broker, which Kafka's
    /// `BrokerHeartbeatTracker` calls a session. A heartbeat opens one, and
    /// so does an unfenced registration when this node becomes controller
    /// (`ClusterControlManager.activate`). An entry this controller only
    /// discovered in the image, or reset for a new incarnation, has none:
    /// its clock runs so that a broker that never heartbeats still expires,
    /// but it does not hold the broker id against a new registration.
    pub(super) contact: bool,
    /// The metadata offset the broker reported on its last heartbeat, or -1
    /// before its first one. Kafka's `BrokerHeartbeatState.metadataOffset`.
    pub(super) metadata_offset: i64,
    /// The metadata offset at which the broker's controlled shutdown is
    /// complete, while the broker is in controlled shutdown. Kafka's
    /// `BrokerHeartbeatState.controlledShutdownOffset`.
    pub(super) controlled_shutdown_offset: Option<i64>,
}

impl BrokerEntry {
    /// Kafka's active broker: alive, unfenced, and not in controlled shutdown.
    /// Only an active broker may be elected, and only an active broker holds
    /// back a controlled shutdown.
    pub(super) fn is_active(&self) -> bool {
        self.state == BrokerLivenessState::Alive
            && !self.fenced
            && self.controlled_shutdown_offset.is_none()
    }

    /// A new entry, contacted now or not.
    pub(super) const fn new(now: Instant, fenced: bool, contact: bool) -> Self {
        Self {
            last_heartbeat: now,
            state: BrokerLivenessState::Alive,
            fenced,
            contact,
            metadata_offset: -1,
            controlled_shutdown_offset: None,
        }
    }
}

/// Controller-side heartbeat registry.
///
/// One instance lives on the `Broker` struct. Handlers call
/// [`record_fenced_heartbeat`](Self::record_fenced_heartbeat) on every incoming
/// `BrokerHeartbeat` RPC. The liveness ticker calls [`tick`](Self::tick)
/// every second to expire stale entries.
pub(crate) struct ControllerLivenessState {
    pub(super) timeout: Duration,
    pub(super) clock: Clock,
    pub(super) brokers: Mutex<HashMap<u64, BrokerEntry>>,
    /// Serializes `BrokerRegistration`: see
    /// [`registration_turn`](Self::registration_turn).
    pub(super) registrations: Mutex<()>,
    /// The controller term the registry was last seeded for, or `u64::MAX`
    /// before the first seed. See [`seed_term`](Self::seed_term).
    pub(super) seeded_term: std::sync::atomic::AtomicU64,
}

impl ControllerLivenessState {
    /// Create a new registry with the given heartbeat timeout.
    pub(crate) fn new(timeout: Time) -> Self {
        Self::with_clock(timeout.to_std(), Clock::Real)
    }

    /// Construct with the selected clock for production or controlled liveness windows.
    pub(super) fn with_clock(timeout: Duration, clock: Clock) -> Self {
        Self {
            timeout,
            clock,
            brokers: Mutex::new(HashMap::new()),
            registrations: Mutex::new(()),
            seeded_term: std::sync::atomic::AtomicU64::new(u64::MAX),
        }
    }

    /// Construct with a [`TestClock`]. Tests outside this module use it to
    /// drive a broker to `Dead` through [`tick`](Self::tick) without a
    /// wall-clock sleep.
    #[cfg(test)]
    pub(crate) fn with_test_clock(timeout: Duration, clock: &TestClock) -> Self {
        Self::with_clock(timeout, clock.clock())
    }
}
