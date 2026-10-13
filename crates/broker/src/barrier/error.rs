//! Errors that the barrier coordinator returns.
//!
//! The coordinator has its own error enum, because the wire codes it needs are
//! not the codes a transaction or a share-state request returns. A handler
//! maps each variant to one Kafka error code at the request boundary.

use {crate::error::BrokerError, krabka_ids::PartitionIndex, thiserror::Error};

/// A barrier control-plane or injection failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub(crate) enum BarrierError {
    /// The state partition of the group has another leader, so this broker
    /// must not allocate an epoch for it.
    #[error("this broker does not coordinate barrier group {group}")]
    NotCoordinator {
        /// The group the caller named.
        group: String,
    },

    /// No group record with this name is live.
    #[error("barrier group {group} does not exist")]
    UnknownGroup {
        /// The group the caller named.
        group: String,
    },

    /// A group with this name is already live, and the caller asked to create
    /// one.
    #[error("barrier group {group} already exists")]
    GroupExists {
        /// The group the caller named.
        group: String,
    },

    /// Another injection or another group edit holds the group entry. The
    /// coordinator runs one injection per group at a time.
    #[error("barrier group {group} already has an injection in flight")]
    InjectionInProgress {
        /// The group the caller named.
        group: String,
    },

    /// The group allocated every representable barrier epoch.
    #[error("barrier group {group} exhausted its epoch space")]
    EpochExhausted {
        /// The group whose epoch cannot advance.
        group: String,
    },

    /// The caller supplied a group definition that the coordinator rejects,
    /// such as an empty topic list or a cut retention below one.
    #[error("invalid barrier group definition: {0}")]
    InvalidDefinition(String),

    /// This broker leads the state partition in the metadata image, but the
    /// log is not open here yet.
    #[error("__barrier_state-{partition} is not open on this broker")]
    StateNotLocal {
        /// The state partition that carries the group.
        partition: PartitionIndex,
    },

    /// The coordinator lost the state partition between the injection-start
    /// record and the cut record. The next coordinator finalises the epoch.
    #[error("barrier group {group} moved to coordinator epoch {current}, above {expected}")]
    CoordinatorEpochChanged {
        /// The group the caller named.
        group: String,
        /// The epoch the injection-start record froze.
        expected: i32,
        /// The epoch the state partition carries now.
        current: i32,
    },

    /// An append to `__barrier_state` failed.
    #[error("__barrier_state append failed: {0}")]
    Persist(#[from] BrokerError),
}
