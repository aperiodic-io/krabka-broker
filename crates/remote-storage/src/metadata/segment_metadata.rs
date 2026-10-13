//! The remote-segment metadata record and the update that advances it.
//!
//! [`RemoteLogSegmentMetadata`] is what the metadata SPI stores for one
//! segment, [`RemoteLogSegmentDetails`] groups the fields its constructor
//! validates, [`RemoteLogSegmentMetadataUpdate`] is the lifecycle update that
//! [`RemoteLogSegmentMetadata::with_update`] applies, and [`CustomMetadata`]
//! is the opaque payload a storage manager may attach at copy time.

use {
    crate::{
        error::RemoteStorageError,
        metadata::{RemoteLogSegmentId, RemoteLogSegmentState},
    },
    krabka_ids::LeaderEpoch,
    std::collections::BTreeMap,
};

#[derive(derive_more::From, derive_more::Into)]
/// Opaque bytes that an [`RemoteStorageManager`](crate::RemoteStorageManager)
/// can return from `copy_log_segment_data`, for example an object-store key
/// or a version id. The caller sends the bytes back on every later call for
/// that segment.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CustomMetadata(pub Vec<u8>);

/// Metadata for one segment that is stored, or is being stored, in the
/// remote tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteLogSegmentMetadata {
    remote_log_segment_id: RemoteLogSegmentId,
    start_offset: i64,
    end_offset: i64,
    max_timestamp_ms: i64,
    broker_id: i32,
    event_timestamp_ms: i64,
    segment_size_in_bytes: i32,
    custom_metadata: Option<CustomMetadata>,
    state: RemoteLogSegmentState,
    segment_leader_epochs: BTreeMap<LeaderEpoch, i64>,
    /// KIP-405 `txnIndexEmpty`: `true` when the segment carries no transaction
    /// index. Serialized as tagged field (tag 0) in the JVM record format.
    /// Defaults to `false`.
    txn_index_empty: bool,
}

/// Size, lifecycle state, and leader-epoch offsets for a remote log segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteLogSegmentDetails {
    segment_size_in_bytes: i32,
    state: RemoteLogSegmentState,
    segment_leader_epochs: BTreeMap<LeaderEpoch, i64>,
}

impl RemoteLogSegmentDetails {
    #[must_use]
    pub fn new(
        segment_size_in_bytes: i32,
        state: RemoteLogSegmentState,
        segment_leader_epochs: BTreeMap<LeaderEpoch, i64>,
    ) -> Self {
        Self {
            segment_size_in_bytes,
            state,
            segment_leader_epochs,
        }
    }
}

impl RemoteLogSegmentMetadata {
    /// Constructs a [`RemoteLogSegmentMetadata`].
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidArgument`] when
    /// `segment_leader_epochs` is empty, `end_offset < start_offset`, or
    /// `segment_size_in_bytes < 0`.
    pub fn new(
        remote_log_segment_id: RemoteLogSegmentId,
        start_offset: i64,
        end_offset: i64,
        max_timestamp_ms: i64,
        broker_id: i32,
        event_timestamp_ms: i64,
        details: RemoteLogSegmentDetails,
    ) -> Result<Self, RemoteStorageError> {
        let RemoteLogSegmentDetails {
            segment_size_in_bytes,
            state,
            segment_leader_epochs,
        } = details;
        if segment_leader_epochs.is_empty() {
            return Err(RemoteStorageError::InvalidArgument(
                "segment_leader_epochs must not be empty".into(),
            ));
        }
        if end_offset < start_offset {
            return Err(RemoteStorageError::InvalidArgument(format!(
                "end_offset ({end_offset}) < start_offset ({start_offset})"
            )));
        }
        if segment_size_in_bytes < 0 {
            return Err(RemoteStorageError::InvalidArgument(format!(
                "segment_size_in_bytes ({segment_size_in_bytes}) must be >= 0"
            )));
        }
        Ok(Self {
            remote_log_segment_id,
            start_offset,
            end_offset,
            max_timestamp_ms,
            broker_id,
            event_timestamp_ms,
            segment_size_in_bytes,
            custom_metadata: None,
            state,
            segment_leader_epochs,
            txn_index_empty: false,
        })
    }

    /// Applies a [`RemoteLogSegmentMetadataUpdate`] and returns the updated
    /// copy. The update advances `state`, refreshes `event_timestamp_ms`
    /// and `broker_id`, and replaces `custom_metadata` when the update
    /// carries `Some`.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidArgument`] if the update's
    /// segment id does not match, or
    /// [`RemoteStorageError::InvalidSegmentTransition`] if the state change
    /// is not permitted from the current state.
    pub fn with_update(
        &self,
        update: &RemoteLogSegmentMetadataUpdate,
    ) -> Result<Self, RemoteStorageError> {
        if update.remote_log_segment_id != self.remote_log_segment_id {
            return Err(RemoteStorageError::InvalidArgument(
                "update segment id does not match metadata segment id".into(),
            ));
        }
        if !self.state.is_valid_transition(update.state) {
            return Err(RemoteStorageError::InvalidSegmentTransition {
                id: self.remote_log_segment_id.clone(),
                from: self.state,
                to: update.state,
            });
        }
        let mut next = self.clone();
        next.state = update.state;
        next.event_timestamp_ms = update.event_timestamp_ms;
        next.broker_id = update.broker_id;
        if update.custom_metadata.is_some() {
            next.custom_metadata.clone_from(&update.custom_metadata);
        }
        Ok(next)
    }

    /// The segment's unique id.
    #[must_use]
    pub fn remote_log_segment_id(&self) -> &RemoteLogSegmentId { &self.remote_log_segment_id }

    /// First offset (inclusive) covered by this segment.
    #[must_use]
    pub fn start_offset(&self) -> i64 { self.start_offset }

    /// Last offset (inclusive) covered by this segment.
    #[must_use]
    pub fn end_offset(&self) -> i64 { self.end_offset }

    /// Highest record timestamp in this segment.
    #[must_use]
    pub fn max_timestamp_ms(&self) -> i64 { self.max_timestamp_ms }

    /// Id of the broker that produced this metadata.
    #[must_use]
    pub fn broker_id(&self) -> i32 { self.broker_id }

    /// Wall-clock time the latest event for this segment was created.
    #[must_use]
    pub fn event_timestamp_ms(&self) -> i64 { self.event_timestamp_ms }

    /// Size of the `.log` data in bytes.
    #[must_use]
    pub fn segment_size_in_bytes(&self) -> i32 { self.segment_size_in_bytes }

    /// Opaque metadata the [`RemoteStorageManager`](crate::RemoteStorageManager)
    /// attached at copy time, if any.
    #[must_use]
    pub fn custom_metadata(&self) -> Option<&CustomMetadata> { self.custom_metadata.as_ref() }

    /// Current lifecycle state.
    #[must_use]
    pub fn state(&self) -> RemoteLogSegmentState { self.state }

    /// Map of leader epoch → first offset that epoch contributed to this
    /// segment.
    #[must_use]
    pub fn segment_leader_epochs(&self) -> &BTreeMap<LeaderEpoch, i64> {
        &self.segment_leader_epochs
    }

    /// The first offset of `epoch` and the earliest offset belonging to a later epoch.
    #[must_use]
    pub fn epoch_bounds(&self, epoch: LeaderEpoch) -> (Option<i64>, Option<i64>) {
        let start = self.segment_leader_epochs.get(&epoch).copied();
        let next = self
            .segment_leader_epochs
            .iter()
            .filter(|(candidate, _)| **candidate > epoch)
            .map(|(_, start)| *start)
            .min();
        (start, next)
    }

    /// Attaches custom metadata in builder style. RSM copy paths use it when
    /// they produce a key before they record `CopySegmentFinished`.
    #[must_use]
    pub fn with_custom_metadata(mut self, custom: CustomMetadata) -> Self {
        self.custom_metadata = Some(custom);
        self
    }

    /// `true` if the segment has no transaction index (KIP-405 `txnIndexEmpty`).
    /// Defaults to `false`. Serialized as the JVM record's tagged field (tag 0).
    #[must_use]
    pub fn txn_index_empty(&self) -> bool { self.txn_index_empty }

    /// Builder-style setter for [`Self::txn_index_empty`].
    #[must_use]
    pub fn with_txn_index_empty(mut self, empty: bool) -> Self {
        self.txn_index_empty = empty;
        self
    }
}

/// An update to an existing [`RemoteLogSegmentMetadata`]'s lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteLogSegmentMetadataUpdate {
    /// The segment being updated.
    pub remote_log_segment_id: RemoteLogSegmentId,
    /// Wall-clock time of this update.
    pub event_timestamp_ms: i64,
    /// New custom metadata, when the update introduces or changes it.
    pub custom_metadata: Option<CustomMetadata>,
    /// The new lifecycle state.
    pub state: RemoteLogSegmentState,
    /// Broker that produced this update.
    pub broker_id: i32,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::metadata::TopicIdPartition,
        assert2::{assert, check},
        uuid::Uuid,
    };

    fn tp() -> TopicIdPartition { TopicIdPartition::new(Uuid::from_u128(1), "orders", 0) }

    fn seg_id() -> RemoteLogSegmentId { RemoteLogSegmentId::new(tp(), Uuid::from_u128(99)) }

    fn epochs() -> BTreeMap<LeaderEpoch, i64> {
        maplit::btreemap! {LeaderEpoch(0) => 0}
    }

    fn metadata_range(
        start: i64,
        end: i64,
        leader_epochs: BTreeMap<LeaderEpoch, i64>,
    ) -> Result<RemoteLogSegmentMetadata, RemoteStorageError> {
        RemoteLogSegmentMetadata::new(
            seg_id(),
            start,
            end,
            123,
            1,
            456,
            RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentStarted,
                leader_epochs,
            ),
        )
    }

    fn started_metadata() -> RemoteLogSegmentMetadata { metadata_range(0, 10, epochs()).unwrap() }

    #[test]
    fn accessors_return_constructed_values() {
        // max_timestamp_ms / segment_size_in_bytes accessors were never read
        // back in the suite; pin them to distinct non-default values.
        let md = RemoteLogSegmentMetadata::new(
            seg_id(),
            0,
            100,
            777, // max_timestamp_ms
            5,
            888,
            RemoteLogSegmentDetails::new(
                4096, // segment_size_in_bytes
                RemoteLogSegmentState::CopySegmentStarted,
                epochs(),
            ),
        )
        .unwrap();
        assert!(md.max_timestamp_ms() == 777);
        assert!(md.segment_size_in_bytes() == 4096);
    }

    #[test]
    fn metadata_rejects_empty_leader_epochs() {
        let err = metadata_range(0, 10, BTreeMap::new()).unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidArgument(_)));
    }

    #[test]
    fn metadata_rejects_end_before_start() {
        let err = metadata_range(10, 5, epochs()).unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidArgument(_)));
    }

    #[test]
    fn with_update_advances_state_and_fields() {
        let started = started_metadata();
        let update = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: seg_id(),
            event_timestamp_ms: 789,
            custom_metadata: Some(CustomMetadata(vec![1, 2, 3])),
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 2,
        };
        let finished = started.with_update(&update).unwrap();
        check!(finished.state() == RemoteLogSegmentState::CopySegmentFinished);
        check!(finished.event_timestamp_ms() == 789);
        check!(finished.broker_id() == 2);
        check!(finished.custom_metadata() == Some(&CustomMetadata(vec![1, 2, 3])));
        // Untouched fields survive.
        check!(finished.start_offset() == 0);
        check!(finished.end_offset() == 10);
    }

    #[test]
    fn with_update_keeps_custom_metadata_when_update_omits_it() {
        let started = started_metadata().with_custom_metadata(CustomMetadata(vec![9]));
        let update = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: seg_id(),
            event_timestamp_ms: 789,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 2,
        };
        let finished = started.with_update(&update).unwrap();
        assert!(finished.custom_metadata() == Some(&CustomMetadata(vec![9])));
    }

    #[test]
    fn with_update_rejects_invalid_transition() {
        let started = started_metadata();
        let update = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: seg_id(),
            event_timestamp_ms: 789,
            custom_metadata: None,
            state: RemoteLogSegmentState::DeleteSegmentFinished,
            broker_id: 2,
        };
        let err = started.with_update(&update).unwrap_err();
        assert!(matches!(
            err,
            RemoteStorageError::InvalidSegmentTransition { .. }
        ));
    }

    #[test]
    fn with_update_rejects_mismatched_id() {
        let started = started_metadata();
        let other = RemoteLogSegmentId::new(tp(), Uuid::from_u128(1234));
        let update = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: other,
            event_timestamp_ms: 789,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 2,
        };
        let err = started.with_update(&update).unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidArgument(_)));
    }

    #[test]
    fn txn_index_empty_defaults_false_and_is_settable() {
        let md = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(1), "t", 0),
                Uuid::from_u128(2),
            ),
            0,
            9,
            9,
            1,
            100,
            RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentStarted,
                maplit::btreemap! {LeaderEpoch(0) => 0},
            ),
        )
        .unwrap();
        assert!(!md.txn_index_empty());
        let md = md.with_txn_index_empty(true);
        assert!(md.txn_index_empty());
    }
}
