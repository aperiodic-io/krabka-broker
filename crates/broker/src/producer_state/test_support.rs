//! Defaulted inputs for non-transactional producer-state commits.

use super::ProducerState;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ProducerEpoch(pub i16);
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ProducerSequence(pub i32);
#[derive(Clone, Copy)]
pub(super) struct SequenceDelta(pub i32);
impl Default for SequenceDelta {
    fn default() -> Self {
        Self(4)
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct ProducerCommitSetup<'a> {
    #[default("t")]
    pub topic: &'a str,
    pub partition: krabka_ids::PartitionIndex,
    #[default(krabka_log::ProducerId(1000))]
    pub producer: krabka_log::ProducerId,
    pub epoch: ProducerEpoch,
    pub sequence: ProducerSequence,
    pub delta: SequenceDelta,
    pub offset: krabka_log::Offset,
    #[default(crate::test_support::UnixMillis(1))]
    pub timestamp: crate::test_support::UnixMillis,
}

pub(super) async fn commit_fixture(state: &ProducerState, setup: ProducerCommitSetup<'_>) {
    state
        .commit(
            setup.topic,
            setup.partition,
            (setup.producer.0, setup.epoch.0),
            (setup.sequence.0, setup.delta.0),
            (setup.offset.0, setup.timestamp.0, false),
        )
        .await;
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) enum RecordOffsets {
    #[default]
    Contiguous,
    FinalOffsetOnly,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct ProducerBatchSetup {
    #[default(krabka_log::ProducerId(42))]
    pub producer: krabka_log::ProducerId,
    #[default(ProducerEpoch(7))]
    pub epoch: ProducerEpoch,
    pub sequence: ProducerSequence,
    #[default(SequenceDelta(0))]
    pub delta: SequenceDelta,
    pub timestamp: crate::test_support::UnixMillis,
    pub offsets: RecordOffsets,
}

pub(super) fn producer_batch(setup: ProducerBatchSetup) -> krabka_protocol::records::RecordBatch {
    use krabka_protocol::records::{Record, RecordBatch};
    let at_offset = |offset_delta| Record {
        offset_delta,
        ..Default::default()
    };
    let records = match setup.offsets {
        RecordOffsets::Contiguous => (0..=setup.delta.0).map(at_offset).collect(),
        RecordOffsets::FinalOffsetOnly => vec![at_offset(setup.delta.0)],
    };
    RecordBatch {
        producer_id: setup.producer.0,
        producer_epoch: setup.epoch.0,
        base_sequence: setup.sequence.0,
        last_offset_delta: setup.delta.0,
        base_timestamp: setup.timestamp.0,
        max_timestamp: setup.timestamp.0,
        records,
        ..Default::default()
    }
}

pub(super) fn opened_log() -> (tempfile::TempDir, krabka_log::LogConfig, krabka_log::Log) {
    let directory = tempfile::tempdir().unwrap();
    let config = krabka_log::LogConfig::default();
    let log = krabka_log::Log::open(directory.path(), config.clone()).unwrap();
    (directory, config, log)
}
