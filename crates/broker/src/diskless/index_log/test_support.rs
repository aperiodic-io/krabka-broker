//! [`MetadataEventLog`] wrappers that let tests drive the projection's
//! catch-up gate: [`PacedReplayLog`] paces the stream `subscribe` returns, so
//! a replay can be slow or silent-but-open, and [`RacingAppendLog`] appends a
//! record from inside `subscribe`, so a test can land another broker's flush
//! in the window between establishing the subscription and reading the
//! watermark. They compose: wrap one in the other.

use {
    crate::{diskless::wal_index::WalFlushRecord, test_support::UnixMillis},
    async_trait::async_trait,
    bytes::Bytes,
    futures_util::{FutureExt as _, StreamExt as _},
    krabka_ids::PartitionIndex,
    krabka_remote_storage_topic::{
        AssignmentHandle, MetadataEventLog, MetadataEventStream, PartitionStart,
    },
    std::{
        sync::{Arc, Mutex as StdMutex},
        time::Duration,
    },
};

/// How fast the wrapped log's subscription delivers what it replays.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ReplayPace {
    /// Deliver one record per interval.
    OneEvery(Duration),
    /// Deliver nothing, ever, without closing the stream. This is the shape a
    /// [`krabka_remote_storage_topic::KafkaMetadataEventLog`] partition takes
    /// when its fetch loop dies while connecting: the shared queue keeps its
    /// sender, so the stream stays open and silent.
    Never,
}

/// Wraps a [`MetadataEventLog`] and paces its subscription. Every other
/// method delegates.
pub(crate) struct PacedReplayLog {
    inner: Arc<dyn MetadataEventLog>,
    pace: ReplayPace,
}

impl PacedReplayLog {
    pub(crate) fn new(inner: Arc<dyn MetadataEventLog>, pace: ReplayPace) -> Arc<Self> {
        Arc::new(Self { inner, pace })
    }
}

#[krabka_macros::metadata_log_delegate(krabka_remote_storage_topic, keyed)]
#[async_trait]
impl MetadataEventLog for PacedReplayLog {
    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        let (stream, handle) = self.inner.subscribe(assignment);
        let pace = self.pace;
        let paced = stream.then(move |event| async move {
            match pace {
                ReplayPace::OneEvery(interval) => tokio::time::sleep(interval).await,
                ReplayPace::Never => std::future::pending::<()>().await,
            }
            event
        });
        (Box::pin(paced), handle)
    }
}

/// Publishes one record from inside `subscribe`, modelling another broker's
/// in-flight flush landing exactly while a restarting projection establishes
/// its subscription. A watermark read *before* subscribing steps over that
/// record; one read after cannot.
#[derive(krabka_macros::FieldDefaults)]
pub(crate) struct RacingAppendSetup {
    pub partition: PartitionIndex,
    pub key: Bytes,
    pub event: Bytes,
}

pub(crate) struct RacingAppendLog {
    inner: Arc<dyn MetadataEventLog>,
    racing: StdMutex<Option<RacingAppendSetup>>,
}

#[bon::bon]
impl RacingAppendLog {
    /// Race the setup's keyed record onto its partition, as the flusher
    /// publishes every index record.
    #[builder]
    pub(crate) fn build(
        inner: Arc<dyn MetadataEventLog>,
        #[builder(into, default)] partition: PartitionIndex,
        #[builder(default)] key: Bytes,
        #[builder(default)] event: Bytes,
    ) -> Arc<Self> {
        let setup = RacingAppendSetup {
            partition,
            key,
            event,
        };
        Arc::new(Self {
            inner,
            racing: StdMutex::new(Some(setup)),
        })
    }
}

#[krabka_macros::metadata_log_delegate(krabka_remote_storage_topic, keyed)]
#[async_trait]
impl MetadataEventLog for RacingAppendLog {
    fn subscribe(
        &self,
        assignment: Vec<PartitionStart>,
    ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
        let racing = self
            .racing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(setup) = racing {
            // `subscribe` is synchronous, so this drives the append to
            // completion in one poll. The in-process fixture's `publish_keyed`
            // never yields, so it always finishes on the first.
            self.inner
                .publish_keyed(setup.partition.0, setup.key, Some(setup.event))
                .now_or_never()
                .expect("the in-process fixture publishes without yielding")
                .expect("racing append");
        }
        self.inner.subscribe(assignment)
    }
}

/// One input WAL range; timestamps and byte spans vary only where a case needs them.
#[derive(Clone, Copy)]
pub(crate) struct WalFlushSetup<'a> {
    pub topic_id: uuid::Uuid,
    pub object_key: &'a str,
    pub first_offset: krabka_log::Offset,
    pub last_offset: krabka_log::Offset,
    pub max_timestamp: UnixMillis,
    pub byte_len: krabka_units::ByteSize,
}

impl Default for WalFlushSetup<'_> {
    fn default() -> Self {
        Self {
            topic_id: uuid::Uuid::from_u128(7),
            object_key: "object-a",
            first_offset: krabka_log::Offset(0),
            last_offset: krabka_log::Offset(3),
            max_timestamp: UnixMillis(0),
            byte_len: krabka_units::bytes(10),
        }
    }
}

impl WalFlushSetup<'_> {
    pub(crate) fn second_object(topic_id: uuid::Uuid) -> Self {
        Self {
            topic_id,
            object_key: "object-b",
            first_offset: krabka_log::Offset(4),
            last_offset: krabka_log::Offset(7),
            ..Default::default()
        }
    }

    /// Offsets zero through two in the six-byte object used by reclaimer tests.
    pub(crate) fn three_records() -> Self {
        Self {
            last_offset: krabka_log::Offset(2),
            byte_len: krabka_units::bytes(6),
            ..Default::default()
        }
    }
}

#[bon::builder]
pub(crate) fn flush_record(
    #[builder(default = WalFlushSetup::default().topic_id)] topic_id: uuid::Uuid,
    #[builder(default = WalFlushSetup::default().object_key)] object_key: &'_ str,
    #[builder(into, default = WalFlushSetup::default().first_offset)]
    first_offset: krabka_log::Offset,
    #[builder(into, default = WalFlushSetup::default().last_offset)]
    last_offset: krabka_log::Offset,
    #[builder(into, default = WalFlushSetup::default().max_timestamp)] max_timestamp: UnixMillis,
    #[builder(default = WalFlushSetup::default().byte_len)] byte_len: krabka_units::ByteSize,
) -> WalFlushRecord {
    use {crate::diskless::wal_index::WalIndexEntry, krabka_units::convert::ByteSizeExt};

    let setup = WalFlushSetup {
        topic_id,
        object_key,
        first_offset,
        last_offset,
        max_timestamp,
        byte_len,
    };
    let WalFlushSetup {
        topic_id,
        object_key,
        first_offset,
        last_offset,
        max_timestamp,
        byte_len,
    } = setup;
    WalFlushRecord {
        object_key: object_key.into(),
        format_version: WalFlushRecord::FORMAT_VERSION,
        entries: vec![WalIndexEntry {
            topic_id,
            partition: 0,
            first_offset: first_offset.0,
            last_offset: last_offset.0,
            byte_start: 0,
            byte_len: u32::try_from(byte_len.bytes_u64())
                .expect("fixture byte span fits WAL format"),
            max_timestamp_ms: max_timestamp.0,
        }],
    }
}

pub(crate) fn second_object_flush_record(topic_id: uuid::Uuid) -> WalFlushRecord {
    let setup = WalFlushSetup::second_object(topic_id);
    flush_record()
        .topic_id(setup.topic_id)
        .object_key(setup.object_key)
        .first_offset(setup.first_offset)
        .last_offset(setup.last_offset)
        .max_timestamp(setup.max_timestamp)
        .byte_len(setup.byte_len)
        .call()
}

pub(crate) fn three_record_flush(topic_id: uuid::Uuid, object_key: &str) -> WalFlushRecord {
    let setup = WalFlushSetup::three_records();
    flush_record()
        .topic_id(topic_id)
        .object_key(object_key)
        .first_offset(setup.first_offset)
        .last_offset(setup.last_offset)
        .max_timestamp(setup.max_timestamp)
        .byte_len(setup.byte_len)
        .call()
}
