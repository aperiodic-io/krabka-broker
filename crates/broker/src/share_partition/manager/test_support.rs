//! Fixtures shared by the unit tests of the share-partition leader manager.
//!
//! The concern modules each carry their own `#[cfg(test)] mod tests`, and they
//! build their manager from here, so every test runs against the same mock
//! metadata source and the same lock duration.

use std::{path::Path, sync::Arc, time::Duration};

use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::{MetadataImage, NodeId};
use krabka_protocol::records::RecordBatch;
use krabka_security::ListenerProtocol;

use super::SharePartitionLeaderManager;
use crate::{
    coordinator::unified::share::config::ShareGroupConfig,
    metadata_source::MetadataSource,
    network::client::InterBrokerClient,
    partition_registry::PartitionRegistry,
    share_coordinator::{
        config::ShareCoordinatorConfig, coordinator::ShareCoordinator,
        persister_client::SharePersister,
    },
    share_partition::dlq::{DlqSink, test_support::RecordingDlq},
    test_support::FakeMetadataSource,
};

pub(crate) const LOCK: Duration = Duration::from_secs(30);

/// A metadata source over `image`, with this node reported as the
/// controller leader.
///
/// An image that holds no brokers is deliberate in the default case: the
/// bootstrap of the share-state topic cannot run against it, so `read_state`
/// on the persister stops early with an error, before any routing. That
/// exercises the best-effort empty-window fallback of `get_or_load` without an
/// inter-broker server.
fn fake_source(image: Arc<MetadataImage>) -> Arc<dyn MetadataSource> {
    Arc::new(
        FakeMetadataSource::builder()
            .image(image)
            .leader(Some(NodeId(1)))
            .build(),
    )
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, derive_more::From, derive_more::Into,
)]
pub(super) struct SessionCapacity(pub usize);

#[derive(krabka_macros::FieldDefaults)]
struct ManagerSetup {
    #[default(SessionCapacity(crate::config::BrokerConfig::default().share_session_cache_max_when_unlimited))]
    capacity: SessionCapacity,
    #[default(Arc::new(RecordingDlq::default()))]
    dlq: Arc<dyn DlqSink>,
}

pub(super) fn manager() -> Arc<SharePartitionLeaderManager> {
    manager_over(
        fake_source(Arc::new(MetadataImage::new(uuid::Uuid::nil()))),
        Arc::new(PartitionRegistry::new()),
    )
}

/// A manager whose controller serves `image`.
///
/// `current_leader_of` and the related methods thus resolve real topic and
/// partition leadership.
pub(super) fn manager_with_image(image: Arc<MetadataImage>) -> Arc<SharePartitionLeaderManager> {
    manager_with_image_and_partitions(image, Arc::new(PartitionRegistry::new()))
}

/// A manager whose controller serves `image` and whose local partitions are
/// `reg`.
///
/// The share-partition start a fresh cell resolves reads both: the image
/// carries the topic and the group config, and the registry carries the log
/// the strategy resolves against.
pub(super) fn manager_with_image_and_partitions(
    image: Arc<MetadataImage>,
    reg: Arc<PartitionRegistry>,
) -> Arc<SharePartitionLeaderManager> {
    manager_over(fake_source(image), reg)
}

/// A manager over `source` and `reg`, for a test that publishes images to the
/// source after the manager exists.
pub(super) fn manager_over(
    controller: Arc<dyn MetadataSource>,
    reg: Arc<PartitionRegistry>,
) -> Arc<SharePartitionLeaderManager> {
    build(controller, reg, ManagerSetup::default())
}

pub(super) fn manager_with_unlimited_fallback(
    fallback: SessionCapacity,
) -> Arc<SharePartitionLeaderManager> {
    build(
        fake_source(Arc::new(MetadataImage::new(uuid::Uuid::nil()))),
        Arc::new(PartitionRegistry::new()),
        ManagerSetup {
            capacity: fallback,
            ..Default::default()
        },
    )
}

/// A manager whose dead-letter records go to `dlq`.
pub(super) fn manager_with_dlq(dlq: Arc<dyn DlqSink>) -> Arc<SharePartitionLeaderManager> {
    build(
        fake_source(Arc::new(MetadataImage::new(uuid::Uuid::nil()))),
        Arc::new(PartitionRegistry::new()),
        ManagerSetup {
            dlq,
            ..Default::default()
        },
    )
}

fn build(
    controller: Arc<dyn MetadataSource>,
    reg: Arc<PartitionRegistry>,
    setup: ManagerSetup,
) -> Arc<SharePartitionLeaderManager> {
    let ManagerSetup { capacity, dlq } = setup;
    let coord = Arc::new(ShareCoordinator::new(
        krabka_audit::NodeId(1),
        reg.clone(),
        ShareCoordinatorConfig::default(),
    ));
    let client = Arc::new(InterBrokerClient::new(None, None));
    let persister = Arc::new(SharePersister::new(
        krabka_audit::NodeId(1),
        coord,
        controller.clone(),
        Arc::default(),
        client,
        ListenerProtocol::Plaintext,
        "INTERNAL".to_string(),
    ));
    SharePartitionLeaderManager::new(
        krabka_audit::NodeId(1),
        reg,
        controller,
        persister,
        Arc::new(ShareGroupConfig::default()),
        capacity.0,
        dlq,
    )
}

/// One timestamped batch of data records.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct TimedValues<'a> {
    pub timestamp: crate::test_support::UnixMillis,
    #[default(&[b"v"])]
    pub values: &'a [&'static [u8]],
}

#[derive(krabka_macros::FieldDefaults)]
pub(crate) struct DataPartitionSetup<'a> {
    #[default("t")]
    pub topic: &'a str,
    pub partition: PartitionIndex,
    pub batches: Vec<TimedValues<'a>>,
    pub high_watermark: Offset,
}

/// Append the selected batches, publish the high watermark, and register a real partition.
pub(crate) async fn open_data_partition(
    reg: &PartitionRegistry,
    log_dir: &Path,
    setup: DataPartitionSetup<'_>,
) {
    let DataPartitionSetup {
        topic,
        partition,
        batches,
        high_watermark,
    } = setup;
    let part = crate::test_support::open_partition(
        log_dir,
        crate::test_support::StandalonePartitionSetup {
            topic,
            partition,
            ..Default::default()
        },
    );
    for TimedValues { timestamp, values } in batches {
        let mut batch = RecordBatch {
            partition_leader_epoch: 0,
            last_offset_delta: i32::try_from(values.len() - 1).expect("record count fits"),
            ..crate::test_support::static_records_batch(values, timestamp)
        };
        part.log
            .lock()
            .expect("partition log lock")
            .append(&mut batch)
            .expect("append records");
    }
    part.replica_state.lock().await.hw = high_watermark;
    reg.insert(topic.into(), partition, part);
}
