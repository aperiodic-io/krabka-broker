//! Transaction, share, and barrier coordinator bring-up. Each coordinator is
//! constructed, given its inter-broker marker transport, and recovered from the
//! current metadata image here, so the startup sequence sees one call instead
//! of three near-identical construct-configure-recover blocks.

use std::sync::Arc;

use super::inter_broker_listener_protocol;
use crate::{
    api_catalog::UnstableApiVersions,
    auto_topic_creation::AutoTopicCreation,
    barrier::{
        config::BarrierConfig, coordinator::BarrierCoordinator,
        handlers::transport::InterBrokerMarkerWriter, metrics::BrokerBarrierMetrics,
    },
    config::BrokerConfig,
    coordinator::GroupCoordinator,
    metadata_source::MetadataSource,
    metrics::BrokerMetrics,
    network::client::InterBrokerClient,
    partition_registry::PartitionRegistry,
    producer_id_manager::ProducerIdManager,
    share_coordinator::{coordinator::ShareCoordinator, persister_client::SharePersister},
    share_partition::{dlq::DlqWriter, manager::SharePartitionLeaderManager},
    txn::coordinator::TxnCoordinator,
};

pub(super) struct CoordinatorStartup {
    pub(super) txn_coordinator: Arc<TxnCoordinator>,
    pub(super) barrier_coordinator: Arc<BarrierCoordinator>,
    pub(super) share_coordinator: Arc<ShareCoordinator>,
    pub(super) share_partition_leaders: Arc<SharePartitionLeaderManager>,
    pub(super) share_persister: Arc<SharePersister>,
    /// The share groups' dead-letter writer, which the broker hands its remote
    /// reader once it has built one.
    pub(super) dead_letters: Arc<DlqWriter>,
}

/// The shared components the coordinators are built on.
pub(super) struct CoordinatorInputs<'a> {
    pub(super) controller: &'a Arc<dyn MetadataSource>,
    pub(super) partitions: &'a Arc<PartitionRegistry>,
    pub(super) group_coordinator: &'a Arc<GroupCoordinator>,
    pub(super) producer_ids: &'a Arc<ProducerIdManager>,
    pub(super) inter_broker_client: &'a Arc<InterBrokerClient>,
    pub(super) auto_topic_creation: &'a Arc<AutoTopicCreation>,
    pub(super) metrics: &'a BrokerMetrics,
}

pub(super) async fn start_coordinators(
    config: &BrokerConfig,
    inputs: CoordinatorInputs<'_>,
) -> CoordinatorStartup {
    let CoordinatorInputs {
        controller,
        partitions,
        group_coordinator,
        producer_ids,
        inter_broker_client,
        auto_topic_creation,
        metrics,
    } = inputs;
    let listener_protocol = inter_broker_listener_protocol(config);
    let mut txn_coordinator = TxnCoordinator::new(
        config.node_id,
        Arc::clone(partitions),
        Arc::clone(producer_ids),
        config.transaction_state_num_partitions,
        config.transaction_recovery_read_max,
    );
    txn_coordinator.set_persist_last_producer_epoch(
        config.features.unstable_api_versions == UnstableApiVersions::Enabled,
    );
    txn_coordinator.configure_marker_transport(
        Arc::clone(controller),
        Arc::clone(inter_broker_client),
        listener_protocol,
        config.inter_broker_listener_name.clone(),
        config.inter_broker_server_name.clone(),
        Arc::clone(group_coordinator),
    );
    let txn_coordinator = Arc::new(txn_coordinator);
    if let Err(error) = txn_coordinator.recover(&controller.current_image()).await {
        tracing::warn!(%error, "transaction coordinator recovery error");
    }
    let mut share_coordinator_config = (*config.share_coordinator).clone();
    share_coordinator_config.load_buffer_size = config.share_coordinator_load_buffer_size;
    // Kafka trunk's newer `ShareCoordinatorShard` rules are what
    // `unstable.api.versions.enable` unlocks; 4.3.1 has none of them.
    share_coordinator_config.trunk_rules = matches!(
        config.features.unstable_api_versions,
        crate::api_catalog::UnstableApiVersions::Enabled
    );
    let share_coordinator = Arc::new(ShareCoordinator::new(
        config.node_id,
        Arc::clone(partitions),
        share_coordinator_config,
    ));
    if let Err(error) = share_coordinator.recover(&controller.current_image()).await {
        tracing::warn!(%error, "share coordinator recovery error");
    }
    let share_persister = Arc::new(SharePersister::new(
        config.node_id,
        Arc::clone(&share_coordinator),
        Arc::clone(controller),
        Arc::clone(auto_topic_creation),
        Arc::clone(inter_broker_client),
        listener_protocol,
        config.inter_broker_listener_name.clone(),
    ));
    group_coordinator.set_share_persister(Arc::clone(&share_persister));
    group_coordinator.set_metadata_source(Arc::clone(controller));
    let dead_letters = Arc::new(DlqWriter::new(
        config,
        Arc::clone(controller),
        Arc::clone(partitions),
        Arc::clone(auto_topic_creation),
        Arc::clone(inter_broker_client),
        listener_protocol,
        metrics.clone(),
    ));
    let share_partition_leaders = SharePartitionLeaderManager::new(
        config.node_id,
        Arc::clone(partitions),
        Arc::clone(controller),
        Arc::clone(&share_persister),
        Arc::new((*config.share_group).clone()),
        config.share_session_cache_max_when_unlimited,
        dead_letters.clone(),
    );
    share_partition_leaders.spawn_lock_sweeper();
    share_partition_leaders.spawn_share_version_watcher();
    let mut barrier_coordinator = BarrierCoordinator::new(
        config.node_id,
        Arc::clone(partitions),
        Arc::clone(controller),
        Arc::clone(auto_topic_creation),
        BarrierConfig {
            state_topic_num_partitions: config.barrier_state_num_partitions,
            recovery_read_max: config.barrier_recovery_read_max,
            injection_timeout: config.barrier_injection_timeout,
            default_retained_cuts: config.barrier_retained_cuts,
            max_retained_cuts: config.barrier_retained_cuts,
            max_groups: config.barrier_max_groups,
            max_topics_per_group: config.barrier_max_topics_per_group,
            min_injection_interval: config.barrier_min_injection_interval,
            ..BarrierConfig::default()
        },
        Arc::new(BrokerBarrierMetrics::new(metrics.clone())),
    );
    // The transport has to be bound before the coordinator goes into its `Arc`.
    // Without it the coordinator marks only the partitions it leads, and every
    // remote partition lands in the `missing` list of the cut.
    barrier_coordinator.configure_marker_transport(Arc::new(InterBrokerMarkerWriter::new(
        config.node_id,
        Arc::clone(controller),
        Arc::clone(inter_broker_client),
        listener_protocol,
        config.inter_broker_listener_name.clone(),
        config.inter_broker_server_name.clone(),
    )));
    let barrier_coordinator = Arc::new(barrier_coordinator);
    // __barrier_state is created when the first group is defined, not here,
    // as the other coordinator topics are created on first use
    // (`AutoTopicCreation`).
    //
    // Recovery below needs no bootstrap: it replays the partitions this broker
    // leads, and a topic that does not exist has none.
    if let Err(error) = barrier_coordinator
        .recover(&controller.current_image())
        .await
    {
        tracing::warn!(%error, "barrier coordinator recovery error");
    }
    CoordinatorStartup {
        txn_coordinator,
        barrier_coordinator,
        share_coordinator,
        share_partition_leaders,
        share_persister,
        dead_letters,
    }
}
