//! Creation of the `__krabka_audit` internal topic.
//!
//! The audit topic is bootstrapped on its own schedule, separate from
//! `__consumer_offsets`, because it is optional and it places one partition on
//! every registered broker instead of spreading a fixed partition count.

use {
    crate::{config::BrokerConfig, error::BrokerError, metadata_source::MetadataSource},
    krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord},
    krabka_raft::{NodeId, RaftError},
    krabka_units::convert::TimeExt as _,
    std::sync::Arc,
};

#[cfg(test)]
mod tests;

/// Create `__krabka_audit` with one partition per registered broker at RF=1.
///
/// Broker-affinity: the i-th broker in ascending node-id order leads partition
/// `i`. Each broker therefore leads exactly one audit partition and writes to
/// it locally.
///
/// The function is idempotent. It treats a `TopicExists` error from the
/// controller as a success, because another node or a restart already created
/// the topic.
///
/// A node with the controller role submits the records only when it is the
/// quorum leader. A broker-only node never leads the quorum, so it submits its
/// own batch, which its metadata source forwards to the leader. Without that
/// path, a cluster with isolated controllers has no node that can place the
/// topic on a broker. The leader counts a topic that a pending batch creates
/// as an existing topic, so two racing submits create the topic once.
///
/// Only a registered broker gets a replica. Before any registration reaches
/// the image, the local node stands in for the broker list, but only when the
/// local node is a broker. A controller-only node hosts no replica. With no
/// registered broker, it submits nothing, and the first broker-only node to
/// start creates the topic.
///
/// After a submit, the function waits up to `audit_partition_wait_timeout` for
/// the topic to reach the local image. The audit pipeline reads that image
/// once, right after startup calls this function, to find the partition that
/// this broker leads.
///
/// The function returns `Ok(())` at once when `config.audit_enabled` is
/// `false`.
///
/// # Errors
///
/// Returns [`BrokerError::Startup`] when the controller refuses the batch for
/// a reason other than an existing topic.
pub async fn bootstrap_audit_topic(
    config: &BrokerConfig,
    controller: &Arc<dyn MetadataSource>,
) -> Result<(), BrokerError> {
    if !config.audit_enabled {
        return Ok(());
    }

    // Copy out the leader id before any `.await` so we don't hold the `Ref`
    // across an await point.
    let am_leader = *controller.watch_leader().borrow() == Some(config.node_id);
    if config.is_controller() && !am_leader {
        return Ok(());
    }

    let replicas = {
        let image = controller.current_image();
        // Already created (idempotent restart or another node beat us).
        if image.topic(&config.audit_topic).is_some() {
            return Ok(());
        }
        audit_replicas(&image, config.is_broker().then_some(config.node_id))
    };
    if replicas.is_empty() {
        tracing::info!(
            topic = %config.audit_topic,
            "no broker is registered, so this controller-only node does not create the audit topic"
        );
        return Ok(());
    }

    let records = audit_topic_records(&config.audit_topic, krabka_format::random_uuid(), &replicas);
    match controller.submit_change(records).await {
        // Idempotent: another node or a restart already created it.
        Ok(_) | Err(RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))) => {}
        Err(e) => return Err(BrokerError::Startup(e.to_string())),
    }
    wait_for_audit_topic(config, &**controller).await;
    Ok(())
}

/// The replica of each audit partition, in partition order: every registered
/// broker, in ascending node-id order.
///
/// `local_broker` is this node when this node is a broker, and `None` on a
/// node whose `process.roles` exclude `broker`. It stands in for the list
/// while the image holds no broker registration. Kafka never places a replica
/// on a node that is not a registered broker. A controller-only node that
/// named itself here would lead a partition that no broker serves, and every
/// `AssignReplicasToDirs` for it would fail with `BROKER_ID_NOT_REGISTERED`.
fn audit_replicas(image: &MetadataImage, local_broker: Option<NodeId>) -> Vec<NodeId> {
    let mut brokers: Vec<NodeId> = image.brokers().map(|broker| broker.node_id).collect();
    if brokers.is_empty() {
        brokers.extend(local_broker);
    }
    brokers.sort_unstable();
    brokers
}

/// The batch that creates `topic` at RF=1 with partition `i` on `replicas[i]`.
fn audit_topic_records(
    topic: &str,
    topic_id: uuid::Uuid,
    replicas: &[NodeId],
) -> Vec<MetadataRecord> {
    let topic_record = MetadataRecord::V1Topic(TopicRecord {
        name: topic.to_owned(),
        topic_id,
        partitions: i32::try_from(replicas.len()).expect("audit partition count overflows i32"),
        replication_factor: 1,
    });
    let partition_records = (0_i32..).zip(replicas).map(|(partition, &replica)| {
        MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.to_owned(),
            partition,
            leader: replica,
            replicas: vec![replica],
            isr: vec![replica],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        })
    });
    std::iter::once(topic_record)
        .chain(partition_records)
        .collect()
}

/// Wait until the local image carries the audit topic, for at most
/// `audit_partition_wait_timeout`.
///
/// A submit returns when the leader applies the batch. A broker-only node
/// sees the batch only after its next metadata fetch, so its image can trail
/// the reply. If the wait runs out, the broker still starts, and the audit
/// pipeline reports that this broker leads no audit partition.
async fn wait_for_audit_topic(config: &BrokerConfig, controller: &dyn MetadataSource) {
    let timeout = config.audit_partition_wait_timeout.to_std();
    let mut images = controller.watch_image();
    let published = tokio::time::timeout(
        timeout,
        images.wait_for(|image| image.topic(&config.audit_topic).is_some()),
    )
    .await
    .is_ok_and(|seen| seen.is_ok());
    if !published {
        tracing::warn!(
            topic = %config.audit_topic,
            ?timeout,
            "the audit topic did not reach the local metadata image in time"
        );
    }
}
