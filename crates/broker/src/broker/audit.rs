//! `FedRAMP` MLA audit pipeline bring-up: the signing key, the on-disk spool,
//! the writer task that drains audit events into the audit topic, and the
//! metrics poller that reports on it. It lives apart from the rest of startup
//! because it also rewraps the configured authorizer.

use std::sync::Arc;

use krabka_ids::PartitionIndex;
use krabka_units::{
    Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    audit_authorizer::AuditingAuthorizer, audit_recovery::recover_from_partition_tail,
    audit_sink::KafkaTopicAuditSink, broker::Broker, config::BrokerConfig,
    metadata_source::MetadataSource, metrics::BrokerMetrics, partition_registry::PartitionRegistry,
    time_util::system_timer,
};

fn audit_signer(config: &BrokerConfig) -> Option<Arc<krabka_audit::FileEd25519Signer>> {
    let (Some(path), Some(key_id)) = (&config.audit_signing_key_path, &config.audit_signing_key_id)
    else {
        tracing::info!("no audit signing key configured; checkpoints disabled");
        return None;
    };
    match krabka_audit::FileEd25519Signer::from_pkcs8_file(path, key_id.clone()) {
        Ok(signer) => Some(Arc::new(signer)),
        Err(error) => {
            tracing::error!(%error, "failed to load audit signing key; checkpoints disabled");
            None
        }
    }
}

fn open_audit_spool(
    config: &BrokerConfig,
) -> Result<krabka_audit::Spool, krabka_audit::AuditError> {
    let directory = if config.audit_spool_dir.is_absolute() {
        config.audit_spool_dir.clone()
    } else {
        config.log_dir.join(&config.audit_spool_dir)
    };
    krabka_audit::Spool::open_with_sync_every(
        &directory,
        config.audit_spool_max,
        config.audit_spool_sync_every_n,
    )
}

fn spawn_audit_metrics(
    stats: Arc<krabka_audit::AuditStats>,
    log: Arc<krabka_audit::AuditLog>,
    metrics: BrokerMetrics,
    poll_interval: Time,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut previous = (0, 0, 0);
        let mut tick = tokio::time::interval(poll_interval.to_std());
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                () = shutdown.cancelled() => return,
            }
            let current = (
                stats.spooled(),
                stats.replayed(),
                stats.dropped() + log.dropped(),
            );
            metrics
                .audit_records_spooled_total
                .inc_by(current.0 - previous.0);
            metrics
                .audit_records_replayed_total
                .inc_by(current.1 - previous.1);
            metrics
                .audit_records_dropped_total
                .inc_by(current.2 - previous.2);
            previous = current;
            metrics
                .audit_spool_depth
                .set(i64::try_from(stats.depth()).unwrap_or(i64::MAX));
            metrics
                .audit_spool_bytes
                .set(stats.spool_bytes().bytes_i64());
        }
    });
}

pub(super) fn start_audit_pipeline(
    config: &mut BrokerConfig,
    controller: &dyn MetadataSource,
    partitions: &Arc<PartitionRegistry>,
    metrics: &BrokerMetrics,
    supervisor_shutdown: &CancellationToken,
) -> (
    Option<PartitionIndex>,
    Arc<krabka_audit::AuditLog>,
    Option<JoinHandle<()>>,
) {
    // The decorator counts every Deny, so it goes on whether or not audit is
    // enabled: with audit off the log below is the disabled one and drops the
    // emit, leaving `authorization_denied_total` as the only denial record.
    let install_auditing_authorizer =
        |config: &mut BrokerConfig, log: &Arc<krabka_audit::AuditLog>| {
            config.authorizer = Arc::new(AuditingAuthorizer::new(
                Arc::clone(&config.authorizer),
                Arc::clone(log),
                metrics.clone(),
            ));
        };
    if !config.audit_enabled {
        let log = krabka_audit::AuditLog::disabled();
        install_auditing_authorizer(config, &log);
        return (None, log, None);
    }
    let image = controller.current_image();
    let led_partition = (0_i32..)
        .map_while(|index| {
            image
                .partition(&config.audit_topic, index)
                .map(|record| (index, record))
        })
        .find(|(_, record)| record.leader == config.node_id)
        .map(|(index, _)| PartitionIndex(index));
    let (spool, mut replay_poisoned) = if led_partition.is_some() {
        match open_audit_spool(config) {
            Ok(spool) => (Some(spool), false),
            Err(error @ krabka_audit::AuditError::Poisoned(_)) => {
                tracing::error!(%error, "audit writer disabled pending explicit spool recovery");
                (None, true)
            }
            Err(error) => {
                tracing::error!(%error, "failed to open audit spool; spooling disabled");
                (None, false)
            }
        }
    } else {
        (None, false)
    };
    let spool_resume = if replay_poisoned {
        None
    } else {
        match spool.as_ref().map(krabka_audit::Spool::resume_point) {
            Some(Ok(resume)) => resume,
            Some(Err(error)) => {
                tracing::error!(%error, "audit writer disabled pending explicit spool recovery");
                replay_poisoned = true;
                None
            }
            None => None,
        }
    };
    let (log, receiver) = spool.as_ref().map_or_else(
        || {
            krabka_audit::AuditLog::new_with_mode(
                config.audit_event_queue_capacity,
                config.audit_failure_mode,
            )
        },
        |spool| {
            krabka_audit::AuditLog::new_with_mode_and_spool(
                config.audit_event_queue_capacity,
                config.audit_failure_mode,
                spool,
            )
        },
    );
    let writer_handle = if replay_poisoned {
        drop(receiver);
        None
    } else if let Some(partition_index) = led_partition {
        let sink = Arc::new(KafkaTopicAuditSink::new(
            Arc::clone(partitions),
            config.audit_topic.clone(),
            partition_index,
            config.node_id,
            metrics.clone(),
        ));
        let resume = spool_resume.or_else(|| {
            partitions
                .get(&config.audit_topic, partition_index)
                .and_then(|partition| {
                    recover_from_partition_tail(
                        &partition,
                        config.audit_tail_window_offsets,
                        config.audit_tail_read_max,
                    )
                })
        });
        let chain = resume.map_or_else(krabka_audit::ChainState::new, |(sequence, head)| {
            krabka_audit::ChainState::resume(sequence, head)
        });
        let stats = Arc::new(krabka_audit::AuditStats::new());
        let writer = krabka_audit::AuditWriter::new(
            receiver,
            krabka_audit::AuditWriterParams {
                sink,
                product: Broker::audit_product(),
                signer: audit_signer(config),
                checkpoint_every_n: config.audit_checkpoint_every_n,
                checkpoint_every: config.audit_checkpoint_every,
                chain,
                spool,
                stats: Arc::clone(&stats),
                replay_every: config.audit_spool_replay_interval,
                timer: system_timer(),
            },
        );
        let writer_handle = tokio::spawn(writer.run());
        spawn_audit_metrics(
            stats,
            log.clone(),
            metrics.clone(),
            config.audit_stats_poll_interval,
            supervisor_shutdown.child_token(),
        );
        Some(writer_handle)
    } else {
        tracing::warn!("no audit partition led by this broker; audit records will drop");
        None
    };
    install_auditing_authorizer(config, &log);
    (led_partition, log, writer_handle)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::minutes;

    use super::*;

    #[tokio::test]
    async fn audit_metrics_cancellation_releases_log_before_next_poll() {
        let stats = Arc::new(krabka_audit::AuditStats::new());
        let (log, _receiver) = krabka_audit::AuditLog::new(1);
        let weak_log = Arc::downgrade(&log);
        let shutdown = CancellationToken::new();
        spawn_audit_metrics(
            stats,
            log.clone(),
            BrokerMetrics::new(),
            minutes(1),
            shutdown.child_token(),
        );
        drop(log);

        shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while weak_log.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("audit metrics task releases log after cancellation");

        assert!(weak_log.upgrade().is_none());
    }
}
