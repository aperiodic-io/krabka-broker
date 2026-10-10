//! Topic-backed remote-log-metadata bring-up. The module holds the kickoff
//! configuration derived from `BrokerConfig`, the retrying bootstrap that swaps
//! the fail-closed placeholder for a live manager, and the watcher and
//! reconciler that keep the manager's metadata-partition assignment current.

use std::sync::Arc;

use krabka_units::convert::TimeExt as _;
use tokio_util::sync::CancellationToken;

use crate::{config::BrokerConfig, host_port::parse_advertised_host_port};

#[derive(Debug, Clone)]
pub(super) struct KafkaSwapKickoff {
    pub(super) cfg: crate::config::KafkaRlmmConfig,
    pub(super) broker_id: i32,
    pub(super) bootstrap_backoff_initial: std::time::Duration,
    pub(super) bootstrap_backoff_max: std::time::Duration,
    pub(super) reconcile_tick: std::time::Duration,
}

#[cfg(test)]
pub(super) fn test_kickoff(cfg: crate::config::KafkaRlmmConfig) -> KafkaSwapKickoff {
    KafkaSwapKickoff {
        cfg,
        broker_id: 1,
        bootstrap_backoff_initial: std::time::Duration::from_millis(10),
        bootstrap_backoff_max: std::time::Duration::from_secs(1),
        reconcile_tick: std::time::Duration::from_secs(1),
    }
}

pub(super) fn kafka_swap_kickoff(config: &BrokerConfig) -> Option<KafkaSwapKickoff> {
    config.remote_storage_backend.as_ref()?;
    // The diskless WAL index is always topic-backed, even when tests opt the
    // KIP-405 RLMM itself into its in-memory implementation.
    let default_metadata_config;
    let metadata_config = match &config.remote_log_metadata {
        crate::config::RlmmKind::TopicBacked(metadata_config) => metadata_config,
        crate::config::RlmmKind::InMemory => {
            default_metadata_config = crate::config::KafkaRlmmConfig::default();
            &default_metadata_config
        }
    };
    let listeners = config.effective_listeners();
    let inter_broker = listeners
        .iter()
        .find(|listener| listener.name == config.inter_broker_listener_name);
    let protocol = inter_broker.map_or(krabka_security::ListenerProtocol::Plaintext, |listener| {
        listener.protocol
    });
    let advertised_host = inter_broker.map_or_else(
        || "localhost".to_owned(),
        |listener| parse_advertised_host_port(&listener.advertised).0,
    );
    let security = (protocol.requires_tls() || protocol.requires_sasl()).then(|| {
        let tls = protocol.requires_tls().then(|| {
            config.tls_config.as_ref().map(|tls| {
                let mut client_tls = krabka_client_core::security::TlsConnectorConfig::default();
                if let Some(path) = &tls.trust_roots_path {
                    client_tls.trust_store =
                        krabka_client_core::security::TrustStore::PemFile(path.clone());
                }
                client_tls.server_name.clone_from(&advertised_host);
                client_tls
            })
        });
        Box::new(krabka_client_core::security::ClientSecurity {
            protocol,
            tls: tls.flatten(),
            sasl: config
                .inter_broker_credentials
                .as_ref()
                .map(crate::network::client::to_client_creds),
            sasl_host: protocol.requires_sasl().then(|| advertised_host.clone()),
        })
    });
    let bootstrap = if !metadata_config.bootstrap.is_empty() {
        metadata_config.bootstrap.clone()
    } else if security.is_some() {
        inter_broker.map_or_else(
            || loopback_bootstrap(config.listen_addr),
            |listener| listener.advertised.clone(),
        )
    } else {
        loopback_bootstrap(config.listen_addr)
    };
    Some(KafkaSwapKickoff {
        cfg: crate::config::KafkaRlmmConfig {
            dispatch_queue_capacity: config.client_dispatch_queue_capacity,
            frame_max: config.client_frame_max,
            bootstrap,
            num_partitions: metadata_config.num_partitions,
            replication: metadata_config.replication,
            min_isr: metadata_config.min_isr,
            snapshot_interval: metadata_config.snapshot_interval,
            topic_create_timeout: metadata_config.topic_create_timeout,
            fetch_max_wait: metadata_config.fetch_max_wait,
            fetch_max_bytes: metadata_config.fetch_max_bytes,
            fetch_retry_backoff: metadata_config.fetch_retry_backoff,
            event_queue_capacity: metadata_config.event_queue_capacity,
            snapshot_dir: if metadata_config.snapshot_dir.as_os_str().is_empty() {
                config.log_dir.join("remote-log-metadata")
            } else {
                metadata_config.snapshot_dir.clone()
            },
            security,
        },
        broker_id: config.broker_id,
        bootstrap_backoff_initial: config.rlmm_bootstrap_backoff_initial.to_std(),
        bootstrap_backoff_max: config.rlmm_bootstrap_backoff_max.to_std(),
        reconcile_tick: config.rlmm_reconcile_tick.to_std(),
    })
}

/// The sorted, deduped set of `__remote_log_metadata` partitions this broker
/// (`node_id`) must consume. There is one entry for each metadata partition
/// that covers a user-topic-partition this node leads or follows, for the
/// metadata topic's `partition_count`.
fn needed_metadata_partitions(
    image: &krabka_metadata::MetadataImage,
    node_id: krabka_metadata::NodeId,
    partition_count: i32,
) -> Vec<i32> {
    let mut tps: Vec<krabka_remote_storage::TopicIdPartition> = Vec::new();
    for topic in image.topics() {
        for p in image.partitions_of(&topic.name) {
            if p.leader == node_id || p.replicas.contains(&node_id) {
                tps.push(krabka_remote_storage::TopicIdPartition::new(
                    topic.topic_id,
                    topic.name.clone(),
                    p.partition,
                ));
            }
        }
    }
    krabka_remote_storage_topic::metadata_partitions_for(tps.iter(), partition_count)
}

/// Next backoff after a failed RLMM bootstrap attempt: double, capped.
fn next_rlmm_backoff(cur: std::time::Duration, max: std::time::Duration) -> std::time::Duration {
    (cur * 2).min(max)
}

/// A connectable loopback `host:port` for the broker's own data listener,
/// used as the default RLMM metadata-client bootstrap when none is configured.
/// A wildcard bind (`0.0.0.0` / `::`) is mapped to loopback so the in-process
/// metadata client has a routable target.
fn loopback_bootstrap(listen: std::net::SocketAddr) -> String {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
    let ip = match listen.ip() {
        IpAddr::V4(v4) if v4 == Ipv4Addr::UNSPECIFIED => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(v6) if v6 == Ipv6Addr::UNSPECIFIED => IpAddr::V6(Ipv6Addr::LOCALHOST),
        other => other,
    };
    std::net::SocketAddr::new(ip, listen.port()).to_string()
}

/// Back off after a failed RLMM bootstrap attempt. Sleeps for the current
/// backoff (advancing it toward the cap), or returns `false` if shutdown
/// fired during the sleep so the caller can abort the bootstrap.
pub(super) async fn rlmm_bootstrap_backoff(
    backoff: &mut std::time::Duration,
    max_backoff: std::time::Duration,
    shutdown: &CancellationToken,
) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => {
            tracing::debug!("topic-backed RLMM bootstrap cancelled during backoff");
            false
        }
        () = tokio::time::sleep(*backoff) => {
            *backoff = next_rlmm_backoff(*backoff, max_backoff);
            true
        }
    }
}

/// Construct the topic-backed
/// [`krabka_remote_storage::RemoteLogMetadataManager`] against the
/// broker's loopback listener and swap it into `swap`. Retries with
/// bounded backoff until it succeeds or shutdown fires. The broker stays on
/// the fail-closed [`krabka_remote_storage_topic::NotReadyRlmm`] placeholder
/// until then.
pub(super) fn metadata_log_config(
    config: &crate::config::KafkaRlmmConfig,
    topic: String,
    client_id: String,
) -> krabka_remote_storage_topic::KafkaMetadataLogConfig {
    krabka_remote_storage_topic::KafkaMetadataLogConfig {
        dispatch_queue_capacity: config.dispatch_queue_capacity,
        frame_max: config.frame_max,
        bootstrap: config.bootstrap.clone(),
        topic,
        num_partitions: config.num_partitions,
        replication: config.replication,
        min_isr: Some(config.min_isr),
        client_id,
        compacted: false,
        provision_topic: true,
        security: config.security.as_deref().cloned(),
        topic_create_timeout: config.topic_create_timeout,
        fetch_max_wait: config.fetch_max_wait,
        fetch_max_bytes: config.fetch_max_bytes,
        fetch_retry_backoff: config.fetch_retry_backoff,
        event_queue_capacity: config.event_queue_capacity,
    }
}

pub(super) async fn bootstrap_topic_rlmm(
    swap: Arc<krabka_remote_storage_topic::SwappableRlmm>,
    cfg: KafkaSwapKickoff,
    runtime: tokio::runtime::Handle,
    metrics: crate::metrics::BrokerMetrics,
    node_id: krabka_metadata::NodeId,
    mut image_rx: tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>>,
    shutdown: CancellationToken,
) {
    let log_cfg = metadata_log_config(
        &cfg.cfg,
        krabka_remote_storage_topic::METADATA_TOPIC.to_owned(),
        format!("krabka-rlmm-broker-{}", cfg.broker_id),
    );

    // Retry the topic-backed bootstrap with bounded backoff until it succeeds
    // or the broker shuts down. Until then the SwappableRlmm stays on the
    // fail-closed NotReadyRlmm placeholder.
    let mut backoff = cfg.bootstrap_backoff_initial;
    let manager = loop {
        metrics.tiered_storage_rlmm_bootstrap_attempts.inc();
        // Race the attempt against shutdown: `KafkaMetadataEventLog::start`
        // dials the broker's listener, and a pending TCP connect can take
        // seconds to fail on some platforms (Windows retransmits SYNs to a
        // closed loopback port instead of failing fast), so the token must
        // be honoured mid-attempt, not just between attempts. `biased`
        // makes an already-cancelled token win before the dial even starts.
        //
        // KafkaMetadataEventLog::start and TopicBasedRemoteLogMetadataManager::start
        // return different error types, so we handle them with separate match arms.
        let started = tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                tracing::debug!("topic-backed RLMM bootstrap cancelled during attempt");
                return;
            }
            res = krabka_remote_storage_topic::KafkaMetadataEventLog::start(log_cfg.clone()) => res,
        };
        let log = match started {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, backoff_ms = backoff.as_millis(),
                    "topic-backed RLMM log start failed; retrying");
                if !rlmm_bootstrap_backoff(&mut backoff, cfg.bootstrap_backoff_max, &shutdown).await
                {
                    return;
                }
                continue;
            }
        };
        let manager = match krabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager::start(
            log.clone(),
            runtime.clone(),
            cfg.cfg.snapshot_dir.clone(),
            cfg.cfg.snapshot_interval.to_std(),
        ) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, backoff_ms = backoff.as_millis(),
                    "topic-backed RLMM manager start failed; retrying");
                if !rlmm_bootstrap_backoff(&mut backoff, cfg.bootstrap_backoff_max, &shutdown).await
                {
                    return;
                }
                continue;
            }
        };
        // `log` is an `Arc`; `manager` holds its own clone. Drop the local
        // binding here — we don't need a separate handle to the log.
        drop(log);
        break manager;
    };
    // Keep the concrete handle so the reconciler can call
    // `reconcile_assignment`; the swap facade only needs the trait object.
    swap.swap(manager.clone());
    metrics.tiered_storage_rlmm_topic_backed.set(1);
    tracing::info!("topic-backed RemoteLogMetadataManager activated");

    // Publish the leadership-derived needed-set on a watch; re-emit whenever
    // the metadata image changes. The initial value is the current image's
    // set, so the bootstrap assignment is leadership-derived (not all
    // partitions).
    let partition_count = cfg.cfg.num_partitions;
    let initial =
        needed_metadata_partitions(&image_rx.borrow_and_update(), node_id, partition_count);
    let (set_tx, set_rx) = tokio::sync::watch::channel(initial);

    // Keep both loops owned by this bootstrap task so broker shutdown can join
    // them before the Tokio runtime drops.
    let image_watcher =
        watch_rlmm_needed_partitions(image_rx, set_tx, node_id, partition_count, shutdown.clone());
    let reconciler = run_rlmm_reconciler(manager, set_rx, cfg.reconcile_tick, shutdown);
    tokio::join!(image_watcher, reconciler);
}

async fn watch_rlmm_needed_partitions(
    mut image_rx: tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>>,
    set_tx: tokio::sync::watch::Sender<Vec<i32>>,
    node_id: krabka_metadata::NodeId,
    partition_count: i32,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            changed = image_rx.changed() => {
                if changed.is_err() {
                    return;
                }
                let set = needed_metadata_partitions(
                    &image_rx.borrow_and_update(),
                    node_id,
                    partition_count,
                );
                set_tx.send_if_modified(|current| {
                    if *current == set {
                        false
                    } else {
                        *current = set;
                        true
                    }
                });
            }
        }
    }
}

/// Run the metadata-partition reconciler on the initial value, every change,
/// and the configured reconciliation cadence.
///
/// The periodic tick is what makes a partition parked at the
/// `HWM_UNKNOWN` sentinel (after a transient assignment-time
/// `high_water_marks` failure) eventually re-attempt its HWM and leave the
/// `NotReady` state, even when the metadata image stays static.
/// `reconcile_assignment` is idempotent for partitions already
/// assigned-and-ready, so the periodic re-apply is cheap.
async fn run_rlmm_reconciler(
    manager: Arc<krabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager>,
    mut set_rx: tokio::sync::watch::Receiver<Vec<i32>>,
    reconcile_tick: std::time::Duration,
    shutdown: CancellationToken,
) {
    let set = set_rx.borrow_and_update().clone();
    manager.reconcile_assignment(&set).await;
    let mut tick = tokio::time::interval(reconcile_tick);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            changed = set_rx.changed() => {
                if changed.is_err() {
                    return;
                }
                let set = set_rx.borrow_and_update().clone();
                manager.reconcile_assignment(&set).await;
            }
            _ = tick.tick() => {
                let set = set_rx.borrow().clone();
                manager.reconcile_assignment(&set).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
