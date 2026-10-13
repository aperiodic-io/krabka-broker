//! The Prometheus `/metrics` HTTP server and the KIP-714 client-metrics
//! receiver, plus the eviction ticker that ages out stale client
//! subscriptions. Both are observability endpoints rather than broker
//! machinery, so they start together in one module.

use {
    crate::{
        client_metrics::{
            ClientMetrics, ClientMetricsManager, prometheus_sink::SharedClientMetricsCollector,
        },
        config::BrokerConfig,
        error::BrokerError,
        metrics::BrokerMetrics,
        metrics_server::run,
    },
    krabka_units::convert::TimeExt as _,
    std::sync::Arc,
    tokio_util::sync::CancellationToken,
};

pub(super) struct ObservabilityStartup {
    pub(super) metrics_bound_addr: Option<std::net::SocketAddr>,
    pub(super) client_metrics: Arc<ClientMetrics>,
}

pub(super) async fn start_observability(
    config: &BrokerConfig,
    metrics: &BrokerMetrics,
    shutdown: &CancellationToken,
) -> Result<ObservabilityStartup, BrokerError> {
    let metrics_bound_addr = if let Some(address) = config.metrics_listen_addr {
        Some(
            run(
                address,
                Arc::clone(&metrics.registry),
                config.profiling.clone(),
                shutdown.child_token(),
            )
            .await
            .map_err(|error| match error {
                krabka_telemetry::profiling::ProfilingError::Io(error) => BrokerError::Io(error),
                krabka_telemetry::profiling::ProfilingError::Config(error) => {
                    BrokerError::InvalidRuntimeConfig(error)
                }
            })?,
        )
    } else {
        None
    };
    let client_metrics = Arc::new(ClientMetrics::new(
        ClientMetricsManager::new(
            config.client_metrics_telemetry_max,
            config.client_metrics_default_interval.millis_i32(),
        ),
        config.client_metrics_otlp_endpoint.clone(),
        config.client_metrics_otlp_protocol,
        config.client_metrics_otlp_queue_capacity,
        config.client_metrics_prom_snapshot_ttl,
        metrics.client_metrics_otlp_dropped_total.clone(),
        metrics.client_metrics_otlp_failed_total.clone(),
    ));
    metrics
        .registry
        .lock()
        .await
        .register_collector(Box::new(SharedClientMetricsCollector(
            client_metrics.prometheus.clone(),
        )));
    let eviction_metrics = Arc::clone(&client_metrics);
    let eviction_shutdown = shutdown.child_token();
    let eviction_tick = config.client_metrics_eviction_tick;
    let stale_push_intervals = config.client_metrics_stale_push_intervals;
    let stale_floor = config.client_metrics_stale_floor;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(eviction_tick.to_std());
        loop {
            tokio::select! {
                () = eviction_shutdown.cancelled() => return,
                _ = tick.tick() => eviction_metrics.manager.evict_stale(
                    stale_push_intervals,
                    stale_floor.to_std(),
                ),
            }
        }
    });
    Ok(ObservabilityStartup {
        metrics_bound_addr,
        client_metrics,
    })
}
