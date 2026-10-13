//! KIP-714 client metrics receiver: the subscription config, the
//! client-instance registry, the OTLP decode, and the Prometheus and OTLP
//! sinks.

pub(crate) mod config;
pub(crate) mod manager;
pub(crate) mod otlp_sink;
pub(crate) mod prometheus_sink;
pub(crate) use manager::ClientMetricsManager;
use {
    self::{otlp_sink::OtlpForwarder, prometheus_sink::ClientMetricsCollector},
    krabka_units::{Time, convert::TimeExt as _},
    std::sync::Arc,
};

/// Broker-held bundle. It holds the manager, which owns the instance state and
/// the matching, plus the two sinks. The metrics registry shares the Prometheus
/// collector.
pub(crate) struct ClientMetrics {
    pub manager: ClientMetricsManager,
    pub prometheus: Arc<ClientMetricsCollector>,
    pub otlp: OtlpForwarder,
}

impl ClientMetrics {
    /// `otlp_endpoint` is `None` when OTLP forwarding is disabled.
    pub(crate) fn new(
        manager: ClientMetricsManager,
        otlp_endpoint: Option<String>,
        otlp_protocol: krabka_telemetry::OtlpProtocol,
        otlp_queue_capacity: usize,
        prometheus_snapshot_ttl: Time,
        dropped: prometheus_client::metrics::counter::Counter,
        failed: prometheus_client::metrics::counter::Counter,
    ) -> Self {
        let otlp = match otlp_endpoint {
            Some(ep) => {
                OtlpForwarder::spawn(ep, otlp_protocol, otlp_queue_capacity, dropped, failed)
            }
            None => OtlpForwarder::disabled(),
        };
        Self {
            manager,
            prometheus: Arc::new(ClientMetricsCollector::new(
                prometheus_snapshot_ttl.to_std(),
            )),
            otlp,
        }
    }

    pub(crate) async fn shutdown(&self) { self.otlp.shutdown().await; }
}
