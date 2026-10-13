//! Metric exposition fixtures that retain the registry guard at the call site.

pub(crate) fn encode_metrics(metrics: &super::BrokerMetrics) -> String {
    let mut body = String::new();
    prometheus_client::encoding::text::encode(
        &mut body,
        &metrics.registry.try_lock().expect("registry"),
    )
    .expect("encode");
    body
}
