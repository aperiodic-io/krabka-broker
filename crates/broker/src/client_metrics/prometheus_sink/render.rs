//! Scrape-time rendering: the `Collector` implementation that turns the live
//! snapshot into `krabka_client_*` series, the shared-handle wrapper a
//! `Registry` can own, and the metric-name sanitizer.
//!
//! Rendering is separate from ingest because it is where the dynamic names
//! are grouped: `prometheus_client` emits a descriptor line per call, so the
//! series have to be bucketed by sanitized name before any of them is
//! encoded, and that grouping is this module's whole reason to exist.

use std::{collections::HashMap, time::Instant};

use prometheus_client::{collector::Collector, encoding::DescriptorEncoder};

use super::collector::{ClientMetricsCollector, StoredPoint};

impl Collector for ClientMetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let now = Instant::now();
        let guard = self.points.lock().expect("prom sink mutex poisoned");

        // Group live series by sanitized metric name so that encode_descriptor
        // is called exactly once per name. prometheus-client 0.24 emits a
        // # HELP / # TYPE line on every encode_descriptor call, so calling it
        // N times for N series sharing the same name would produce duplicate
        // descriptor lines → invalid OpenMetrics output.
        let mut by_name: HashMap<String, Vec<(&str, &str, &StoredPoint)>> = HashMap::new();
        for ((metric, instance, client, _), sp) in guard.iter() {
            if !Self::is_live(now.duration_since(sp.at), self.ttl) {
                continue;
            }
            by_name.entry(sanitize(metric)).or_default().push((
                instance.as_str(),
                client.as_str(),
                sp,
            ));
        }

        for (name, series) in &by_name {
            let Some((_, _, first)) = series.first() else {
                continue;
            };
            let mut metric_encoder = encoder.encode_descriptor(
                name,
                "client-reported metric (KIP-714)",
                None,
                first.value.metric_type(),
            )?;
            for (instance, client, point) in series {
                if !point.value.same_type(&first.value) {
                    continue;
                }
                let mut labels = vec![
                    ("client_instance_id".to_string(), (*instance).to_string()),
                    ("client_id".to_string(), (*client).to_string()),
                ];
                labels.extend(point.attributes.clone());
                let family_encoder = metric_encoder.encode_family(&labels)?;
                point.value.encode(family_encoder)?;
            }
        }
        Ok(())
    }
}

/// Newtype wrapper around `Arc<ClientMetricsCollector>` that implements
/// `prometheus_client::collector::Collector`. It lets `register_collector` add
/// the shared collector to a `Registry`.
#[derive(Debug)]
pub(crate) struct SharedClientMetricsCollector(pub std::sync::Arc<ClientMetricsCollector>);

impl prometheus_client::collector::Collector for SharedClientMetricsCollector {
    fn encode(
        &self,
        encoder: prometheus_client::encoding::DescriptorEncoder,
    ) -> Result<(), std::fmt::Error> {
        self.0.encode(encoder)
    }
}

/// Prometheus metric names allow `[a-zA-Z0-9_:]`. This function maps every
/// other character to `_`, and adds the prefix `krabka_client_`.
fn sanitize(metric: &str) -> String {
    let body: String = metric
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("krabka_client_{body}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::client_metrics::prometheus_sink::{DataPoint, PointValue};

    fn encode_collector(collector: impl Collector + 'static) -> String {
        use prometheus_client::registry::Registry;
        let mut registry = Registry::default();
        registry.register_collector(Box::new(collector));
        let mut output = String::new();
        prometheus_client::encoding::text::encode(&mut output, &registry).unwrap();
        output
    }

    #[test]
    fn ingest_then_encode_contains_series() {
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        sink.ingest(&[DataPoint {
            metric: "org.apache.kafka.consumer.fetch.size".into(),
            client_instance_id: "11111111-1111-1111-1111-111111111111".into(),
            client_id: "svc-1".into(),
            attributes: vec![("rack".into(), "a".into())],
            value: PointValue::Gauge(42.0),
            delta_start: None,
        }]);
        let buf = encode_collector(sink);
        assert2::assert!(
            buf.contains("client_instance_id=\"11111111-1111-1111-1111-111111111111\""),
            "got:\n{buf}"
        );
        assert2::assert!(
            buf.contains("rack=\"a\""),
            "attribute label missing:\n{buf}"
        );
        assert2::assert!(
            buf.contains("# TYPE krabka_client_org_apache_kafka_consumer_fetch_size gauge"),
            "gauge type missing:\n{buf}"
        );
        assert2::assert!(buf.contains("42"), "value missing:\n{buf}");
    }

    #[test]
    fn counter_and_histogram_keep_their_prometheus_types() {
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        sink.ingest(&[
            DataPoint {
                metric: "requests".into(),
                client_instance_id: "i".into(),
                client_id: "c".into(),
                attributes: vec![],
                value: PointValue::Counter(7.0),
                delta_start: None,
            },
            DataPoint {
                metric: "latency".into(),
                client_instance_id: "i".into(),
                client_id: "c".into(),
                attributes: vec![],
                value: PointValue::Histogram {
                    count: 3,
                    sum: 9.5,
                    buckets: vec![(1.0, 1), (5.0, 2), (f64::INFINITY, 3)],
                },
                delta_start: None,
            },
        ]);
        let output = encode_collector(sink);

        assert2::assert!(
            output.contains("# TYPE krabka_client_requests counter"),
            "{output}"
        );
        assert2::assert!(
            output.contains("# TYPE krabka_client_latency histogram"),
            "{output}"
        );
        assert2::assert!(output.contains("krabka_client_latency_count"), "{output}");
        assert2::assert!(output.contains("krabka_client_latency_sum"), "{output}");
        assert2::assert!(output.contains("le=\"5.0\""), "{output}");
    }

    #[test]
    fn multiple_series_same_metric_encode_once() {
        let sink = ClientMetricsCollector::new(std::time::Duration::from_mins(1));
        sink.ingest(&[
            DataPoint {
                metric: "org.apache.kafka.consumer.fetch.size".into(),
                client_instance_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
                client_id: "c1".into(),
                attributes: vec![],
                value: PointValue::Gauge(1.0),
                delta_start: None,
            },
            DataPoint {
                metric: "org.apache.kafka.consumer.fetch.size".into(),
                client_instance_id: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into(),
                client_id: "c2".into(),
                attributes: vec![],
                value: PointValue::Gauge(2.0),
                delta_start: None,
            },
        ]);
        // Encoding must succeed without a duplicate-descriptor parse error.
        let buf = encode_collector(sink);
        // ... and emit exactly ONE HELP line for the metric name.
        let help_count = buf
            .matches("# HELP krabka_client_org_apache_kafka_consumer_fetch_size")
            .count();
        assert2::assert!(
            help_count == 1,
            "expected exactly one HELP line, got {help_count}:\n{buf}"
        );
        // Both series present.
        assert2::assert!(
            buf.contains("c1") && buf.contains("c2"),
            "both series must render:\n{buf}"
        );
    }

    #[test]
    fn mixed_types_with_one_sanitized_name_do_not_cross_encode() {
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        sink.ingest(&[
            DataPoint {
                metric: "same.name".into(),
                client_instance_id: "gauge".into(),
                client_id: "c".into(),
                attributes: vec![],
                value: PointValue::Gauge(1.0),
                delta_start: None,
            },
            DataPoint {
                metric: "same-name".into(),
                client_instance_id: "counter".into(),
                client_id: "c".into(),
                attributes: vec![],
                value: PointValue::Counter(2.0),
                delta_start: None,
            },
        ]);

        let output = encode_collector(sink);
        assert2::assert!(
            output.contains("client_instance_id=\"gauge\"")
                ^ output.contains("client_instance_id=\"counter\""),
            "{output}"
        );
    }

    #[test]
    fn shared_wrapper_delegates_encoding_and_sanitize_preserves_valid_punctuation() {
        let sink = std::sync::Arc::new(ClientMetricsCollector::new(Duration::from_mins(1)));
        sink.ingest(&[DataPoint {
            metric: "valid_name:total-bad".into(),
            client_instance_id: "i".into(),
            client_id: "c".into(),
            attributes: vec![],
            value: PointValue::Gauge(3.0),
            delta_start: None,
        }]);

        let output = encode_collector(SharedClientMetricsCollector(sink));
        assert2::assert!(
            output.contains("krabka_client_valid_name:total_bad"),
            "{output}"
        );
    }
}
