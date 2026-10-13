//! Flattening an OTLP `MetricsData` payload into the broker's Prometheus data
//! points.
//!
//! KIP-714 clients push metrics in OTLP, and the broker's Prometheus sink wants
//! one flat point per series. This module holds that translation: which OTLP
//! shapes become which `PointValue`, how resource, scope and point attributes
//! merge into labels, and the label sanitization Prometheus requires. It is
//! separate from the request handling because it is a pure function over a
//! decoded payload.

use crate::client_metrics::prometheus_sink::{DataPoint, PointValue};

/// Flattens an OTLP `MetricsData` into Prometheus data points. Sum and Gauge
/// become numbers, and a Histogram becomes count and sum gauges. The function
/// is best-effort and skips unknown shapes.
pub(super) fn flatten_for_prometheus(
    md: &opentelemetry_proto::tonic::metrics::v1::MetricsData,
    instance: &str,
    client_id: &str,
) -> Vec<DataPoint> {
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value as AnyValueKind},
        metrics::v1::{AggregationTemporality, metric::Data, number_data_point::Value},
    };
    let mut out = Vec::new();
    let num = |v: &Value| -> f64 {
        match v {
            Value::AsDouble(d) => *d,
            Value::AsInt(i) => i
                .to_string()
                .parse()
                .expect("every i64 has a finite f64 representation"),
        }
    };
    let attribute_value = |value: &AnyValue| -> Option<String> {
        match value.value.as_ref()? {
            AnyValueKind::StringValue(value) => Some(value.clone()),
            AnyValueKind::BoolValue(value) => Some(value.to_string()),
            AnyValueKind::IntValue(value) => Some(value.to_string()),
            AnyValueKind::DoubleValue(value) => Some(value.to_string()),
            AnyValueKind::BytesValue(value) => Some(hex::encode(value)),
            AnyValueKind::ArrayValue(_)
            | AnyValueKind::KvlistValue(_)
            | AnyValueKind::StringValueStrindex(_) => None,
        }
    };
    let attributes = |sets: &[&[KeyValue]]| {
        let mut labels = sets
            .iter()
            .flat_map(|set| set.iter())
            .filter_map(|attribute| {
                let name = sanitize_prometheus_label(&attribute.key);
                if matches!(name.as_str(), "client_id" | "client_instance_id") {
                    return None;
                }
                Some((name, attribute_value(attribute.value.as_ref()?)?))
            })
            .collect::<Vec<_>>();
        labels.sort();
        labels.dedup_by(|left, right| left.0 == right.0);
        labels
    };
    for rm in &md.resource_metrics {
        for sm in &rm.scope_metrics {
            for m in &sm.metrics {
                let point = |point_attributes: &[KeyValue], value, delta_start| DataPoint {
                    metric: m.name.clone(),
                    client_instance_id: instance.to_string(),
                    client_id: client_id.to_string(),
                    attributes: attributes(&[
                        rm.resource
                            .as_ref()
                            .map_or(&[], |r| r.attributes.as_slice()),
                        sm.scope.as_ref().map_or(&[], |s| s.attributes.as_slice()),
                        point_attributes,
                    ]),
                    value,
                    delta_start,
                };
                let mut numbers =
                    |points: &[opentelemetry_proto::tonic::metrics::v1::NumberDataPoint],
                     monotonic: bool,
                     delta: bool| {
                        out.extend(points.iter().filter_map(|dp| {
                            let value = num(dp.value.as_ref()?);
                            Some(point(
                                &dp.attributes,
                                if monotonic {
                                    PointValue::Counter(value)
                                } else {
                                    PointValue::Gauge(value)
                                },
                                delta.then_some(dp.start_time_unix_nano),
                            ))
                        }));
                    };
                match &m.data {
                    Some(Data::Gauge(g)) => numbers(&g.data_points, false, false),
                    Some(Data::Sum(s)) => numbers(
                        &s.data_points,
                        s.is_monotonic,
                        s.aggregation_temporality == AggregationTemporality::Delta as i32,
                    ),
                    Some(Data::Histogram(h)) => {
                        for dp in &h.data_points {
                            let Some(sum) = dp.sum else {
                                continue;
                            };
                            let mut buckets = dp
                                .explicit_bounds
                                .iter()
                                .copied()
                                .zip(dp.bucket_counts.iter().copied())
                                .collect::<Vec<_>>();
                            if let Some(infinite) = dp.bucket_counts.get(dp.explicit_bounds.len()) {
                                buckets.push((f64::MAX, *infinite));
                            }
                            out.push(point(
                                &dp.attributes,
                                PointValue::Histogram {
                                    count: dp.count,
                                    sum,
                                    buckets,
                                },
                                (h.aggregation_temporality == AggregationTemporality::Delta as i32)
                                    .then_some(dp.start_time_unix_nano),
                            ));
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    out
}

fn sanitize_prometheus_label(label: &str) -> String {
    let mut sanitized = label
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.starts_with(|character: char| character.is_ascii_digit()) {
        sanitized.insert(0, '_');
    }
    sanitized
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::push_telemetry::test_support::{gauge_metric, metrics_data, number_point},
        assert2::{assert, check},
        opentelemetry_proto::tonic::{
            common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
            metrics::v1::{
                AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
                NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
            },
            resource::v1::Resource,
        },
    };

    #[test]
    fn flatten_for_prometheus_preserves_gauge_sum_and_histogram_points() {
        let md = metrics_data(vec![
            gauge_metric("cpu.utilization", number_data_point::Value::AsDouble(0.75)),
            Metric {
                name: "requests.total".into(),
                data: Some(metric::Data::Sum(Sum {
                    data_points: vec![NumberDataPoint {
                        start_time_unix_nano: 7,
                        ..number_point(number_data_point::Value::AsInt(42))
                    }],
                    is_monotonic: true,
                    aggregation_temporality: AggregationTemporality::Delta as i32,
                })),
                ..Default::default()
            },
            Metric {
                name: "latency.ms".into(),
                data: Some(metric::Data::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint {
                        count: 3,
                        sum: Some(9.5),
                        start_time_unix_nano: 7,
                        ..Default::default()
                    }],
                    aggregation_temporality: AggregationTemporality::Delta as i32,
                })),
                ..Default::default()
            },
            Metric {
                name: "missing.sum".into(),
                data: Some(metric::Data::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint {
                        count: 1,
                        sum: None,
                        ..Default::default()
                    }],
                    ..Default::default()
                })),
                ..Default::default()
            },
        ]);

        let points = flatten_for_prometheus(&md, "instance-1", "client-a");

        assert!(points.len() == 3, "{points:?}");
        check!(
            points[0].client_instance_id.as_str() == "instance-1",
            "{points:?}"
        );
        check!(points[0].client_id.as_str() == "client-a", "{points:?}");
        assert!(points[0].metric == "cpu.utilization", "{points:?}");
        assert!(
            matches!(points[0].value, PointValue::Gauge(value) if (value - 0.75).abs() < f64::EPSILON)
        );
        assert!(points[1].metric == "requests.total", "{points:?}");
        assert!(
            matches!(points[1].value, PointValue::Counter(value) if (value - 42.0).abs() < f64::EPSILON)
        );
        assert!(points[1].delta_start == Some(7));
        assert!(points[2].metric == "latency.ms", "{points:?}");
        assert!(
            matches!(points[2].value, PointValue::Histogram { count: 3, sum, .. } if (sum - 9.5).abs() < f64::EPSILON)
        );
        assert!(points[2].delta_start == Some(7));
    }

    fn string_attribute(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.into())),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn flatten_for_prometheus_sanitizes_and_deduplicates_attribute_labels() {
        let mut point = number_point(number_data_point::Value::AsInt(1));
        point.attributes = vec![
            string_attribute("dup.key", "point"),
            string_attribute("client-id", "spoofed"),
        ];
        let md = MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![
                        string_attribute("9bad-key", "resource"),
                        string_attribute("dup.key", "resource"),
                    ],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        attributes: vec![string_attribute("dup.key", "scope")],
                        ..Default::default()
                    }),
                    metrics: vec![Metric {
                        name: "requests".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![point],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let points = flatten_for_prometheus(&md, "instance", "client");

        assert!(
            points[0].attributes
                == vec![
                    ("_9bad_key".into(), "resource".into()),
                    ("dup_key".into(), "point".into()),
                ]
        );
    }
}
