//! What `#[derive(RegisterMetrics)]` constructs and registers, read back from
//! the text exposition of the registry it registers into. `prometheus-client`
//! ends every help text with a period of its own.

use {
    assert2::assert,
    krabka_macros::RegisterMetrics,
    prometheus_client::{
        encoding::{EncodeLabelSet, text::encode},
        metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
        registry::Registry,
    },
};

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct Labels {
    api: String,
}

#[derive(RegisterMetrics)]
struct Metrics {
    #[metric(help = "Records received")]
    records_total: Counter,
    #[metric(
        name = "lag_records",
        help = "Records behind \
                the leader"
    )]
    lag: Gauge,
    #[metric(help = "Seconds a request took", buckets = [0.5, 1.0])]
    latency_seconds: Histogram,
    #[metric(help = "Seconds a request took, per api", buckets = [2.0])]
    api_latency_seconds: Family<Labels, Histogram>,
    #[metric(skip, new = Gauge::default())]
    unregistered_gauge: Gauge,
    #[metric(skip)]
    scratch: Vec<u8>,
}

fn exposition(metrics: &Metrics) -> String {
    let mut registry = Registry::default();
    metrics.register(&mut registry);
    let mut text = String::new();
    encode(&mut text, &registry).unwrap();
    text
}

#[test]
fn registers_every_field_in_order_under_its_name_help_and_buckets() {
    let metrics = Metrics::unregistered();
    metrics.records_total.inc();
    metrics.lag.set(3);
    metrics.latency_seconds.observe(0.75);
    metrics
        .api_latency_seconds
        .get_or_create(&Labels {
            api: "Fetch".to_owned(),
        })
        .observe(1.0);
    metrics.unregistered_gauge.set(9);

    assert!(
        exposition(&metrics)
            == "# HELP records Records received.\n\
                # TYPE records counter\n\
                records_total 1\n\
                # HELP lag_records Records behind the leader.\n\
                # TYPE lag_records gauge\n\
                lag_records 3\n\
                # HELP latency_seconds Seconds a request took.\n\
                # TYPE latency_seconds histogram\n\
                latency_seconds_sum 0.75\n\
                latency_seconds_count 1\n\
                latency_seconds_bucket{le=\"0.5\"} 0\n\
                latency_seconds_bucket{le=\"1.0\"} 1\n\
                latency_seconds_bucket{le=\"+Inf\"} 1\n\
                # HELP api_latency_seconds Seconds a request took, per api.\n\
                # TYPE api_latency_seconds histogram\n\
                api_latency_seconds_sum{api=\"Fetch\"} 1.0\n\
                api_latency_seconds_count{api=\"Fetch\"} 1\n\
                api_latency_seconds_bucket{le=\"2.0\",api=\"Fetch\"} 1\n\
                api_latency_seconds_bucket{le=\"+Inf\",api=\"Fetch\"} 1\n\
                # EOF\n"
    );
    assert!(metrics.scratch.is_empty());
}
