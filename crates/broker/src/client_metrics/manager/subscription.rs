//! Subscription matching for KIP-714 client metrics: which subscriptions a
//! connecting client matches, the union of the metric prefixes and the
//! smallest push interval across them, and the stable subscription id derived
//! from that result.

use krabka_metadata::MetadataImage;
use uuid::Uuid;

use super::{ClientAttributes, ComputedSubscription};
use crate::client_metrics::config::{self, ALL_METRICS};

/// Kafka's `ClientMetricsManager.createClientInstance`: the push interval
/// starts at `default_interval_ms` (Kafka's
/// [`config::INTERVAL_MS_DEFAULT`] unless the operator moved it) and every
/// matched subscription lowers it, whether or not it names any metric.
pub(crate) fn compute_subscription(
    image: &MetadataImage,
    attrs: &ClientAttributes,
    default_interval_ms: i32,
) -> ComputedSubscription {
    let mut matched_metrics: Vec<String> = Vec::new();
    let mut push_interval_ms = default_interval_ms;
    let mut any_star = false;

    for (_name, configs) in image.client_metrics_subscriptions() {
        let rules = match configs.get(config::KEY_MATCH) {
            Some(v) => match config::parse_match_rules(v) {
                Ok(r) => r,
                Err(_) => continue,
            },
            None => Vec::new(),
        };
        if !rules.iter().all(|r| selector_matches(r, attrs)) {
            continue;
        }
        let metrics = configs
            .get(config::KEY_METRICS)
            .map_or_else(Vec::new, |v| config::parse_metrics(v));
        if metrics.iter().any(|m| m == ALL_METRICS) {
            any_star = true;
        }
        for m in metrics {
            if !matched_metrics.contains(&m) {
                matched_metrics.push(m);
            }
        }
        push_interval_ms =
            push_interval_ms.min(config::effective_interval_ms(configs, default_interval_ms));
    }

    let metrics = if any_star {
        vec![ALL_METRICS.to_string()]
    } else {
        matched_metrics
    };
    ComputedSubscription {
        metrics,
        push_interval_ms,
    }
}

fn selector_matches(rule: &config::MatchRule, attrs: &ClientAttributes) -> bool {
    use config::MatchSelector::{
        Id, InstanceId, SoftwareName, SoftwareVersion, SourceAddress, SourcePort,
    };
    let target: std::borrow::Cow<'_, str> = match rule.selector {
        InstanceId => attrs.client_instance_id.to_string().into(),
        Id => (&attrs.client_id).into(),
        SoftwareName => (&attrs.software_name).into(),
        SoftwareVersion => (&attrs.software_version).into(),
        SourceAddress => (&attrs.source_address).into(),
        SourcePort => attrs.source_port.to_string().into(),
    };
    // The pattern is anchored at both ends, so this is Kafka's full match. A
    // pattern that exceeds `fancy_regex`'s backtrack limit counts as no match.
    rule.pattern.is_match(target.as_ref()).unwrap_or(false)
}

/// Stable, change-sensitive subscription id.
///
/// It is the CRC32C over a canonical, sorted rendering of the metric set and
/// the push interval, XOR-ed with the instance-id hash. It stays consistent
/// across a re-fetch. It is not byte-identical to the JVM broker's id, and it
/// does not need to be.
pub(crate) fn subscription_id(sub: &ComputedSubscription, client_instance_id: Uuid) -> i32 {
    let mut sorted = sub.metrics.clone();
    sorted.sort();
    let rendered = format!("[{}]{}", sorted.join(", "), sub.push_interval_ms);
    // CRC32C output is u32; reinterpreting as i32 is intentional — the value
    // is used only for equality checks, not arithmetic.
    let crc = crc32c::crc32c(rendered.as_bytes()).cast_signed();
    crc ^ uuid_hashcode(client_instance_id)
}

/// Reproduces `java.util.UUID.hashCode()`, so the shape matches.
fn uuid_hashcode(id: Uuid) -> i32 {
    let bytes = id.as_bytes();
    let msb = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let lsb = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
    let hilo = msb ^ lsb;
    let hilo_bytes = hilo.to_be_bytes();
    let high = i32::from_be_bytes(hilo_bytes[..4].try_into().expect("four-byte high half"));
    let low = i32::from_be_bytes(hilo_bytes[4..].try_into().expect("four-byte low half"));
    high ^ low
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::{ClientMetricsConfigRecord, MetadataImage, MetadataRecord};
    use uuid::Uuid;

    use super::*;
    use crate::client_metrics::manager::test_support::{apply_config, attrs, img_with};

    /// The subscription of a broker that keeps Kafka's default interval.
    fn compute_subscription(
        image: &MetadataImage,
        attrs: &ClientAttributes,
    ) -> ComputedSubscription {
        super::compute_subscription(image, attrs, config::INTERVAL_MS_DEFAULT)
    }

    /// The `client_metrics_default_interval` runtime key moves the interval
    /// that a client with no subscription, or a subscription with no
    /// `interval.ms`, is given; a subscription still lowers it (#1246).
    #[test]
    fn the_default_interval_is_the_one_the_broker_is_given() {
        let rows = [
            ("no subscription", MetadataImage::new(Uuid::nil()), 600_000),
            (
                "a subscription without interval.ms",
                img_with("all", &[("metrics", "*")]),
                600_000,
            ),
            (
                "a subscription below the default",
                img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]),
                60_000,
            ),
            (
                "a subscription above the default",
                img_with("all", &[("metrics", "*"), ("interval.ms", "3600000")]),
                600_000,
            ),
        ];
        for (name, img, expected) in rows {
            assert!(
                super::compute_subscription(&img, &attrs(), 600_000).push_interval_ms == expected,
                "row {name}"
            );
        }
    }

    #[test]
    fn no_subscription_means_no_metrics() {
        let img = MetadataImage::new(Uuid::nil());
        let m = compute_subscription(&img, &attrs());
        assert!(m.metrics.is_empty());
        check!(m.push_interval_ms == 300_000);
    }

    #[test]
    fn match_all_empty_match_applies() {
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let m = compute_subscription(&img, &attrs());
        check!(m.metrics == vec!["*".to_string()]);
        check!(m.push_interval_ms == 60_000);
    }

    #[test]
    fn selector_filters_clients() {
        let img = img_with(
            "java-only",
            &[
                ("metrics", "a."),
                ("match", "client_software_name=apache-kafka-java"),
            ],
        );
        let m = compute_subscription(&img, &attrs());
        check!(m.metrics == vec!["a.".to_string()]);

        let img2 = img_with(
            "py-only",
            &[
                ("metrics", "a."),
                ("match", "client_software_name=kafka-python"),
            ],
        );
        let m2 = compute_subscription(&img2, &attrs());
        assert!(
            m2.metrics.is_empty(),
            "java client must not match python selector"
        );
    }

    #[test]
    fn min_interval_and_metric_union_across_subs() {
        let mut img = img_with("s1", &[("metrics", "a."), ("interval.ms", "60000")]);
        apply_config(
            &mut img,
            "s2",
            &[("metrics", "b."), ("interval.ms", "30000")],
        );
        let m = compute_subscription(&img, &attrs());
        let mut got = m.metrics.clone();
        got.sort();
        check!(got == vec!["a.".to_string(), "b.".to_string()]);
        check!(m.push_interval_ms == 30_000);
    }

    #[test]
    fn an_empty_metrics_list_collects_nothing() {
        // KIP-714: `*` is the value that means every metric. An empty list
        // names no prefix, so it contributes nothing to the client's set --
        // `apache/kafka:4.3.1` builds the same set by adding each matching
        // subscription's metrics, and an empty one adds none. The registry
        // row documents the key that way.
        let img = img_with("empty", &[("metrics", ""), ("interval.ms", "60000")]);
        let m = compute_subscription(&img, &attrs());
        assert!(m.metrics.is_empty());

        let unset = img_with("unset", &[("interval.ms", "60000")]);
        let m2 = compute_subscription(&unset, &attrs());
        assert!(m2.metrics.is_empty());
    }

    #[test]
    fn star_collapses_union() {
        let mut img = img_with("s1", &[("metrics", "a.")]);
        apply_config(&mut img, "s2", &[("metrics", "*")]);
        let m = compute_subscription(&img, &attrs());
        check!(m.metrics == vec!["*".to_string()]);
    }

    /// Kafka's `createClientInstance` caps the interval at 300000, lets every
    /// matched subscription lower it, and full-matches the last pattern of
    /// each selector (#712).
    #[test]
    fn subscription_matches_kafkas_client_metrics_manager() {
        type Subscription = (&'static str, Vec<(&'static str, &'static str)>);
        let with = |client_id: &str, software: &str| ClientAttributes {
            client_id: client_id.into(),
            software_name: software.into(),
            ..attrs()
        };
        let rows: Vec<(
            &str,
            Vec<Subscription>,
            ClientAttributes,
            ComputedSubscription,
        )> = vec![
            (
                "an interval above the default is capped",
                vec![("s1", vec![("metrics", "a."), ("interval.ms", "600000")])],
                attrs(),
                ComputedSubscription {
                    metrics: vec!["a.".into()],
                    push_interval_ms: 300_000,
                },
            ),
            (
                "a matched subscription with no metrics lowers the interval",
                vec![
                    ("s1", vec![("metrics", "a."), ("interval.ms", "60000")]),
                    ("s2", vec![("metrics", ""), ("interval.ms", "1000")]),
                ],
                attrs(),
                ComputedSubscription {
                    metrics: vec!["a.".into()],
                    push_interval_ms: 1_000,
                },
            ),
            (
                "no subscription",
                vec![],
                attrs(),
                ComputedSubscription {
                    metrics: vec![],
                    push_interval_ms: 300_000,
                },
            ),
            (
                "an alternation full-matches its longer branch",
                vec![(
                    "s1",
                    vec![("metrics", "*"), ("match", "client_id=app|app-1")],
                )],
                with("app-1", "apache-kafka-java"),
                ComputedSubscription {
                    metrics: vec!["*".into()],
                    push_interval_ms: 300_000,
                },
            ),
            (
                "a partial match does not match",
                vec![("s1", vec![("metrics", "*"), ("match", "client_id=app")])],
                with("app-1", "apache-kafka-java"),
                ComputedSubscription {
                    metrics: vec![],
                    push_interval_ms: 300_000,
                },
            ),
            (
                "a missing KIP-511 name is unknown",
                vec![(
                    "s1",
                    vec![("metrics", "*"), ("match", "client_software_name=unknown")],
                )],
                with("svc-1", "unknown"),
                ComputedSubscription {
                    metrics: vec!["*".into()],
                    push_interval_ms: 300_000,
                },
            ),
            (
                "a repeated selector keeps its last pattern",
                vec![(
                    "s1",
                    vec![("metrics", "*"), ("match", "client_id=a,client_id=b")],
                )],
                with("b", "apache-kafka-java"),
                ComputedSubscription {
                    metrics: vec!["*".into()],
                    push_interval_ms: 300_000,
                },
            ),
        ];
        for (name, subscriptions, client, expected) in rows {
            let mut img = MetadataImage::new(Uuid::nil());
            for (sub_name, configs) in subscriptions {
                img.apply(&MetadataRecord::V1ClientMetricsConfig(
                    ClientMetricsConfigRecord {
                        name: sub_name.into(),
                        configs: configs
                            .into_iter()
                            .map(|(k, v)| (k.to_owned(), v.to_owned()))
                            .collect(),
                    },
                ));
            }
            check!(
                compute_subscription(&img, &client) == expected,
                "row {name}"
            );
        }
    }

    #[test]
    fn subscription_id_stable_and_change_sensitive() {
        let a = attrs();
        let s1 = ComputedSubscription {
            metrics: vec!["a.".into(), "b.".into()],
            push_interval_ms: 60_000,
        };
        let id1 = subscription_id(&s1, a.client_instance_id);
        let s1b = ComputedSubscription {
            metrics: vec!["b.".into(), "a.".into()],
            push_interval_ms: 60_000,
        };
        check!(id1 == subscription_id(&s1b, a.client_instance_id));
        let s2 = ComputedSubscription {
            metrics: vec!["a.".into(), "b.".into()],
            push_interval_ms: 30_000,
        };
        check!(id1 != subscription_id(&s2, a.client_instance_id));
        let s3 = ComputedSubscription {
            metrics: vec!["a.".into()],
            push_interval_ms: 60_000,
        };
        check!(id1 != subscription_id(&s3, a.client_instance_id));
    }
}
