//! `ListConfigResources` (`api_key=74`). KIP-1142 enumeration of every
//! resource an admin client can target with `DescribeConfigs` /
//! `AlterConfigs`. The same RPC at v0 was historically called
//! `ListClientMetricsResources` (KIP-714) and returned only client-metrics
//! subscriptions. v1 generalises it with a `resource_types` filter.
//!
//! Krabka surfaces `TOPIC` (2), `BROKER` (4), `BROKER_LOGGER` (8),
//! `CLIENT_METRICS` (16), and `GROUP` (32), which is the set the JVM
//! `ListConfigResourcesRequest.supportedResourceTypes()` reports at v1.
//! `BROKER_LOGGER` enumerates the same names `BROKER` does — one per node —
//! because a logger resource is addressed by the id of the node whose loggers
//! it names.
//!
//! `CLIENT_METRICS` enumerates configured subscription names from the
//! metadata image (see `MetadataImage::client_metrics_subscriptions`).

use krabka_metadata::AclOperation;
use krabka_protocol::owned::{
    list_config_resources_request::ListConfigResourcesRequest,
    list_config_resources_response::{ConfigResource, ListConfigResourcesResponse},
};

#[cfg(test)]
use crate::authorizer::SimpleAclAuthorizer;
use crate::{
    broker::Broker,
    codes,
    handlers::{
        RequestContext, acl_denied,
        acl_wire::CLUSTER_RESOURCE_NAME,
        describe_configs::{
            RESOURCE_TYPE_BROKER, RESOURCE_TYPE_BROKER_LOGGER, RESOURCE_TYPE_CLIENT_METRICS,
            RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
        },
    },
};

/// Default set returned when v1 callers omit `resource_types` (KIP-1142).
/// Mirrors the JVM admin client's expectation: every supported type the
/// broker can describe configs for.
const DEFAULT_RESOURCE_TYPES: [i8; 5] = [
    RESOURCE_TYPE_TOPIC,
    RESOURCE_TYPE_BROKER,
    RESOURCE_TYPE_BROKER_LOGGER,
    RESOURCE_TYPE_CLIENT_METRICS,
    RESOURCE_TYPE_GROUP,
];

pub(crate) fn handle(
    broker: &Broker,
    req: &ListConfigResourcesRequest,
    version: i16,
    ctx: &RequestContext<'_>,
) -> ListConfigResourcesResponse {
    let image = broker.controller.current_image();

    // Whole-request gate. Kafka's `KafkaApis.handleListConfigResources`
    // authorizes `DescribeConfigs` on the cluster. Only `AlterConfigs` and
    // `All` imply it, so a principal with only `Describe` (or `Read`,
    // `Write`, `Delete`, `Alter`) gets `CLUSTER_AUTHORIZATION_FAILED`.
    if acl_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        krabka_metadata::ResourceType::Cluster,
        CLUSTER_RESOURCE_NAME,
        AclOperation::DescribeConfigs,
    ) {
        return ListConfigResourcesResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        };
    }

    // Kafka's `KafkaApis.handleListConfigResources`: if any requested type is
    // not in `ListConfigResourcesRequest.supportedResourceTypes()`, the whole
    // request fails with `UNSUPPORTED_VERSION` and no resources, rather than
    // silently dropping the type it does not recognize.
    if version >= 1 && req.resource_types.iter().any(|rt| !is_supported_type(*rt)) {
        return ListConfigResourcesResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            config_resources: vec![],
            ..Default::default()
        };
    }

    let resources = collect_resources(
        &image,
        version,
        &req.resource_types,
        ctx.connection_listener_name,
    );

    ListConfigResourcesResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        config_resources: resources,
        ..Default::default()
    }
}

/// Whether `rt` is one of the types `ListConfigResourcesRequest.
/// supportedResourceTypes()` reports at v1: `TOPIC`, `BROKER`,
/// `BROKER_LOGGER`, `CLIENT_METRICS`, `GROUP`.
fn is_supported_type(rt: i8) -> bool {
    DEFAULT_RESOURCE_TYPES.contains(&rt)
}

/// Resolve the effective filter and enumerate each requested type from the
/// image, as Kafka's `KafkaApis.handleListConfigResources` does. v0 gives
/// client-metrics only. v1 with an empty list gives every supported type. v1
/// with explicit types gives the caller's filter, tested by membership, so a
/// repeated type lists its resources once. The rows follow Kafka's fixed type
/// order: `GROUP`, `CLIENT_METRICS`, `BROKER_LOGGER`, `BROKER`, then `TOPIC`.
/// `BROKER` and `BROKER_LOGGER` list only the brokers with an endpoint on
/// `listener` (`KRaftMetadataCache.getBrokerNodes(listenerName)`). Within one
/// type the rows are sorted (names lexicographically, broker ids numerically)
/// so the wire payload does not depend on `MetadataImage`'s hash order. Callers
/// at v1 must have already rejected any unsupported requested type with
/// `UNSUPPORTED_VERSION`.
fn collect_resources(
    image: &krabka_metadata::MetadataImage,
    version: i16,
    requested: &[i8],
    listener: &str,
) -> Vec<ConfigResource> {
    let wants = |rt: i8| {
        if version < 1 {
            // v0 (legacy ListClientMetricsResources): always client metrics.
            rt == RESOURCE_TYPE_CLIENT_METRICS
        } else {
            requested.is_empty() || requested.contains(&rt)
        }
    };
    let rows = |resource_type: i8, mut names: Vec<String>| {
        names.sort();
        names.into_iter().map(move |resource_name| ConfigResource {
            resource_name,
            resource_type,
            ..Default::default()
        })
    };

    let mut broker_ids: Vec<u64> = image
        .brokers()
        .filter(|b| b.endpoints.iter().any(|endpoint| endpoint.name == listener))
        .map(|b| b.node_id.0)
        .collect();
    broker_ids.sort_unstable();
    let broker_rows = |resource_type: i8| {
        broker_ids.iter().map(move |id| ConfigResource {
            resource_name: id.to_string(),
            resource_type,
            ..Default::default()
        })
    };

    let mut out: Vec<ConfigResource> = Vec::new();
    if wants(RESOURCE_TYPE_GROUP) {
        out.extend(rows(
            RESOURCE_TYPE_GROUP,
            image.group_configs().map(|(id, _)| id.clone()).collect(),
        ));
    }
    if wants(RESOURCE_TYPE_CLIENT_METRICS) {
        out.extend(rows(
            RESOURCE_TYPE_CLIENT_METRICS,
            image
                .client_metrics_subscriptions()
                .map(|(name, _)| name.clone())
                .collect(),
        ));
    }
    if wants(RESOURCE_TYPE_BROKER_LOGGER) {
        out.extend(broker_rows(RESOURCE_TYPE_BROKER_LOGGER));
    }
    if wants(RESOURCE_TYPE_BROKER) {
        out.extend(broker_rows(RESOURCE_TYPE_BROKER));
    }
    if wants(RESOURCE_TYPE_TOPIC) {
        out.extend(rows(
            RESOURCE_TYPE_TOPIC,
            image.topics().map(|t| t.name.clone()).collect(),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, TopicRecord};
    use uuid::Uuid;

    use super::*;
    use crate::{broker::BrokerHandle, test_support::DenyAll};

    const VERSION: i16 = 1;

    const LISTENER: &str = "PLAINTEXT";

    /// A case name, the request version, its `resource_types`, and the
    /// `(type, name)` rows expected in order.
    type Case<'a> = (&'a str, i16, Vec<i8>, Vec<(i8, &'a str)>);

    fn broker_on(id: u64, listener: &str) -> MetadataRecord {
        MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            endpoints: vec![krabka_metadata::BrokerEndpoint {
                name: listener.into(),
                host: "127.0.0.1".into(),
                port: 9092,
                protocol: krabka_security::ListenerProtocol::Plaintext,
            }],
            ..broker_registration(krabka_raft::NodeId(id))
        })
    }

    /// Topics `t-b` and `t-a`, brokers 10 and 2 on `PLAINTEXT` and broker 3
    /// on `SSL` only, group config `g-1` and subscription `sub-1`.
    fn populated_image() -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        for name in ["t-b", "t-a"] {
            img.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 1,
            }));
        }
        img.apply(&broker_on(10, LISTENER));
        img.apply(&broker_on(2, LISTENER));
        img.apply(&broker_on(3, "SSL"));
        img.apply(&MetadataRecord::V1GroupConfig(
            krabka_metadata::GroupConfigRecord {
                group_id: "g-1".into(),
                configs: maplit::btreemap! {"streams.num.standby.replicas".into() => "1".into()},
            },
        ));
        img.apply(&MetadataRecord::V1ClientMetricsConfig(
            krabka_metadata::ClientMetricsConfigRecord {
                name: "sub-1".into(),
                configs: maplit::btreemap! {"interval.ms".into() => "60000".into()},
            },
        ));
        img
    }

    fn rows(expected: &[(i8, &str)]) -> Vec<ConfigResource> {
        expected
            .iter()
            .map(|&(resource_type, name)| {
                tagged_wire!(ConfigResource {
                    resource_name: name.to_string(),
                    resource_type,
                })
            })
            .collect()
    }

    /// Kafka's `KafkaApis.handleListConfigResources`: membership decides which
    /// types are listed (a repeated type lists once), the rows follow the
    /// fixed order `GROUP`, `CLIENT_METRICS`, `BROKER_LOGGER`, `BROKER`,
    /// `TOPIC`, and broker rows come from `getBrokerNodes(listenerName)`, so
    /// broker 3 (SSL only) is absent. v0 lists client metrics only.
    macro_rules! listing_fixture {
        (($handle:ident, $directory:ident, $broker:ident, $context:ident), $topic:expr) => {
            let ($handle, $directory) =
                start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
            seed_topic(&$handle, $topic).await;
            let $broker = $handle.broker_arc_for_test();
            test_ctx!($context, "admin");
        };
    }

    #[test]
    fn collect_resources_matches_kafka_filter_and_order() {
        let img = populated_image();
        let all = [
            (RESOURCE_TYPE_GROUP, "g-1"),
            (RESOURCE_TYPE_CLIENT_METRICS, "sub-1"),
            (RESOURCE_TYPE_BROKER_LOGGER, "2"),
            (RESOURCE_TYPE_BROKER_LOGGER, "10"),
            (RESOURCE_TYPE_BROKER, "2"),
            (RESOURCE_TYPE_BROKER, "10"),
            (RESOURCE_TYPE_TOPIC, "t-a"),
            (RESOURCE_TYPE_TOPIC, "t-b"),
        ];
        let cases: Vec<Case<'_>> = vec![
            (
                "v0 lists client metrics",
                0,
                vec![],
                vec![(RESOURCE_TYPE_CLIENT_METRICS, "sub-1")],
            ),
            ("v1 empty filter lists every type", 1, vec![], all.to_vec()),
            (
                "v1 repeated topic type lists once",
                1,
                vec![RESOURCE_TYPE_TOPIC, RESOURCE_TYPE_TOPIC],
                vec![(RESOURCE_TYPE_TOPIC, "t-a"), (RESOURCE_TYPE_TOPIC, "t-b")],
            ),
            (
                "v1 filter order does not change row order",
                1,
                vec![
                    RESOURCE_TYPE_TOPIC,
                    RESOURCE_TYPE_BROKER,
                    RESOURCE_TYPE_GROUP,
                ],
                vec![
                    (RESOURCE_TYPE_GROUP, "g-1"),
                    (RESOURCE_TYPE_BROKER, "2"),
                    (RESOURCE_TYPE_BROKER, "10"),
                    (RESOURCE_TYPE_TOPIC, "t-a"),
                    (RESOURCE_TYPE_TOPIC, "t-b"),
                ],
            ),
            (
                "v1 broker logger names one resource per node on the listener",
                1,
                vec![RESOURCE_TYPE_BROKER_LOGGER],
                vec![
                    (RESOURCE_TYPE_BROKER_LOGGER, "2"),
                    (RESOURCE_TYPE_BROKER_LOGGER, "10"),
                ],
            ),
            (
                "v1 client metrics filter",
                1,
                vec![RESOURCE_TYPE_CLIENT_METRICS],
                vec![(RESOURCE_TYPE_CLIENT_METRICS, "sub-1")],
            ),
        ];
        for (name, version, requested, expected) in cases {
            let out = collect_resources(&img, version, &requested, LISTENER);
            assert2::check!(out == rows(&expected), "case {name}");
        }
    }

    crate::test_support::context_helper!(client_id = "admin-client");

    use crate::test_support::{
        broker_registration, describe_configs_acl_cases, grant_cluster_operation,
        start_broker_with_authorizer_no_audit as start_broker, test_ctx,
    };

    async fn seed_topic(handle: &BrokerHandle, name: &str) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 1,
            })])
            .await
            .expect("seed topic");
    }

    /// Kafka's `KafkaApis.handleListConfigResources`: a request naming a type
    /// outside `ListConfigResourcesRequest.supportedResourceTypes()` answers
    /// `UNSUPPORTED_VERSION` (35) for the whole request, with no resources —
    /// even when the request also names types krabka does support. Type 64 is
    /// past every type KIP-1142 defines.
    #[tokio::test]
    async fn v1_unsupported_resource_type_fails_the_whole_request() {
        listing_fixture!((broker_handle, _dir, broker, ctx), "t-a");

        let unsupported = unthrottled_wire!(ListConfigResourcesResponse {
            error_code: codes::UNSUPPORTED_VERSION,
            config_resources: vec![],
        });

        for (name, resource_types) in [
            ("unsupported type alone", vec![64]),
            (
                "supported type mixed with unsupported",
                vec![RESOURCE_TYPE_TOPIC, 64, RESOURCE_TYPE_BROKER],
            ),
        ] {
            let req = ListConfigResourcesRequest {
                resource_types,
                ..Default::default()
            };

            let resp = handle(&broker, &req, VERSION, &ctx);

            assert2::check!(resp == unsupported, "case {name}");
        }
        broker_handle.shutdown().await;
    }

    /// The all-supported case in the same table Kafka's issue laid out: a
    /// request naming only `TOPIC` still succeeds and returns the topic.
    /// Real Kafka's `handleListConfigResources` enumerates every topic in the
    /// metadata cache for `TOPIC`, internal topics included, so the expected
    /// set is built from the live image (`collect_resources`) rather than
    /// hardcoded to just the seeded topic — a running broker also carries
    /// `__consumer_offsets`, seeded at startup like real Kafka does. Same
    /// pattern as `cluster_describe_configs_gates_the_enumeration`.
    #[tokio::test]
    async fn v1_all_supported_types_still_succeed() {
        listing_fixture!((broker_handle, _dir, broker, ctx), "t-a");

        let req = ListConfigResourcesRequest {
            resource_types: vec![RESOURCE_TYPE_TOPIC],
            ..Default::default()
        };
        let resp = handle(&broker, &req, VERSION, &ctx);

        let topics = collect_resources(
            &broker.controller.current_image(),
            VERSION,
            &[RESOURCE_TYPE_TOPIC],
            LISTENER,
        );
        assert!(topics.iter().any(|r| r.resource_name == "t-a"));
        let expected = unthrottled_wire!(ListConfigResourcesResponse {
            error_code: codes::NONE,
            config_resources: topics,
        });
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn denied_handler_response_preserves_error_fields() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            deny_all,
            context(ctx, "alice")
        );
        let req = ListConfigResourcesRequest {
            resource_types: vec![RESOURCE_TYPE_TOPIC],
            ..Default::default()
        };

        let resp = handle(&broker, &req, VERSION, &ctx);

        let expected = unthrottled_wire!(ListConfigResourcesResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            config_resources: vec![],
        });
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka's `KafkaApis.handleListConfigResources` authorizes
    /// `DescribeConfigs` on the cluster (#660). Only `AlterConfigs` and `All`
    /// imply it. `Describe` and the operations that imply `Describe` get
    /// `CLUSTER_AUTHORIZATION_FAILED` and no resources.
    #[tokio::test]
    async fn cluster_describe_configs_gates_the_enumeration() {
        let (broker_handle, _dir) = start_broker(Arc::new(SimpleAclAuthorizer::new(
            std::collections::HashSet::new(),
        )))
        .await;
        seed_topic(&broker_handle, "orders").await;
        let broker = broker_handle.broker_arc_for_test();
        let req = ListConfigResourcesRequest {
            resource_types: vec![RESOURCE_TYPE_TOPIC],
            ..Default::default()
        };
        let topics = collect_resources(
            &broker.controller.current_image(),
            VERSION,
            &[RESOURCE_TYPE_TOPIC],
            LISTENER,
        );
        assert!(topics.iter().any(|r| r.resource_name == "orders"));
        let allowed = unthrottled_wire!(ListConfigResourcesResponse {
            error_code: codes::NONE,
            config_resources: topics,
        });
        let denied = unthrottled_wire!(ListConfigResourcesResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            config_resources: vec![],
        });

        let mut cases = describe_configs_acl_cases(&allowed, &denied);
        cases.insert(3, ("read", Some(AclOperation::Read), &denied));
        for (user, grant, expected) in cases {
            if let Some(operation) = grant {
                grant_cluster_operation(&broker_handle, user, operation).await;
            }
            test_ctx!(ctx, user);

            let resp = handle(&broker, &req, VERSION, &ctx);

            assert2::check!(resp == *expected, "user {user} with grant {grant:?}");
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn successful_handler_response_preserves_resource_fields() {
        listing_fixture!((broker_handle, _dir, broker, ctx), "orders");
        let req = ListConfigResourcesRequest {
            resource_types: vec![RESOURCE_TYPE_TOPIC],
            ..Default::default()
        };

        let resp = handle(&broker, &req, VERSION, &ctx);

        assert!(resp.error_code == codes::NONE);
        assert!(resp.throttle_time_ms == 0);
        let resource = resp
            .config_resources
            .iter()
            .find(|r| r.resource_name == "orders")
            .expect("seeded topic resource");
        assert!(resource.resource_type == RESOURCE_TYPE_TOPIC);
        broker_handle.shutdown().await;
    }
}
