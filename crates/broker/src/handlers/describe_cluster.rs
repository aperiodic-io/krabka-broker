//! `DescribeCluster` (`api_key=60`).
//!
//! This handler projects registrations from the metadata image and broker
//! fencing from the controller's heartbeat registry. Unlike most admin
//! handlers, it never refuses the whole request for a missing ACL: any
//! authenticated principal can read the cluster id, controller id and broker
//! list. Kafka has no `Describe` gate in
//! `KafkaApis.handleDescribeCluster`, and clients such as a Kafka Connect
//! worker rely on `Admin.describeCluster()` succeeding under least-privilege
//! ACLs (#704).
//!
//! KIP-430: when the request sets the
//! `include_cluster_authorized_operations` flag, the response carries a
//! bitfield of the cluster operations the principal is authorized for. If the
//! flag is not set, the field stays at `i32::MIN`, which is Kafka's "not
//! present" sentinel. If the flag is set but the principal lacks `Describe`
//! on `Cluster("kafka-cluster")`, the bitfield is `0` rather than absent --
//! `Describe` gates only this field, not the rest of the response.

use krabka_metadata::ResourceType;
use krabka_protocol::owned::{
    describe_cluster_request::DescribeClusterRequest,
    describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
};

use crate::{
    codes,
    handlers::{
        acl_wire::CLUSTER_RESOURCE_NAME, authorized_operations::authorized_operations_bits,
        offline_replicas::listener_endpoint,
    },
};

/// `DescribeCluster` `endpoint_type` (KIP-919): `1` = BROKER.
const ENDPOINT_TYPE_BROKER: i8 = 1;
/// `DescribeCluster` `endpoint_type` (KIP-919): `2` = CONTROLLER.
const ENDPOINT_TYPE_CONTROLLER: i8 = 2;

/// The `broker_id` a registration's [`NodeId`](krabka_metadata::NodeId) projects
/// to on the wire.
///
/// `DescribeClusterResponse.brokers[].brokerId` is an int32, and so is the
/// `nodeId` every registration path validates, so the conversion cannot fail
/// for a registration the controller accepted. `-1` is the sentinel a client
/// reads as "no such node" if one ever did.
// cargo-mutants: an unobservable sentinel. No registration this broker can hold
// carries a node id above `i32::MAX`, so nothing a test constructs reaches the
// `-1` arm and no mutation of it changes an observable byte. Only the fallback
// is skipped -- `handle` itself stays in the sweep, because the branches around
// it (endpoint type, authorization, fenced filtering, KIP-430 opt-in) are all
// wire-visible and are asserted by the tests below.
#[cfg_attr(test, mutants::skip)]
fn wire_broker_id(node_id: u64) -> i32 {
    i32::try_from(node_id).unwrap_or(-1)
}

context_handler! {
    DescribeClusterRequest => DescribeClusterResponse,
    (broker, req, version, ctx),
    {
        let image = broker.controller.current_image();

        // KIP-919 endpoint-type check, matching Kafka's `AuthHelper` exactly. A
        // broker listener only ever serves `BROKER`; the requested type decides
        // which error the whole response carries. Neither branch echoes the
        // requested `endpoint_type` back -- the error response leaves it at the
        // schema default (`1`), same as Kafka's `AuthHelper.computeDescribeClusterResponse`.
        if req.endpoint_type == ENDPOINT_TYPE_CONTROLLER {
            let resp = DescribeClusterResponse {
                error_code: codes::MISMATCHED_ENDPOINT_TYPE,
                error_message: Some(
                    "The request was sent to an endpoint of type BROKER, but we wanted an endpoint \
                 of type CONTROLLER"
                        .into(),
                ),
                ..Default::default()
            };
            return Ok(resp);
        }
        if req.endpoint_type != ENDPOINT_TYPE_BROKER {
            // Anything other than BROKER or CONTROLLER is EndpointType.UNKNOWN.
            // Kafka's v0 schema predates KIP-919's endpoint_type field and has no
            // `UNSUPPORTED_ENDPOINT_TYPE` in its error-code table, so v0 answers
            // INVALID_REQUEST instead.
            let error_code = if version == 0 {
                codes::INVALID_REQUEST
            } else {
                codes::UNSUPPORTED_ENDPOINT_TYPE
            };
            let resp = DescribeClusterResponse {
                error_code,
                error_message: Some(format!("Unsupported endpoint type {}", req.endpoint_type)),
                ..Default::default()
            };
            return Ok(resp);
        }

        // KIP-919: a broker listener serves only BROKERS. KIP-1073 excludes known
        // dead/fenced brokers unless the request opts in, and marks included
        // unavailable rows as fenced. Unknown liveness entries remain eligible
        // while a newly elected controller seeds its heartbeat registry.
        let unavailable = crate::handlers::offline_replicas::unavailable_brokers(broker, &image).await;

        // Kafka lists only the brokers with an endpoint on the request's listener
        // (`KRaftMetadataCache.getBrokerNodes(listenerName)`); a broker without
        // one is left out rather than advertised at another listener's address.
        let listener = ctx.connection_listener_name;
        let brokers: Vec<DescribeClusterBroker> = image
            .brokers()
            .filter(|b| req.include_fenced_brokers || !unavailable.contains(&b.node_id.0))
            .filter_map(|b| {
                let endpoint = listener_endpoint(b, listener)?;
                Some(DescribeClusterBroker {
                    broker_id: wire_broker_id(b.node_id.0),
                    host: endpoint.host.clone(),
                    port: i32::from(endpoint.port),
                    rack: b.rack.clone(),
                    is_fenced: unavailable.contains(&b.node_id.0),
                    ..Default::default()
                })
            })
            .collect();

        // controller_id: an unfenced registered broker, not the quorum leader.
        // `Metadata` answers from the same helper. See `handlers::controller_id`.
        // `AuthHelper.computeDescribeClusterResponse` answers -1 for an id that
        // the broker list does not hold, so the id is drawn from the brokers with
        // an endpoint on this listener, and checked against the list.
        let controller_id =
            crate::handlers::controller_id::advertised_controller_id_among(&image, &unavailable, |b| {
                listener_endpoint(b, listener).is_some()
            });
        let controller_id = if brokers.iter().any(|b| b.broker_id == controller_id) {
            controller_id
        } else {
            crate::handlers::controller_id::NO_CONTROLLER_ID
        };

        // KIP-430: only populate the bitfield when the client asked for it;
        // otherwise leave the wire-default `i32::MIN` ("not present") sentinel.
        // `Describe` gates only this field (matching Kafka's
        // `AuthHelper.computeDescribeClusterResponse`), never the rest of the
        // response: without `Describe` the bitfield reads `0` even though the
        // principal may hold other Cluster operations.
        let cluster_authorized_operations = if req.include_cluster_authorized_operations {
            if crate::handlers::cluster_describe_denied(broker.config.authorizer.as_ref(), &image, ctx)
            {
                0
            } else {
                authorized_operations_bits(
                    broker.config.authorizer.as_ref(),
                    &image,
                    ctx,
                    ResourceType::Cluster,
                    CLUSTER_RESOURCE_NAME,
                )
            }
        } else {
            i32::MIN
        };

        Ok(DescribeClusterResponse {
            error_code: codes::NONE,
            error_message: None,
            // Echo the requested endpoint type (KIP-919). v0 has no such field; the
            // request default of `1` keeps the response byte-identical there.
            endpoint_type: req.endpoint_type,
            // Kafka's `Uuid.toString()` is URL-safe unpadded base64 of the 16 raw
            // bytes, not `java.util.UUID`'s hyphenated form. See #1042.
            cluster_id: crate::cluster_id::encode(image.cluster_id()),
            controller_id,
            brokers,
            cluster_authorized_operations,
            throttle_time_ms: 0,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_metadata::{AclOperation, BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord};
    use krabka_security::ListenerProtocol;

    use super::*;
    use crate::{
        broker::{Broker, BrokerHandle},
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 2;

    crate::test_support::context_helper!(client_id = "admin-client");

    use crate::test_support::{start_broker_with_authorizer_no_audit as start_broker, test_ctx};

    fn request(include_ops: bool) -> DescribeClusterRequest {
        DescribeClusterRequest {
            include_cluster_authorized_operations: include_ops,
            endpoint_type: 1,
            ..Default::default()
        }
    }

    async fn seed_broker(handle: &BrokerHandle) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    broker_epoch: -1,
                    host: "legacy-host".into(),
                    port: 19092,
                    rack: Some("rack-a".into()),
                    endpoints: vec![BrokerEndpoint {
                        name: "PLAINTEXT".into(),
                        host: "broker-a".into(),
                        port: 29092,
                        protocol: ListenerProtocol::Plaintext,
                    }],
                    ..crate::test_support::broker_registration(krabka_raft::NodeId(42))
                },
            )])
            .await
            .expect("seed broker registration");
    }

    /// #704: a principal without cluster `Describe` is never refused the
    /// whole request. Kafka's `handleDescribeCluster` has no `authorize` call
    /// at all; the missing grant only zeroes `cluster_authorized_operations`,
    /// gated below by `the_authorized_operations_bitfield` table.
    #[tokio::test]
    async fn a_principal_without_describe_still_gets_full_cluster_data() {
        let (broker_handle, _dir) = start_broker(Arc::new(
            crate::test_support::ControllerPeerAllowed(DenyAll),
        ))
        .await;
        seed_broker(&broker_handle).await;
        let broker = broker_handle.broker_arc_for_test();
        test_ctx!(ctx, "alice");

        let resp = handle(&broker, request(false), VERSION, &ctx)
            .await
            .expect("handle");

        // `start_broker` self-registers as broker 1 with a dynamic
        // 127.0.0.1 host/port, so the broker list is compared by content
        // (as `broker_endpoint_response_preserves_non_default_fields` does)
        // rather than as a whole struct against a literal.
        check_cluster_data(&broker, &resp);
        assert!(resp.brokers.len() == 2);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn broker_endpoint_response_preserves_non_default_fields() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_broker(&broker_handle).await;
        let broker = broker_handle.broker_arc_for_test();
        test_ctx!(ctx, "admin");

        let resp = handle(&broker, request(false), VERSION, &ctx)
            .await
            .expect("handle");

        check_cluster_data(&broker, &resp);
        broker_handle.shutdown().await;
    }

    /// KIP-919 endpoint-type table, matching Kafka's
    /// `AuthHelper.computeDescribeClusterResponse` byte for byte: a
    /// `CONTROLLER` request against a broker listener gets
    /// `MISMATCHED_ENDPOINT_TYPE` with Kafka's own message; any other
    /// non-`BROKER` value gets `UNSUPPORTED_ENDPOINT_TYPE`. Neither error
    /// response echoes the requested `endpoint_type`; it stays at the schema
    /// default (`1`). This all happens before the authorizer is consulted, so
    /// `DenyAll` proves the endpoint-type check is not an authorization path.
    ///
    /// `endpoint_type` is a v1+ field (KIP-919 predates v0), so a v0 request
    /// always decodes it at the schema default of `1` (BROKER) regardless of
    /// what bytes follow -- there is no wire-reachable way to drive a v0
    /// request into either error arm, and this table does not try to.
    #[tokio::test]
    async fn endpoint_type_errors_match_kafka() {
        struct Case {
            name: &'static str,
            version: i16,
            endpoint_type: i8,
            error_code: i16,
            error_message: &'static str,
        }
        let cases = [
            Case {
                name: "controller endpoint type on a broker listener",
                version: VERSION,
                endpoint_type: 2,
                error_code: codes::MISMATCHED_ENDPOINT_TYPE,
                error_message: "The request was sent to an endpoint of type BROKER, but we \
                                 wanted an endpoint of type CONTROLLER",
            },
            Case {
                name: "unknown endpoint type 0 at v2",
                version: VERSION,
                endpoint_type: 0,
                error_code: codes::UNSUPPORTED_ENDPOINT_TYPE,
                error_message: "Unsupported endpoint type 0",
            },
            Case {
                name: "unknown endpoint type 3 at v1",
                version: 1,
                endpoint_type: 3,
                error_code: codes::UNSUPPORTED_ENDPOINT_TYPE,
                error_message: "Unsupported endpoint type 3",
            },
        ];

        for case in cases {
            broker_fixture!(
                (broker_handle, _dir, broker),
                deny_all,
                context(ctx, "alice")
            );
            let req = DescribeClusterRequest {
                endpoint_type: case.endpoint_type,
                ..Default::default()
            };

            let got = handle(&broker, req, case.version, &ctx)
                .await
                .expect("handle");

            let expected = unthrottled_wire!(DescribeClusterResponse {
                error_code: case.error_code,
                error_message: Some(case.error_message.into()),
                endpoint_type: 1,
                cluster_id: String::new(),
                controller_id: -1,
                brokers: vec![],
                cluster_authorized_operations: i32::MIN,
            });
            assert!(got == expected, "{}", case.name);
            broker_handle.shutdown().await;
        }
    }

    /// `DescribeClusterResponse.ClusterId` reports Kafka's base64 `Uuid` form,
    /// not `java.util.UUID`'s hyphenated form (#1042). The expected string is
    /// what `org.apache.kafka.common.Uuid(0x0102030405060708L,
    /// 0x090a0b0c0d0e0f10L).toString()` produces for the same 16 bytes.
    #[tokio::test]
    async fn reports_cluster_id_in_kafka_base64_form() {
        known_cluster_fixture!((known_cluster_id, broker_handle, _dir, broker));
        test_ctx!(ctx, "describer");

        let resp = handle(&broker, request(false), VERSION, &ctx)
            .await
            .expect("handle");

        assert!(resp.cluster_id.as_str() == "AQIDBAUGBwgJCgsMDQ4PEA");
        broker_handle.shutdown().await;
    }

    /// KIP-430: the bitfield is filled only when the request opts in. Without
    /// the flag the field keeps the `i32::MIN` "not present" sentinel, which
    /// `broker_endpoint_response_preserves_non_default_fields` pins; with it,
    /// the response carries the operations the principal actually holds on the
    /// singleton `Cluster` resource.
    #[tokio::test]
    async fn the_authorized_operations_bitfield_is_filled_only_on_opt_in() {
        let authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker(Arc::clone(&authorizer) as _)
        );
        test_ctx!(ctx, "admin");

        let resp = handle(&broker, request(true), VERSION, &ctx)
            .await
            .expect("handle");

        let expected = authorized_operations_bits(
            authorizer.as_ref(),
            &broker.controller.current_image(),
            &ctx,
            ResourceType::Cluster,
            CLUSTER_RESOURCE_NAME,
        );
        assert!(expected != i32::MIN);
        assert!(resp.cluster_authorized_operations == expected);
        broker_handle.shutdown().await;
    }

    /// #704: `Describe` gates `cluster_authorized_operations` only, and it
    /// gates the *whole* bitfield, not per-operation. A principal that holds
    /// some other Cluster ACL (here `AlterConfigs`) but not `Describe` still
    /// reads `0`, exactly as `AuthHelper.computeDescribeClusterResponse`
    /// does -- it never falls through to computing a partial mask.
    #[tokio::test]
    async fn cluster_authorized_operations_reads_zero_without_describe_even_with_other_acls() {
        let authorizer = Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
            std::collections::HashSet::new(),
        ));
        let (broker_handle, _dir) =
            start_broker(Arc::clone(&authorizer) as Arc<dyn crate::authorizer::Authorizer>).await;
        broker_handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1AccessControlEntry(
                crate::test_support::allow_acl(crate::test_support::AllowAclSetup {
                    resource_type: ResourceType::Cluster,
                    resource_name: CLUSTER_RESOURCE_NAME,
                    operation: AclOperation::AlterConfigs,
                    ..Default::default()
                }),
            )])
            .await
            .expect("seed ACL");
        let broker = broker_handle.broker_arc_for_test();
        test_ctx!(ctx, "alice");

        let resp = handle(&broker, request(true), VERSION, &ctx)
            .await
            .expect("handle");

        assert!(resp.error_code == codes::NONE);
        assert!(resp.cluster_authorized_operations == 0);
        broker_handle.shutdown().await;
    }

    /// `wire_broker_id` is the `-1` sentinel fallback that the sweep skips: an
    /// id the controller can register always projects to itself.
    #[test]
    fn a_registered_node_id_projects_to_itself() {
        assert!(wire_broker_id(42) == 42);
    }

    #[tokio::test]
    async fn fenced_brokers_require_explicit_opt_in() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        seed_broker(&broker_handle).await;
        let broker = broker_handle.broker_arc_for_test();
        broker.liveness.record_fenced_heartbeat(42).await;
        assert!(broker.liveness.apply_fencing(42, true, true).await);
        test_ctx!(ctx, "admin");

        let response = handle(&broker, request(false), VERSION, &ctx)
            .await
            .expect("exclude fenced broker");
        assert!(response.brokers.iter().all(|row| row.broker_id != 42));

        let mut include_fenced = request(false);
        include_fenced.include_fenced_brokers = true;
        let response = handle(&broker, include_fenced, VERSION, &ctx)
            .await
            .expect("include fenced broker");
        let fenced = response
            .brokers
            .iter()
            .find(|row| row.broker_id == 42)
            .expect("fenced broker row");
        assert!(fenced.is_fenced);

        broker_handle.shutdown().await;
    }

    fn registration(node_id: u64, listeners: &[&str]) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord {
            broker_epoch: -1,
            host: "legacy-host".into(),
            port: 19092,
            endpoints: listeners
                .iter()
                .enumerate()
                .map(|(index, name)| BrokerEndpoint {
                    name: (*name).into(),
                    host: format!("{}-{node_id}", name.to_lowercase()),
                    port: 30_000 + u16::try_from(index).expect("port"),
                    protocol: ListenerProtocol::Plaintext,
                })
                .collect(),
            ..crate::test_support::broker_registration(krabka_raft::NodeId(node_id))
        }
    }

    /// #778: Kafka lists only the brokers with an endpoint on the request's
    /// listener (`KRaftMetadataCache.getBrokerNodes(listenerName)`), and
    /// `AuthHelper.computeDescribeClusterResponse` answers -1 for a
    /// `controller_id` the list does not hold. Broker 1 is the test broker
    /// itself, which serves `PLAINTEXT` only.
    #[tokio::test]
    async fn brokers_without_an_endpoint_on_the_request_listener_are_left_out() {
        let broker_row = |node_id: i32, listener: &str, port: i32| {
            tagged_wire!(DescribeClusterBroker {
                broker_id: node_id,
                host: format!("{}-{node_id}", listener.to_lowercase()),
                port,
                rack: None,
                is_fenced: false,
            })
        };
        let cases: [(&str, Vec<DescribeClusterBroker>, &[i32]); 3] = [
            ("EXTERNAL", vec![broker_row(43, "EXTERNAL", 30_001)], &[43]),
            ("INTERNAL", vec![], &[-1]),
            (
                "PLAINTEXT",
                vec![
                    broker_row(42, "PLAINTEXT", 30_000),
                    broker_row(43, "PLAINTEXT", 30_000),
                ],
                &[1, 42, 43],
            ),
        ];

        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        broker_handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![
                MetadataRecord::V1BrokerRegistration(registration(42, &["PLAINTEXT"])),
                MetadataRecord::V1BrokerRegistration(registration(43, &["PLAINTEXT", "EXTERNAL"])),
            ])
            .await
            .expect("seed broker registrations");
        let broker = broker_handle.broker_arc_for_test();
        request_identity!((p, peer), principal("admin"));

        for (listener, expected_brokers, controllers) in cases {
            let ctx = crate::handlers::RequestContext::new(
                &p,
                &peer,
                "admin-client",
                "test-connection",
                false,
                listener,
            );
            let mut response = handle(&broker, request(false), VERSION, &ctx)
                .await
                .expect("handle");
            // The test broker's own row carries an ephemeral port; the seeded
            // rows are compared whole.
            let lists_self = response.brokers.iter().any(|row| row.broker_id == 1);
            assert!(lists_self == (listener == "PLAINTEXT"), "{listener}");
            response.brokers.retain(|row| row.broker_id != 1);
            response.brokers.sort_by_key(|row| row.broker_id);
            assert!(response.brokers == expected_brokers, "{listener}");
            assert!(
                controllers.contains(&response.controller_id),
                "{listener}: {}",
                response.controller_id
            );
        }
        broker_handle.shutdown().await;
    }
    fn check_cluster_data(broker: &Broker, resp: &DescribeClusterResponse) {
        assert!(
            (
                resp.error_code,
                resp.error_message.clone(),
                resp.endpoint_type,
                resp.cluster_id.clone(),
                // Not requested: stays at the "not present" sentinel even
                // though Describe is denied (#704 gates only this field,
                // never the rest of the response).
                resp.cluster_authorized_operations,
                resp.throttle_time_ms
            ) == (
                codes::NONE,
                None,
                1,
                crate::cluster_id::encode(broker.controller.current_image().cluster_id()),
                i32::MIN,
                0
            )
        );
        let seeded_row = resp
            .brokers
            .iter()
            .find(|b| b.broker_id == 42)
            .expect("seeded broker row");
        let expected_seeded_row = tagged_wire!(DescribeClusterBroker {
            broker_id: 42,
            host: "broker-a".into(),
            port: 29092,
            rack: Some("rack-a".into()),
            is_fenced: false,
        });
        assert!(*seeded_row == expected_seeded_row);
    }
}
