//! `DescribeCluster` (KIP-919) on the controller listener: the controller
//! registrations that lets an `AdminClient` bootstrapped with
//! `--bootstrap-controller` discover the controllers, and the encoder that turns
//! them into a response body.
//!
//! The answer follows Kafka's `ControllerApis.handleDescribeCluster` and
//! `AuthHelper.computeDescribeClusterResponse`.

use {
    crate::{ClusterGrants, ClusterOperation, error::RaftError, kraft::KraftController},
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    bytes::{Bytes, BytesMut},
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::describe_cluster_response::{
        DescribeClusterBroker, DescribeClusterResponse,
    },
};

/// `DescribeCluster` (KIP-919) — served on the controller listener so an
/// `AdminClient` bootstrapped with `--bootstrap-controller` can discover the
/// quorum's controller endpoints directly from the leader.
pub(super) const API_KEY_DESCRIBE_CLUSTER: i16 = 60;

/// The listener name of every controller-listener connection.
///
/// Kafka projects the registrations onto the name of the listener that the
/// request arrived on. A krabka node has one controller listener, and it
/// registers that listener under this name.
pub(super) const CONTROLLER_LISTENER_NAME: &str = "CONTROLLER";

/// Kafka's `EndpointType.BROKER`.
const ENDPOINT_TYPE_BROKER: i8 = 1;
/// Kafka's `EndpointType.CONTROLLER`.
const ENDPOINT_TYPE_CONTROLLER: i8 = 2;
/// Kafka's `INVALID_REQUEST`.
const INVALID_REQUEST: i16 = 42;
/// Kafka's `MISMATCHED_ENDPOINT_TYPE`.
const MISMATCHED_ENDPOINT_TYPE: i16 = 114;
/// Kafka's `UNSUPPORTED_ENDPOINT_TYPE`.
const UNSUPPORTED_ENDPOINT_TYPE: i16 = 115;
/// The schema default of `cluster_authorized_operations`: not requested.
const AUTHORIZED_OPERATIONS_NOT_REQUESTED: i32 = i32::MIN;

/// Serve `DescribeCluster` (60, KIP-919) on the controller listener.
///
/// The listener has already checked `ALTER` on the cluster, as Kafka's
/// `ControllerApis.handleDescribeCluster` does. `grants` answers the KIP-430
/// bitfield, and `listener_name` is the listener the request arrived on.
pub(super) async fn describe_cluster_response_body(
    version: i16,
    body: &[u8],
    engine: &KraftController,
    grants: &dyn ClusterGrants,
    listener_name: &str,
) -> Result<Bytes, RaftError> {
    use krabka_protocol::{Decode, owned::describe_cluster_request::DescribeClusterRequest};

    let mut cur = body;
    let req = DescribeClusterRequest::decode(&mut cur, version)?;
    let leader_id = engine
        .quorum_state()
        .await
        .ok()
        .and_then(|qs| qs.leader_id)
        .and_then(|l| i32::try_from(l.0).ok())
        .unwrap_or(-1);
    let authorized_operations = || {
        if grants.allows(ClusterOperation::Describe) {
            grants.cluster_authorized_operations()
        } else {
            0
        }
    };
    let resp = describe_cluster_response(
        version,
        &req,
        &engine.current_image(),
        listener_name,
        leader_id,
        authorized_operations,
    );
    let mut buf = BytesMut::new();
    krabka_protocol::Encode::encode(&resp, &mut buf, version)?;
    Ok(buf.freeze())
}

/// Kafka's `AuthHelper.computeDescribeClusterResponse` for the controller
/// endpoint type.
///
/// An unknown endpoint type answers `UNSUPPORTED_ENDPOINT_TYPE` and a broker
/// endpoint type `MISMATCHED_ENDPOINT_TYPE`, both `INVALID_REQUEST` at v0,
/// in an otherwise default response. The node list holds each controller
/// registration with an endpoint named `listener_name`, and the controller id
/// is `leader_id` only when that list holds it.
fn describe_cluster_response(
    version: i16,
    req: &krabka_protocol::owned::describe_cluster_request::DescribeClusterRequest,
    image: &MetadataImage,
    listener_name: &str,
    leader_id: i32,
    authorized_operations: impl FnOnce() -> i32,
) -> DescribeClusterResponse {
    let refuse = |error_code: i16, message: String| DescribeClusterResponse {
        error_code: if version == 0 {
            INVALID_REQUEST
        } else {
            error_code
        },
        error_message: Some(message),
        ..Default::default()
    };
    match req.endpoint_type {
        ENDPOINT_TYPE_CONTROLLER => {}
        ENDPOINT_TYPE_BROKER => {
            return refuse(
                MISMATCHED_ENDPOINT_TYPE,
                "The request was sent to an endpoint of type CONTROLLER, but we wanted an \
                 endpoint of type BROKER"
                    .into(),
            );
        }
        other => {
            return refuse(
                UNSUPPORTED_ENDPOINT_TYPE,
                format!("Unsupported endpoint type {other}"),
            );
        }
    }
    let cluster_authorized_operations = if req.include_cluster_authorized_operations {
        authorized_operations()
    } else {
        AUTHORIZED_OPERATIONS_NOT_REQUESTED
    };
    let brokers = controller_nodes(image, listener_name);
    // "If the provided controller ID is not in the node list, return -1
    // instead to avoid confusing the client."
    let controller_id = if brokers.iter().any(|b| b.broker_id == leader_id) {
        leader_id
    } else {
        -1
    };
    DescribeClusterResponse {
        // Kafka's `Uuid.toString()` is URL-safe unpadded base64 of the 16 raw
        // bytes, not `java.util.UUID`'s hyphenated form. See #1042.
        cluster_id: URL_SAFE_NO_PAD.encode(image.cluster_id().as_bytes()),
        controller_id,
        cluster_authorized_operations,
        brokers,
        endpoint_type: ENDPOINT_TYPE_CONTROLLER,
        ..Default::default()
    }
}

/// Kafka's `ControllerRegistrationsPublisher.describeClusterControllers`: one
/// node for each controller registration that has an endpoint named
/// `listener_name`, with no rack. The rows are ordered by node id.
fn controller_nodes(image: &MetadataImage, listener_name: &str) -> Vec<DescribeClusterBroker> {
    let mut nodes: Vec<DescribeClusterBroker> = image
        .controllers()
        .filter_map(|registration| {
            let endpoint = registration
                .endpoints
                .iter()
                .find(|endpoint| endpoint.name == listener_name)?;
            Some(DescribeClusterBroker {
                broker_id: i32::try_from(registration.node_id.0).ok()?,
                host: endpoint.host.clone(),
                port: i32::from(endpoint.port),
                rack: None,
                ..Default::default()
            })
        })
        .collect();
    nodes.sort_by_key(|node| node.broker_id);
    nodes
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            UnstableApiVersions,
            server::{
                api_versions::{ApiVersionsView, api_versions_response_body},
                test_support::{decoded, test_engine_with_voters, voter},
            },
        },
        assert2::check,
        krabka_metadata::{BrokerEndpoint, ControllerRegistrationRecord, MetadataRecord, NodeId},
        krabka_protocol::{
            Decode, Encode, owned::describe_cluster_request::DescribeClusterRequest,
        },
        uuid::Uuid,
    };

    /// The controller registrations of a case: each node id and its
    /// listener names.
    type Registrations<'a> = &'a [(u64, &'a [&'a str])];

    /// One case: its name, the request version, endpoint type and include
    /// flag, the registrations, the connection listener, the leader id and
    /// the expected response.
    type Case<'a> = (
        &'a str,
        i16,
        i8,
        bool,
        Registrations<'a>,
        &'a str,
        i32,
        DescribeClusterResponse,
    );

    fn registration(id: u64, listeners: &[&str]) -> MetadataRecord {
        MetadataRecord::V1ControllerRegistration(ControllerRegistrationRecord {
            node_id: NodeId(id),
            incarnation_id: Uuid::from_u128(u128::from(id)),
            zk_migration_ready: false,
            endpoints: listeners
                .iter()
                .map(|name| BrokerEndpoint {
                    name: (*name).to_string(),
                    host: format!("c{id}-{}", name.to_ascii_lowercase()),
                    port: 9093,
                    protocol: krabka_security::ListenerProtocol::Plaintext,
                })
                .collect(),
            features: std::collections::BTreeMap::new(),
        })
    }

    fn node(id: i32, listener: &str) -> DescribeClusterBroker {
        DescribeClusterBroker {
            broker_id: id,
            host: format!("c{id}-{}", listener.to_ascii_lowercase()),
            port: 9093,
            rack: None,
            ..Default::default()
        }
    }

    fn refused(error_code: i16, message: &str) -> DescribeClusterResponse {
        DescribeClusterResponse {
            error_code,
            error_message: Some(message.to_string()),
            ..Default::default()
        }
    }

    const MISMATCHED: &str = "The request was sent to an endpoint of type CONTROLLER, but we \
                              wanted an endpoint of type BROKER";

    /// Kafka's `AuthHelper.computeDescribeClusterResponse` over the
    /// registrations of `ControllerRegistrationsPublisher`: the endpoint-type
    /// refusals, the node list of the connection's listener, the controller
    /// id that must be in that list, and the KIP-430 bitfield.
    #[test]
    fn describe_cluster_follows_compute_describe_cluster_response() {
        const OPS: i32 = 1 << 7 | 1 << 8;
        let ok = |brokers: Vec<DescribeClusterBroker>, controller_id: i32, ops: i32| {
            DescribeClusterResponse {
                cluster_id: "AAAAAAAAAAAAAAAAAAAAAA".into(),
                controller_id,
                cluster_authorized_operations: ops,
                brokers,
                endpoint_type: 2,
                ..Default::default()
            }
        };
        let all_controller: &[(u64, &[&str])] = &[(1, &["CONTROLLER"]), (2, &["CONTROLLER"])];
        let cases: Vec<Case<'_>> = vec![
            (
                "every registration on the listener",
                1,
                2,
                false,
                all_controller,
                "CONTROLLER",
                1,
                ok(
                    vec![node(1, "CONTROLLER"), node(2, "CONTROLLER")],
                    1,
                    i32::MIN,
                ),
            ),
            (
                "only registrations with the connection's listener",
                1,
                2,
                false,
                &[(1, &["CTRL_SSL"]), (2, &["CONTROLLER"])],
                "CTRL_SSL",
                2,
                ok(vec![node(1, "CTRL_SSL")], -1, i32::MIN),
            ),
            (
                "a registered non-voter is listed",
                1,
                2,
                false,
                &[(1, &["CONTROLLER"]), (3, &["CONTROLLER"])],
                "CONTROLLER",
                3,
                ok(
                    vec![node(1, "CONTROLLER"), node(3, "CONTROLLER")],
                    3,
                    i32::MIN,
                ),
            ),
            (
                "no leader",
                1,
                2,
                false,
                all_controller,
                "CONTROLLER",
                -1,
                ok(
                    vec![node(1, "CONTROLLER"), node(2, "CONTROLLER")],
                    -1,
                    i32::MIN,
                ),
            ),
            (
                "authorized operations requested",
                2,
                2,
                true,
                &[(1, &["CONTROLLER"])],
                "CONTROLLER",
                1,
                ok(vec![node(1, "CONTROLLER")], 1, OPS),
            ),
            (
                "broker endpoint type",
                1,
                1,
                false,
                all_controller,
                "CONTROLLER",
                1,
                refused(114, MISMATCHED),
            ),
            (
                "unknown endpoint type",
                2,
                7,
                false,
                all_controller,
                "CONTROLLER",
                1,
                refused(115, "Unsupported endpoint type 7"),
            ),
            (
                "unknown endpoint type zero",
                1,
                0,
                false,
                all_controller,
                "CONTROLLER",
                1,
                refused(115, "Unsupported endpoint type 0"),
            ),
            (
                "v0 defaults to the broker endpoint type",
                0,
                1,
                false,
                all_controller,
                "CONTROLLER",
                1,
                refused(42, MISMATCHED),
            ),
        ];
        for (
            name,
            version,
            endpoint_type,
            include_ops,
            registrations,
            listener,
            leader,
            expected,
        ) in cases
        {
            let mut image = MetadataImage::new(Uuid::nil());
            for (id, listeners) in registrations {
                image.apply(&registration(*id, listeners));
            }
            let req = DescribeClusterRequest {
                endpoint_type,
                include_cluster_authorized_operations: include_ops,
                ..Default::default()
            };
            let resp = describe_cluster_response(version, &req, &image, listener, leader, || OPS);
            check!(resp == expected, "{name}");
        }
    }

    /// A principal without `DESCRIBE` on the cluster reads a zero bitfield,
    /// and the engine path encodes a body that decodes at the request version.
    #[tokio::test]
    async fn describe_cluster_body_zeroes_operations_without_describe() {
        struct AlterOnly;
        impl ClusterGrants for AlterOnly {
            fn allows(&self, operation: ClusterOperation) -> bool {
                operation == ClusterOperation::Alter
            }

            fn cluster_authorized_operations(&self) -> i32 { 1 << 7 }
        }

        let (engine, _dir) = test_engine_with_voters(1, [voter(1, Vec::new())]);
        let req = DescribeClusterRequest {
            endpoint_type: 2,
            include_cluster_authorized_operations: true,
            ..Default::default()
        };
        let mut body = BytesMut::new();
        req.encode(&mut body, 1).expect("describe request");
        let body = super::describe_cluster_response_body(
            1,
            &body,
            &engine,
            &AlterOnly,
            CONTROLLER_LISTENER_NAME,
        )
        .await
        .expect("describe cluster");

        let mut cur = &body[..];
        let resp = DescribeClusterResponse::decode(&mut cur, 1).expect("describe response");
        check!(cur.is_empty());
        let expected = DescribeClusterResponse {
            cluster_id: "AAAAAAAAAAAAAAAAAAAAAA".into(),
            cluster_authorized_operations: 0,
            endpoint_type: 2,
            ..Default::default()
        };
        check!(resp == expected);
    }

    /// `DescribeCluster` (60) is advertised so clients negotiate it (KIP-919).
    #[test]
    fn describe_cluster_is_advertised() {
        use krabka_protocol::owned::api_versions_response::ApiVersionsResponse;

        let image = MetadataImage::new(Uuid::nil());
        let av = api_versions_response_body(4, ApiVersionsView {
            image: &image,
            metadata_offset: -1,
            admin_router: None,
            unstable: UnstableApiVersions::Disabled,
        });
        let avr = decoded::<ApiVersionsResponse>(&av, 4);
        check!(avr.api_keys.iter().any(|k| k.api_key == 60));
    }
}
