//! Wire drivers for the two config RPCs the throttle keys travel on:
//! `IncrementalAlterConfigs` (`api_key` 44) in both its SASL and its PLAINTEXT
//! flavour, and `DescribeConfigs` (`api_key` 32).
//!
//! Setting a throttle is what every test in the suite does first, so the
//! request-shaping boilerplate is collected here and the tests keep only the
//! resource rows they care about.

use {
    super::test_support::*,
    crate::support::configs::{
        ConfigOperation, incremental_config, incremental_request, incremental_resource,
    },
    krabka_protocol::owned::{
        describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
        describe_configs_response::DescribeConfigsResult,
    },
    std::net::SocketAddr,
};

pub type ConfigOperations = Vec<(String, Option<String>, i8)>;
pub type ConfigResources = Vec<(i8, String, ConfigOperations)>;

/// Drive `IncrementalAlterConfigs` (`api_key=44`) over a SASL/PLAIN connection.
/// `resources` is a list of `(resource_type, name, [(config_name, value, op)])`.
/// Returns the top-level error code from the first resource response. 0 means
/// success.
pub async fn drive_incremental_alter_configs(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    resources: ConfigResources,
) -> i16 {
    let mut stream = kafka_wire::sasl_plain_authenticate(addr, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        user,
        password: (pass.as_bytes()).to_vec(),
    })
    .await
    .expect("SASL authenticate for IncrementalAlterConfigs");
    incremental_alter_configs_on(&mut stream, resources).await
}

/// Drive `IncrementalAlterConfigs` (`api_key=44`) over a PLAINTEXT connection.
/// There is no SASL, and the compat shim allows everything.
pub async fn drive_incremental_alter_configs_plaintext(
    addr: SocketAddr,
    resources: ConfigResources,
) -> i16 {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    incremental_alter_configs_on(&mut stream, resources).await
}

/// Drive `DescribeConfigs` (`api_key=32`, version=1) over a SASL/PLAIN connection.
/// Returns the decoded per-resource results.
#[allow(dead_code)]
pub async fn drive_describe_configs(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    resources: Vec<DescribeConfigsResource>,
) -> Vec<DescribeConfigsResult> {
    const VERSION: i16 = 1;

    use krabka_protocol::owned::describe_configs_response::DescribeConfigsResponse;

    let req = DescribeConfigsRequest {
        resources,
        include_synonyms: false,
        include_documentation: false,
        ..Default::default()
    };

    let mut stream = kafka_wire::sasl_plain_authenticate(addr, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        user,
        password: (pass.as_bytes()).to_vec(),
    })
    .await
    .expect("SASL authenticate for DescribeConfigs");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode DescribeConfigs");
    let resp_bytes = kafka_wire::round_trip()
        .stream(&mut stream)
        .api_key(32)
        .version(VERSION)
        .client_id(CLIENT_ID)
        .body(&body)
        .call()
        .await
        .expect("DescribeConfigs round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        DescribeConfigsResponse::decode(&mut cur, VERSION).expect("decode DescribeConfigsResponse");

    resp.results
}

async fn incremental_alter_configs_on(stream: &mut TcpStream, resources: ConfigResources) -> i16 {
    const VERSION: i16 = 1;

    use krabka_protocol::owned::incremental_alter_configs_response::IncrementalAlterConfigsResponse;

    let req = incremental_request()
        .resources(
            resources
                .into_iter()
                .map(|(resource_type, resource_name, configs)| {
                    incremental_resource(
                        resource_type,
                        resource_name,
                        configs
                            .into_iter()
                            .map(|(name, value, config_operation)| {
                                incremental_config(
                                    name,
                                    value,
                                    ConfigOperation::from_wire(config_operation),
                                )
                            })
                            .collect(),
                    )
                })
                .collect(),
        )
        .call();

    let resp: IncrementalAlterConfigsResponse = kafka_wire::exchange()
        .stream(stream)
        .request(&req)
        .api_key(44)
        .version(VERSION)
        .client_id(CLIENT_ID)
        .header(RequestHeaderEncoding::Flexible)
        .call()
        .await
        .expect("IncrementalAlterConfigs round-trip");

    resp.responses.first().map_or(0, |r| r.error_code)
}
