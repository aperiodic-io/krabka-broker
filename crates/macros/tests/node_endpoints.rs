//! The endpoint derive moves owned fields and leaves wire extensions at default.

use krabka_macros::NodeEndpoints;

#[derive(Debug, PartialEq, NodeEndpoints)]
#[node_endpoints(targets(WireEndpoint))]
struct Endpoint {
    node_id: i32,
    host: String,
    port: i32,
    rack: Option<String>,
}

#[derive(Default)]
struct WireEndpoint {
    node_id: i32,
    host: String,
    port: i32,
    rack: Option<String>,
    extensions: Vec<u8>,
}

#[yare::parameterized(
    no_rack = { None },
    with_rack = { Some(String::from("rack-b")) },
)]
fn owned_endpoint_strings_move_and_wire_extensions_stay_default(rack: Option<String>) {
    let source = Endpoint {
        node_id: 7,
        host: String::from("outside.example"),
        port: 9093,
        rack,
    };
    let host_allocation = source.host.as_ptr();
    let expected_rack = source.rack.clone();
    let target = WireEndpoint::from(source);
    assert2::assert!(target.node_id == 7);
    assert2::assert!(target.host == "outside.example");
    assert2::assert!(target.host.as_ptr() == host_allocation);
    assert2::assert!(target.port == 9093);
    assert2::assert!(target.rack == expected_rack);
    assert2::assert!(target.extensions.is_empty());
}
