//! The endpoint derive moves owned fields and leaves wire extensions at default.

use {assert2::assert, krabka_macros::NodeEndpoints};

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

#[test]
fn owned_endpoint_strings_move_and_wire_extensions_stay_default() {
    for rack in [None, Some(String::from("rack-b"))] {
        let source = Endpoint {
            node_id: 7,
            host: String::from("outside.example"),
            port: 9093,
            rack,
        };
        let host_allocation = source.host.as_ptr();
        let expected_rack = source.rack.clone();
        let target = WireEndpoint::from(source);
        assert!(target.node_id == 7);
        assert!(target.host == "outside.example");
        assert!(target.host.as_ptr() == host_allocation);
        assert!(target.port == 9093);
        assert!(target.rack == expected_rack);
        assert!(target.extensions.is_empty());
    }
}
