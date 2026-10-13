//! Drivers for the client-facing requests the enforcement tests issue as an
//! ordinary (non-super) principal — `Produce`, `Fetch`, `Metadata`,
//! `JoinGroup`, and `InitProducerId` — with the request builders they need.
//! All of them share `sasl_plain_authenticate` and the `round_trip` framing
//! primitive, and each drives one request on a freshly authenticated
//! connection.

pub use crate::kafka_wire::{drive_produce_as_plain, single_record_produce_request};
use {
    crate::{
        FETCH_VERSION, INIT_PRODUCER_ID_VERSION, JOIN_GROUP_VERSION, METADATA_VERSION,
        support::classic::{ClassicTimeouts, classic_join_request, join_protocol},
    },
    krabka_protocol::owned::{
        fetch_request::FetchRequest, fetch_response::FetchResponse,
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse, join_group_request::JoinGroupRequest,
        join_group_response::JoinGroupResponse, metadata_request::MetadataRequest,
        metadata_response::MetadataResponse,
    },
};

krabka_macros::wire_test_driver!(plain pub drive_fetch_as_plain {
    request: FetchRequest,
    response: FetchResponse,
    api: crate::support::wire::ApiRequestSetup {
        api_key: krabka_ids::ApiKey(1),
        version: krabka_ids::ApiVersion(FETCH_VERSION),
    },
    operation: "Fetch",
});

krabka_macros::wire_test_driver!(plain pub drive_metadata_as_plain {
    request: MetadataRequest,
    response: MetadataResponse,
    api: crate::support::wire::ApiRequestSetup {
        api_key: krabka_ids::ApiKey(3),
        version: krabka_ids::ApiVersion(METADATA_VERSION),
    },
    operation: "Metadata",
});

krabka_macros::wire_test_driver!(plain pub drive_join_group_as_plain {
    request: JoinGroupRequest,
    response: JoinGroupResponse,
    api: crate::support::wire::ApiRequestSetup {
        api_key: krabka_ids::ApiKey(11),
        version: krabka_ids::ApiVersion(JOIN_GROUP_VERSION),
    },
    operation: "JoinGroup",
});

krabka_macros::wire_test_driver!(plain pub drive_init_producer_id_as_plain {
    request: InitProducerIdRequest,
    response: InitProducerIdResponse,
    api: crate::support::wire::ApiRequestSetup {
        api_key: krabka_ids::ApiKey(22),
        version: krabka_ids::ApiVersion(INIT_PRODUCER_ID_VERSION),
    },
    operation: "InitProducerId",
});

/// Build a single-protocol `JoinGroup` request with an empty `member_id`, so
/// the broker first responds with `MEMBER_ID_REQUIRED` and a generated id.
/// The request proposes the `range` assignor, the only one the
/// broker negotiates in MVP.
pub fn join_group_request(group_id: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_instance_id: None,
        ..classic_join_request()
            .group_id(group_id.to_string())
            .timeouts(ClassicTimeouts {
                rebalance: krabka_units::millis(60_000),
                ..Default::default()
            })
            .protocol_type("consumer".to_string())
            .protocols(vec![join_protocol(
                "range".to_string(),
                bytes::Bytes::new(),
            )])
            .call()
    }
}
