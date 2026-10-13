//! Handler tests for the request-level checks that `ShareFetch` and
//! `ShareAcknowledge` run before the share session.
//!
//! Kafka's `KafkaApis.handleShareFetchRequest` and
//! `KafkaApis.handleShareAcknowledgeRequest` refuse a null group id with
//! `INVALID_REQUEST`, then check `Read` on the group, then refuse a member id
//! that `isMemberIdValid` does not accept (null, empty, or longer than 36
//! characters) with `INVALID_REQUEST`. They do not ask the group coordinator
//! whether the member exists.

use {
    crate::{
        authorizer::AuthorizationResult,
        broker::Broker,
        codes,
        handlers::{RequestContext, test_support::StartedBroker},
        test_support::{
            decode_response, encode_request, start_broker_with_authorizer_no_audit,
            try_dispatch_context,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::owned::{
        share_acknowledge_request::ShareAcknowledgeRequest,
        share_acknowledge_response::{self, ShareAcknowledgeResponse},
        share_fetch_request::ShareFetchRequest,
        share_fetch_response::{self, ShareFetchResponse},
    },
    std::sync::Arc,
};

/// The share group that the principal may read.
const READABLE: &str = "readable-group";

/// The acquisition lock timeout of the test broker's share-group config.
const LOCK_TIMEOUT_MS: i32 = 30_000;

/// Allows `Read` on [`READABLE`] only, and every other operation.
#[derive(Debug)]
struct ReadOneGroup;

test_authorizer!(ReadOneGroup, (self, _source, request), {
    if request.resource_type == ResourceType::Group
        && request.operation == AclOperation::Read
        && request.resource_name != READABLE
    {
        AuthorizationResult::Deny
    } else {
        AuthorizationResult::Allow
    }
});

async fn start() -> StartedBroker {
    let (broker, dir) = start_broker_with_authorizer_no_audit(Arc::new(ReadOneGroup)).await;
    StartedBroker { broker, dir }
}

macro_rules! share_request_identity {
    (($user:ident, $address:ident, $ctx:ident)) => {
        krabka_macros::request_test_identity! {
            bind($user, $address, $ctx);
            user ["share-consumer"];
            client ["share-client"];
        };
    };
}

/// One row: the group id, the member id, and the top-level error code of
/// each API.
struct Case {
    name: &'static str,
    group: Option<&'static str>,
    member: Option<String>,
    fetch_error: i16,
    acknowledge_error: i16,
}

fn cases() -> Vec<Case> {
    let valid = || Some("m".repeat(36));
    vec![
        Case {
            name: "valid ids",
            group: Some(READABLE),
            member: valid(),
            // The fetch at epoch 0 opens the session that the acknowledge at
            // epoch 1 then continues.
            fetch_error: codes::NONE,
            acknowledge_error: codes::NONE,
        },
        Case {
            name: "null group id",
            group: None,
            member: valid(),
            fetch_error: codes::INVALID_REQUEST,
            acknowledge_error: codes::INVALID_REQUEST,
        },
        Case {
            name: "null member id",
            group: Some(READABLE),
            member: None,
            fetch_error: codes::INVALID_REQUEST,
            acknowledge_error: codes::INVALID_REQUEST,
        },
        Case {
            name: "empty member id",
            group: Some(READABLE),
            member: Some(String::new()),
            fetch_error: codes::INVALID_REQUEST,
            acknowledge_error: codes::INVALID_REQUEST,
        },
        Case {
            name: "37-character member id",
            group: Some(READABLE),
            member: Some("m".repeat(37)),
            fetch_error: codes::INVALID_REQUEST,
            acknowledge_error: codes::INVALID_REQUEST,
        },
        Case {
            // Kafka checks the group `Read` before the member id.
            name: "denied group and empty member id",
            group: Some("other-group"),
            member: Some(String::new()),
            fetch_error: codes::GROUP_AUTHORIZATION_FAILED,
            acknowledge_error: codes::GROUP_AUTHORIZATION_FAILED,
        },
    ]
}

/// The top-level shape Kafka answers: rows and a lock timeout only when the
/// request got past every check.
fn fetch_response(error_code: i16) -> ShareFetchResponse {
    ShareFetchResponse {
        error_code,
        acquisition_lock_timeout_ms: if error_code == codes::NONE {
            LOCK_TIMEOUT_MS
        } else {
            0
        },
        ..Default::default()
    }
}

fn acknowledge_response(error_code: i16) -> ShareAcknowledgeResponse {
    ShareAcknowledgeResponse {
        error_code,
        acquisition_lock_timeout_ms: if error_code == codes::NONE {
            LOCK_TIMEOUT_MS
        } else {
            0
        },
        ..Default::default()
    }
}

/// Dispatches one `ShareFetch` request and returns the encoded response.
async fn dispatch_share_fetch(
    broker: &Broker,
    fetch: &ShareFetchRequest,
    version: i16,
    ctx: &RequestContext<'_>,
) -> Bytes {
    try_dispatch_context(
        broker,
        krabka_protocol::owned::share_fetch_request::API_KEY,
        version,
        &encode_request(fetch, version),
        ctx,
    )
    .await
    .expect("handle share fetch")
}

#[tokio::test]
async fn group_and_member_ids_are_checked_in_kafka_order() {
    let StartedBroker { broker, dir: _dir } = start().await;
    let shared = broker.broker_arc_for_test();
    share_request_identity!((user, address, ctx));
    let fetch_version = share_fetch_response::MAX_VERSION;
    let acknowledge_version = share_acknowledge_response::MAX_VERSION;

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases() {
        let fetch = ShareFetchRequest {
            group_id: case.group.map(str::to_owned),
            member_id: case.member.clone(),
            share_session_epoch: 0,
            ..Default::default()
        };
        let fetched = dispatch_share_fetch(&shared, &fetch, fetch_version, &ctx).await;
        let acknowledge = ShareAcknowledgeRequest {
            group_id: case.group.map(str::to_owned),
            member_id: case.member.clone(),
            share_session_epoch: 1,
            ..Default::default()
        };
        let acknowledged = try_dispatch_context(
            &shared,
            krabka_protocol::owned::share_acknowledge_request::API_KEY,
            acknowledge_version,
            &encode_request(&acknowledge, acknowledge_version),
            &ctx,
        )
        .await
        .expect("handle share acknowledge");
        actual.push((
            case.name,
            decode_response::<ShareFetchResponse>(&fetched, fetch_version),
            decode_response::<ShareAcknowledgeResponse>(&acknowledged, acknowledge_version),
        ));
        expected.push((
            case.name,
            fetch_response(case.fetch_error),
            acknowledge_response(case.acknowledge_error),
        ));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// Kafka's `ShareFetch` does not look the member up in the group
/// coordinator, so a group with a live share actor that does not know the
/// member still serves it.
#[tokio::test]
async fn a_member_the_group_does_not_know_still_fetches() {
    let StartedBroker { broker, dir: _dir } = start().await;
    let shared = broker.broker_arc_for_test();
    share_request_identity!((user, address, ctx));
    let version = share_fetch_response::MAX_VERSION;
    let _actor = shared.group_coordinator.get_or_create_share(READABLE);

    let fetch = ShareFetchRequest {
        group_id: Some(READABLE.into()),
        member_id: Some("not-a-member".into()),
        share_session_epoch: 0,
        ..Default::default()
    };
    let response = dispatch_share_fetch(&shared, &fetch, version, &ctx).await;

    assert!(
        decode_response::<ShareFetchResponse>(&response, version) == fetch_response(codes::NONE)
    );
    broker.shutdown().await;
}
