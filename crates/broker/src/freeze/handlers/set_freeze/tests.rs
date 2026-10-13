//! The fixtures that the `SetTopicFreeze` unit tests share.
//!
//! One operator, Alice, holds a key in the trust set and authenticates on the
//! connection, so a test can present a record she signed as well as one the
//! trust set refuses.

use {
    crate::{
        cluster_id::encode,
        config::BrokerConfig,
        freeze::signing::freeze_signing_bytes,
        handlers::RequestContext,
        operator_keys::{OperatorKeyEntry, OperatorKeys},
        test_support::{
            FrozenTopicsImageSetup, OperatorKeyFileSetup, ed25519_public_key_file,
            frozen_topics_image, principal, start_broker_no_audit_with,
        },
    },
    krabka_metadata::{MetadataImage, PatternType, TopicFreezeRecord},
    krabka_protocol::krabka::freeze::SetTopicFreezeRequest,
    krabka_security::Principal,
    ring::signature::Ed25519KeyPair,
    std::net::SocketAddr,
    tempfile::TempDir,
    uuid::Uuid,
};

/// Alice's identity and request context, with caller-owned borrowed storage.
macro_rules! freeze_fixture {
    ($image:ident, $principal:ident, $peer:ident, $ctx:ident; $entries:expr $(; $env:ident, $config:ident)?) => {
        let $image = crate::freeze::handlers::set_freeze::tests::image($entries);
        let $principal = crate::test_support::principal(crate::freeze::handlers::set_freeze::tests::ALICE_NAME);
        let $peer = crate::freeze::handlers::set_freeze::tests::peer();
        let $ctx = crate::freeze::handlers::set_freeze::tests::context(&$principal, &$peer);
        $(let $env = crate::freeze::handlers::set_freeze::checks::FreezeEnv {
            config: &$config,
            image: &$image,
            ctx: &$ctx,
        };)?
    };
}

mod approval;
mod audit;
mod scope;
mod signature;

const CLUSTER: Uuid = Uuid::from_u128(0x5150);
const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
/// Alice as an `[[operator_keys]]` entry and a record's `set_by` name her:
/// the Kafka form, which is also how `break_glass.approvers` spells her.
const ALICE: &str = "User:alice";
/// Alice as a listener authenticates her, which is the bare session name.
/// A `Principal` carries this, and the handler is what adds the `User:`.
/// Putting [`ALICE`] here instead would hide the bug this pair exists to
/// catch: the test would pass while a real connection produced `alice`.
const ALICE_NAME: &str = "alice";
const ALICE_KEY: &str = "alice-yubi";

fn image(entries: &[(&str, PatternType)]) -> MetadataImage {
    frozen_topics_image(entries, FrozenTopicsImageSetup { cluster: CLUSTER })
}

fn peer() -> SocketAddr { "10.0.0.1:51120".parse().expect("peer address") }

fn context<'a>(principal: &'a Principal, peer: &'a SocketAddr) -> RequestContext<'a> {
    RequestContext::new(
        principal,
        peer,
        "krabka-guard",
        "conn-1",
        false,
        "PLAINTEXT",
    )
}

/// Borrow Alice's caller-owned context while inspecting a supplied image.
fn with_freeze_env(
    config: &BrokerConfig,
    image: &MetadataImage,
    check: impl FnOnce(&super::checks::FreezeEnv<'_>),
) {
    let principal = principal(ALICE_NAME);
    let peer = peer();
    let ctx = context(&principal, &peer);
    let env = super::checks::FreezeEnv {
        config,
        image,
        ctx: &ctx,
    };
    check(&env);
}

// A broker configuration with alice's operator key loaded.
fn config_with_alice(dir: &TempDir) -> (BrokerConfig, Ed25519KeyPair) {
    let (pair, path) = ed25519_public_key_file(dir.path(), OperatorKeyFileSetup::default());
    let keys = OperatorKeys::load(&[OperatorKeyEntry {
        key_id: ALICE_KEY.to_owned(),
        principal: ALICE.to_owned(),
        public_key_path: path,
    }])
    .expect("load trust set");
    let config = BrokerConfig {
        operator_keys: keys,
        ..BrokerConfig::default()
    };
    (config, pair)
}

fn alice_key_configuration() -> (TempDir, BrokerConfig, Ed25519KeyPair) {
    let dir = TempDir::new().expect("tempdir");
    let (config, pair) = config_with_alice(&dir);
    (dir, config, pair)
}

fn freeze_request(scope: &str, pattern_type: i8) -> SetTopicFreezeRequest {
    SetTopicFreezeRequest {
        scope: scope.to_owned(),
        pattern_type,
        frozen: true,
        reason: "DR cutover".to_owned(),
        ..SetTopicFreezeRequest::default()
    }
}

// `record` signed by `pair` for the test cluster, in the same base64 form
// `krabka-guard` reads from `DescribeCluster` and `check_signature` verifies
// against (#1082).
fn sign(pair: &Ed25519KeyPair, record: &TopicFreezeRecord) -> Vec<u8> {
    let bytes = freeze_signing_bytes(&encode(CLUSTER), record);
    pair.sign(&bytes).as_ref().to_vec()
}

#[tokio::test]
async fn handle_processes_request_and_encodes_response() {
    use krabka_protocol::Encode;

    let dir = tempfile::TempDir::new().unwrap();
    let (config, _) = config_with_alice(&dir);
    let (broker_handle, _dir) = start_broker_no_audit_with(|cfg| {
        cfg.operator_keys = config.operator_keys;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();

    let p = principal(ALICE_NAME);
    let s = peer();
    let ctx = context(&p, &s);
    let req = freeze_request(
        "test-topic",
        krabka_protocol::krabka::freeze::PATTERN_TYPE_LITERAL,
    );
    let mut req_bytes = bytes::BytesMut::new();
    req.encode(&mut req_bytes, 0).unwrap();

    let resp_bytes = super::handle(&broker, 0, 1, &req_bytes, &ctx)
        .await
        .expect("handle");
    assert2::check!(!resp_bytes.is_empty());

    broker_handle.shutdown().await;
}
