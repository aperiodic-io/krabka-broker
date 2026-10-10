//! The operator signature, from the command that signs it to the registry entry
//! an auditor re-verifies after a restart.
//!
//! `freeze.require_signature` governs the freeze direction alone: a freeze is
//! the safe direction and has to be reachable in one command, while a thaw
//! always demands the proof. These cases hold that asymmetry in place, and they
//! hold the replay defence with it -- a signature captured from a freeze must
//! not authorize the thaw that lifts it.

use std::path::Path;

use assert2::check;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle, codes};
use krabka_client_core::Client;
use krabka_protocol::{
    krabka::freeze::{DescribedTopicFreeze, PATTERN_TYPE_LITERAL, SetTopicFreezeRequest},
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    control_plane::{cluster_id, freeze_request, set_freeze, wait_for_registry_len},
    signing::{SignedFreeze, signed_request, verifies_locally},
    support::{self, OperatorKey, client::connect_owned},
    wire::{CONTROL, accepted, create_topic, now_ms, refused},
};

async fn check_frozen_then_shutdown(
    broker: BrokerHandle,
    client: &Client,
    frozen: WireUuid,
    control: WireUuid,
) {
    crate::wire::check_produce!(&broker, client;
        "orders", frozen => refused("literal", "orders", "incident", 0), CONTROL, control => accepted(1));
    broker.shutdown().await;
}

/// [`support::start_with_operator_key`] with `freeze.require_signature` on.
///
/// The harness helper takes the default, and a running broker's configuration
/// is not mutable, so the cases that need the strict setting build it here.
/// Everything else is what the harness builds: the same trust set, the same
/// single-approver break-glass set, the same plaintext listener.
async fn start_requiring_signatures(dir: &Path, key: &OperatorKey) -> (BrokerHandle, Client) {
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    config.operator_keys = krabka_broker::operator_keys::OperatorKeys::load(&[key.entry()])
        .expect("load the operator trust set");
    config.break_glass.approvers = vec![support::ANONYMOUS.to_owned()];
    config.freeze.require_signature = true;
    let broker = Broker::start(config).await.expect("broker start");
    let client = connect_owned(
        broker.listen_addr().to_string(),
        "krabka-broker-test",
        "client build",
    )
    .await;
    (broker, client)
}

/// `freeze.require_signature` is what decides whether the broker takes an
/// operator's word for a freeze.
///
/// The asymmetry it controls is deliberate and easy to lose: a freeze is the
/// safe direction, and an operator has to reach it in one command on a cluster
/// where nobody installed key material yet. That leaves a registry that can
/// hold proved entries beside attested ones, and this setting is the only thing
/// that removes the mixture. The refused arm carries its own control: the
/// topic never froze, so it keeps taking writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn require_signature_decides_whether_an_unsigned_freeze_is_accepted() {
    for (label, require_signature, code, after) in [
        (
            "the default takes the operator's attestation",
            false,
            codes::NONE,
            refused("literal", "orders", "incident", 1),
        ),
        (
            "require_signature demands the proof",
            true,
            codes::OPERATOR_SIGNATURE_REQUIRED,
            accepted(2),
        ),
    ] {
        let (_keys, _logs, broker, client, _key, frozen, control) =
            operator_case(require_signature).await;
        crate::wire::check_produce!(&broker, &client, "orders", frozen => accepted(1),
            "{label}");

        let response = set_freeze(
            &client,
            freeze_request(PATTERN_TYPE_LITERAL, "orders", "incident"),
        )
        .await;
        check!(response.error_code == code, "{label}: {response:?}");
        wait_for_registry_len(&client, usize::from(code == codes::NONE)).await;

        crate::wire::check_produce!(&broker, &client;
            "orders", frozen => after, CONTROL, control => accepted(1); "{label}");
        broker.shutdown().await;
    }
}

/// A signed freeze reaches the registry with its `key_id` and its signature
/// intact, and the signature verifies away from the broker.
///
/// The signature is the only part of a freeze record that the broker cannot
/// forge, so it is the only part that proves who set it. That proof is worth
/// nothing unless the exact bytes the operator signed come back out of
/// `DescribeTopicFreezes`, which is why the whole entry is compared rather than
/// a field at a time: a broker that dropped the signature, re-stamped
/// `set_at_ms`, or rewrote `set_by` would still answer every other case here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_freeze_round_trips_with_its_key_id_and_signature_intact() {
    let (_keys, _logs, broker, client, key, frozen, control) = operator_case(false).await;

    let cluster = cluster_id(&client).await;
    let set_at_ms = now_ms();
    let request = incident_request(&key, &cluster, set_at_ms);
    let signature = request.signature.clone();
    let response = set_freeze(&client, request).await;
    check!(
        response.error_code == codes::NONE,
        "signed freeze: {response:?}"
    );

    let entries = wait_for_registry_len(&client, 1).await;
    check!(
        entries[0]
            == DescribedTopicFreeze {
                scope: "orders".to_owned(),
                pattern_type: PATTERN_TYPE_LITERAL,
                reason: "incident".to_owned(),
                set_by: support::ANONYMOUS.to_owned(),
                set_at_ms,
                proposal_id: WireUuid::ZERO,
                key_id: key.key_id.clone(),
                signature,
                ..DescribedTopicFreeze::default()
            }
    );
    check!(verifies_locally(&key, &cluster, &entries[0]));

    check_frozen_then_shutdown(broker, &client, frozen, control).await;
}

/// An unsigned thaw is refused whatever `freeze.require_signature` says.
///
/// This is the half of the asymmetry that carries the security of the whole
/// feature. A freeze that one unsigned command can lift is exactly as strong as
/// the one credential that sends it, and when the incident is a compromise the
/// attacker already holds that credential. `require_signature` is about the
/// freeze direction alone, so it is asserted with the setting both on and off:
/// a thaw that started depending on it would look correct in the strict
/// configuration and be wide open in the default one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsigned_thaw_is_refused_whatever_require_signature_says() {
    for (label, require_signature) in [
        ("with require_signature off", false),
        ("with require_signature on", true),
    ] {
        let (_keys, _logs, broker, client, key, frozen, control) =
            operator_case(require_signature).await;

        let (_cluster, response) = sign_incident(&client, &key).await;
        check!(response.error_code == codes::NONE, "{label}: {response:?}");
        wait_for_registry_len(&client, 1).await;

        let thaw = set_freeze(
            &client,
            SetTopicFreezeRequest {
                scope: "orders".to_owned(),
                pattern_type: PATTERN_TYPE_LITERAL,
                frozen: false,
                reason: "let me back in".to_owned(),
                proposal_id: WireUuid(*uuid::Uuid::new_v4().as_bytes()),
                set_at_ms: now_ms(),
                ..SetTopicFreezeRequest::default()
            },
        )
        .await;
        check!(
            thaw.error_code == codes::OPERATOR_SIGNATURE_REQUIRED,
            "{label}: {thaw:?}"
        );

        check!(
            wait_for_registry_len(&client, 1).await[0].scope == "orders",
            "{label}"
        );
        crate::wire::check_produce!(&broker, &client;
            "orders", frozen => refused("literal", "orders", "incident", 0), CONTROL, control => accepted(1); "{label}");
        broker.shutdown().await;
    }
}

/// A signature captured from a freeze cannot be replayed as the thaw.
///
/// `frozen` and `set_at_ms` are both inside the signed bytes for this attack
/// and no other. Drop `frozen` from the payload and the freeze record and the
/// thaw record differ by one byte that nothing covers, so one captured
/// signature would authorize both directions -- and the direction an attacker
/// wants is the one that lifts the freeze. Both replays are asserted only on
/// the error code, because all six signature checks answer
/// `OPERATOR_SIGNATURE_INVALID` on purpose: a code that separated them would
/// tell an attacker which check they got past.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signature_captured_from_a_freeze_is_refused_as_a_thaw() {
    let (_keys, _logs, broker, client, key, frozen, control) = operator_case(false).await;

    let cluster = cluster_id(&client).await;
    let set_at_ms = now_ms();
    let freeze = incident_request(&key, &cluster, set_at_ms);
    let captured = freeze.signature.clone();
    check!(set_freeze(&client, freeze).await.error_code == codes::NONE);
    wait_for_registry_len(&client, 1).await;

    // The two shapes the capture can take: the record the attacker holds, with
    // only `frozen` flipped, and the same signature carried forward onto a
    // fresh timestamp so the "newer than the entry it replaces" rule cannot be
    // what refuses it.
    for (label, replay_at_ms) in [
        ("the otherwise identical thaw", set_at_ms),
        ("the same signature on a fresh timestamp", now_ms() + 1_000),
    ] {
        let thaw = set_freeze(
            &client,
            SetTopicFreezeRequest {
                scope: "orders".to_owned(),
                pattern_type: PATTERN_TYPE_LITERAL,
                frozen: false,
                reason: "incident".to_owned(),
                proposal_id: WireUuid::ZERO,
                set_at_ms: replay_at_ms,
                key_id: key.key_id.clone(),
                signature: captured.clone(),
                ..SetTopicFreezeRequest::default()
            },
        )
        .await;
        check!(
            thaw.error_code == codes::OPERATOR_SIGNATURE_INVALID,
            "{label}: {thaw:?}"
        );
    }

    check!(wait_for_registry_len(&client, 1).await[0].scope == "orders");
    check_frozen_then_shutdown(broker, &client, frozen, control).await;
}

/// A signature survives a controller restart and still verifies from the
/// reloaded image.
///
/// This is the durability claim the design makes about the proof rather than
/// about the state: an auditor holding the operator public keys can say who
/// froze a topic, from a broker that was not running when they signed. A broker
/// that kept the registry across a restart but dropped the signature would pass
/// every other durability case and quietly turn every proved entry into an
/// attested one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signature_survives_a_controller_restart_and_still_verifies() {
    let (keys, logs) = operator_directories();
    let key = support::mint_operator_key(keys.path(), "alice-yubi", support::ANONYMOUS);
    let (broker, client, mut config) = support::start_with_operator_key(logs.path(), &key).await;
    create_topic(&broker, &client, "orders").await;
    create_topic(&broker, &client, CONTROL).await;

    let (cluster, response) = sign_incident(&client, &key).await;
    check!(
        response.error_code == codes::NONE,
        "signed freeze: {response:?}"
    );
    let before = wait_for_registry_len(&client, 1).await;
    drop(client);
    broker.shutdown().await;

    // The harness cannot infer the mode on a second boot, and a node that
    // re-bootstraps comes back with an empty registry -- which would make this
    // case fail for a reason that has nothing to do with the signature.
    config.bootstrap_mode = BootstrapMode::Rejoin;
    let broker = support::start_reusing_addrs(&config, "the signed-freeze restart").await;
    let client = connect_owned(
        broker.listen_addr().to_string(),
        "krabka-broker-test",
        "client build",
    )
    .await;
    crate::wire::wait_controlled_partitions(&broker, "orders").await;

    let after = wait_for_registry_len(&client, 1).await;
    check!(after == before);
    check!(cluster_id(&client).await == cluster);
    check!(verifies_locally(&key, &cluster, &after[0]));

    let frozen = support::topic_id_for(&client, "orders").await;
    let control = support::topic_id_for(&client, CONTROL).await;
    check_frozen_then_shutdown(broker, &client, frozen, control).await;
}

async fn operator_fixture(
    keys: &Path,
    logs: &Path,
    require_signature: bool,
) -> (BrokerHandle, Client, OperatorKey, WireUuid, WireUuid) {
    let key = support::mint_operator_key(keys, "alice-yubi", support::ANONYMOUS);
    let (broker, client) = if require_signature {
        start_requiring_signatures(logs, &key).await
    } else {
        let (broker, client, _) = support::start_with_operator_key(logs, &key).await;
        (broker, client)
    };
    let (frozen, control) = crate::wire::create_controlled_topic(&broker, &client, "orders").await;
    (broker, client, key, frozen, control)
}

/// The incident fixture shared by the signed freeze, replay and recovery cases.
fn incident_request(key: &OperatorKey, cluster: &str, set_at_ms: i64) -> SetTopicFreezeRequest {
    signed_request(&SignedFreeze {
        key,
        cluster_id: cluster,
        pattern_type: PATTERN_TYPE_LITERAL,
        scope: "orders",
        frozen: true,
        reason: "incident",
        set_at_ms,
        proposal_id: uuid::Uuid::nil(),
    })
}

/// Keep the signing keys and persistent broker logs alive for the whole case.
fn operator_directories() -> (tempfile::TempDir, tempfile::TempDir) {
    let keys = tempfile::tempdir().expect("tempdir");
    let logs = tempfile::tempdir().expect("tempdir");
    (keys, logs)
}

/// Own both directories beside the operator fixture, in the original local drop order.
async fn operator_case(
    require_signature: bool,
) -> (
    tempfile::TempDir,
    tempfile::TempDir,
    BrokerHandle,
    Client,
    OperatorKey,
    WireUuid,
    WireUuid,
) {
    let (keys, logs) = operator_directories();
    let (broker, client, key, frozen, control) =
        operator_fixture(keys.path(), logs.path(), require_signature).await;
    (keys, logs, broker, client, key, frozen, control)
}

async fn sign_incident(
    client: &Client,
    key: &OperatorKey,
) -> (
    String,
    krabka_protocol::krabka::freeze::SetTopicFreezeResponse,
) {
    let cluster = cluster_id(client).await;
    let response = set_freeze(client, incident_request(key, &cluster, now_ms())).await;
    (cluster, response)
}
