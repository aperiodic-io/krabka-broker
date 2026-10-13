//! The operator signature, from the command that signs it to the registry entry
//! an auditor re-verifies after a restart.
//!
//! `freeze.require_signature` governs the freeze direction alone: a freeze is
//! the safe direction and has to be reachable in one command, while a thaw
//! always demands the proof. These cases hold that asymmetry in place, and they
//! hold the replay defence with it -- a signature captured from a freeze must
//! not authorize the thaw that lifts it.

use {
    crate::{
        control_plane::{cluster_id, freeze_request, set_freeze, wait_for_registry_len},
        signing::{SignedFreeze, signed_request, verifies_locally},
        support::{
            self, OperatorKey,
            client::{BrokerWithClient, connect_to_broker},
        },
        wire::{
            CONTROL, accepted, create_controlled_topic, create_topic, finish_with_control_record,
            now_ms, produce_outcome, refused, wait_controlled_partitions,
        },
    },
    assert2::check,
    krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle, codes},
    krabka_client_core::Client,
    krabka_protocol::{
        krabka::freeze::{DescribedTopicFreeze, PATTERN_TYPE_LITERAL, SetTopicFreezeRequest},
        primitives::uuid::Uuid as WireUuid,
    },
    std::path::Path,
};

async fn check_frozen_then_shutdown(
    broker: BrokerHandle,
    client: &Client,
    frozen: WireUuid,
    control: WireUuid,
) {
    check!(
        produce_outcome(&broker, client, "orders", frozen).await
            == refused("literal", "orders", "incident", 0)
    );
    check!(produce_outcome(&broker, client, CONTROL, control).await == accepted(1));
    broker.shutdown().await;
}

/// [`support::start_with_operator_key`] with `freeze.require_signature` on.
///
/// The harness helper takes the default, and a running broker's configuration
/// is not mutable, so the cases that need the strict setting build it here.
/// Everything else is what the harness builds: the same trust set, the same
/// single-approver break-glass set, the same plaintext listener.
async fn start_requiring_signatures(dir: &Path, key: &OperatorKey) -> BrokerWithClient {
    let mut config = BrokerConfig::for_tests(dir.to_path_buf());
    config.operator_keys = krabka_broker::operator_keys::OperatorKeys::load(&[key.entry()])
        .expect("load the operator trust set");
    config.break_glass.approvers = vec![support::ANONYMOUS.to_owned()];
    config.freeze.require_signature = true;
    let broker = Broker::start(config).await.expect("broker start");
    let client = connect_to_broker(&broker).await;
    BrokerWithClient { broker, client }
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
#[yare::parameterized(
    attestation = { false },
    required_signature = { true },
)]
#[test_macro(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn require_signature_decides_whether_an_unsigned_freeze_is_accepted(require_signature: bool) {
    let (code, after) = if require_signature {
        (codes::OPERATOR_SIGNATURE_REQUIRED, accepted(2))
    } else {
        (codes::NONE, refused("literal", "orders", "incident", 1))
    };
    let OperatorCase {
        keys: _keys,
        logs: _logs,
        broker,
        client,
        frozen,
        control,
        ..
    } = operator_case(require_signature).await;
    check!(produce_outcome(&broker, &client, "orders", frozen).await == accepted(1));

    let response = set_freeze(
        &client,
        freeze_request(PATTERN_TYPE_LITERAL, "orders", "incident"),
    )
    .await;
    check!(response.error_code == code, "{response:?}");
    wait_for_registry_len(&client, usize::from(code == codes::NONE)).await;

    check!(produce_outcome(&broker, &client, "orders", frozen).await == after);
    finish_with_control_record(broker, &client, control).await;
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
    let OperatorCase {
        keys: _keys,
        logs: _logs,
        broker,
        client,
        key,
        frozen,
        control,
    } = operator_case(false).await;

    let FreshIncident {
        cluster,
        set_at_ms,
        request,
    } = fresh_incident_request(&client, &key).await;
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
#[yare::parameterized(
    default = { false },
    required_signature = { true },
)]
#[test_macro(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn an_unsigned_thaw_is_refused_whatever_require_signature_says(require_signature: bool) {
    let OperatorCase {
        keys: _keys,
        logs: _logs,
        broker,
        client,
        key,
        frozen,
        control,
    } = operator_case(require_signature).await;

    let SignedIncident { response, .. } = sign_incident(&client, &key).await;
    check!(response.error_code == codes::NONE, "{response:?}");
    wait_for_registry_len(&client, 1).await;

    let thaw = set_freeze(&client, SetTopicFreezeRequest {
        scope: "orders".to_owned(),
        pattern_type: PATTERN_TYPE_LITERAL,
        frozen: false,
        reason: "let me back in".to_owned(),
        proposal_id: WireUuid(*uuid::Uuid::new_v4().as_bytes()),
        set_at_ms: now_ms(),
        ..SetTopicFreezeRequest::default()
    })
    .await;
    check!(
        thaw.error_code == codes::OPERATOR_SIGNATURE_REQUIRED,
        "{thaw:?}"
    );

    check!(wait_for_registry_len(&client, 1).await[0].scope == "orders");
    check!(
        produce_outcome(&broker, &client, "orders", frozen).await
            == refused("literal", "orders", "incident", 0)
    );
    finish_with_control_record(broker, &client, control).await;
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
    let case = operator_case(false).await;
    let (broker, client, key) = (case.broker, case.client, case.key);
    let (frozen, control) = (case.frozen, case.control);

    let FreshIncident {
        set_at_ms,
        request: freeze,
        ..
    } = fresh_incident_request(&client, &key).await;
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
        let thaw = set_freeze(&client, SetTopicFreezeRequest {
            scope: "orders".to_owned(),
            pattern_type: PATTERN_TYPE_LITERAL,
            frozen: false,
            reason: "incident".to_owned(),
            proposal_id: WireUuid::ZERO,
            set_at_ms: replay_at_ms,
            key_id: key.key_id.clone(),
            signature: captured.clone(),
            ..SetTopicFreezeRequest::default()
        })
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
    let OperatorDirectories { keys, logs } = operator_directories();
    let key = support::mint_operator_key(keys.path(), "alice-yubi", support::ANONYMOUS);
    let started = support::start_with_operator_key(logs.path(), &key).await;
    let broker = started.broker;
    let client = started.client;
    let mut config = started.config;
    create_topic(&broker, &client, "orders").await;
    create_topic(&broker, &client, CONTROL).await;

    let SignedIncident { cluster, response } = sign_incident(&client, &key).await;
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
    let client = connect_to_broker(&broker).await;
    wait_controlled_partitions(&broker, "orders").await;

    let after = wait_for_registry_len(&client, 1).await;
    check!(after == before);
    check!(cluster_id(&client).await == cluster);
    check!(verifies_locally(&key, &cluster, &after[0]));

    let frozen = support::topic_id_for(&client, "orders").await;
    let control = support::topic_id_for(&client, CONTROL).await;
    check_frozen_then_shutdown(broker, &client, frozen, control).await;
}

/// A running broker, its client, the operator key it trusts and the ids of the
/// frozen and control topics.
struct OperatorFixture {
    broker: BrokerHandle,
    client: Client,
    key: OperatorKey,
    frozen: WireUuid,
    control: WireUuid,
}

async fn operator_fixture(keys: &Path, logs: &Path, require_signature: bool) -> OperatorFixture {
    let key = support::mint_operator_key(keys, "alice-yubi", support::ANONYMOUS);
    let BrokerWithClient { broker, client } = if require_signature {
        start_requiring_signatures(logs, &key).await
    } else {
        let started = support::start_with_operator_key(logs, &key).await;
        BrokerWithClient {
            broker: started.broker,
            client: started.client,
        }
    };
    let (frozen, control) = create_controlled_topic(&broker, &client, "orders").await;
    OperatorFixture {
        broker,
        client,
        key,
        frozen,
        control,
    }
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

/// The cluster id, signing time and signed freeze request of a fresh incident.
struct FreshIncident {
    cluster: String,
    set_at_ms: i64,
    request: SetTopicFreezeRequest,
}

async fn fresh_incident_request(client: &Client, key: &OperatorKey) -> FreshIncident {
    let cluster = cluster_id(client).await;
    let set_at_ms = now_ms();
    let request = incident_request(key, &cluster, set_at_ms);
    FreshIncident {
        cluster,
        set_at_ms,
        request,
    }
}

/// The signing keys directory and the persistent broker logs directory.
struct OperatorDirectories {
    keys: tempfile::TempDir,
    logs: tempfile::TempDir,
}

/// Keep the signing keys and persistent broker logs alive for the whole case.
fn operator_directories() -> OperatorDirectories {
    let keys = tempfile::tempdir().expect("tempdir");
    let logs = tempfile::tempdir().expect("tempdir");
    OperatorDirectories { keys, logs }
}

/// Both directories beside the operator fixture, in the original local drop
/// order.
struct OperatorCase {
    keys: tempfile::TempDir,
    logs: tempfile::TempDir,
    broker: BrokerHandle,
    client: Client,
    key: OperatorKey,
    frozen: WireUuid,
    control: WireUuid,
}

/// Own both directories beside the operator fixture, in the original local drop order.
async fn operator_case(require_signature: bool) -> OperatorCase {
    let OperatorDirectories { keys, logs } = operator_directories();
    let OperatorFixture {
        broker,
        client,
        key,
        frozen,
        control,
    } = operator_fixture(keys.path(), logs.path(), require_signature).await;
    OperatorCase {
        keys,
        logs,
        broker,
        client,
        key,
        frozen,
        control,
    }
}

/// The cluster id and the answer to a signed freeze of `orders`.
struct SignedIncident {
    cluster: String,
    response: krabka_protocol::krabka::freeze::SetTopicFreezeResponse,
}

async fn sign_incident(client: &Client, key: &OperatorKey) -> SignedIncident {
    let cluster = cluster_id(client).await;
    let response = set_freeze(client, incident_request(key, &cluster, now_ms())).await;
    SignedIncident { cluster, response }
}
