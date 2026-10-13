// rustc 1.95 clippy::pedantic ICEs on these test files (an upstream bug
// in clippy's body-analysis pass that also bites `acl_handlers.rs` and
// `admin_handlers.rs`). Disable pedantic locally; the rest of the
// workspace still enforces the full pedantic gate.

//! KIP-429 batch-4 T4A: broker-side `JoinGroup` protocol-set negotiation tests.

mod support;

use {
    crate::support::{
        classic::{ClassicTimeouts, classic_join_request, join_protocol},
        client::connect_owned,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        join_group_request::JoinGroupRequest, join_group_response::JoinGroupResponse,
    },
    std::time::Duration,
};

// Kafka error codes consumed by these tests.
const ERR_NONE: i16 = 0;
const ERR_INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
const ERR_MEMBER_ID_REQUIRED: i16 = 79;

/// Boots a single-broker no-auth test cluster. It returns the handle, a shared
/// bootstrap address string, and the tempdir guard.
///
/// Tests build a client per member with `connect_client`, because the broker
/// processes the requests on one TCP connection in sequence. Two concurrent
/// `JoinGroup` waits over one `Client` would deadlock the second member behind
/// the first member's `INITIAL_REBALANCE_DELAY` wait.
use crate::support::start_group_coordinator as start_broker;

/// Builds a fresh `Client`, and therefore a fresh TCP connection, against
/// `bootstrap`. Each member in a concurrent test gets its own client.
async fn connect_client(bootstrap: &str, client_id: &str) -> Client {
    connect_owned(bootstrap, client_id, "client build").await
}

/// Builds a `JoinGroup` request that proposes `protocols` in caller order,
/// with `protocol_type` and `member_id`. `session_timeout` and
/// `rebalance_timeout` are short, so a stuck rebalance fails the test quickly
/// instead of holding the runtime.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ProtocolJoinSetup<'a> {
    #[default("g")]
    group_id: &'a str,
    member_id: &'a str,
    #[default("consumer")]
    protocol_type: &'a str,
    protocols: &'a [ProposedProtocol<'a>],
}

/// A protocol a member proposes: its name and its metadata.
#[derive(Clone, Copy)]
struct ProposedProtocol<'a> {
    name: &'a str,
    metadata: &'a [u8],
}

impl<'a> ProposedProtocol<'a> {
    const fn new(name: &'a str, metadata: &'a [u8]) -> Self { Self { name, metadata } }
}

#[bon::builder]
fn join_group_request(
    #[builder(default = "g")] group_id: &'_ str,
    #[builder(default)] member_id: &'_ str,
    #[builder(default = "consumer")] protocol_type: &'_ str,
    #[builder(default)] protocols: &'_ [ProposedProtocol<'_>],
) -> JoinGroupRequest {
    let setup = ProtocolJoinSetup {
        group_id,
        member_id,
        protocol_type,
        protocols,
    };
    JoinGroupRequest {
        group_instance_id: None,
        ..classic_join_request()
            .group_id(setup.group_id.to_string())
            .member_id(setup.member_id.to_string())
            .timeouts(ClassicTimeouts {
                rebalance: krabka_units::millis(60_000),
                ..Default::default()
            })
            .protocol_type(setup.protocol_type.to_string())
            .protocols(
                setup
                    .protocols
                    .iter()
                    .map(|protocol| {
                        join_protocol(
                            protocol.name.to_string(),
                            Bytes::copy_from_slice(protocol.metadata),
                        )
                    })
                    .collect(),
            )
            .call()
    }
}

/// First-round `JoinGroup` with an empty `member_id`. The broker replies with
/// `MEMBER_ID_REQUIRED (79)` and the member id that it generated (KIP-394).
/// This function asserts both, and returns the member id for the second
/// round.
async fn bootstrap_member_id(
    client: &Client,
    group_id: &str,
    protocol_type: &str,
    protocols: &[ProposedProtocol<'_>],
) -> String {
    let req = join_group_request()
        .group_id(group_id)
        .protocol_type(protocol_type)
        .protocols(protocols)
        .call();
    let resp = client
        .send(req)
        .await
        .expect("first JoinGroup must round-trip");
    assert!(
        resp.error_code == ERR_MEMBER_ID_REQUIRED,
        "first JoinGroup (empty member_id) must return MEMBER_ID_REQUIRED (79), got {resp:?}"
    );
    assert!(
        !resp.member_id.is_empty(),
        "broker must return a non-empty generated member_id on MEMBER_ID_REQUIRED"
    );
    resp.member_id
}

/// Second-round `JoinGroup` with the member id that the broker supplied. It
/// returns the raw response, so the caller can assert on `error_code` and
/// `protocol_name`.
async fn second_join(
    client: &Client,
    group_id: &str,
    member_id: &str,
    protocol_type: &str,
    protocols: &[ProposedProtocol<'_>],
) -> JoinGroupResponse {
    let req = join_group_request()
        .group_id(group_id)
        .member_id(member_id)
        .protocol_type(protocol_type)
        .protocols(protocols)
        .call();
    client
        .send(req)
        .await
        .expect("second JoinGroup must round-trip")
}

/// Two-step `JoinGroup` against a group that is already stable, or that has a
/// single member. It bootstraps a member id, then joins again at once.
///
/// The second call blocks for up to `INITIAL_REBALANCE_DELAY`, about 3 s,
/// before the broker completes the rebalance and returns NONE. It returns the
/// full response.
async fn full_join(
    client: &Client,
    group_id: &str,
    protocol_type: &str,
    protocols: &[ProposedProtocol<'_>],
) -> JoinGroupResponse {
    let member_id = bootstrap_member_id(client, group_id, protocol_type, protocols).await;
    second_join(client, group_id, &member_id, protocol_type, protocols).await
}

/// Members A and B propose disjoint protocol lists: `range` only against
/// `cooperative-sticky` only. Kafka's `supportsProtocols` gate turns away
/// the member that joins second with `INCONSISTENT_GROUP_PROTOCOL (23)`, and
/// the group rebalances without it, so the other member completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_intersection_returns_inconsistent_group_protocol() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let group_id = "cg-empty-intersection";

    let [resp_a, resp_b] = race_consumer_joins(&bootstrap, group_id, [
        RacingMember {
            client_id: "member-a",
            protocols: RANGE_ONLY,
            timeout_context: "member A second JoinGroup timed out",
            join_context: "member A task panic",
        },
        RacingMember {
            client_id: "member-b",
            protocols: COOPERATIVE_ONLY,
            timeout_context: "member B second JoinGroup timed out",
            join_context: "member B task panic",
        },
    ])
    .await;
    handle.shutdown().await;

    let mut codes = [resp_a.error_code, resp_b.error_code];
    codes.sort_unstable();
    assert!(
        codes == [ERR_NONE, ERR_INCONSISTENT_GROUP_PROTOCOL],
        "exactly one of (A, B) must return INCONSISTENT_GROUP_PROTOCOL (23); got A={resp_a:?} B={resp_b:?}"
    );
}

/// Three members. A and B vote first for `cooperative-sticky`, and C votes
/// first for `range`. Both names are in every member's list, so the
/// intersection is `{cooperative-sticky, range}`. `cooperative-sticky` wins by
/// 2 votes to 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vote_picks_cooperative_when_majority() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let group_id = "cg-vote-cooperative";

    let [resp_a, resp_b, resp_c] = race_consumer_joins(&bootstrap, group_id, [
        RacingMember {
            client_id: "member-a",
            protocols: COOPERATIVE_FIRST,
            timeout_context: "member A second JoinGroup timed out",
            join_context: "member A task panic",
        },
        RacingMember {
            client_id: "member-b",
            protocols: COOPERATIVE_FIRST,
            timeout_context: "member B second JoinGroup timed out",
            join_context: "member B task panic",
        },
        RacingMember {
            client_id: "member-c",
            protocols: RANGE_FIRST,
            timeout_context: "member C second JoinGroup timed out",
            join_context: "member C task panic",
        },
    ])
    .await;
    handle.shutdown().await;

    for (label, resp) in [("A", &resp_a), ("B", &resp_b), ("C", &resp_c)] {
        assert!(
            resp.error_code == ERR_NONE,
            "member {label} must succeed, got {resp:?}"
        );
        assert!(
            resp.protocol_name.as_deref() == Some("cooperative-sticky"),
            "member {label} must see protocol_name=cooperative-sticky (2 votes vs 1 for range), got {resp:?}"
        );
    }
}

/// Two members, with one vote each, and both names in the intersection. The
/// tie must break lexicographically. `'c' < 'r'`, so `cooperative-sticky`
/// wins.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vote_ties_broken_lexicographically() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let group_id = "cg-tie";

    // One TCP connection per racing member.
    let [resp_a, resp_b] = race_consumer_joins(&bootstrap, group_id, [
        RacingMember {
            client_id: "member-a",
            protocols: RANGE_FIRST,
            timeout_context: "member A second JoinGroup timed out",
            join_context: "member A task panic",
        },
        RacingMember {
            client_id: "member-b",
            protocols: COOPERATIVE_FIRST,
            timeout_context: "member B second JoinGroup timed out",
            join_context: "member B task panic",
        },
    ])
    .await;
    handle.shutdown().await;

    for (label, resp) in [("A", &resp_a), ("B", &resp_b)] {
        assert!(
            resp.error_code == ERR_NONE,
            "member {label} must succeed, got {resp:?}"
        );
        assert!(
            resp.protocol_name.as_deref() == Some("cooperative-sticky"),
            "tie must break lexicographically to cooperative-sticky ('c' < 'r'), got {resp:?}"
        );
    }
}

/// A single member that proposes `[range]` lands on `range`. This is the
/// simple check that the negotiation primitive still handles the simplest
/// case.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_member_picks_its_first_protocol() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let client = connect_client(&bootstrap, "single-member").await;
    let resp = full_join(&client, "cg-single", "consumer", &[ProposedProtocol::new(
        "range", b"",
    )])
    .await;
    handle.shutdown().await;

    assert!(
        resp.error_code == ERR_NONE,
        "single-member JoinGroup must succeed, got {resp:?}"
    );
    assert!(
        resp.protocol_name.as_deref() == Some("range"),
        "single-member must land on its only proposed protocol, got {resp:?}"
    );
}

/// Member A establishes the group with `protocol_type = "consumer"`. Member B
/// then joins with `protocol_type = "stream"`. The broker must reject B with
/// `INCONSISTENT_GROUP_PROTOCOL` before B enters the rebalance. The
/// type-mismatch check runs on the second-round join.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_type_mismatch_rejected() {
    let (handle, bootstrap, _tempdir) = start_broker().await;
    let client_a = connect_client(&bootstrap, "member-a").await;
    let client_b = connect_client(&bootstrap, "member-b").await;
    let group_id = "cg-type-mismatch";

    // Member A: full two-step, completes the rebalance. After this the
    // group has `protocol_type = Some("consumer")` and is Stable.
    let resp_a = full_join(&client_a, group_id, "consumer", &[ProposedProtocol::new(
        "range", b"",
    )])
    .await;
    assert!(
        resp_a.error_code == ERR_NONE,
        "member A must complete first-round rebalance, got {resp_a:?}"
    );
    assert!(resp_a.protocol_name.as_deref() == Some("range"));

    // Member B joins with `protocol_type = "stream"`. Kafka's
    // `supportsProtocols` gate runs before a member id is handed out, so
    // the first join already fails, under the unknown member id.
    let resp_b = client_b
        .send(
            join_group_request()
                .group_id(group_id)
                .protocol_type("stream")
                .protocols(&[ProposedProtocol::new("range", b"")])
                .call(),
        )
        .await
        .expect("JoinGroup must round-trip");
    handle.shutdown().await;

    assert!(
        resp_b
            == JoinGroupResponse {
                error_code: ERR_INCONSISTENT_GROUP_PROTOCOL,
                protocol_name: None,
                ..JoinGroupResponse::default()
            },
        "member B with protocol_type=stream must hit INCONSISTENT_GROUP_PROTOCOL on a consumer group, got {resp_b:?}"
    );
}

type Protocols = &'static [ProposedProtocol<'static>];

/// One member of a race: its client id, the protocols it proposes, and the
/// messages for a timed-out or panicked second join.
struct RacingMember {
    client_id: &'static str,
    protocols: Protocols,
    timeout_context: &'static str,
    join_context: &'static str,
}

const RANGE_ONLY: Protocols = &[ProposedProtocol::new("range", b"")];
const COOPERATIVE_ONLY: Protocols = &[ProposedProtocol::new("cooperative-sticky", b"")];
const RANGE_FIRST: Protocols = &[
    ProposedProtocol::new("range", b""),
    ProposedProtocol::new("cooperative-sticky", b""),
];
const COOPERATIVE_FIRST: Protocols = &[
    ProposedProtocol::new("cooperative-sticky", b""),
    ProposedProtocol::new("range", b""),
];

// Connect every member, then bootstrap every id, then spawn every second join.
// Awaiting the tasks in input order preserves the original racing window.
async fn race_consumer_joins<const N: usize>(
    bootstrap: &str,
    group: &str,
    members: [RacingMember; N],
) -> [JoinGroupResponse; N] {
    let mut clients = Vec::with_capacity(N);
    for member in &members {
        clients.push(connect_client(bootstrap, member.client_id).await);
    }
    let mut member_ids = Vec::with_capacity(N);
    for (client, member) in clients.iter().zip(&members) {
        member_ids.push(bootstrap_member_id(client, group, "consumer", member.protocols).await);
    }
    let groups: Vec<_> = (0..N).map(|_| group.to_owned()).collect();
    let mut joins = Vec::with_capacity(N);
    for (((client, member_id), group), member) in
        clients.into_iter().zip(member_ids).zip(groups).zip(members)
    {
        let RacingMember {
            protocols,
            timeout_context,
            join_context,
            ..
        } = member;
        let task = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_secs(10),
                second_join(&client, &group, &member_id, "consumer", protocols),
            )
            .await
            .expect(timeout_context)
        });
        joins.push((task, join_context));
    }
    let mut responses = Vec::with_capacity(N);
    for (join, context) in joins {
        responses.push(join.await.expect(context));
    }
    responses
        .try_into()
        .expect("one response per joining member")
}
