//! Construction of the joiner's own identity on the wire: the `CONTROLLER`
//! listener it advertises, the bootstrap server it dials next, and the
//! `AddRaftVoterRequest` body itself.
//!
//! These functions do no network I/O, which is why they are their own file:
//! both loops in this module build the same identity, and the unit tests can
//! check it without a socket. The one thing they read from the system is the
//! host name that a controller bound to a wildcard address advertises.

use {
    crate::host_port::{HostPort, advertised_host, local_host_name, parse_host_port},
    krabka_protocol::owned::add_raft_voter_request::{AddRaftVoterRequest, Listener},
};

/// The `CONTROLLER` listener of a controller bound to `bound`: the bound
/// address, or this machine's host name for a wildcard bind, as
/// [`crate::host_port::advertised_host`] decides.
pub(super) fn controller_listener(bound: std::net::SocketAddr) -> Listener {
    Listener {
        name: "CONTROLLER".to_string(),
        host: advertised_host(bound.ip(), local_host_name),
        port: bound.port(),
        ..Default::default()
    }
}

/// The controller endpoint a voter RPC should publish for this node.
///
/// A controller bound to a concrete address publishes that address: it is what
/// the socket actually answers on, and it is what the operator chose.
///
/// A controller bound to `0.0.0.0` has no address of its own. `advertised`,
/// this node's own `controller.quorum.voters` entry, is how the rest of the
/// cluster is configured to reach it, so that is the address to publish.
/// Without one, [`controller_listener`] publishes this machine's host name, as
/// Kafka publishes the canonical host name for a wildcard controller listener.
/// A loopback address would replace a committed endpoint that every other node
/// can reach with one that resolves, for whoever reads it, back to the reader,
/// so it is published only by a machine that reports no host name.
pub(super) fn advertised_controller_listener(
    advertised: Option<&str>,
    bound: std::net::SocketAddr,
) -> Listener {
    if !bound.ip().is_unspecified() {
        return controller_listener(bound);
    }
    let Some(HostPort { host, port }) = advertised
        .and_then(parse_host_port)
        .filter(|endpoint| !endpoint.host.is_empty())
    else {
        return controller_listener(bound);
    };
    Listener {
        name: "CONTROLLER".to_string(),
        host,
        port,
        ..Default::default()
    }
}

pub(super) fn select_bootstrap_server(bootstrap_servers: &[String], attempt: usize) -> &str {
    &bootstrap_servers[attempt % bootstrap_servers.len()]
}

/// Where a join request goes, and where that server came from.
pub(super) struct JoinTarget {
    /// The `<host>:<port>` to dial.
    pub(super) server: String,
    /// Whether `server` is a bootstrap server rather than the current leader.
    pub(super) from_bootstrap: bool,
}

/// The controller endpoint that a join request goes to: the current leader's
/// `CONTROLLER` endpoint, or a bootstrap server while no leader is known.
///
/// Kafka's observer sends `AddRaftVoter` to the leader that it fetches from
/// (`KafkaRaftClient.maybeSendAddVoterRequest` uses `state.leaderNode`). A
/// bootstrap server only helps to find that leader. After the leadership moves
/// away from it, a bootstrap server answers `NOT_LEADER_OR_FOLLOWER` to every
/// join, so a join that always dialed it would never finish.
///
/// `attempt` rotates across the bootstrap servers, and
/// [`JoinTarget::from_bootstrap`] says whether the target is a bootstrap
/// server.
pub(super) fn join_target(
    quorum: &krabka_raft::QuorumState,
    bootstrap_servers: &[String],
    attempt: usize,
) -> JoinTarget {
    let leader_endpoint = quorum
        .current_leader
        .and_then(|leader| quorum.voter_nodes.get(&leader))
        .and_then(|leader| {
            leader
                .endpoints
                .iter()
                .find(|endpoint| endpoint.name.eq_ignore_ascii_case("CONTROLLER"))
                .or_else(|| leader.endpoints.first())
        });
    match leader_endpoint {
        Some(endpoint) => JoinTarget {
            server: format!("{}:{}", endpoint.host, endpoint.port),
            from_bootstrap: false,
        },
        None => JoinTarget {
            server: select_bootstrap_server(bootstrap_servers, attempt).to_owned(),
            from_bootstrap: true,
        },
    }
}

pub(super) fn build_add_raft_voter_request(
    cluster_id: Option<uuid::Uuid>,
    voter_id: i32,
    directory_id: krabka_protocol::primitives::uuid::Uuid,
    listener: Listener,
    timeout_ms: i32,
) -> AddRaftVoterRequest {
    AddRaftVoterRequest {
        cluster_id: cluster_id.map(|u| u.to_string()),
        timeout_ms,
        voter_id,
        voter_directory_id: directory_id,
        listeners: vec![listener],
        ack_when_committed: false,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        bytes::BytesMut,
        krabka_protocol::{Decode, Encode, owned::add_raft_voter_request},
    };

    struct Endpoint {
        name: &'static str,
        port: u16,
    }

    struct Voter {
        id: u64,
        endpoints: &'static [Endpoint],
    }

    fn quorum(leader: Option<u64>, voters: &[Voter]) -> krabka_raft::QuorumState {
        krabka_raft::QuorumState {
            current_term: 3,
            current_leader: leader.map(krabka_raft::NodeId),
            voters: voters
                .iter()
                .map(|voter| krabka_raft::NodeId(voter.id))
                .collect(),
            voter_nodes: voters
                .iter()
                .map(|Voter { id, endpoints }| {
                    (krabka_raft::NodeId(*id), krabka_raft::Node {
                        directory_id: uuid::Uuid::from_u128(u128::from(*id)),
                        endpoints: endpoints
                            .iter()
                            .map(|Endpoint { name, port }| krabka_metadata::VoterEndpoint {
                                name: (*name).to_owned(),
                                host: "127.0.0.1".to_owned(),
                                port: *port,
                            })
                            .collect(),
                        kraft_version: krabka_metadata::KRaftVersionRange::default(),
                    })
                })
                .collect(),
            ..Default::default()
        }
    }

    fn two_voter_quorum(leader: Option<u64>) -> krabka_raft::QuorumState {
        quorum(leader, &[
            Voter {
                id: 1,
                endpoints: &[Endpoint {
                    name: "CONTROLLER",
                    port: 9001,
                }],
            },
            Voter {
                id: 2,
                endpoints: &[
                    Endpoint {
                        name: "REPLICATION",
                        port: 9102,
                    },
                    Endpoint {
                        name: "CONTROLLER",
                        port: 9002,
                    },
                ],
            },
        ])
    }

    fn leader_without_controller_quorum() -> krabka_raft::QuorumState {
        quorum(Some(1), &[Voter {
            id: 1,
            endpoints: &[Endpoint {
                name: "REPLICATION",
                port: 9101,
            }],
        }])
    }

    #[test]
    fn controller_listener_uses_bound_controller_endpoint() {
        let listener = controller_listener("192.0.2.10:19093".parse().unwrap());
        assert2::assert!((listener.name) == ("CONTROLLER"));
        assert2::assert!((listener.host) == ("192.0.2.10"));
        assert2::assert!((listener.port) == (19093));
    }

    /// A concrete bind is the address the socket answers on, so it is
    /// published even when the node also carries an advertised endpoint.
    #[test]
    fn a_bound_controller_publishes_the_address_it_answers_on() {
        let listener = advertised_controller_listener(
            Some("controller.example:19093"),
            "192.0.2.10:19093".parse().unwrap(),
        );

        assert!((listener.host.as_str(), listener.port) == ("192.0.2.10", 19093));
    }

    /// A wildcard bind has no address of its own, so the configured endpoint
    /// is published rather than the `127.0.0.1` guess.
    #[test]
    fn a_wildcard_bind_publishes_the_configured_endpoint() {
        let listener = advertised_controller_listener(
            Some("controller.example:19093"),
            "0.0.0.0:19093".parse().unwrap(),
        );

        assert!(
            (
                listener.name.as_str(),
                listener.host.as_str(),
                listener.port
            ) == ("CONTROLLER", "controller.example", 19093)
        );
    }

    /// With nothing configured, a wildcard bind publishes this machine's host
    /// name, as Kafka does. A controller of a dynamic quorum configures no
    /// voters, so this is what its voter updates publish, and a loopback
    /// address there would send every other node's heartbeats back to itself.
    #[test]
    fn a_wildcard_bind_with_nothing_configured_publishes_the_host_name() {
        let bound = "0.0.0.0:19093".parse().unwrap();

        assert!(
            advertised_controller_listener(None, bound)
                == Listener {
                    name: "CONTROLLER".to_owned(),
                    host: crate::host_port::local_host_name()
                        .unwrap_or_else(|| "127.0.0.1".to_owned()),
                    port: 19093,
                    ..Default::default()
                }
        );
    }

    /// An endpoint the port cannot be read out of is no better than nothing.
    #[yare::parameterized(
        missing_port = { "controller.example" },
        empty_port = { "controller.example:" },
        missing_host = { ":19093" },
    )]
    fn an_unparseable_advertised_endpoint_falls_back_to_the_bound_listener(endpoint: &str) {
        let bound = "0.0.0.0:19093".parse().unwrap();

        assert2::assert!(
            advertised_controller_listener(Some(endpoint), bound) == controller_listener(bound),
            "{endpoint}"
        );
    }

    #[test]
    fn select_bootstrap_server_wraps_attempts() {
        let servers: Vec<String> = ["127.0.0.1:9092", "127.0.0.1:9093", "127.0.0.1:9094"]
            .into_iter()
            .map(str::to_owned)
            .collect();

        assert2::assert!((select_bootstrap_server(&servers, 0)) == (servers[0].as_str()));
        assert2::assert!((select_bootstrap_server(&servers, 2)) == (servers[2].as_str()));
        assert2::assert!((select_bootstrap_server(&servers, 3)) == (servers[0].as_str()));
        assert2::assert!((select_bootstrap_server(&servers, 5)) == (servers[2].as_str()));
    }

    #[yare::parameterized(
        leader_on_second_bootstrap = {
            "the leader moved off the first bootstrap server",
            two_voter_quorum(Some(2)),
            0,
            ("127.0.0.1:9002", false),
        },
        leader_without_controller_endpoint = {
            "the leader has no CONTROLLER endpoint",
            leader_without_controller_quorum(),
            0,
            ("127.0.0.1:9101", false),
        },
        unknown_leader = {
            "no leader is known",
            two_voter_quorum(None),
            1,
            ("127.0.0.1:9002", true),
        },
        leader_not_in_voter_set = {
            "the leader is not in the voter set this node knows",
            two_voter_quorum(Some(7)),
            2,
            ("127.0.0.1:9001", true),
        },
    )]
    fn a_join_goes_to_the_known_leader_and_to_a_bootstrap_server_otherwise(
        case: &str,
        quorum: krabka_raft::QuorumState,
        attempt: usize,
        expected: (&str, bool),
    ) {
        let bootstrap: Vec<String> = ["127.0.0.1:9001", "127.0.0.1:9002"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        let target = join_target(&quorum, &bootstrap, attempt);
        assert2::assert!(
            (target.server.as_str(), target.from_bootstrap) == expected,
            "{case}"
        );
    }

    #[test]
    fn build_add_raft_voter_request_carries_joiner_identity() {
        let cluster_id = uuid::Uuid::from_u128(0xCAFE);
        let dir = uuid::Uuid::from_u128(0xD1E);
        let listener = controller_listener("127.0.0.1:19093".parse().unwrap());
        let req = build_add_raft_voter_request(
            Some(cluster_id),
            7,
            krabka_protocol::primitives::uuid::Uuid(*dir.as_bytes()),
            listener,
            1_234,
        );

        let cluster_id_string = cluster_id.to_string();
        assert!(matches!(
            (
                req.cluster_id.as_deref(),
                req.timeout_ms,
                req.voter_id,
                req.voter_directory_id.0,
                req.listeners.len(),
                req.listeners[0].name.as_str(),
                req.listeners[0].host.as_str(),
                req.listeners[0].port,
            ),
            (Some(id), 1_234, 7, directory_id, 1, "CONTROLLER", "127.0.0.1", 19093)
                if id == cluster_id_string && directory_id == *dir.as_bytes()
        ));
        assert!(!req.ack_when_committed);
    }

    #[test]
    fn build_add_raft_voter_request_encodes_ack_when_committed() {
        let listener = controller_listener("127.0.0.1:19093".parse().unwrap());
        let req = build_add_raft_voter_request(
            None,
            7,
            krabka_protocol::primitives::uuid::Uuid(*uuid::Uuid::from_u128(7).as_bytes()),
            listener,
            30_000,
        );
        let version = add_raft_voter_request::MAX_VERSION;
        let mut bytes = BytesMut::new();

        req.encode(&mut bytes, version).expect("encode request");
        let decoded =
            AddRaftVoterRequest::decode(&mut bytes.freeze(), version).expect("decode request");

        assert!(!decoded.ack_when_committed);
    }
}
