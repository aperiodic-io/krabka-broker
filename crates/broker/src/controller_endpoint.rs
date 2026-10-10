//! Where a broker sends the RPCs KIP-919 puts on the controller listener.
//!
//! `BrokerHeartbeat` and `AssignReplicasToDirs` are both addressed to the
//! active controller, and Kafka carries both on the controller's CONTROLLER
//! listener. That is the one endpoint a controller-only node publishes for
//! itself, so a broker that resolves the leader through `image.broker()` and
//! dials the inter-broker listener reaches nothing at all in a role-separated
//! cluster: `register_broker` deliberately skips a node whose `process.roles`
//! exclude `broker`.
//!
//! Resolution here is the KIP-853 voter set, then the statically configured
//! quorum -- the same two-tier lookup `ControllerHandle::voter_addr` uses for
//! the leader it forwards a rejected `submit_change` to.

/// The listener name a controller advertises for the RPCs its peers address
/// to it.
pub(crate) const CONTROLLER_LISTENER_NAME: &str = "CONTROLLER";

/// One voter's controller-listener endpoint, by the same convention
/// `krabka_raft::Node::controller_addr` uses to dial a raft peer: the endpoint
/// named CONTROLLER, or the first one when the voter advertises no such name.
///
/// The fallback is what lets a controller whose `controller.listener.names` is
/// something other than the literal `CONTROLLER` still be reachable. The
/// controller-registration decoder accepts any non-empty unique name, so the
/// literal is a convention rather than a guarantee.
fn controller_endpoint(endpoints: &[krabka_metadata::VoterEndpoint]) -> Option<(String, u16)> {
    endpoints
        .iter()
        .find(|endpoint| endpoint.name.eq_ignore_ascii_case(CONTROLLER_LISTENER_NAME))
        .or_else(|| endpoints.first())
        .map(|endpoint| (endpoint.host.clone(), endpoint.port))
}

/// Where to address a controller-listener RPC aimed at controller `leader`.
///
/// This is the KIP-853 voter set, then the statically configured quorum, which
/// is exactly how `ControllerHandle::voter_addr` resolves the leader it
/// forwards a rejected `submit_change` to. Reusing that resolution keeps these
/// RPCs on the same address the raft transport already reaches the leader on,
/// and avoids three traps that the leader's `ControllerRegistrationRecord`
/// would walk into:
///
/// * A controller registration is only published from `metadata.version` 15
///   upward (`ONLINE_DOWNGRADE_MIN_LEVEL`), so on a cluster finalized at 7-14
///   there is no record to read and nothing would ever be sent.
/// * `self_controller_registration_record` falls back to `controller_listen_addr`
///   for a node absent from its own `controller_quorum_voters`, which publishes
///   a wildcard bind address such as `0.0.0.0` for a dynamically joined
///   controller. Voter endpoints carry the advertised host instead.
/// * The record is written asynchronously after startup, so it lags the voter
///   set that raft is already dialling.
pub(crate) fn leader_endpoint(
    image: &krabka_metadata::MetadataImage,
    static_voters: &[(krabka_raft::NodeId, String)],
    leader: krabka_raft::NodeId,
) -> Option<(String, u16)> {
    image
        .voters()
        .get(leader)
        .and_then(|voter| controller_endpoint(&voter.endpoints))
        .or_else(|| {
            static_voters
                .iter()
                .find(|(node_id, _)| *node_id == leader)
                .and_then(|(_, endpoint)| crate::host_port::parse_host_port(endpoint))
        })
}

/// The id that the `index`th bootstrap server stands under, counted down from
/// `u64::MAX` as the raft transport numbers its own bootstrap address book.
///
/// Kafka gives bootstrap servers negative ids, below every node id. A
/// [`krabka_raft::NodeId`] is unsigned, so these ids are above every node id.
fn bootstrap_server_id(index: usize) -> krabka_raft::NodeId {
    krabka_raft::NodeId(u64::MAX - u64::try_from(index).unwrap_or(u64::MAX))
}

/// Every controller a broker-only node can address, in the order it tries
/// them: each voter of the KIP-853 voter set in the image, then each
/// statically configured voter that set does not name, then each bootstrap
/// server that no voter is already reached on, under [`bootstrap_server_id`].
///
/// A node of a dynamic quorum configures only bootstrap servers. They are all
/// it has until it reads the voter set from the log. After that, the voters
/// come first, because an answer names the leader by its node id. The
/// bootstrap servers stay at the end of the list. So a node whose voters are
/// all unreachable can still find the quorum, as Kafka's `RequestManager`
/// falls back to them when it has no leader to ask.
pub(crate) fn quorum_targets(
    image: &krabka_metadata::MetadataImage,
    static_voters: &[(krabka_raft::NodeId, String)],
    bootstrap_servers: &[String],
) -> Vec<(krabka_raft::NodeId, String)> {
    let mut targets: Vec<(krabka_raft::NodeId, String)> = image
        .voters()
        .iter()
        .filter_map(|voter| {
            controller_endpoint(&voter.endpoints)
                .map(|(host, port)| (voter.id, format!("{host}:{port}")))
        })
        .collect();
    for (id, endpoint) in static_voters {
        if targets.iter().all(|(known, _)| known != id) {
            targets.push((*id, endpoint.clone()));
        }
    }
    for (index, server) in bootstrap_servers.iter().enumerate() {
        if targets.iter().all(|(_, endpoint)| endpoint != server) {
            targets.push((bootstrap_server_id(index), server.clone()));
        }
    }
    targets
}

/// Everything a broker needs to open an authenticated connection to the
/// controller leader's controller listener.
///
/// The dialer is the one the raft transport uses for its peers, so a request
/// built from this travels the channel the quorum already authenticates on:
/// TLS and SASL when the controller listener is configured for them, plain TCP
/// when it is not. A bare `krabka_client_core::Client` would send plaintext
/// Kafka frames at a secured controller port and fail every attempt.
pub(crate) struct ControllerDialer {
    /// Shared outbound dialer that runs the controller listener's TLS / SASL.
    pub outbound_client: std::sync::Arc<crate::network::client::InterBrokerClient>,
    pub listener_protocol: krabka_security::ListenerProtocol,
    /// SNI and SASL server name for the controller listener, matching the one
    /// the raft dialer presents.
    pub server_name: String,
    /// The statically configured quorum, `controller_quorum_voters`. It backs
    /// the voter set as a source of the leader's address, for the window
    /// before the committed voter set names it. See [`leader_endpoint`].
    pub quorum_voters: Vec<(krabka_raft::NodeId, String)>,
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn endpoints(named: &[(&str, &str, u16)]) -> Vec<krabka_metadata::VoterEndpoint> {
        named
            .iter()
            .map(|&(name, host, port)| krabka_metadata::VoterEndpoint {
                name: name.to_string(),
                host: host.to_string(),
                port,
            })
            .collect()
    }

    /// These RPCs go to the leader's controller listener, so that is the
    /// endpoint to pick -- never a sibling listener advertised alongside it.
    /// A voter that names the listener differently, as a JVM controller with a
    /// custom `controller.listener.names` does, still has to be reachable, so
    /// the sole endpoint is used when no name matches.
    #[test]
    fn controller_endpoint_prefers_the_controller_listener_and_falls_back() {
        let cases = [
            (
                &[
                    ("BROKER", "broker-host", 9_092u16),
                    ("CONTROLLER", "ctrl-host", 9_093),
                ][..],
                Some(("ctrl-host".to_string(), 9_093)),
            ),
            // Named in another case: Kafka listener names are case-insensitive.
            (
                &[("controller", "ctrl-host", 9_093)][..],
                Some(("ctrl-host".to_string(), 9_093)),
            ),
            // A custom name: the only endpoint there is, so it is the one.
            (
                &[("CTRL", "ctrl-host", 9_093)][..],
                Some(("ctrl-host".to_string(), 9_093)),
            ),
            (&[][..], None),
        ];
        for (named, want) in cases {
            assert!(controller_endpoint(&endpoints(named)) == want, "{named:?}");
        }
    }

    fn image_with_voter(node: u64, host: &str, port: u16) -> krabka_metadata::MetadataImage {
        crate::test_support::voter_image(crate::test_support::VoterImageSetup {
            node: krabka_ids::NodeId(node),
            endpoints: endpoints(&[("CONTROLLER", host, port)]),
        })
    }

    /// The voter set answers first, and the configured quorum answers for a
    /// leader the committed voter set does not name yet.
    ///
    /// The fallback is what keeps a cluster finalized below
    /// `metadata.version` 15 heartbeating: no controller registration is
    /// published there at all, so an image-only resolution would fence every
    /// broker in the cluster.
    #[test]
    fn leader_endpoint_reads_the_voter_set_then_the_configured_quorum() {
        let static_voters = vec![
            (krabka_raft::NodeId(1), "static-one:9193".to_string()),
            (krabka_raft::NodeId(2), "static-two:9293".to_string()),
        ];
        let image = image_with_voter(1, "voter-one", 9_093);

        // Voter set wins for a leader it names.
        assert!(
            leader_endpoint(&image, &static_voters, krabka_raft::NodeId(1))
                == Some(("voter-one".to_string(), 9_093))
        );
        // A leader the voter set does not name falls back to the config.
        assert!(
            leader_endpoint(&image, &static_voters, krabka_raft::NodeId(2))
                == Some(("static-two".to_string(), 9_293))
        );
        // An image with no voters at all still resolves from the config.
        let empty = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(
            leader_endpoint(&empty, &static_voters, krabka_raft::NodeId(1))
                == Some(("static-one".to_string(), 9_193))
        );
        // A leader nothing knows about yields nothing.
        assert!(leader_endpoint(&image, &static_voters, krabka_raft::NodeId(9)).is_none());
    }

    /// The voter set comes first, then the configured voters it does not name,
    /// then the bootstrap servers that no voter is reached on. A dynamic quorum
    /// configures only bootstrap servers, so before its node has read the voter
    /// set they are the whole list.
    #[test]
    fn quorum_targets_put_voters_before_bootstrap_servers() {
        let node = krabka_raft::NodeId;
        let owned = |targets: &[(u64, &str)]| -> Vec<(krabka_raft::NodeId, String)> {
            targets
                .iter()
                .map(|&(id, endpoint)| (node(id), endpoint.to_owned()))
                .collect()
        };
        let empty = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let dynamic = image_with_voter(3001, "ducker12", 9_592);
        let bootstrap = ["ducker12:9592".to_owned(), "ducker13:9592".to_owned()];
        let static_voters = owned(&[(1, "static-one:9193"), (2, "static-two:9293")]);
        // (what, image, static voters, bootstrap servers, targets)
        let cases = [
            ("nothing configured", &empty, vec![], &[][..], vec![]),
            (
                "bootstrap servers before the voter set is known",
                &empty,
                vec![],
                &bootstrap[..],
                owned(&[(u64::MAX, "ducker12:9592"), (u64::MAX - 1, "ducker13:9592")]),
            ),
            (
                "the voter set takes over the bootstrap server it is reached on",
                &dynamic,
                vec![],
                &bootstrap[..],
                owned(&[(3001, "ducker12:9592"), (u64::MAX - 1, "ducker13:9592")]),
            ),
            (
                "configured voters the voter set does not name",
                &image_with_voter(1, "voter-one", 9_093),
                static_voters.clone(),
                &[][..],
                owned(&[(1, "voter-one:9093"), (2, "static-two:9293")]),
            ),
            (
                "a static quorum with no voter set",
                &empty,
                static_voters.clone(),
                &[][..],
                static_voters,
            ),
        ];
        for (what, image, static_voters, bootstrap, expected) in cases {
            assert!(
                quorum_targets(image, &static_voters, bootstrap) == expected,
                "{what}"
            );
        }
    }
}
