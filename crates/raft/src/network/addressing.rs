//! The two lookups a peer send performs before it reaches the wire: which
//! address a voter's controller listener is on, and which api version goes
//! with the api key being sent.
//!
//! Both are pure functions over the static voter set and the KIP-595 codec
//! table, so they are kept apart from the connection cache that calls them.

use krabka_ids::{ApiKey, ApiVersion};
use krabka_metadata::voters::VoterSet;

use crate::{
    kraft::{transport::PeerApi, types::NodeId},
    types::controller_endpoint_addr,
};

/// Resolves a voter's controller-listener address from the voter set.
///
/// By convention this is the endpoint named `CONTROLLER`. If there is no such
/// endpoint, this function falls back to the first one.
pub(super) fn controller_addr(voters: &VoterSet, id: NodeId) -> Option<String> {
    let voter = voters.get(id)?;
    controller_endpoint_addr(&voter.endpoints)
}

/// KIP-595 api version for each api key.
///
/// The engine encodes every KIP-595 body at the one version its codec names,
/// so this reads those constants rather than restating them. A peer send
/// negotiates down from it when the peer advertises less (`negotiation`), so
/// the version on the header and the version in the bytes cannot drift apart.
pub(crate) fn api_version_for(key: ApiKey) -> ApiVersion {
    ApiVersion(PeerApi::from_api_key(key.get()).map_or(0, PeerApi::version))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kraft::transport::api_key;

    #[bon::builder]
    fn voter_fixture(endpoints: Vec<krabka_metadata::VoterEndpoint>) -> krabka_metadata::Voter {
        krabka_metadata::Voter {
            id: NodeId(7),
            directory_id: uuid::Uuid::nil(),
            endpoints,
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        }
    }

    #[test]
    fn controller_addr_prefers_controller_endpoint_and_reports_unknown_voter() {
        let voters = VoterSet::from_voters([voter_fixture()
            .endpoints(vec![
                krabka_metadata::VoterEndpoint {
                    name: "REPLICATION".into(),
                    host: "replication-host".into(),
                    port: 9092,
                },
                krabka_metadata::VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "controller-host".into(),
                    port: 9093,
                },
            ])
            .call()]);

        assert2::assert!(
            controller_addr(&voters, NodeId(7)) == Some("controller-host:9093".to_string())
        );
        assert2::assert!(controller_addr(&voters, NodeId(8)) == None);
    }

    #[test]
    fn controller_addr_falls_back_to_first_endpoint() {
        let voters = VoterSet::from_voters([voter_fixture()
            .endpoints(vec![krabka_metadata::VoterEndpoint {
                name: "PLAINTEXT".into(),
                host: "only-host".into(),
                port: 9094,
            }])
            .call()]);

        assert2::assert!(controller_addr(&voters, NodeId(7)) == Some("only-host:9094".to_string()));
    }

    #[test]
    fn api_version_for_matches_kip595_codecs() {
        for (_case, key, want) in [
            ("vote", api_key::VOTE, 2),
            ("begin quorum epoch", api_key::BEGIN_QUORUM_EPOCH, 1),
            ("end quorum epoch", api_key::END_QUORUM_EPOCH, 1),
            ("fetch snapshot", api_key::FETCH_SNAPSHOT, 1),
            ("fetch", api_key::FETCH, 18),
            ("unknown API", -123, 0),
        ] {
            assert2::assert!(api_version_for(ApiKey(key)) == want);
        }
    }
}
