//! Field projection shared by log and snapshot voter records. Admission and
//! overflow policies remain with the caller that owns the wire format.

use {
    krabka_metadata::{NodeId, Voter, VoterEndpoint, voters::KRaftVersionRange},
    krabka_protocol::owned::voters_record::{
        Endpoint as WireVoterEndpoint, KRaftVersionFeature as WireKRaftVersionFeature,
        Voter as WireVoter, VotersRecord as WireVotersRecord,
    },
    uuid::Uuid,
};

pub(crate) fn to_wire(voter: &Voter, id: i32, min: i16, max: i16) -> WireVoter {
    WireVoter {
        voter_id: id,
        voter_directory_id: krabka_protocol::primitives::uuid::Uuid(*voter.directory_id.as_bytes()),
        endpoints: voter
            .endpoints
            .iter()
            .map(|endpoint| WireVoterEndpoint {
                name: endpoint.name.clone(),
                host: endpoint.host.clone(),
                port: endpoint.port,
                ..Default::default()
            })
            .collect(),
        k_raft_version_feature: WireKRaftVersionFeature {
            min_supported_version: min,
            max_supported_version: max,
            ..Default::default()
        },
        ..Default::default()
    }
}

pub(crate) fn record(voters: Vec<WireVoter>) -> WireVotersRecord {
    WireVotersRecord {
        version: 0,
        voters,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    }
}

pub(crate) fn from_wire(
    voter: &WireVoter,
    id: NodeId,
    directory_id: Uuid,
    kraft_version: KRaftVersionRange,
) -> Voter {
    Voter {
        id,
        directory_id,
        endpoints: voter
            .endpoints
            .iter()
            .map(|endpoint| VoterEndpoint {
                name: endpoint.name.clone(),
                host: endpoint.host.clone(),
                port: endpoint.port,
            })
            .collect(),
        kraft_version,
    }
}
