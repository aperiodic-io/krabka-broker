//! Translation of the KIP-853 voter set between its `krabka-metadata` form and
//! the `VotersRecord` control record a snapshot carries on disk.
//!
//! The writer and the reader both need this translation and neither owns it, so
//! it lives in its own module. Every range check that rejects a malformed voter
//! id or `kraft.version` bound is here.

use {
    crate::{
        error::RaftError,
        voter_wire::{from_wire, record, to_wire},
    },
    krabka_metadata::{NodeId, VoterSet, voters::KRaftVersionRange},
    krabka_protocol::owned::voters_record::VotersRecord as WireVotersRecord,
    uuid::Uuid,
};

pub(super) fn voter_set_to_wire(voters: &VoterSet) -> Result<WireVotersRecord, RaftError> {
    let voters = voters
        .iter()
        .map(|voter| {
            let id = i32::try_from(voter.id.0)
                .map_err(|_| RaftError::ChangeRejected("snapshot voter id exceeds int32".into()))?;
            let min = i16::try_from(voter.kraft_version.min).map_err(|_| {
                RaftError::ChangeRejected("snapshot minimum kraft.version exceeds int16".into())
            })?;
            let max = i16::try_from(voter.kraft_version.max).map_err(|_| {
                RaftError::ChangeRejected("snapshot maximum kraft.version exceeds int16".into())
            })?;
            Ok(to_wire(voter, id, min, max))
        })
        .collect::<Result<Vec<_>, RaftError>>()?;
    Ok(record(voters))
}

pub(super) fn voter_set_from_wire(record: &WireVotersRecord) -> Result<VoterSet, RaftError> {
    let voters = record
        .voters
        .iter()
        .map(|voter| {
            let id = u64::try_from(voter.voter_id)
                .map_err(|_| RaftError::ChangeRejected("negative voter id in snapshot".into()))?;
            let min = u16::try_from(voter.k_raft_version_feature.min_supported_version).map_err(
                |_| RaftError::ChangeRejected("negative minimum kraft.version in snapshot".into()),
            )?;
            let max = u16::try_from(voter.k_raft_version_feature.max_supported_version).map_err(
                |_| RaftError::ChangeRejected("negative maximum kraft.version in snapshot".into()),
            )?;
            if min > max {
                return Err(RaftError::ChangeRejected(
                    "inverted kraft.version range in snapshot".into(),
                ));
            }
            Ok(from_wire(
                voter,
                NodeId(id),
                Uuid::from_bytes(voter.voter_directory_id.0),
                KRaftVersionRange { min, max },
            ))
        })
        .collect::<Result<Vec<_>, RaftError>>()?;
    let voter_count = voters.len();
    let voters = VoterSet::from_voters(voters);
    if voters.len() != voter_count {
        return Err(RaftError::ChangeRejected(
            "duplicate voter id in snapshot".into(),
        ));
    }
    Ok(voters)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        krabka_metadata::{Voter, VoterEndpoint},
    };

    #[test]
    fn snapshot_voters_wire_round_trip_and_version_range() {
        let voter = Voter {
            id: NodeId(2),
            directory_id: Uuid::nil(),
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9092,
            }],
            kraft_version: KRaftVersionRange { min: 1, max: 1 },
        };
        let set = VoterSet::from_voters([voter]);
        let wire = voter_set_to_wire(&set).unwrap();
        assert2::assert!(wire.version == 0);
        assert2::assert!(wire.voters[0].k_raft_version_feature.min_supported_version == 1);
        assert2::assert!(wire.voters[0].k_raft_version_feature.max_supported_version == 1);

        // min == max must succeed
        let decoded = voter_set_from_wire(&wire).unwrap();
        assert2::assert!(decoded == set);

        // min > max must fail
        let mut inverted = wire.clone();
        inverted.voters[0]
            .k_raft_version_feature
            .min_supported_version = 2;
        inverted.voters[0]
            .k_raft_version_feature
            .max_supported_version = 1;
        assert2::assert!(voter_set_from_wire(&inverted).is_err());
    }
}
