// docs:begin quorum_state
use assert2::assert;
use krabka_metadata::{KRaftVersionRange, Voter, VoterEndpoint, VoterSet};
use krabka_raft::kraft::{NodeId, QuorumState, QuorumStateMachine};
use krabka_units::prelude::millis;
use uuid::Uuid;

fn main() {
    let node = NodeId(1);
    let voters = VoterSet::from_voters([Voter {
        id: node,
        directory_id: Uuid::new_v4(),
        endpoints: vec![VoterEndpoint {
            name: "CONTROLLER".into(),
            host: "127.0.0.1".into(),
            port: 9093,
        }],
        kraft_version: KRaftVersionRange { min: 0, max: 1 },
    }]);
    let state = QuorumState::bootstrap(Uuid::new_v4(), voters);
    let machine = QuorumStateMachine::new(node, state, millis(1_000));
    assert!(machine.is_voter());
}
// docs:end quorum_state
