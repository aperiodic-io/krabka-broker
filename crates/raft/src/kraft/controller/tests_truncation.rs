use assert2::assert;
use krabka_protocol::{
    owned::k_raft_version_record::KRaftVersionRecord, records::metadata::control::ControlRecord,
};

use super::*;
use crate::kraft::{
    controller::{
        control_state::voter_set_to_wire,
        records::typed_control_batch,
        test_support::{EngineSetup, build_engine_only},
    },
    types::LogOffsetMetadata,
};

#[tokio::test]
async fn truncation_restores_histories_at_the_retained_batch_end() {
    for fetch_response in [false, true] {
        let (mut engine, _dir) = build_engine_only(EngineSetup {
            ids: &[NodeId(1), NodeId(2)],
            ..Default::default()
        });
        let voters = voter_set_to_wire(engine.controls.latest_voters());
        for version in [0, 1] {
            let mut batch = typed_control_batch(
                2,
                &[
                    ControlRecord::KRaftVersion(KRaftVersionRecord {
                        k_raft_version: version,
                        ..Default::default()
                    }),
                    ControlRecord::Voters(voters.clone()),
                ],
            )
            .unwrap();
            engine.log.append(&mut batch, 0).unwrap();
            engine.apply_control_batch(&batch).unwrap();
        }
        engine.advance_and_apply(Offset(2));
        assert!(engine.controls.latest_version() == 1);
        assert!(engine.controls.committed_version == 0);
        engine.on_event(Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(2),
            leader_epoch: 3,
        });
        let point = LogOffsetMetadata {
            offset: 3,
            epoch: 2,
        };
        if fetch_response {
            let response = wire::PeerResponse::Fetch(wire::FetchAnswer {
                error_code: 0,
                leader: wire::QuorumLeader {
                    leader_id: Some(NodeId(2)),
                    epoch: 3,
                    endpoint: None,
                },
                diverging: Some(point),
                snapshot_id: None,
                hwm: 2,
                log_start_offset: 0,
                records: bytes::Bytes::new(),
            })
            .encode();
            engine.on_fetch_response(NodeId(2), &response);
        } else {
            engine.execute_one_local(Action::TruncateTo(point));
        }
        assert!(engine.log.log_end_offset() == Offset(2));
        assert!(engine.log.hwm() == Offset(2));
        assert!(
            engine
                .controls
                .version_history
                .keys()
                .copied()
                .collect::<Vec<_>>()
                == vec![-1, 0]
        );
        assert!(
            engine
                .controls
                .voter_history
                .keys()
                .copied()
                .collect::<Vec<_>>()
                == vec![-1, 1]
        );
        assert!(engine.controls.latest_version() == 0);
        assert!(engine.controls.committed_version == 0);
    }
}
