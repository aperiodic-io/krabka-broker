//! Coordinator schemas and native commit actions keep their observable contracts.

extern crate self as krabka_log;

use {
    assert2::assert,
    futures_util::lock::Mutex,
    krabka_protocol::owned::{
        consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
    std::sync::{Arc, Mutex as StdMutex},
};

#[derive(Debug, PartialEq, Eq)]
struct HeartbeatReply {
    error_code: i16,
    member_epoch: i32,
}

trait HeartbeatStatus {
    fn status(self) -> HeartbeatReply;
}

krabka_macros::heartbeat_status!(
    ConsumerGroupHeartbeatResponse,
    ShareGroupHeartbeatResponse,
    StreamsGroupHeartbeatResponse
);

#[test]
fn real_protocol_replies_preserve_failure_codes_and_sentinel_epochs() {
    let expected = HeartbeatReply {
        error_code: 113,
        member_epoch: -2,
    };
    assert!(
        ConsumerGroupHeartbeatResponse {
            error_code: 113,
            member_epoch: -2,
            ..Default::default()
        }
        .status()
            == expected
    );
    assert!(
        ShareGroupHeartbeatResponse {
            error_code: 113,
            member_epoch: -2,
            ..Default::default()
        }
        .status()
            == expected
    );
    assert!(
        StreamsGroupHeartbeatResponse {
            error_code: 113,
            member_epoch: -2,
            ..Default::default()
        }
        .status()
            == expected
    );
}

pub struct Offset(pub i64);

struct Replica {
    hw: Offset,
}

struct Partition {
    replica_state: Mutex<Replica>,
}

#[derive(Clone, Copy)]
struct LeaderMove {
    leader: u64,
    epoch: i32,
}

struct ChangeCase {
    moves_to: Option<LeaderMove>,
    hw_later: Option<i64>,
    expected: &'static [&'static str],
}

#[tokio::test]
async fn optional_commit_actions_capture_first_and_notify_after_the_watermark_write() {
    for case in [
        ChangeCase {
            moves_to: None,
            hw_later: None,
            expected: &["capture"],
        },
        ChangeCase {
            moves_to: Some(LeaderMove {
                leader: 2,
                epoch: 9,
            }),
            hw_later: None,
            expected: &["capture", "leader"],
        },
        ChangeCase {
            moves_to: None,
            hw_later: Some(12),
            expected: &["capture", "notify"],
        },
        ChangeCase {
            moves_to: Some(LeaderMove {
                leader: 2,
                epoch: 9,
            }),
            hw_later: Some(12),
            expected: &["capture", "leader", "notify"],
        },
    ] {
        let partition = Arc::new(Partition {
            replica_state: Mutex::new(Replica { hw: Offset(0) }),
        });
        let events = Arc::new(StdMutex::new(Vec::new()));
        let task = krabka_macros::schedule_commit_changes!(case, partition;
            captures {
                events.lock().unwrap().push("capture");
                let events = Arc::clone(&events);
            }
            leader(next_leader, next_epoch) {
                assert!(next_leader == 2 && next_epoch == 9);
                events.lock().unwrap().push("leader");
            }
            notify {
                assert!(partition.replica_state.lock().await.hw.0 == 12);
                events.lock().unwrap().push("notify");
            }
        );
        assert!(partition.replica_state.lock().await.hw.0 == 0);
        task.await.unwrap();
        assert!(*events.lock().unwrap() == case.expected);
        assert!(partition.replica_state.lock().await.hw.0 == case.hw_later.unwrap_or(0));
    }
}
