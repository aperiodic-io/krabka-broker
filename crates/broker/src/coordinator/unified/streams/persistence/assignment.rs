//! The two per-member task-assignment records, at key versions 21 and 22.
//!
//! The target assignment holds the active, standby, and warmup tasks the
//! coordinator wants a member to own. The current member assignment holds what
//! the member owns now, together with its reconciliation epochs and state and
//! any active task that is pending revocation. Each role is a map from a
//! subtopology id to the partitions of that subtopology.
//!
//! # Layout
//!
//! From `StreamsGroupTargetAssignmentMemberValue.json` and
//! `StreamsGroupCurrentMemberAssignmentValue.json` at Apache Kafka tag `4.3.1`.
//! Both declare `"flexibleVersions": "0+"`.
//!
//! - Target: `ActiveTasks`, `StandbyTasks` and `WarmupTasks`, each a
//!   `[]TaskIds{SubtopologyId string, Partitions []int32}`.
//! - Current: `MemberEpoch` (int32), `PreviousMemberEpoch` (int32), `State`
//!   (int8), then six `[]TaskIds`: the three roles, and then the three
//!   pending-revocation lists in the same role order. Its `TaskIds` adds the
//!   tagged `AssignmentEpochs` (tag 0, a nullable `[]int32`, default null), the
//!   epoch at which each partition was assigned (KIP-1251). Kafka's
//!   `newStreamsGroupCurrentAssignmentRecord` writes it for the active tasks
//!   and the active tasks pending revocation, and leaves it null for standby
//!   and warmup tasks.
//!
//! Every role has its own revocation list, as in Kafka's
//! `CurrentAssignmentBuilder`.

use bytes::BufMut;
use krabka_protocol::ProtocolError;

use super::codec::{
    decode_task_map, decode_task_map_with_epochs, encode_task_map, encode_task_map_with_epochs,
};
use crate::{
    coordinator::unified::{
        persistence::{
            flex::{get_i8, value_codec},
            get_i32,
        },
        streams::state::{StreamsMemberAssignmentState, TaskMap},
    },
    error::BrokerError,
};

/// The member state as `org.apache.kafka.coordinator.group.streams.MemberState`
/// numbers it. Kafka's streams enum does not start at zero the way the consumer
/// one does: `STABLE` is 1, `UNREVOKED_TASKS` is 2 and `UNRELEASED_TASKS` is 3.
/// The broker's own [`StreamsMemberAssignmentState`] counts from zero, so the
/// two meet here rather than anywhere a reader of the group state could confuse
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamsMemberWireState {
    Stable = 1,
    UnrevokedTasks = 2,
    UnreleasedTasks = 3,
}

impl From<StreamsMemberAssignmentState> for StreamsMemberWireState {
    fn from(v: StreamsMemberAssignmentState) -> Self {
        match v {
            StreamsMemberAssignmentState::Stable => Self::Stable,
            StreamsMemberAssignmentState::UnrevokedTasks => Self::UnrevokedTasks,
            StreamsMemberAssignmentState::UnreleasedTasks => Self::UnreleasedTasks,
        }
    }
}

impl From<StreamsMemberWireState> for StreamsMemberAssignmentState {
    fn from(v: StreamsMemberWireState) -> Self {
        match v {
            StreamsMemberWireState::Stable => Self::Stable,
            StreamsMemberWireState::UnrevokedTasks => Self::UnrevokedTasks,
            StreamsMemberWireState::UnreleasedTasks => Self::UnreleasedTasks,
        }
    }
}

impl StreamsMemberWireState {
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn from_i8(v: i8) -> Result<Self, BrokerError> {
        match v {
            1 => Ok(Self::Stable),
            2 => Ok(Self::UnrevokedTasks),
            3 => Ok(Self::UnreleasedTasks),
            _ => Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown streams MemberState",
            ))),
        }
    }
}

/// Key v21 value: a member's target task assignment, by role. Each role maps a
/// subtopology id to the partitions of that subtopology that the member holds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamsGroupTargetAssignmentMemberValue {
    pub active: TaskMap,
    pub standby: TaskMap,
    pub warmup: TaskMap,
}

value_codec! {
    StreamsGroupTargetAssignmentMemberValue("StreamsGroupTargetAssignmentMemberValue"),
    encode(&self) -> buf {
        encode_task_map(buf, &self.active);
        encode_task_map(buf, &self.standby);
        encode_task_map(buf, &self.warmup);
    }
    decode(buf) {
        let active = decode_task_map(buf)?;
        let standby = decode_task_map(buf)?;
        let warmup = decode_task_map(buf)?;
        Ok(Self {
            active,
            standby,
            warmup,
        })
    }
}

/// Key v22 value: a member's current in-flight task assignment, with the
/// reconciliation epochs and state, and the tasks of each role pending
/// revocation.
#[derive(Debug, Clone, PartialEq, Eq, krabka_macros::FieldDefaults)]
pub struct StreamsGroupCurrentMemberAssignmentValue {
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    #[default(StreamsMemberWireState::Stable)]
    pub state: StreamsMemberWireState,
    pub active: TaskMap,
    pub standby: TaskMap,
    pub warmup: TaskMap,
    pub active_pending_revocation: TaskMap,
    pub standby_pending_revocation: TaskMap,
    pub warmup_pending_revocation: TaskMap,
    /// The `AssignmentEpochs` of the `ActiveTasks` entries that carry one, by
    /// subtopology id, in the order of their partitions. Kafka 4.3.1 writes
    /// one for every active task entry.
    pub active_epochs: TaskMap,
    /// The `AssignmentEpochs` of the `ActiveTasksPendingRevocation` entries
    /// that carry one.
    pub active_pending_revocation_epochs: TaskMap,
}

value_codec! {
    StreamsGroupCurrentMemberAssignmentValue("StreamsGroupCurrentMemberAssignmentValue"),
    encode(&self) -> buf {
        buf.put_i32(self.member_epoch);
        buf.put_i32(self.previous_member_epoch);
        buf.put_i8(self.state as i8);
        encode_task_map_with_epochs(buf, &self.active, &self.active_epochs);
        encode_task_map(buf, &self.standby);
        encode_task_map(buf, &self.warmup);
        encode_task_map_with_epochs(
            buf,
            &self.active_pending_revocation,
            &self.active_pending_revocation_epochs,
        );
        encode_task_map(buf, &self.standby_pending_revocation);
        encode_task_map(buf, &self.warmup_pending_revocation);
    }
    decode(buf) {
        let member_epoch = get_i32(buf)?;
        let previous_member_epoch = get_i32(buf)?;
        let state = StreamsMemberWireState::from_i8(get_i8(buf)?)?;
        let (active, active_epochs) = decode_task_map_with_epochs(buf)?;
        let standby = decode_task_map(buf)?;
        let warmup = decode_task_map(buf)?;
        let (active_pending_revocation, active_pending_revocation_epochs) =
            decode_task_map_with_epochs(buf)?;
        let standby_pending_revocation = decode_task_map(buf)?;
        let warmup_pending_revocation = decode_task_map(buf)?;
        Ok(Self {
            member_epoch,
            previous_member_epoch,
            state,
            active,
            standby,
            warmup,
            active_pending_revocation,
            standby_pending_revocation,
            warmup_pending_revocation,
            active_epochs,
            active_pending_revocation_epochs,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::{
        streams::persistence::{
            KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT, KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER,
            StreamsGroupKey, encode_current_member_assignment_key,
            encode_target_assignment_member_key, parse_streams_key,
        },
        test_support::{peek_version, wire_bytes},
    };

    #[test]
    fn target_assignment_member_bytes_match_kafka_schema() {
        let mut active = BTreeMap::new();
        active.insert("0".to_string(), vec![1]);
        let v = StreamsGroupTargetAssignmentMemberValue {
            active,
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
        };
        let want = wire_bytes(&[
            "0000", "02",   // one ActiveTasks entry
            "0230", // SubtopologyId "0"
            "02",   // one partition
            "00000001", "00", // TaskIds tagged fields
            "01", // empty StandbyTasks
            "01", // empty WarmupTasks
            "00", // message tagged fields
        ]);
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn target_assignment_member_round_trip() {
        let kb = encode_target_assignment_member_key("g1", "m1").unwrap();
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::TargetAssignmentMember {
                    group_id: "g1".into(),
                    member_id: "m1".into(),
                }
        );

        let mut active = BTreeMap::new();
        active.insert("0".to_string(), vec![0, 1, 2]);
        active.insert("1".to_string(), vec![]);
        let mut standby = BTreeMap::new();
        standby.insert("0".to_string(), vec![3]);
        let v = StreamsGroupTargetAssignmentMemberValue {
            active,
            standby,
            warmup: BTreeMap::new(),
        };
        assert!(StreamsGroupTargetAssignmentMemberValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn current_member_assignment_bytes_match_kafka_schema() {
        let v = StreamsGroupCurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: StreamsMemberWireState::UnrevokedTasks,
            ..Default::default()
        };
        let want = wire_bytes(&[
            "0000",
            "00000005",
            "00000004",
            "02",           // streams MemberState.UNREVOKED_TASKS
            "010101010101", // six empty task lists
            "00",           // message tagged fields
        ]);
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn current_member_assignment_round_trip() {
        let kb = encode_current_member_assignment_key("g1", "m1").unwrap();
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::CurrentMemberAssignment {
                    group_id: "g1".into(),
                    member_id: "m1".into(),
                }
        );

        let mut active = BTreeMap::new();
        active.insert("0".to_string(), vec![0, 1]);
        let mut pending = BTreeMap::new();
        pending.insert("0".to_string(), vec![2]);
        let v = StreamsGroupCurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: StreamsMemberWireState::UnrevokedTasks,
            active,
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
            active_pending_revocation: pending,
            standby_pending_revocation: maplit::btreemap! {"1".to_string() => vec![0]},
            warmup_pending_revocation: maplit::btreemap! {"2".to_string() => vec![4]},
            active_epochs: maplit::btreemap! {"0".to_string() => vec![3, 5]},
            active_pending_revocation_epochs: maplit::btreemap! {"0".to_string() => vec![2]},
        };
        assert!(StreamsGroupCurrentMemberAssignmentValue::decode(&v.encode()).unwrap() == v);
    }

    /// The `TaskIds` of the current assignment carries `AssignmentEpochs` as
    /// tagged field 0, a compact `[]int32`, as Kafka 4.3.1's
    /// `toTaskIdsWithEpochs` writes it for every active entry, and without it
    /// as `toTaskIds` writes the standby and warmup entries.
    #[test]
    fn current_member_assignment_epochs_match_kafka_schema() {
        let v = StreamsGroupCurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: StreamsMemberWireState::UnrevokedTasks,
            active: maplit::btreemap! {"0".to_string() => vec![1, 2]},
            standby: maplit::btreemap! {"1".to_string() => vec![0]},
            active_pending_revocation: maplit::btreemap! {"0".to_string() => vec![3]},
            active_epochs: maplit::btreemap! {"0".to_string() => vec![4, 5]},
            active_pending_revocation_epochs: maplit::btreemap! {"0".to_string() => vec![3]},
            ..Default::default()
        };
        let mut want: Vec<u8> = vec![0x00, 0x00];
        want.extend_from_slice(&5i32.to_be_bytes());
        want.extend_from_slice(&4i32.to_be_bytes());
        want.push(0x02); // streams MemberState.UNREVOKED_TASKS
        // ActiveTasks: one entry, "0", partitions [1, 2], one tagged field:
        // tag 0, nine bytes, AssignmentEpochs [4, 5].
        want.extend_from_slice(b"\x02\x020\x03");
        want.extend_from_slice(&1i32.to_be_bytes());
        want.extend_from_slice(&2i32.to_be_bytes());
        want.extend_from_slice(b"\x01\x00\x09\x03");
        want.extend_from_slice(&4i32.to_be_bytes());
        want.extend_from_slice(&5i32.to_be_bytes());
        // StandbyTasks: one entry, "1", partitions [0], no tagged field.
        want.extend_from_slice(b"\x02\x021\x02");
        want.extend_from_slice(&0i32.to_be_bytes());
        want.push(0x00);
        want.push(0x01); // empty WarmupTasks
        // ActiveTasksPendingRevocation: "0", [3], AssignmentEpochs [3].
        want.extend_from_slice(b"\x02\x020\x02");
        want.extend_from_slice(&3i32.to_be_bytes());
        want.extend_from_slice(b"\x01\x00\x05\x02");
        want.extend_from_slice(&3i32.to_be_bytes());
        want.extend_from_slice(&[0x01, 0x01]); // empty standby and warmup revocations
        want.push(0x00); // message tagged fields
        assert!(&v.encode()[..] == &want[..]);
        assert!(StreamsGroupCurrentMemberAssignmentValue::decode(&want).unwrap() == v);
    }

    #[test]
    fn wire_state_matches_kafkas_streams_member_state() {
        // org.apache.kafka.coordinator.group.streams.MemberState.
        assert!(StreamsMemberWireState::Stable as i8 == 1);
        assert!(StreamsMemberWireState::UnrevokedTasks as i8 == 2);
        assert!(StreamsMemberWireState::UnreleasedTasks as i8 == 3);
        assert!(StreamsMemberWireState::from_i8(0).is_err());
        for state in [
            StreamsMemberAssignmentState::Stable,
            StreamsMemberAssignmentState::UnrevokedTasks,
            StreamsMemberAssignmentState::UnreleasedTasks,
        ] {
            let wire = StreamsMemberWireState::from(state);
            assert!(StreamsMemberAssignmentState::from(wire) == state);
        }
    }

    #[test]
    fn task_map_multi_subtopology_empty_partitions_round_trip() {
        // A task map with several subtopologies, some carrying no partitions,
        // must survive encode/decode unchanged.
        let mut active = BTreeMap::new();
        active.insert("0".to_string(), vec![0, 1, 2, 3]);
        active.insert("1".to_string(), vec![]);
        active.insert("2".to_string(), vec![7]);
        let v = StreamsGroupTargetAssignmentMemberValue {
            active,
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
        };
        let decoded = StreamsGroupTargetAssignmentMemberValue::decode(&v.encode()).unwrap();
        assert!(decoded == v);
    }

    #[test]
    fn assignment_records_reject_a_missing_tagged_trailer() {
        let t = StreamsGroupTargetAssignmentMemberValue::default().encode();
        assert!(StreamsGroupTargetAssignmentMemberValue::decode(&t[..t.len() - 1]).is_err());
        let c = StreamsGroupCurrentMemberAssignmentValue::default().encode();
        assert!(StreamsGroupCurrentMemberAssignmentValue::decode(&c[..c.len() - 1]).is_err());
    }
}
