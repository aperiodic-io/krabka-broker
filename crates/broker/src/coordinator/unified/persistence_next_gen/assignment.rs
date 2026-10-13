//! The KIP-848 target and current assignment records, and the topic-partition
//! list codec they share.
//!
//! [`TargetAssignmentMemberValue`] at key version 7 holds the assignment the
//! coordinator computed for one member. [`CurrentMemberAssignmentValue`] at key
//! version 8 holds what that member has converged on, its
//! [`MemberAssignmentState`], and the partitions it still owes back.
//! [`AssignedTopicPartitions`] is the leaf record that both value types repeat.
//!
//! # Layout
//!
//! From `ConsumerGroupTargetAssignmentMemberValue.json` and
//! `ConsumerGroupCurrentMemberAssignmentValue.json` at Apache Kafka tag
//! `4.3.1`. Both declare `"flexibleVersions": "0+"`.
//!
//! - Target: `TopicPartitions` (`[]TopicPartition{TopicId uuid, Partitions
//!   []int32}`).
//! - Current: `MemberEpoch` (int32), `PreviousMemberEpoch` (int32), `State`
//!   (int8), `AssignedPartitions` and `PartitionsPendingRevocation`, both
//!   `[]TopicPartitions{TopicId uuid, Partitions []int32}` with a tagged
//!   `AssignmentEpochs` (tag 0, nullable `[]int32`, default null). The
//!   broker always writes it, one epoch per partition, as Kafka's
//!   `GroupCoordinatorRecordHelpers.toTopicPartitions` does (KIP-1251).
//!
//! A `uuid` is sixteen raw bytes with no length prefix, arrays are compact, and
//! each element struct as well as the message carries a tagged-field trailer.

use {
    crate::{
        coordinator::unified::persistence::{
            flex::{
                array_value_codec, get_assigned_topics, get_compact_array, get_i32_array, get_uuid,
                put_assigned_topics, put_compact_array, put_i32_array, put_tagged_fields, put_uuid,
                read_tagged, value_codec,
            },
            get_i32,
        },
        error::BrokerError,
    },
    bytes::{Buf, BufMut, BytesMut},
    krabka_protocol::{
        ProtocolError,
        primitives::{array, fixed, uuid::Uuid},
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignedTopicPartitions {
    pub topic_id: Uuid,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TargetAssignmentMemberValue {
    pub topic_partitions: Vec<AssignedTopicPartitions>,
}

array_value_codec!(
    TargetAssignmentMemberValue("ConsumerGroupTargetAssignmentMemberValue"),
    topic_partitions,
    encode_topic_partitions,
    decode_topic_partitions
);

/// The member's reconciliation state, with Kafka's discriminants from
/// `org.apache.kafka.coordinator.group.modern.MemberState`: `STABLE` is 0,
/// `UNREVOKED_PARTITIONS` is 1, and `UNRELEASED_PARTITIONS` is 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemberAssignmentState {
    Stable = 0,
    UnrevokedPartitions = 1,
    UnreleasedPartitions = 2,
}

impl MemberAssignmentState {
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn from_i8(v: i8) -> Result<Self, BrokerError> {
        match v {
            0 => Ok(Self::Stable),
            1 => Ok(Self::UnrevokedPartitions),
            2 => Ok(Self::UnreleasedPartitions),
            _ => Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown MemberAssignmentState",
            ))),
        }
    }
}

/// The `TopicPartitions` struct of `ConsumerGroupCurrentMemberAssignmentValue`:
/// the partitions of one topic and, in its tag 0, the epoch at which each was
/// assigned to the member (KIP-1251).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentTopicPartitions {
    pub topic_id: Uuid,
    pub partitions: Vec<i32>,
    /// The tagged `AssignmentEpochs`, of the same length as `partitions`.
    /// `None` is the schema's null default, which the encoder omits.
    pub assignment_epochs: Option<Vec<i32>>,
}

impl CurrentTopicPartitions {
    /// The assignment epoch of each partition, as Kafka's
    /// `Utils.assignmentFromTopicPartitions` reads them: from
    /// `assignment_epochs` when it has one epoch per partition, and otherwise
    /// `default_epoch`, raised to 0, for every partition.
    #[must_use]
    pub fn epochs(&self, default_epoch: i32) -> Vec<(i32, i32)> {
        match &self.assignment_epochs {
            Some(epochs) if epochs.len() == self.partitions.len() => self
                .partitions
                .iter()
                .copied()
                .zip(epochs.iter().copied())
                .collect(),
            _ => self
                .partitions
                .iter()
                .map(|&partition| (partition, default_epoch.max(0)))
                .collect(),
        }
    }
}

/// The tag of `AssignmentEpochs` in the current assignment's
/// `TopicPartitions`.
const TAG_ASSIGNMENT_EPOCHS: u32 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentMemberAssignmentValue {
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub state: MemberAssignmentState,
    pub assigned_partitions: Vec<CurrentTopicPartitions>,
    pub partitions_pending_revocation: Vec<CurrentTopicPartitions>,
}

value_codec! {
    CurrentMemberAssignmentValue("ConsumerGroupCurrentMemberAssignmentValue"),
    encode(&self) -> buf {
        buf.put_i32(self.member_epoch);
        buf.put_i32(self.previous_member_epoch);
        buf.put_i8(self.state as i8);
        encode_current_topic_partitions(buf, &self.assigned_partitions);
        encode_current_topic_partitions(buf, &self.partitions_pending_revocation);
    }
    decode(buf) {
        let member_epoch = get_i32(buf)?;
        let previous_member_epoch = get_i32(buf)?;
        if buf.remaining() < 1 {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "missing state byte",
            )));
        }
        let state = MemberAssignmentState::from_i8(buf.get_i8())?;
        let assigned_partitions = decode_current_topic_partitions(buf)?;
        let partitions_pending_revocation = decode_current_topic_partitions(buf)?;
        Ok(Self {
            member_epoch,
            previous_member_epoch,
            state,
            assigned_partitions,
            partitions_pending_revocation,
        })
    }
}

fn encode_topic_partitions(buf: &mut BytesMut, items: &[AssignedTopicPartitions]) {
    put_assigned_topics(buf, items, |tp| (&tp.topic_id, &tp.partitions));
}

fn decode_topic_partitions(buf: &mut &[u8]) -> Result<Vec<AssignedTopicPartitions>, BrokerError> {
    get_assigned_topics(buf, |topic_id, partitions| AssignedTopicPartitions {
        topic_id,
        partitions,
    })
}

fn encode_current_topic_partitions(buf: &mut BytesMut, items: &[CurrentTopicPartitions]) {
    put_compact_array(buf, items.iter(), |buf, tp| {
        put_uuid(buf, tp.topic_id.0);
        put_i32_array(buf, &tp.partitions);
        let mut tags = Vec::new();
        if let Some(epochs) = &tp.assignment_epochs {
            let mut payload = BytesMut::new();
            put_i32_array(&mut payload, epochs);
            tags.push((TAG_ASSIGNMENT_EPOCHS, payload.freeze()));
        }
        put_tagged_fields(buf, tags);
    });
}

fn decode_current_topic_partitions(
    buf: &mut &[u8],
) -> Result<Vec<CurrentTopicPartitions>, BrokerError> {
    get_compact_array(buf, |buf| {
        let topic_id = Uuid(get_uuid(buf)?);
        let partitions = get_i32_array(buf)?;
        let mut assignment_epochs = None;
        read_tagged(buf, |tag, payload| {
            if tag != TAG_ASSIGNMENT_EPOCHS {
                return Ok(false);
            }
            assignment_epochs = get_nullable_i32_array(payload)?;
            Ok(true)
        })?;
        Ok(CurrentTopicPartitions {
            topic_id,
            partitions,
            assignment_epochs,
        })
    })
}

/// Reads a compact nullable `[]int32`.
fn get_nullable_i32_array(buf: &mut &[u8]) -> Result<Option<Vec<i32>>, ProtocolError> {
    let Some(n) = array::get_nullable_array_len(buf, true)? else {
        return Ok(None);
    };
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(fixed::get_i32(buf)?);
    }
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use {super::*, crate::coordinator::unified::test_support::wire_bytes, assert2::assert};

    #[test]
    fn target_assignment_member_bytes_match_kafka_schema() {
        let v = TargetAssignmentMemberValue {
            topic_partitions: vec![AssignedTopicPartitions {
                topic_id: Uuid([1; 16]),
                partitions: vec![0, 1],
            }],
        };
        let want = wire_bytes(&[
            "0000",                             // value version 0
            "02",                               // one TopicPartition
            "01010101010101010101010101010101", // TopicId, sixteen raw bytes
            "03",                               // two partitions
            "00000000",
            "00000001",
            "00", // TopicPartition tagged fields
            "00", // message tagged fields
        ]);
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn target_assignment_member_roundtrip() {
        let v = TargetAssignmentMemberValue {
            topic_partitions: vec![AssignedTopicPartitions {
                topic_id: Uuid([1; 16]),
                partitions: vec![0, 1, 2],
            }],
        };
        assert!(TargetAssignmentMemberValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn current_member_assignment_bytes_match_kafka_schema() {
        let v = CurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: MemberAssignmentState::UnrevokedPartitions,
            assigned_partitions: vec![CurrentTopicPartitions {
                topic_id: Uuid([2; 16]),
                partitions: vec![7, 8],
                assignment_epochs: Some(vec![3, 5]),
            }],
            partitions_pending_revocation: vec![CurrentTopicPartitions {
                topic_id: Uuid([3; 16]),
                partitions: vec![1],
                assignment_epochs: None,
            }],
        };
        let want = wire_bytes(&[
            "0000",
            "00000005",
            "00000004",
            "01", // MemberState.UNREVOKED_PARTITIONS
            "02", // one assigned TopicPartitions
            "02020202020202020202020202020202",
            "03", // two partitions
            "00000007",
            "00000008",
            "01", // one tagged field
            "00", // tag 0, AssignmentEpochs
            "09", // payload size: 1 + 2 * 4
            "03", // two epochs
            "00000003",
            "00000005",
            "02", // one pending TopicPartitions
            "03030303030303030303030303030303",
            "02", // one partition
            "00000001",
            "00", // null AssignmentEpochs is omitted
            "00", // message tagged fields
        ]);
        let encoded = v.encode();
        assert!(&encoded[..] == &want[..]);
        assert!(CurrentMemberAssignmentValue::decode(&encoded).unwrap() == v);
    }

    /// Kafka's `Utils.assignmentFromTopicPartitions`: the stored epochs when
    /// there is one per partition, and otherwise the member epoch raised to 0.
    #[test]
    fn assignment_epochs_fall_back_to_the_member_epoch() {
        type Row = (Option<Vec<i32>>, i32, Vec<(i32, i32)>);
        let rows: [Row; 4] = [
            (Some(vec![2, 4]), 6, vec![(0, 2), (1, 4)]),
            (None, 6, vec![(0, 6), (1, 6)]),
            (Some(vec![2]), 6, vec![(0, 6), (1, 6)]),
            (None, -2, vec![(0, 0), (1, 0)]),
        ];
        let mut actual = Vec::new();
        for (assignment_epochs, member_epoch, _) in &rows {
            let tp = CurrentTopicPartitions {
                topic_id: Uuid([1; 16]),
                partitions: vec![0, 1],
                assignment_epochs: assignment_epochs.clone(),
            };
            actual.push((
                assignment_epochs.clone(),
                *member_epoch,
                tp.epochs(*member_epoch),
            ));
        }
        assert!(actual == rows);
    }

    /// A null `AssignmentEpochs` written with the tag present decodes as
    /// absent, and an unknown tag is skipped.
    #[test]
    fn current_topic_partitions_tag_decoding() {
        let mut encoded = BytesMut::new();
        encoded.put_i16(0);
        encoded.put_i32(1);
        encoded.put_i32(0);
        encoded.put_i8(0);
        encoded.put_u8(0x02); // one assigned TopicPartitions
        encoded.extend_from_slice(&[4u8; 16]);
        encoded.put_u8(0x02);
        encoded.put_i32(0);
        encoded.put_u8(0x02); // two tagged fields
        encoded.put_u8(0x00); // tag 0
        encoded.put_u8(0x01); // size 1
        encoded.put_u8(0x00); // null array
        encoded.put_u8(0x05); // unknown tag 5
        encoded.put_u8(0x01);
        encoded.put_u8(0x7f);
        encoded.put_u8(0x01); // empty pending
        encoded.put_u8(0x00);
        let want = CurrentMemberAssignmentValue {
            member_epoch: 1,
            previous_member_epoch: 0,
            state: MemberAssignmentState::Stable,
            assigned_partitions: vec![CurrentTopicPartitions {
                topic_id: Uuid([4; 16]),
                partitions: vec![0],
                assignment_epochs: None,
            }],
            partitions_pending_revocation: vec![],
        };
        assert!(CurrentMemberAssignmentValue::decode(&encoded).unwrap() == want);
    }

    #[test]
    fn member_state_discriminants_match_kafka() {
        // org.apache.kafka.coordinator.group.modern.MemberState.
        assert!(MemberAssignmentState::Stable as i8 == 0);
        assert!(MemberAssignmentState::UnrevokedPartitions as i8 == 1);
        assert!(MemberAssignmentState::UnreleasedPartitions as i8 == 2);
        assert!(
            MemberAssignmentState::from_i8(1).unwrap()
                == MemberAssignmentState::UnrevokedPartitions
        );
        assert!(MemberAssignmentState::from_i8(3).is_err());
    }

    #[test]
    fn assignment_records_reject_a_missing_tagged_trailer() {
        let t = TargetAssignmentMemberValue::default().encode();
        assert!(TargetAssignmentMemberValue::decode(&t[..t.len() - 1]).is_err());
    }
}
