//! The read-only projection of a share group that the `ShareGroupDescribe`
//! handler consumes, together with its rendering into the wire row. It is its
//! own file because presenting group state is separate from the state machine
//! that maintains it.

use {
    super::records::member_metadata_value,
    crate::{
        codes,
        coordinator::unified::share::{assignor::ShareGroupAssignor, state::ShareGroupState},
    },
    krabka_protocol::{
        owned::share_group_describe_response::{DescribedGroup, Member as DescribeMember},
        primitives::uuid::Uuid,
    },
    std::collections::HashMap,
};

/// Read-only projection of [`ShareGroupState`], consumed by the
/// `ShareGroupDescribe` handler (a later dispatch).
#[derive(Debug, Clone)]
pub struct ShareDescribeView {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub group_state: String,
    pub assignor_name: String,
    pub members: Vec<ShareDescribeMember>,
}

#[derive(Debug, Clone)]
pub struct ShareDescribeMember {
    pub member_id: String,
    pub member_epoch: i32,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
}

impl ShareDescribeView {
    /// Render this view into a `ShareGroupDescribe` `DescribedGroup` wire row,
    /// as Kafka's `ShareGroup.asDescribedGroup` does. Each assigned topic is
    /// named from `image`, and a topic the image no longer holds is left out
    /// (`ShareGroupMember.topicPartitionsFromMap`). Members are sorted by id,
    /// topics by name and partitions ascending, so the answer does not depend
    /// on map order. `authorized_operations` keeps its default, because the
    /// handler owns the ACL outcome.
    #[must_use]
    pub fn into_described_group(self, image: &krabka_metadata::MetadataImage) -> DescribedGroup {
        use krabka_protocol::owned::common::share_group_describe_response::{
            assignment::Assignment, topic_partitions::TopicPartitions,
        };

        let mut members: Vec<DescribeMember> = self
            .members
            .into_iter()
            .map(|m| {
                let mut topic_partitions: Vec<TopicPartitions> = m
                    .assigned_partitions
                    .into_iter()
                    .filter_map(|(tid, mut parts)| {
                        let name = image.topic_name_by_id(&uuid::Uuid::from_bytes(tid.0))?;
                        parts.sort_unstable();
                        Some(TopicPartitions {
                            topic_id: tid,
                            topic_name: name.to_string(),
                            partitions: parts,
                            ..Default::default()
                        })
                    })
                    .collect();
                topic_partitions.sort_by(|a, b| a.topic_name.cmp(&b.topic_name));
                DescribeMember {
                    member_id: m.member_id,
                    rack_id: m.rack_id,
                    member_epoch: m.member_epoch,
                    client_id: m.client_id,
                    client_host: m.client_host,
                    subscribed_topic_names: m.subscribed_topic_names,
                    assignment: Assignment {
                        topic_partitions,
                        ..Default::default()
                    },
                    ..Default::default()
                }
            })
            .collect();
        members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
        DescribedGroup {
            group_id: self.group_id,
            group_state: self.group_state,
            group_epoch: self.group_epoch,
            assignment_epoch: self.assignment_epoch,
            assignor_name: self.assignor_name,
            members,
            error_code: codes::NONE,
            ..Default::default()
        }
    }
}

pub(super) fn build_describe(state: &ShareGroupState) -> ShareDescribeView {
    let group_state = if state.members.is_empty() {
        "Empty"
    } else {
        "Stable"
    };
    ShareDescribeView {
        group_id: state.group_id.clone(),
        group_epoch: state.group_epoch,
        assignment_epoch: state.target.epoch,
        group_state: group_state.into(),
        assignor_name: ShareGroupAssignor.name().into(),
        members: state
            .members
            .values()
            .map(|m| {
                let member_id = m.member_id.clone();
                let metadata = member_metadata_value(m);
                ShareDescribeMember {
                    member_id,
                    member_epoch: m.member_epoch,
                    rack_id: metadata.rack_id,
                    client_id: metadata.client_id,
                    client_host: metadata.client_host,
                    subscribed_topic_names: metadata.subscribed_topic_names,
                    assigned_partitions: m.assigned_partitions.clone(),
                }
            })
            .collect(),
    }
}
