//! The next-gen-to-classic direction of a KIP-848 live migration.
//!
//! It re-expresses a consumer group's hosted classic members as classic
//! members, seeding each one from the server-computed target so no partition
//! looks revoked across the flip, and builds the atomic record batch that
//! tombstones the next-gen records and writes the classic one.

use {
    super::assignment::{consumer_assignment_blob, embedded_protocol_version},
    crate::coordinator::unified::{
        actor::{GroupTombstone, PendingRecords, classic_group_metadata_record},
        classic_state::{ClassicGroup as ClassicState, Member as ClassicMember, select_protocol},
        consumer_state::GroupState as ConsumerState,
        reconciler::ReconcileInput,
    },
    krabka_verified::{
        GroupMigrationDirection, GroupMigrationRecordAction, consumer_downgrade_epoch,
        group_migration_record_plan,
    },
};

/// Converts a consumer group back into a classic group during a KIP-848
/// downgrade.
///
/// Every member becomes a classic [`ClassicMember`] again, restored from its
/// [`ClassicMemberFacade`]. Its assignment seed is the server-computed target,
/// translated to a `ConsumerProtocolAssignment` blob, so the member keeps its
/// partitions across the flip with no false revoke. Committed offsets live on
/// the kind-agnostic `Group` container, and this function does not change
/// them.
///
/// The `leaving` members, which a fence removes, are left out, as Kafka's
/// `ClassicGroup.fromConsumerGroup` leaves out its `leavingMembers`.
///
/// Precondition: every other member is a hosted classic member, that is
/// `classic.is_some()`.
pub(crate) fn convert_consumer_to_classic(
    state: &ConsumerState,
    leaving: &[String],
    image: &ReconcileInput,
) -> ClassicState {
    let mut classic = ClassicState::new(state.group_id.clone());
    classic.protocol_type = Some("consumer".into());
    let mut members: Vec<_> = state
        .members
        .iter()
        .filter(|(member_id, _)| !leaving.contains(member_id))
        .collect();
    members.sort_unstable_by_key(|(member_id, _)| member_id.as_str());
    for (mid, m) in members {
        let facade = m
            .classic
            .as_ref()
            .expect("downgrade precondition: all members are hosted classic members");
        let cm = ClassicMember::new(
            mid.clone(),
            m.client_id.clone(),
            m.client_host.clone(),
            facade.session_timeout,
            m.rebalance_timeout,
            facade.supported_protocols.clone(),
        )
        .with_instance_id(m.instance_id.clone());
        classic.add_member(cm);
    }
    // Kafka's `ClassicGroup.fromConsumerGroup` seeds each member with its
    // TARGET assignment, serialized at the version of the member's metadata
    // for the selected protocol.
    let selected = select_protocol(&classic.members);
    for (mid, member) in &mut classic.members {
        let metadata = selected
            .as_deref()
            .and_then(|name| member.protocols.iter().find(|(n, _)| n == name))
            .or_else(|| member.protocols.first())
            .map(|(_, metadata)| metadata.as_ref());
        let version = metadata
            .and_then(embedded_protocol_version)
            .unwrap_or_default();
        let target = state
            .target
            .per_member
            .get(mid)
            .cloned()
            .unwrap_or_default();
        member.assignment = Some(consumer_assignment_blob(&target, image, version));
    }
    if let Some(name) = selected {
        classic.complete_rebalance(&name);
        // Drive to Stable so a downgraded member's first Heartbeat/SyncGroup
        // reads its seed assignment instead of REBALANCE_IN_PROGRESS.
        let assignments: std::collections::HashMap<String, bytes::Bytes> = classic
            .members
            .iter()
            .filter_map(|(id, m)| m.assignment.clone().map(|a| (id.clone(), a)))
            .collect();
        classic.install_assignments(assignments);
    }
    // Set generation_id LAST so neither complete_rebalance (+1) nor
    // install_assignments overrides the consumer group's epoch.
    classic.generation_id = consumer_downgrade_epoch(
        state
            .members
            .values()
            .filter(|member| !leaving.contains(&member.member_id))
            .all(|member| member.classic.is_some()),
        state.group_epoch,
    )
    .expect("downgrade precondition: every member has a classic facade");
    classic
}

/// The atomic record batch for a downgrade.
///
/// It tombstones the consumer group's group-level next-gen k3 `GroupMetadata`
/// and k6 `TargetAssignmentMetadata`, and every member's k5, k7, and k8. It
/// then writes the classic k2 `GroupMetadata` for the re-expressed classic
/// group. All of that is in one batch, so the flip is all-or-nothing, and
/// bootstrap replay sees a clean next-gen drop followed by a classic write.
/// Log order decides.
///
/// The k6 tombstone matters under log compaction. `__consumer_offsets` is
/// compacted. Without the tombstone, a surviving post-upgrade k6 write, which
/// is group-level and never tombstoned per member, would outlive the collected
/// k3. It would then re-create a next-gen seed on replay and bring the
/// downgraded group back as next-gen.
pub(crate) fn downgrade_pending_records(
    consumer: &ConsumerState,
    classic: &ClassicState,
    now_ms: i64,
) -> PendingRecords {
    let plan =
        group_migration_record_plan(GroupMigrationDirection::Downgrade, consumer.members.len());
    let pending = PendingRecords {
        // Kafka's `convertToClassicGroup` writes
        // `ConsumerGroup.createGroupTombstoneRecords`, which tombstones every
        // member's records, the target metadata, the resolved regular
        // expressions, the deprecated k4 record and the group epoch.
        group_tombstone: (plan.next_gen_group == GroupMigrationRecordAction::Tombstone
            && plan.next_gen_target == GroupMigrationRecordAction::Tombstone
            && plan.member_metadata == GroupMigrationRecordAction::Tombstone)
            .then(|| GroupTombstone {
                members: consumer.members.keys().cloned().collect(),
                regexes: consumer.resolved_regex_names(),
            }),
        classic_group_metadata: (plan.classic_group == GroupMigrationRecordAction::Write)
            .then(|| classic_group_metadata_record(classic, now_ms)),
        ..Default::default()
    };
    assert2::debug_assert!(
        pending
            .group_tombstone
            .as_ref()
            .is_some_and(|tombstone| tombstone.members.len() == plan.member_count)
    );
    pending
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::persistence_next_gen::{NextGenKey, encode_key},
        assert2::{assert, check},
        bytes::Buf,
        krabka_protocol::{
            Decode, owned::consumer_protocol_assignment::ConsumerProtocolAssignment,
            primitives::uuid::Uuid,
        },
    };

    #[test]
    fn downgrade_re_expresses_members_as_classic() {
        use {
            crate::coordinator::unified::{
                classic_state::GroupState as ClassicGroupState,
                consumer_state::{ClassicMemberFacade, GroupState, MemberState},
                persistence_next_gen::MemberAssignmentState,
            },
            std::time::{Duration, Instant},
        };

        let t1 = Uuid([1; 16]);
        let image = ReconcileInput {
            topic_id_by_name: [("orders".to_string(), t1)].into(),
            ..Default::default()
        };
        let mut state = GroupState::new("g");
        state.group_epoch = 7;
        let m = MemberState {
            member_id: "m1".into(),
            instance_id: Some("inst-a".into()),
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: ["orders".to_string()].into(),
            subscribed_topic_regex: None,
            server_assignor: None,
            rebalance_timeout: Duration::from_mins(1),
            member_epoch: 7,
            previous_member_epoch: 6,
            assignment_state: MemberAssignmentState::Stable,
            // A hosted classic member never acks epochs, so its
            // `assigned_partitions` stays EMPTY — its real partitions live in
            // the group's target (set below). The downgrade must seed from the
            // target, not from this empty map.
            assigned_partitions: std::collections::HashMap::new(),
            partitions_pending_revocation: std::collections::HashMap::new(),
            assignment_epochs: std::collections::HashMap::new(),
            last_seen: Instant::now(),
            // Subscription metadata whose embedded-protocol version is 1, the
            // version the member's seed assignment is serialized at.
            classic: Some(ClassicMemberFacade {
                supported_protocols: vec![(
                    "range".into(),
                    bytes::Bytes::from_static(b"\x00\x01meta"),
                )],
                session_timeout: Duration::from_secs(30),
            }),
        };
        state.add_or_update_member(m);
        // The member's real partitions are in the server target, not in
        // `assigned_partitions`.
        state.target.epoch = 7;
        state
            .target
            .per_member
            .insert("m1".into(), [(t1, vec![0, 1])].into());

        let classic = convert_consumer_to_classic(&state, &[], &image);
        assert!(classic.group_id == "g");
        assert!(classic.generation_id == 7);
        let member = classic.members.get("m1").expect("member preserved");
        assert!(member.group_instance_id.as_deref() == Some("inst-a"));
        assert!(member.session_timeout == Duration::from_secs(30));
        let asn = member.assignment.clone().expect("seed assignment");
        let mut cur = &asn[..];
        let version = cur.get_i16();
        assert!(version == 1);
        let decoded = ConsumerProtocolAssignment::decode(&mut cur, 1).unwrap();
        check!(decoded.assigned_partitions[0].topic == "orders");
        check!(decoded.assigned_partitions[0].partitions == vec![0, 1]);
        // Group must land in Stable so the first Heartbeat/SyncGroup after
        // downgrade does not trigger a spurious full rebalance.
        check!(classic.state == ClassicGroupState::Stable);
        // Seed assignment is still intact after stabilization.
        let asn2 = member
            .assignment
            .clone()
            .expect("seed assignment still set after stabilize");
        check!(asn2 == asn);
        // complete_rebalance must have set the protocol metadata coherently.
        check!(classic.protocol_name.as_deref() == Some("range"));
        check!(classic.leader_id.as_deref() == Some("m1"));

        let first = downgrade_pending_records(&state, &classic, 7)
            .to_batch("g", 7)
            .unwrap();
        let second = downgrade_pending_records(&state, &classic, 7)
            .to_batch("g", 7)
            .unwrap();
        // Kafka's `convertToClassicGroup`: the group's tombstones in the
        // order of `ConsumerGroup.createGroupTombstoneRecords`, the deprecated
        // k4 record always among them, then the classic k2 value.
        let ng = |key: NextGenKey| encode_key(&key).unwrap();
        let keys: Vec<_> = first
            .records
            .iter()
            .map(|record| (record.key.clone().unwrap(), record.value.is_none()))
            .collect();
        let member = || ("g".to_string(), "m1".to_string());
        check!(
            keys == vec![
                (
                    ng(NextGenKey::CurrentMemberAssignment {
                        group_id: member().0,
                        member_id: member().1,
                    }),
                    true
                ),
                (
                    ng(NextGenKey::TargetAssignmentMember {
                        group_id: member().0,
                        member_id: member().1,
                    }),
                    true
                ),
                (
                    ng(NextGenKey::TargetAssignmentMetadata {
                        group_id: "g".into()
                    }),
                    true
                ),
                (
                    ng(NextGenKey::MemberMetadata {
                        group_id: member().0,
                        member_id: member().1,
                    }),
                    true
                ),
                (
                    ng(NextGenKey::PartitionMetadata {
                        group_id: "g".into()
                    }),
                    true
                ),
                (
                    ng(NextGenKey::GroupMetadata {
                        group_id: "g".into()
                    }),
                    true
                ),
                (
                    crate::coordinator::unified::persistence::GroupMetadataValue::encode_key("g")
                        .unwrap(),
                    false
                ),
            ]
        );
        assert!(first.records == second.records);
    }
}
