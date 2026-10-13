//! Conversion between the live share-group state and the [`ShareGroupSeed`]
//! that bootstrap replay produces. Both directions of that round trip live
//! together so a change to one stays matched by the other.

use {
    super::records::{
        current_assignment_value, member_metadata_value, state_partition_metadata_from,
        target_assignment_value,
    },
    crate::coordinator::unified::{
        ShareGroupSeed,
        assignor::TopicIdPartition,
        share::{
            persistence::TopicPartitionsInfo,
            state::{ShareGroupState, ShareMemberState},
        },
    },
    krabka_protocol::primitives::uuid::Uuid,
    std::collections::{HashMap, HashSet},
};

pub(super) fn apply_seed(state: &mut ShareGroupState, seed: ShareGroupSeed) {
    state.group_epoch = seed.group_epoch;
    state.metadata_hash = seed.metadata_hash;
    state.target.epoch = seed.target_epoch;
    state.assignment_timestamp_ms = seed.assignment_timestamp_ms;
    for (mid, meta) in seed.members {
        let subs: HashSet<String> = meta.subscribed_topic_names.into_iter().collect();
        let mut m = ShareMemberState::joining(mid.clone(), meta.client_id, meta.client_host, subs);
        m.rack_id = meta.rack_id;
        state.members.insert(mid, m);
    }
    crate::coordinator::unified::seeds::hydrate_member_epochs!(state, seed; m, cur {
            for (tid, parts) in cur.assigned_partitions {
                m.assigned_partitions.insert(tid, parts);
            }
    });
    for (mid, tv) in seed.target_per_member {
        let entry: HashMap<Uuid, Vec<i32>> = tv.topic_partitions.into_iter().collect();
        state.target.per_member.insert(mid, entry);
    }
    // KIP-932: rehydrate the already-Initialized share-state set so the
    // lifecycle hook skips partitions whose state survived the restart. The
    // record names every topic it lists, so the names come back with it and
    // the next record the group writes keeps naming those topics even if the
    // metadata image has since dropped them.
    //
    // The partitions the group was still initializing come back as
    // initializing, stamped with the replay time as Kafka's replay does, so
    // the lifecycle hook retries them once the retry interval passes.
    state.initialized.clear();
    state.initializing.clear();
    state.deleting.clear();
    state.topic_names.clear();
    let replayed_at = super::records::chrono_now_ms();
    restore_named_partitions(
        state,
        &seed.state_partition_metadata.initializing,
        |state, partition| {
            state.initializing.insert(partition, replayed_at);
        },
    );
    restore_named_partitions(
        state,
        &seed.state_partition_metadata.initialized,
        ShareGroupState::mark_initialized,
    );
    for topic in &seed.state_partition_metadata.deleting {
        state
            .deleting
            .insert(Uuid(*topic.topic_id.as_bytes()), topic.topic_name.clone());
    }
    state.forget_unused_topic_names();
}

/// Restore every topic name before applying its partition lifecycle action.
fn restore_named_partitions(
    state: &mut ShareGroupState,
    topics: &[TopicPartitionsInfo],
    mut restore: impl FnMut(&mut ShareGroupState, TopicIdPartition),
) {
    for topic in topics {
        let tid = Uuid(*topic.topic_id.as_bytes());
        state.topic_names.insert(tid, topic.topic_name.clone());
        for p in &topic.partitions {
            restore(state, TopicIdPartition {
                topic_id: tid,
                partition: *p,
            });
        }
    }
}

/// Snapshot a `ShareGroupState` into a `ShareGroupSeed` that can restore
/// a freshly-respawned actor. It mirrors what bootstrap replay produces.
pub(super) fn snapshot_seed(state: &ShareGroupState) -> ShareGroupSeed {
    crate::coordinator::unified::seeds::snapshot_member_maps! { state;
        members, current_per_member, target_per_member;
        member_metadata_value, current_assignment_value; |mid, m| {
            if let Some(target) = state.target.per_member.get(mid) {
                target_per_member.insert(mid.clone(), target_assignment_value(target));
            }
        }
    }
    ShareGroupSeed {
        group_epoch: state.group_epoch,
        metadata_hash: state.metadata_hash,
        target_epoch: state.target.epoch,
        assignment_timestamp_ms: state.assignment_timestamp_ms,
        members,
        target_per_member,
        current_per_member,
        // KIP-932 lifecycle: project the live Initialized set back into the
        // persisted record so the cache (and a respawned actor) stay consistent
        // with what the lifecycle hook wrote to the log.
        state_partition_metadata: state_partition_metadata_from(state),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::share::{
            persistence::{ShareGroupStatePartitionMetadataValue, TopicPartitionsInfo},
            state::share_partition,
        },
        assert2::{assert, check},
    };

    #[test]
    fn snapshot_seed_round_trips_through_apply() {
        // A populated state → snapshot_seed → apply_seed reconstructs members,
        // epochs, and assignments (the bootstrap-replay invariant).
        let id = Uuid([7; 16]);
        let mut state = ShareGroupState::new("g");
        let mut m = ShareMemberState::joining(
            "m1",
            "c1",
            "/127.0.0.1",
            ["t".to_string()].into_iter().collect(),
        );
        m.member_epoch = 3;
        m.assigned_partitions.insert(id, vec![0, 1]);
        state.members.insert("m1".into(), m);
        state.group_epoch = 3;
        state.target.epoch = 3;
        state
            .target
            .per_member
            .insert("m1".into(), [(id, vec![0, 1])].into());

        let seed = snapshot_seed(&state);
        let mut restored = ShareGroupState::new("g");
        apply_seed(&mut restored, seed);

        assert!(restored.group_epoch == 3);
        assert!(restored.target.epoch == 3);
        let rm = restored.members.get("m1").expect("member restored");
        check!(rm.member_epoch == 3);
        check!(rm.assigned_partitions[&id] == vec![0, 1]);
        check!(restored.target.per_member["m1"][&id] == vec![0, 1]);
    }

    #[test]
    fn seed_round_trip_keeps_the_topic_name_of_every_initialized_topic() {
        // The name a topic was initialized or is deleted under survives the trip through the
        // persisted record, so a restarted group keeps naming it even when the
        // metadata image no longer resolves the id.
        let id = Uuid([7; 16]);
        let mut state = ShareGroupState::new("g");
        state.initialized.insert(share_partition(id, 0));
        state.initialized.insert(share_partition(id, 1));
        state.initializing.insert(share_partition(id, 2), 0);
        state.topic_names.insert(id, "orders".to_owned());
        state.deleting.insert(Uuid([8; 16]), "carts".to_owned());

        let seed = snapshot_seed(&state);
        let mut restored = ShareGroupState::new("g");
        apply_seed(&mut restored, seed.clone());

        check!(restored.topic_names == state.topic_names);
        check!(restored.initialized == state.initialized);
        check!(restored.deleting == state.deleting);
        check!(restored.initializing.keys().collect::<Vec<_>>() == vec![&share_partition(id, 2)]);
        assert!(snapshot_seed(&restored).state_partition_metadata == seed.state_partition_metadata);
    }

    #[test]
    fn apply_seed_drops_a_name_with_no_initialized_partition() {
        // A record entry with no partitions contributes nothing to the
        // Initialized set, so its name is not carried either.
        let id = Uuid([7; 16]);
        let mut restored = ShareGroupState::new("g");
        apply_seed(&mut restored, ShareGroupSeed {
            state_partition_metadata: ShareGroupStatePartitionMetadataValue {
                initializing: Vec::new(),
                initialized: vec![TopicPartitionsInfo {
                    topic_id: uuid::Uuid::from_bytes([7; 16]),
                    topic_name: "orders".to_owned(),
                    partitions: Vec::new(),
                }],
                deleting: Vec::new(),
            },
            ..Default::default()
        });

        check!(restored.topic_names.is_empty());
        check!(!restored.initialized.iter().any(|tp| tp.topic_id == id));
    }
}
