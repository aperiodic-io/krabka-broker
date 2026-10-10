//! The KIP-227 partition-set diff: it drops forgotten partitions from a
//! session's cached set and merges the requested topics back in.
//!
//! `apply_incremental` is the pure core of an incremental fetch, with no lock
//! and no cache state around it, so the property test below and the stateright
//! model in `fetch_session_model` can drive it directly.

use std::collections::HashMap;

use krabka_protocol::{
    owned::fetch_request::{FetchTopic, ForgottenTopic},
    primitives::uuid::Uuid as WireUuid,
};

use super::state::{CachedPartitionState, FetchSessionKey};

/// Pure core of the incremental-fetch session update (KIP-227). It drops
/// forgotten partitions, then merges the requested topics into a session's
/// partition map.
///
/// - **forget**: a `ForgottenTopic` matches a cached key by either
///   `topic_name` (Fetch v 12 and below) **or** `topic_id` (v 13 and above),
///   plus the partition.
/// - **merge**: for each requested partition, this function finds a cached key
///   by either identity half plus the partition, and updates its desired state
///   in place. It inserts a new default-state key only when the partition is
///   truly not cached. That rule stops a partial-identity copy from shadowing
///   a fully resolved key.
///
/// `fetch_session_model` exercises the asymmetry between the OR-match forget
/// and the either-half-match merge exhaustively.
pub(crate) fn apply_incremental(
    partitions: &mut HashMap<FetchSessionKey, CachedPartitionState>,
    forgotten: &[ForgottenTopic],
    topics: &[FetchTopic],
) {
    for ft in forgotten {
        partitions.retain(|k, _| {
            let topic_match = (!ft.topic.is_empty() && k.topic_name == ft.topic)
                || (ft.topic_id != WireUuid::ZERO && k.topic_id == ft.topic_id);
            if !topic_match {
                return true;
            }
            !ft.partitions.contains(&k.partition)
        });
    }

    for t in topics {
        for fp in &t.partitions {
            let existing_key = partitions
                .keys()
                .find(|k| {
                    k.partition == fp.partition
                        && ((!t.topic.is_empty() && k.topic_name == t.topic)
                            || (t.topic_id != WireUuid::ZERO && k.topic_id == t.topic_id))
                })
                .cloned();
            let key = existing_key.unwrap_or_else(|| FetchSessionKey {
                topic_name: t.topic.clone(),
                topic_id: t.topic_id,
                partition: fp.partition,
            });
            let entry = partitions.entry(key).or_default();
            entry.fetch_offset = fp.fetch_offset;
            entry.max_bytes = fp.partition_max_bytes;
            entry.current_leader_epoch = fp.current_leader_epoch;
            entry.last_fetched_epoch = fp.last_fetched_epoch;
            entry.log_start_offset = fp.log_start_offset;
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::fetch_session::test_support::{ForgottenTopicSetup, forgotten_topic};

    #[test]
    fn forgotten_topic_name_drops_only_matching_topic_partition() {
        let mut partitions = maplit::hashmap! {
        FetchSessionKey {
                topic_name: "t".into(),
                topic_id: WireUuid::ZERO,
                partition: 0,
            } => CachedPartitionState::default(),
        FetchSessionKey {
                topic_name: "u".into(),
                topic_id: WireUuid::ZERO,
                partition: 0,
            } => CachedPartitionState::default()};
        let forgotten = vec![forgotten_topic(ForgottenTopicSetup::default())];

        apply_incremental(&mut partitions, &forgotten, &[]);

        assert!(!partitions.contains_key(&FetchSessionKey {
            topic_name: "t".into(),
            topic_id: WireUuid::ZERO,
            partition: 0,
        }));
        assert!(partitions.contains_key(&FetchSessionKey {
            topic_name: "u".into(),
            topic_id: WireUuid::ZERO,
            partition: 0,
        }));
    }

    #[test]
    fn forgotten_topic_id_drops_only_matching_topic_partition() {
        let tid = WireUuid([1u8; 16]);
        let other_tid = WireUuid([2u8; 16]);
        let mut partitions = maplit::hashmap! {
        FetchSessionKey {
                topic_name: "t".into(),
                topic_id: tid,
                partition: 0,
            } => CachedPartitionState::default(),
        FetchSessionKey {
                topic_name: "u".into(),
                topic_id: other_tid,
                partition: 0,
            } => CachedPartitionState::default()};
        let forgotten = vec![forgotten_topic(ForgottenTopicSetup {
            topic: String::new(),
            topic_id: tid,
            ..Default::default()
        })];

        apply_incremental(&mut partitions, &forgotten, &[]);

        assert!(!partitions.contains_key(&FetchSessionKey {
            topic_name: "t".into(),
            topic_id: tid,
            partition: 0,
        }));
        assert!(partitions.contains_key(&FetchSessionKey {
            topic_name: "u".into(),
            topic_id: other_tid,
            partition: 0,
        }));
    }
}

/// Large-N random fuzzing of `apply_incremental`, the KIP-227 forget and merge
/// path. It complements the exhaustive `fetch_session_model`. Random sequences
/// of incremental fetches over a small topic, id, and partition universe, with
/// random identity halves, must keep no-shadow, subscription fidelity, and
/// no-orphan-default after every step.
#[cfg(test)]
mod fuzz {
    use std::collections::HashMap;

    use krabka_protocol::{
        owned::fetch_request::{FetchPartition, FetchTopic},
        primitives::uuid::Uuid as WireUuid,
    };
    use proptest::prelude::*;

    use super::{CachedPartitionState, FetchSessionKey, apply_incremental};
    use crate::fetch_session::test_support::{ForgottenTopicSetup, forgotten_topic};

    // name index 0 = empty (id-only wire form), 1 = "A", 2 = "B".
    fn name_of(i: u8) -> String {
        ["", "A", "B"][i as usize].to_string()
    }
    // id index 0 = ZERO (name-only wire form), 1 = U, 2 = V.
    fn id_of(i: u8) -> WireUuid {
        [WireUuid::ZERO, WireUuid([1; 16]), WireUuid([2; 16])][i as usize]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]
        #[test]
        fn forget_merge_invariants(
            ops in proptest::collection::vec(
                // (forget name, forget id, forget partition,
                //  do-subscribe, sub name, sub id, sub partition, sub max_bytes)
                (0u8..3, 0u8..3, 0i32..2, any::<bool>(), 0u8..3, 0u8..3, 0i32..2, 1i32..4),
                0..200,
            )
        ) {
            let mut partitions: HashMap<FetchSessionKey, CachedPartitionState> = HashMap::new();
            for (fname, fid, fp, do_sub, sname, sid, sp, mb) in ops {
                // A forget with an all-empty identity matches nothing — skip it
                // (the wire never carries a topic with neither name nor id).
                let forgotten = if fname == 0 && fid == 0 {
                    vec![]
                } else {
                    vec![forgotten_topic(ForgottenTopicSetup {topic: name_of(fname), topic_id: id_of(fid), partitions: vec![krabka_ids::PartitionIndex(fp)]})]
                };
                let subscribe = do_sub && !(sname == 0 && sid == 0);
                let topics = if subscribe {
                    vec![FetchTopic {
                        topic: name_of(sname),
                        topic_id: id_of(sid),
                        partitions: vec![FetchPartition {
                            partition: sp,
                            partition_max_bytes: mb,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }]
                } else {
                    vec![]
                };

                apply_incremental(&mut partitions, &forgotten, &topics);

                // No shadow: no two keys share a logical partition.
                let keys: Vec<_> = partitions.keys().cloned().collect();
                for (i, a) in keys.iter().enumerate() {
                    for b in &keys[i + 1..] {
                        let shadow = a.partition == b.partition
                            && ((!a.topic_name.is_empty() && a.topic_name == b.topic_name)
                                || (a.topic_id != WireUuid::ZERO && a.topic_id == b.topic_id));
                        prop_assert!(!shadow, "shadow: {a:?} vs {b:?}");
                    }
                }

                // No orphan default: every cached entry carries a subscribed
                // max_bytes (merge always sets it; we only ever request >= 1).
                prop_assert!(partitions.values().all(|v| v.max_bytes != 0));

                // Subscription fidelity: a subscribed partition is reflected with
                // the requested max_bytes by some key matching the request.
                if subscribe {
                    let name = name_of(sname);
                    let id = id_of(sid);
                    let present = partitions.iter().any(|(k, st)| {
                        k.partition == sp
                            && ((!name.is_empty() && k.topic_name == name)
                                || (id != WireUuid::ZERO && k.topic_id == id))
                            && st.max_bytes == mb
                    });
                    prop_assert!(present, "subscription not reflected");
                }
            }
        }
    }
}
