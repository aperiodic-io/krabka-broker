//! Tests for the reconstruction of the KIP-932 share-group and KIP-1071
//! streams-group seeds from their persisted records.

use assert2::{assert, check};
use krabka_protocol::records::RecordBatch;

use super::replay::{Replayed, apply_record, apply_tombstone};
use crate::coordinator::persistence;

fn apply_member_tombstones<K: AsRef<[u8]>>(
    coordinator: &std::sync::Arc<crate::coordinator::GroupCoordinator>,
    keys: [K; 2],
) {
    for bytes in keys {
        let key = persistence::parse_key(bytes.as_ref()).unwrap();
        apply_tombstone(coordinator, &mut Replayed::default(), key).unwrap();
    }
}

/// A replay of a share-group's records must rebuild the cached seed, so
/// that a freshly-spawned actor restores the same membership after a
/// restart.
///
/// The records are the group metadata, the member metadata, the target
/// assignment, and the current assignment.
#[tokio::test]
async fn share_group_records_replay_into_seed() {
    use krabka_protocol::primitives::uuid::Uuid;

    use crate::coordinator::unified::share::persistence as sp;

    let coord = super::test_support::bare_coordinator();

    let tid = Uuid([9; 16]);
    // Drive the same path bootstrap takes: parse_key on the encoded key,
    // then apply_record on the value bytes.
    let recs: Vec<(bytes::Bytes, bytes::Bytes)> = vec![
        (
            sp::encode_share_key(&sp::ShareGroupKey::GroupMetadata {
                group_id: "sg".into(),
            })
            .unwrap(),
            sp::ShareGroupMetadataValue {
                epoch: 4,
                metadata_hash: 0,
            }
            .encode(),
        ),
        (
            sp::encode_share_key(&sp::ShareGroupKey::MemberMetadata {
                group_id: "sg".into(),
                member_id: "m1".into(),
            })
            .unwrap(),
            sp::ShareGroupMemberMetadataValue {
                rack_id: None,
                client_id: "c1".into(),
                client_host: "/127.0.0.1".into(),
                subscribed_topic_names: vec!["t".into()],
            }
            .encode(),
        ),
        (
            sp::encode_share_key(&sp::ShareGroupKey::CurrentMemberAssignment {
                group_id: "sg".into(),
                member_id: "m1".into(),
            })
            .unwrap(),
            sp::ShareGroupCurrentMemberAssignmentValue {
                member_epoch: 4,
                previous_member_epoch: 3,
                assigned_partitions: vec![(tid, vec![0, 1])],
            }
            .encode(),
        ),
    ];
    super::test_support::replay_values(&coord, recs);

    // Type locked + seed reconstructed.
    assert!(coord.group_type("sg") == Some(crate::coordinator::unified::GroupType::Share));
    let seed = coord.cached_share_seed("sg").expect("seed cached");
    check!(seed.group_epoch == 4);
    check!(seed.members.contains_key("m1"));
    check!(seed.current_per_member["m1"].member_epoch == 4);

    // Kafka's `shareGroupFenceMember` order removes the member: its current
    // assignment tombstone leaves it at epoch -1, and then its member
    // tombstone removes it.
    apply_member_tombstones(
        &coord,
        [
            sp::encode_share_key(&sp::ShareGroupKey::CurrentMemberAssignment {
                group_id: "sg".into(),
                member_id: "m1".into(),
            })
            .unwrap(),
            sp::encode_share_key(&sp::ShareGroupKey::MemberMetadata {
                group_id: "sg".into(),
                member_id: "m1".into(),
            })
            .unwrap(),
        ],
    );
    let seed = coord.cached_share_seed("sg").expect("seed still present");
    assert!(!seed.members.contains_key("m1"), "tombstone removed member");
}

/// A replay of a streams-group's records must lock the group type to
/// Streams and rebuild the cached seed.
///
/// The records are the group metadata, the member metadata, and the
/// current assignment. A member tombstone removes that member from the
/// seed.
#[tokio::test]
async fn streams_group_records_replay_into_seed() {
    use crate::coordinator::unified::streams::persistence as sp;

    let coord = super::test_support::bare_coordinator();

    // Drive the same path bootstrap takes: parse_key on the encoded key,
    // then apply_record on the value bytes.
    let recs: Vec<(bytes::Bytes, bytes::Bytes)> = vec![
        (
            sp::encode_streams_key(&sp::StreamsGroupKey::GroupMetadata {
                group_id: "stg".into(),
            })
            .unwrap(),
            sp::StreamsGroupMetadataValue {
                epoch: 7,
                metadata_hash: 0,
                validated_topology_epoch: 2,
                last_assignment_configs: Some(vec![sp::LastAssignmentConfig {
                    key: "num.standby.replicas".into(),
                    value: "0".into(),
                }]),
                // A record that a broker with the topology description plugin
                // wrote, as Kafka trunk does: one without the plugin keeps it.
                description: sp::DescriptionEpochs {
                    stored: 2,
                    failed: -1,
                },
            }
            .encode(),
        ),
        (
            sp::encode_streams_key(&sp::StreamsGroupKey::MemberMetadata {
                group_id: "stg".into(),
                member_id: "m1".into(),
            })
            .unwrap(),
            crate::coordinator::unified::test_support::plain_streams_member(2).encode(),
        ),
        (
            sp::encode_streams_key(&sp::StreamsGroupKey::CurrentMemberAssignment {
                group_id: "stg".into(),
                member_id: "m1".into(),
            })
            .unwrap(),
            crate::coordinator::unified::test_support::stable_streams_assignment(
                (7, 6),
                maplit::btreemap! {"0".to_string() => vec![0, 1]},
            )
            .encode(),
        ),
    ];
    super::test_support::replay_values(&coord, recs);

    // Type locked to Streams + seed reconstructed.
    assert!(coord.group_type("stg") == Some(crate::coordinator::unified::GroupType::Streams));
    let seed = coord.cached_streams_seed("stg").expect("seed cached");
    check!(seed.group_epoch == 7);
    check!(seed.validated_topology_epoch == 2);
    check!(
        seed.description_epochs
            == sp::DescriptionEpochs {
                stored: 2,
                failed: -1
            }
    );
    check!(
        seed.last_assignment_configs
            == maplit::btreemap! {"num.standby.replicas".to_string() => "0".to_string()}
    );
    check!(seed.members.contains_key("m1"));
    check!(seed.current_per_member["m1"].member_epoch == 7);

    // Kafka's `removeStreamsMember` order removes the member: its current
    // assignment tombstone leaves it at epoch -1, and then its member
    // tombstone removes it.
    apply_member_tombstones(
        &coord,
        [
            sp::encode_streams_key(&sp::StreamsGroupKey::CurrentMemberAssignment {
                group_id: "stg".into(),
                member_id: "m1".into(),
            })
            .unwrap(),
            sp::encode_streams_key(&sp::StreamsGroupKey::MemberMetadata {
                group_id: "stg".into(),
                member_id: "m1".into(),
            })
            .unwrap(),
        ],
    );
    let seed = coord
        .cached_streams_seed("stg")
        .expect("seed still present");
    assert!(!seed.members.contains_key("m1"), "tombstone removed member");
}

#[test]
fn a_malformed_record_publishes_nothing_and_a_child_record_creates_its_group() {
    use crate::coordinator::unified::{
        share::persistence as share, streams::persistence as streams,
    };

    let coord = super::test_support::bare_coordinator();
    let batch = RecordBatch::default();
    let mut acc = Replayed::default();

    let malformed_share = persistence::parse_key(
        &share::encode_share_key(&share::ShareGroupKey::GroupMetadata {
            group_id: "bad-share".into(),
        })
        .unwrap(),
    )
    .unwrap();
    check!(
        apply_record(
            &coord,
            &mut acc,
            malformed_share,
            &bytes::Bytes::from_static(&[0]),
            &batch,
        )
        .is_err()
    );
    check!(coord.group_type("bad-share").is_none());
    check!(coord.cached_share_seed("bad-share").is_none());

    let orphan_streams = persistence::parse_key(
        &streams::encode_streams_key(&streams::StreamsGroupKey::MemberMetadata {
            group_id: "orphan-streams".into(),
            member_id: "m".into(),
        })
        .unwrap(),
    )
    .unwrap();
    apply_record(
        &coord,
        &mut acc,
        orphan_streams,
        &streams::StreamsGroupMemberMetadataValue {
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "h".into(),
            process_id: "p".into(),
            user_endpoint: None,
            client_tags: vec![],
            rebalance_timeout_ms: 1,
            topology_epoch: 0,
        }
        .encode(),
        &batch,
    )
    .unwrap();
    // Kafka's `getOrMaybeCreatePersistedStreamsGroup(groupId, true)`: the
    // member record creates the group.
    check!(
        coord.group_type("orphan-streams") == Some(crate::coordinator::unified::GroupType::Streams)
    );
    assert!(
        coord
            .cached_streams_seed("orphan-streams")
            .is_some_and(|seed| seed.members.contains_key("m"))
    );
}
