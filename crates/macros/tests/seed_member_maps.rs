//! Caller-scope seed statements preserve ordered projections and conditional restoration.

use {
    assert2::assert,
    krabka_macros::{hydrate_member_epochs, snapshot_member_maps},
    std::{cell::RefCell, collections::HashMap},
};

#[derive(Clone, Debug, Default, PartialEq)]
struct AssignmentState {
    member_epoch: i32,
    previous_member_epoch: i32,
    payload: String,
}

#[derive(Default)]
struct State {
    members: HashMap<String, AssignmentState>,
}

struct Seed {
    current_per_member: HashMap<String, AssignmentState>,
    untouched: String,
}

#[test]
fn snapshot_projects_metadata_then_current_then_conditional_targets_in_caller_scope() {
    let state = State {
        members: ["included", "excluded"]
            .map(|id| {
                (id.to_owned(), AssignmentState {
                    member_epoch: 4,
                    previous_member_epoch: 3,
                    payload: id.to_owned(),
                })
            })
            .into(),
    };
    let observations = RefCell::new(Vec::new());
    let metadata = |member: &AssignmentState| {
        observations
            .borrow_mut()
            .push((member.payload.clone(), "metadata"));
        member.payload.clone()
    };
    let assignment = |member: &AssignmentState| {
        observations
            .borrow_mut()
            .push((member.payload.clone(), "current"));
        member.clone()
    };
    snapshot_member_maps! { state;
        members, current, targets;
        metadata, assignment; |id, member| {
            observations.borrow_mut().push((id.clone(), "target"));
            if member.payload == "included" {
                // These caller bindings and a native nested macro remain visible.
                targets.insert(id.clone(), format!("target:{}", current[id].member_epoch));
            }
        }
    }
    assert!(
        members
            == HashMap::from([
                ("included".to_owned(), "included".to_owned()),
                ("excluded".to_owned(), "excluded".to_owned()),
            ])
    );
    assert!(current == state.members);
    assert!(targets == HashMap::from([("included".to_owned(), "target:4".to_owned())]));
    let observations = observations.into_inner();
    assert!(observations.len() == 6);
    let (members, remainder) = observations.as_chunks::<3>();
    assert!(remainder.is_empty());
    for member in members {
        assert!(member[0].0 == member[1].0 && member[1].0 == member[2].0);
        assert!(
            member
                .iter()
                .map(|(_, stage)| *stage)
                .eq(["metadata", "current", "target"])
        );
    }
}

#[test]
fn hydration_restores_only_existing_members_before_native_operations_without_capturing_mid() {
    let mut state = State {
        members: [
            ("present".to_owned(), AssignmentState::default()),
            ("metadata-only".to_owned(), AssignmentState {
                member_epoch: 9,
                ..AssignmentState::default()
            }),
        ]
        .into(),
    };
    let mut records = HashMap::new();
    for id in ["present", "orphan"] {
        records.insert(id.to_owned(), AssignmentState {
            member_epoch: 6,
            previous_member_epoch: 5,
            payload: id.to_owned(),
        });
    }
    let seed = Seed {
        current_per_member: records,
        untouched: "retained".into(),
    };
    let metadata_only = state.members["metadata-only"].clone();
    let mid = "caller";
    let mut restored = 0;
    hydrate_member_epochs!(state, seed; member, current {
        assert!(member.member_epoch == current.member_epoch);
        assert!(member.previous_member_epoch == current.previous_member_epoch);
        let select = |&(first, _): &(i32, i32)| first;
        member.payload = format!("{mid}:{}:{}", current.payload, select(&(1, 2)));
        restored += 1;
    });
    assert!(restored == 1);
    assert!(state.members["present"].payload == "caller:present:1");
    assert!(state.members["metadata-only"] == metadata_only);
    assert!(!state.members.contains_key("orphan"));
    assert!(seed.untouched == "retained");
    assert!(mid == "caller");
}
