//! Member fixtures the `classic_state` submodule tests share.

use std::time::Duration;

use bytes::Bytes;

use super::member::Member;

pub(super) fn sample_member(id: &str) -> Member {
    Member::new(
        id,
        "test-client",
        "127.0.0.1",
        Duration::from_secs(30),
        Duration::from_mins(1),
        vec![("range".into(), Bytes::new())],
    )
}

pub(super) fn member_with_protocols(id: &str, protocols: Vec<(&str, &[u8])>) -> Member {
    Member::new(
        id,
        "test-client",
        "127.0.0.1",
        Duration::from_secs(30),
        Duration::from_mins(1),
        protocols
            .into_iter()
            .map(|(n, b)| (n.to_string(), Bytes::copy_from_slice(b)))
            .collect(),
    )
}

pub(super) fn static_member(member_id: &str, instance_id: &str) -> Member {
    sample_member(member_id).with_instance_id(Some(instance_id.to_string()))
}

/// Complete a range rebalance and install the test's assignment for member m1.
pub(super) fn stabilize_m1(group: &mut super::group::ClassicGroup) {
    group.complete_rebalance("range");
    group.install_assignments(std::collections::HashMap::from([(
        "m1".into(),
        Bytes::from_static(b"assignment-bytes"),
    )]));
    assert2::assert!(group.state == super::group::GroupState::Stable);
}
