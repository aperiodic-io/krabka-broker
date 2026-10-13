//! Native member fields and concrete topology traversal use real protocol types.

use {
    assert2::assert,
    krabka_protocol::{
        UnknownTaggedFields,
        owned::{
            common::{
                consumer_group_describe_response::assignment::Assignment,
                streams_group_heartbeat_request::topic_info::TopicInfo,
            },
            consumer_group_describe_response::Member,
            streams_group_heartbeat_request::{Subtopology, Topology},
        },
    },
    std::cell::RefCell,
};

thread_local! {
    static OPERATIONS: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

fn assignment(operation: &'static str) -> Assignment {
    OPERATIONS.with_borrow_mut(|events| events.push(operation));
    Assignment::default()
}

krabka_macros::member_identity_fixtures! {
    plain fn plain_member(member_id: &str) -> Member {
        assignment: assignment("plain assignment"),
        target_assignment: assignment("plain target"),
        member_type: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
    tagged fn wire_member(identity: &str) -> Member {
        assignment: {
            let value = assignment("wire assignment");
            if identity == "early" { return Member::default(); }
            value
        },
        target_assignment: assignment("wire target"),
        member_type: 0,
    }
}

#[test]
fn member_identity_and_native_extra_fields_keep_their_order_and_early_exit() {
    OPERATIONS.with_borrow_mut(Vec::clear);
    let plain = plain_member("view");
    let wire = wire_member("wire");
    assert!(plain.member_id == "view" && wire.member_id == "wire");
    assert!(
        wire.member_epoch == 5 && wire.client_id == "client" && wire.client_host == "/10.0.0.1"
    );
    assert!(
        wire.instance_id.is_none()
            && wire.rack_id.is_none()
            && wire.subscribed_topic_regex.is_none()
    );
    assert!(wire.subscribed_topic_names == ["orders", "payments"]);
    assert!(wire.unknown_tagged_fields == UnknownTaggedFields::default());
    OPERATIONS.with_borrow(|events| {
        assert!(
            events
                == &[
                    "plain assignment",
                    "plain target",
                    "wire assignment",
                    "wire target",
                ]
        );
    });
    OPERATIONS.with_borrow_mut(Vec::clear);
    assert!(wire_member("early") == Member::default());
    OPERATIONS.with_borrow(|events| assert!(events == &["wire assignment"]));
}

// An independent, linear dedupe keeps the test oracle separate from the broker's set.
fn dedup_first_seen<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    names.into_iter().fold(Vec::new(), |mut retained, name| {
        if retained.iter().all(|value| value != name) {
            retained.push(name.to_owned());
        }
        retained
    })
}

krabka_macros::required_topics! {
    fn topics(topology: &Topology) -> Vec<String>;
}

fn topic_info(name: &str) -> TopicInfo {
    TopicInfo {
        name: name.into(),
        ..TopicInfo::default()
    }
}

#[test]
fn every_topic_role_contributes_once_in_first_seen_order() {
    let topology = Topology {
        subtopologies: vec![
            Subtopology {
                source_topics: vec!["source".into(), "shared".into()],
                repartition_sink_topics: vec!["sink".into()],
                repartition_source_topics: vec![topic_info("shared"), topic_info("repartition")],
                state_changelog_topics: vec![topic_info("changelog")],
                ..Subtopology::default()
            },
            Subtopology {
                source_topics: vec!["sink".into(), "last".into()],
                ..Subtopology::default()
            },
        ],
        ..Topology::default()
    };
    assert!(
        topics(&topology)
            == [
                "source",
                "shared",
                "sink",
                "repartition",
                "changelog",
                "last"
            ]
    );
    assert!(topics(&Topology::default()).is_empty());
}
