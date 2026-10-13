//! The capture itself: the host-side `DescribeGroups` call, the assertions that
//! calibrate Krabka's handler against the JVM answer, and the test that walks
//! both images in turn.

use {
    crate::{
        CLASSIC_IMAGE, GROUP, HOST_BOOTSTRAP, NEXT_GEN_GROUP, NEXT_GEN_IMAGE, TYPELESS_GROUP,
        groups_docker::{ContainerGuard, docker_pull, docker_run_kafka},
        groups_fixture::{group_json, write_fixture},
        groups_setup::{prepare_classic_groups, prepare_next_gen_group, wait_for_broker},
        support::client::connect_owned,
    },
    assert2::assert,
    krabka_protocol::owned::{
        describe_groups_request::DescribeGroupsRequest,
        describe_groups_response::{DescribeGroupsResponse, DescribedGroup},
    },
};

async fn describe_real_groups(groups: &[&str]) -> DescribeGroupsResponse {
    let client = connect_owned(HOST_BOOTSTRAP, "cap", "client build against real kafka").await;
    let response = client
        .send(DescribeGroupsRequest {
            groups: groups.iter().map(|group| (*group).to_string()).collect(),
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .expect("DescribeGroups against real kafka");
    client.close();
    response
}

fn assert_classic_group(classic: &DescribedGroup) {
    assert!(
        classic.error_code == 0,
        "classic group describe error: {classic:?}"
    );
    assert!(classic.protocol_type == "consumer");
    assert!(classic.protocol_data == "range");
    assert!(classic.group_state == "Stable");
    assert!(classic.members.len() == 1);
    let member = &classic.members[0];
    assert!(!member.member_metadata.is_empty());
    assert!(&member.member_metadata[..2] == [0x00, 0x03]);
}

fn persist_and_assert(response: &DescribeGroupsResponse) {
    let classic = response
        .groups
        .iter()
        .find(|group| group.group_id == GROUP)
        .unwrap_or_else(|| panic!("group {GROUP} missing: {response:?}"));
    let typeless = response
        .groups
        .iter()
        .find(|group| group.group_id == TYPELESS_GROUP)
        .unwrap_or_else(|| panic!("group {TYPELESS_GROUP} missing: {response:?}"));
    let fixture = serde_json::json!({
        "provenance": {
            "image": CLASSIC_IMAGE,
            "api_key": 15,
            "note": "Real cp-kafka DescribeGroups. cp/JVM is the authority for protocol_type / protocol_data / member_metadata.",
        },
        "classic_consumer_group": group_json(classic),
        "typeless_group": group_json(typeless),
    });
    write_fixture(
        "real_kafka_classic.json",
        &serde_json::to_string_pretty(&fixture).unwrap(),
    );
    assert_classic_group(classic);
    assert!(typeless.error_code == 0);
    assert!(typeless.protocol_type == "");
    assert!(typeless.protocol_data == "");
}

fn persist_and_assert_next_gen(response: &DescribeGroupsResponse) {
    let next_gen = response
        .groups
        .iter()
        .find(|group| group.group_id == NEXT_GEN_GROUP)
        .unwrap_or_else(|| panic!("group {NEXT_GEN_GROUP} missing: {response:?}"));
    let fixture = serde_json::json!({
        "provenance": {
            "image": NEXT_GEN_IMAGE,
            "api_key": 15,
            "note": "Real Apache Kafka next-generation consumer-group DescribeGroups authority capture.",
        },
        "next_gen_consumer_group": group_json(next_gen),
    });
    write_fixture(
        "real_kafka_next_gen.json",
        &serde_json::to_string_pretty(&fixture).unwrap(),
    );
    assert!(next_gen.error_code == krabka_broker::codes::GROUP_ID_NOT_FOUND);
    assert!(next_gen.error_message.as_deref() == Some("Group g-next is not a classic group."));
    assert!(next_gen.protocol_type.is_empty());
    assert!(next_gen.protocol_data.is_empty());
    assert!(next_gen.group_state == "Dead");
    assert!(next_gen.members.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures classic and next-generation real-Kafka DescribeGroups metadata"]
async fn capture_real_kafka_describe_groups() {
    docker_pull(CLASSIC_IMAGE);
    {
        docker_run_kafka(CLASSIC_IMAGE, false);
        let _guard = ContainerGuard;
        wait_for_broker("kafka-broker-api-versions");
        prepare_classic_groups();
        let response = describe_real_groups(&[GROUP, TYPELESS_GROUP]).await;
        persist_and_assert(&response);
    }

    docker_pull(NEXT_GEN_IMAGE);
    docker_run_kafka(NEXT_GEN_IMAGE, true);
    let _guard = ContainerGuard;
    wait_for_broker("/opt/kafka/bin/kafka-broker-api-versions.sh");
    prepare_next_gen_group();
    let response = describe_real_groups(&[NEXT_GEN_GROUP]).await;
    persist_and_assert_next_gen(&response);
}
