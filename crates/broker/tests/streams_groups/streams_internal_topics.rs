//! The internal-topic provisioning scenario: a stateful subtopology makes the
//! broker auto-create the state-changelog topic it declares.
//!
//! The scenario has its own heartbeat loop rather than reusing the shared
//! `join_and_converge`, because it must repeat the topology on every follow-up
//! heartbeat and must wait for the `MISSING_INTERNAL_TOPICS` status to clear as
//! well as for the active task to land.

use std::time::Duration;

use assert2::assert;
use krabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo;

use crate::streams_harness::{
    active_partitions_for, boot, connect, create_topic, finalize_streams_version, first_join,
    follow_up, status_codes, topology,
};

/// A stateful subtopology, that is, one with a state-changelog topic, drives
/// the broker to auto-create the changelog internal topic. Once that topic
/// exists, the member converges with no `MISSING_INTERNAL_TOPICS` status, which
/// is status code 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateful_member_triggers_internal_topic_creation() {
    let (broker, bootstrap, _dir) = boot().await;
    let client = connect(&bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, "sk-input", 1).await;

    let changelog = TopicInfo {
        name: "app-store-changelog".into(),
        partitions: 0, // broker derives from the subtopology's task count
        replication_factor: 1,
        topic_configs: vec![],
        ..Default::default()
    };
    let topo = topology("sk-input", vec![changelog]);

    // First join. The very first reconcile may emit MISSING_INTERNAL_TOPICS (3)
    // because the changelog is created asynchronously and a re-read of the image
    // may not yet observe it; retry until the active task lands with no
    // missing-internal-topics status.
    let mut resp = client
        .send(first_join("streams-app-2", topo.clone()))
        .await
        .expect("first heartbeat");
    let mut member_id = resp.member_id.clone();
    let mut converged = false;
    for _ in 0..15 {
        if resp.error_code == 14 {
            // COORDINATOR_LOAD_IN_PROGRESS: retry the first join with the topology.
            resp = client
                .send(first_join("streams-app-2", topo.clone()))
                .await
                .expect("retry first heartbeat");
            member_id = resp.member_id.clone();
            continue;
        }
        assert!(resp.error_code == 0, "heartbeat error: {resp:?}");
        let missing_internal = status_codes(&resp).contains(&3);
        if active_partitions_for(&resp, "0") == vec![0] && !missing_internal {
            converged = true;
            break;
        }
        // intentional: backoff between heartbeats while polling the RPC response
        // for active-task assignment plus clearing of the MISSING_INTERNAL_TOPICS
        // status. This convergence is coordinator-local; it has no metadata-image
        // signal or metric, so a bounded re-heartbeat loop is the only observer.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let active = crate::support::streams::request_active_tasks(&resp);
        // A heartbeat after the join carries no topology: Kafka refuses one.
        let hb = follow_up(crate::support::streams::StreamsFollowUpSetup {
            group: "streams-app-2",
            member_id: &member_id,
            epoch: crate::support::streams::StreamsMemberEpoch(resp.member_epoch),
            active,
        });
        resp = client.send(hb).await.expect("follow-up heartbeat");
        member_id = resp.member_id.clone();
    }

    assert!(
        converged,
        "member never converged to active task [0] with no MISSING_INTERNAL_TOPICS; \
         last response: {resp:?}"
    );
    assert!(
        !status_codes(&resp).contains(&3),
        "no MISSING_INTERNAL_TOPICS (3) status once converged, got {:?}",
        resp.status
    );

    // The changelog internal topic must now exist in the controller image with
    // one partition (matching the single-partition source / subtopology task
    // count).
    let image = broker.controller_image_for_test();
    let changelog_rec = image.topic("app-store-changelog");
    let changelog_rec = changelog_rec.unwrap_or_else(|| {
        panic!(
            "changelog topic 'app-store-changelog' must be auto-created; topics present: {:?}",
            image.topics().map(|t| &t.name).collect::<Vec<_>>()
        )
    });
    assert!(
        changelog_rec.partitions == 1,
        "changelog topic must have 1 partition, got {}",
        changelog_rec.partitions
    );
}
