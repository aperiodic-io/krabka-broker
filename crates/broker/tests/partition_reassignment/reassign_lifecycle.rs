//! Reassignment lifecycle tests over PLAINTEXT: start a reassignment, observe
//! it in flight, complete it through an ISR catch-up, and cancel it with null
//! replicas.
//!
//! Each test boots its own 3-broker cluster, sends
//! `AlterPartitionReassignments` to the current controller leader, and then
//! reads the outcome back out of the metadata image.

use assert2::{assert, check};

use crate::{
    log_dir_wire::{alter_replica_log_dirs, describe_log_dirs},
    plaintext_cluster::{
        broker_id, controller_leader_addr, node_id, start_three_broker_plaintext_cluster,
        start_three_broker_plaintext_cluster_with_log_dirs, wait_partition_exists,
    },
    plaintext_wire::create_topic_plaintext,
    reassign_rpc::{drive_alter_reassignments, drive_list_reassignments},
};

// Each expansion keeps the same per-test static mutex as the original scenarios.
macro_rules! case_lock {
    () => {{
        static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        $crate::support::lazy_mutex(&LOCK).lock().await
    }};
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preplacement_log_dir_preference_survives_until_reassignment_materializes_the_replica() {
    let extras = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let paths = extras.each_ref().map(|dir| dir.path().to_path_buf());
    let (h1, h2, h3, _d1, _d2, _d3, addr) =
        start_three_broker_plaintext_cluster_with_log_dirs(&paths).await;
    create_topic_plaintext(
        addr,
        crate::kafka_wire::AutomaticTopicSetup {
            client_id: crate::plaintext_wire::CLIENT_ID,
            topic: crate::support::topics::ConfiguredTopicSetup {
                name: ("preplaced").into(),
                replicas: crate::support::topics::TopicReplicationFactor(2),
                ..Default::default()
            },
        },
    )
    .await;
    wait_partition_exists(&h1, "preplaced", 0).await;

    let partition = h1
        .partition_record_for_test("preplaced", 0)
        .expect("partition");
    let new_replica = (1..=3)
        .find(|id| !partition.replicas.contains(&node_id(*id)))
        .expect("unassigned broker");
    let target_idx = usize::try_from(new_replica - 1).unwrap();
    let handles = [&h1, &h2, &h3];
    let target_addr = handles[target_idx].listen_addr();
    let target_dir = extras[target_idx].path();
    // The preference is keyed by topic ID from the target broker's metadata.
    wait_partition_exists(handles[target_idx], "preplaced", 0).await;

    let preference = alter_replica_log_dirs(target_addr, target_dir, "preplaced", vec![0]).await;
    assert!(preference.results[0].partitions[0].error_code == 9);

    let staying = broker_id(partition.replicas[0]);
    let raft_addr = controller_leader_addr(&handles).await;
    let response = drive_alter_reassignments(
        raft_addr,
        vec![("preplaced", 0, Some(vec![staying, new_replica]))],
    )
    .await;
    assert!(response[0].1 == vec![(0, 0)]);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let described = describe_log_dirs(target_addr).await;
        let materialized = described.results.iter().any(|result| {
            std::fs::canonicalize(&result.log_dir).ok().as_deref()
                == std::fs::canonicalize(target_dir).ok().as_deref()
                && result.topics.iter().any(|topic| {
                    topic.name == "preplaced"
                        && topic.partitions.iter().any(|partition| {
                            partition.partition_index == 0 && !partition.is_future_key
                        })
                })
        });
        if materialized {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "replica was not materialized in the preferred log dir"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(target_dir.join("preplaced-0").is_dir());

    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

/// Test 1: send `AlterPartitionReassignments`, then inject an ISR that
/// includes the new replica. The background task sees that the adding set is
/// inside the ISR, completes the reassignment, and clears `adding_replicas`
/// and `removing_replicas`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_then_complete_via_isr_catchup() {
    let _g = case_lock!();

    let (h1, h2, h3, _d1, _d2, _d3, _addr1) = foo_cluster().await;

    // Find which brokers are in `replicas` initially — choose target accordingly.
    let pr = h1.partition_record_for_test("foo", 0).expect("partition");
    let initial_replicas = pr.replicas.clone();
    assert!(initial_replicas.len() == 2);
    // Pick the third broker (not in initial_replicas) as the new replica.
    let new_replica: i32 = (1..=3)
        .find(|n| !initial_replicas.contains(&node_id(*n)))
        .expect("free broker");
    let removing = broker_id(*initial_replicas.last().unwrap());
    let staying = broker_id(*initial_replicas.first().unwrap());
    let target = vec![staying, new_replica];

    // Send alter to controller leader (whichever broker leads raft).
    let raft_addr = controller_leader_addr(&[&h1, &h2, &h3]).await;
    let resp = drive_alter_reassignments(raft_addr, vec![("foo", 0, Some(target.clone()))]).await;
    assert!(
        resp[0].1 == vec![(0, 0)],
        "expected error_code=0; got {:?}",
        resp
    );

    // Wait for the image to reflect the in-flight reassignment.
    h1.wait_for_image(|img| {
        img.partition("foo", 0)
            .is_some_and(|p| !p.adding_replicas.is_empty())
    })
    .await;
    let pr_after_alter = h1.partition_record_for_test("foo", 0).expect("partition");
    assert!(
        pr_after_alter
            .adding_replicas
            .contains(&node_id(new_replica)),
        "adding_replicas should contain new_replica; pr={pr_after_alter:?}"
    );
    assert!(
        pr_after_alter
            .removing_replicas
            .contains(&node_id(removing)),
        "removing_replicas should contain removing; pr={pr_after_alter:?}"
    );

    // Inject ISR including the new replica so the background task completes the reassignment.
    let injected = krabka_metadata::PartitionRecord {
        isr: vec![node_id(staying), node_id(new_replica), node_id(removing)],
        ..pr_after_alter.clone()
    };
    h1.submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(injected))
        .await
        .expect("inject");

    // The background task should observe adding ⊆ isr and complete, clearing
    // adding_replicas and removing_replicas.
    h1.wait_for_image(|img| {
        img.partition("foo", 0)
            .is_some_and(|p| p.adding_replicas.is_empty() && p.removing_replicas.is_empty())
    })
    .await;
    let pr = h1.partition_record_for_test("foo", 0).expect("partition");
    let actual: std::collections::HashSet<u64> = pr.replicas.iter().map(|n| n.0).collect();
    let expected: std::collections::HashSet<u64> = target.iter().map(|n| node_id(*n).0).collect();
    assert!(
        actual == expected,
        "replicas after completion should match target; pr={pr:?}"
    );
    // Clean up.
    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

/// Test 2: after `AlterPartitionReassignments` starts a reassignment, the
/// `ListPartitionReassignments` handler must return the in-flight rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_in_flight_returns_pending_rows() {
    let _g = case_lock!();

    let (h1, h2, h3, _d1, _d2, _d3, _addr) = foo_cluster().await;

    let pr = h1.partition_record_for_test("foo", 0).expect("partition");
    let new_replica: i32 = (1..=3)
        .find(|n| !pr.replicas.contains(&node_id(*n)))
        .expect("free");
    let staying = broker_id(*pr.replicas.first().unwrap());
    let target = vec![staying, new_replica];

    let raft_addr = controller_leader_addr(&[&h1, &h2, &h3]).await;
    drive_alter_reassignments(raft_addr, vec![("foo", 0, Some(target))]).await;

    // Wait for the image to reflect adding_replicas, then list.
    h1.wait_for_image(|img| {
        img.partition("foo", 0)
            .is_some_and(|p| !p.adding_replicas.is_empty())
    })
    .await;

    let listed = drive_list_reassignments(raft_addr, None).await;
    let foo = listed
        .iter()
        .find(|(n, _)| n == "foo")
        .expect("foo should appear in list");
    assert!(
        foo.1.len() == 1,
        "expected 1 partition in-flight; got {:?}",
        foo.1
    );
    check!(foo.1[0].0 == 0, "expected partition_index=0");
    check!(
        foo.1[0].2 == vec![new_replica],
        "expected adding_replicas=[new_replica]; got {:?}",
        foo.1[0].2
    );

    // Clean up.
    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

/// Test 3: cancel an in-flight reassignment with `target=None`, that is null
/// replicas. The partition record must return to the original replica set,
/// with empty `adding_replicas` and `removing_replicas`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_via_null_replicas_reverts() {
    let _g = case_lock!();

    let (h1, h2, h3, _d1, _d2, _d3, _addr) = foo_cluster().await;

    let pr = h1.partition_record_for_test("foo", 0).expect("partition");
    let original_replicas = pr.replicas.clone();
    let new_replica: i32 = (1..=3)
        .find(|n| !original_replicas.contains(&node_id(*n)))
        .expect("free");
    let new_replica_id = u64::try_from(new_replica).expect("broker ID is non-negative");
    let staying = broker_id(*original_replicas.first().unwrap());
    let target = vec![staying, new_replica];

    // Stop the target broker after registration. It remains a valid target in
    // metadata, but cannot join the ISR and let the background task complete
    // the reassignment before this test cancels it.
    let mut handles = [Some(h1), Some(h2), Some(h3)];
    let offline = handles
        .iter_mut()
        .find(|handle| {
            handle
                .as_ref()
                .is_some_and(|handle| handle.node_id() == new_replica_id)
        })
        .and_then(Option::take)
        .expect("new replica handle");
    offline.shutdown().await;

    let observer = handles
        .iter()
        .flatten()
        .find(|handle| {
            original_replicas
                .iter()
                .any(|replica| replica.0 == handle.node_id())
        })
        .expect("live original replica");
    let mut leader_rx = observer.watch_leader_for_test();
    let leader = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if let Some(leader) = *leader_rx.borrow_and_update()
                && handles
                    .iter()
                    .flatten()
                    .any(|handle| handle.node_id() == leader.0)
            {
                return leader;
            }
            leader_rx
                .changed()
                .await
                .expect("leader watch remains open");
        }
    })
    .await
    .expect("surviving controller leader");
    let raft_addr = handles
        .iter()
        .flatten()
        .find(|handle| handle.node_id() == leader.0)
        .expect("leader handle")
        .listen_addr();
    drive_alter_reassignments(raft_addr, vec![("foo", 0, Some(target))]).await;

    observer
        .wait_for_image(|img| {
            img.partition("foo", 0)
                .is_some_and(|p| !p.adding_replicas.is_empty())
        })
        .await;

    // Cancel: replicas = None.
    let resp = drive_alter_reassignments(raft_addr, vec![("foo", 0, None)]).await;
    assert!(
        resp[0].1 == vec![(0, 0)],
        "cancel should succeed; got {:?}",
        resp
    );

    // Wait for the image to reflect the cancellation.
    observer
        .wait_for_image(|img| {
            img.partition("foo", 0).is_some_and(|p| {
                p.replicas == original_replicas
                    && p.adding_replicas.is_empty()
                    && p.removing_replicas.is_empty()
            })
        })
        .await;
    let pr_after_cancel = observer
        .partition_record_for_test("foo", 0)
        .expect("partition");
    assert!(
        pr_after_cancel.replicas == original_replicas,
        "replicas should revert to original after cancel; pr={pr_after_cancel:?}"
    );
    // Clean up.
    for handle in handles.into_iter().flatten() {
        handle.shutdown().await;
    }
}

async fn foo_cluster() -> (
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    krabka_broker::BrokerHandle,
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
    std::net::SocketAddr,
) {
    let cluster = start_three_broker_plaintext_cluster().await;
    create_topic_plaintext(
        cluster.6,
        crate::kafka_wire::AutomaticTopicSetup {
            client_id: crate::plaintext_wire::CLIENT_ID,
            topic: crate::support::topics::ConfiguredTopicSetup {
                name: ("foo").into(),
                replicas: crate::support::topics::TopicReplicationFactor(2),
                ..Default::default()
            },
        },
    )
    .await;
    wait_partition_exists(&cluster.0, "foo", 0).await;
    cluster
}
