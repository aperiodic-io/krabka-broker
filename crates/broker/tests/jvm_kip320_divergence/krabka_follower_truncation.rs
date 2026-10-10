//! Scenario 3: a Krabka follower truncates a divergent suffix from a JVM leader.
//!
//! This is the reverse direction of scenario 2. The scenario parks replication
//! behind a phantom leader, appends a Krabka-only suffix, then promotes the JVM
//! replica, and asserts that the Krabka follower truncates to the shared prefix
//! and resumes at the JVM leader's exact log end offset.

use std::time::{Duration, Instant};

use krabka_metadata::{LeaderEpoch, MetadataRecord};

use crate::{
    docker::{produce_lines_via_jvm, set_container_paused},
    dump_log::{dump_log_in_container, max_offset_in_dump},
    topic_admin::{LEADER_WAIT, wait_for_described_leader},
};

/// Step 2 of Task 11, reverse direction. A Krabka follower replicates from a
/// JVM leader. The test parks replication behind a phantom leader, appends a
/// suffix only to the Krabka replica at a new epoch, and then promotes the JVM
/// replica. It asserts that the Krabka follower observes the JVM leader's
/// `diverging_epoch`, truncates to their shared prefix, and subsequently copies
/// a fresh JVM-authored suffix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + a published controller/data port; Linux-bound"]
async fn kip320_krabka_follower_truncates_from_jvm_leader() {
    const TOPIC: &str = "krabka-kip320-krabka-follower";
    let (container, cluster, bootstrap_all, prefix_leo) = crate::mixed_cluster::prepare_divergence(
        TOPIC,
        crate::mixed_cluster::DivergenceDirection::KrabkaFollower,
    )
    .await;
    let c1 = &cluster.krabka[0].0;

    // 3. Park replication behind a phantom leader before appending the
    //    Krabka-only suffix. This makes the divergent state deterministic:
    //    neither the JVM replica nor broker 2 can copy the forged records.

    c1.wait_until_partition_present(TOPIC, 0).await;
    let partition = c1
        .partition_record_for_test(TOPIC, 0)
        .expect("partition record present after wait");
    let parked_epoch = LeaderEpoch(partition.leader_epoch.0 + 1);

    // Freeze the JVM replica before parking replication. Otherwise it can
    // copy part of the deliberately forged suffix while the phantom-leader
    // metadata is still propagating, making its authoritative prefix longer.
    set_container_paused(&container, true);

    c1.submit_metadata_record_for_test(MetadataRecord::V1Partition(
        crate::mixed_cluster::single_leader_record(
            &partition,
            crate::mixed_cluster::SingleLeaderSetup {
                topic: TOPIC,
                leader: krabka_broker::NodeId(99),
                epoch: parked_epoch,
                ..Default::default()
            },
        ),
    ))
    .await
    .expect("park reverse-direction replicas behind phantom leader");
    c1.wait_until_local_partition_target(TOPIC, 0, krabka_broker::NodeId(99), parked_epoch)
        .await;

    c1.produce_records_for_test(TOPIC, 0, 5)
        .await
        .expect("append divergent suffix on parked Krabka replica");
    let krabka_leo_diverged = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    eprintln!(
        "KRABKA[kip320] reverse: Krabka replica LEO {prefix_leo} -> {krabka_leo_diverged} (forced divergent suffix)"
    );
    assert2::assert!(
        krabka_leo_diverged == prefix_leo + 5,
        "Krabka-only divergent suffix should add five records"
    );

    // 4. Promote the JVM replica at the next epoch. Its log still ends at the
    //    shared prefix, so the Krabka follower must truncate before fetching.
    let jvm_epoch = LeaderEpoch(parked_epoch.0 + 1);
    c1.submit_metadata_record_for_test(MetadataRecord::V1Partition(
        crate::mixed_cluster::single_leader_record(
            &partition,
            crate::mixed_cluster::SingleLeaderSetup {
                topic: TOPIC,
                leader: krabka_broker::NodeId(3),
                epoch: jvm_epoch,
                partition_epoch_delta: crate::mixed_cluster::PartitionEpochDelta(2),
            },
        ),
    ))
    .await
    .expect("promote JVM broker for reverse-direction recovery");

    set_container_paused(&container, false);

    wait_for_described_leader(&bootstrap_all, TOPIC, 3, LEADER_WAIT).await;

    // 5. Observe the truncation itself, before adding any new leader records.
    //    Equal final LEOs alone would not distinguish truncate-and-refetch from
    //    leaving the bogus suffix in place.
    let dl = Instant::now() + Duration::from_secs(45);
    let mut final_leo = krabka_leo_diverged;
    loop {
        final_leo = c1.local_log_end_offset(TOPIC, 0).unwrap_or(final_leo);
        if final_leo == prefix_leo {
            break;
        }
        assert2::assert!(
            Instant::now() <= dl,
            "Krabka follower did not truncate to JVM prefix LEO {prefix_leo}; current LEO={final_leo}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let jvm_prefix_dump =
        dump_log_in_container(&container, &format!("/tmp/kraft-mixed-logs/{TOPIC}-0"));
    assert2::assert!(
        max_offset_in_dump(&jvm_prefix_dump) == Some(prefix_leo - 1),
        "JVM leader should retain exactly the shared prefix:\n{jvm_prefix_dump}"
    );
    assert2::assert!(
        !jvm_prefix_dump.contains("test-record-"),
        "Krabka-only divergent suffix leaked to JVM leader:\n{jvm_prefix_dump}"
    );

    // 6. Prove that replication resumes from the truncated boundary by writing
    //    a shorter, JVM-authored suffix and waiting for Krabka's exact LEO.
    let authoritative = (0..3)
        .map(|i| format!("jvm-authoritative-{i}"))
        .collect::<Vec<_>>();
    produce_lines_via_jvm(&bootstrap_all, TOPIC, &authoritative);
    c1.wait_until_local_log_end_offset(TOPIC, 0, prefix_leo + 3)
        .await;
    final_leo = c1.local_log_end_offset(TOPIC, 0).unwrap_or(0);
    assert2::assert!(
        final_leo == prefix_leo + 3,
        "Krabka follower did not resume at the JVM leader's exact LEO"
    );
    eprintln!(
        "KRABKA[kip320] reverse: truncated from {krabka_leo_diverged} to {prefix_leo}, then followed JVM to {final_leo}"
    );

    cluster.shutdown().await;
}
