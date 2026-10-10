//! Restart recovery: a follower that snapshots, dies, and is reopened over its
//! own data dir rebuilds its metadata image from the checkpoint plus the log.

use crate::{
    harness as fixture,
    harness::{
        STAGGERED_TIMEOUTS, await_single_leader, await_until, metadata_log, topic_record, voter_set,
    },
};

/// 4. Restart recovery: commit, snapshot, drop one engine, reopen it over its
///    dir, then assert that the image is rebuilt from the checkpoint and the
///    log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovers_image() {
    let (net, ids) = crate::harness::three_voter_network();
    let cid = uuid::Uuid::from_u128(400);

    // Keep per-node data dirs so we can reopen one.
    let dirs: fixture::HashMap<_, _> = ids
        .iter()
        .copied()
        .zip(crate::harness::start_engines(
            &net,
            crate::harness::SimClusterSetup {
                ids: &ids,
                cluster_id: cid,
                ..Default::default()
            },
        ))
        .collect();

    let (leader, _epoch) = await_single_leader(&net, &ids, fixture::Duration::from_secs(10)).await;

    // Commit a topic and ensure it is replicated everywhere.
    tokio::time::timeout(
        fixture::Duration::from_secs(10),
        net.get(leader)
            .unwrap()
            .submit_change(vec![topic_record("persistent", 9)]),
    )
    .await
    .expect("submit did not hang")
    .expect("submit ok");

    for &id in &ids {
        let ctrl = net.get(id).unwrap();
        await_until(fixture::Duration::from_secs(10), || {
            ctrl.current_image().topic("persistent").map(|_| ())
        })
        .await;
    }

    // Pick a follower to restart so the cluster keeps a leader meanwhile.
    let victim = *ids.iter().find(|&&id| id != leader).unwrap();
    let victim_ctrl = net.get(victim).unwrap();
    // Snapshot the victim's image, then drop it.
    victim_ctrl.trigger_snapshot().await.unwrap();
    victim_ctrl.shutdown().await;
    net.remove(victim);
    // intentional: let the shutdown-signalled engine task exit and drop its
    // KraftLog before we reopen the same data dir. `shutdown()` only sends
    // `Command::Shutdown`; the loop is spawned fire-and-forget with no JoinHandle,
    // so there is no accessor to await loop teardown / log-handle release.
    tokio::time::sleep(fixture::Duration::from_millis(50)).await;

    let victim_dir = dirs.get(&victim).unwrap().path().to_path_buf();
    let reopened = fixture::KraftController::open(
        victim_dir,
        victim,
        cid,
        uuid::Uuid::nil(),
        voter_set(&ids),
        STAGGERED_TIMEOUTS[usize::try_from(victim.0 - 1).unwrap()],
        None,
        fixture::ControllerFetchMissLimit::default(),
        fixture::MetadataRaftCommandQueueCapacity::default(),
        fixture::MetadataRaftFetchMax::default(),
        fixture::Arc::new(net.as_peer(victim)),
        0,
        krabka_units::prelude::bytes(0),
        krabka_units::prelude::millis(0),
        fixture::MetadataSnapshotFetchMax::default(),
        metadata_log(),
        krabka_raft::kraft::Activation::default(),
    )
    .expect("reopen");
    // The recovered image must contain the committed topic.
    assert2::assert!(reopened.current_image().topic("persistent").is_some());
    net.register(victim, reopened);

    crate::harness::shutdown_nodes(&net, &ids).await;
}
