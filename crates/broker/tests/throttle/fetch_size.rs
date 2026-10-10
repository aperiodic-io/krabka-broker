//! Tests 3 to 6: the leader-side throttle caps the bytes a replica Fetch
//! returns, an unthrottled partition is left alone, a list that names only the
//! follower throttles nothing, and a follower in the ISR is never throttled.
//!
//! All run on the PLAINTEXT cluster: the throttle decision is made on the
//! fetch path and needs no principal, so the compat shim that allows every
//! operation while there are no ACLs keeps the setup to the minimum. They are
//! the tests that give the suite its name — the config tests only prove the key
//! round-trips, these prove it is enforced.

use assert2::assert;

use crate::{
    cluster::{
        add_follower, add_follower_in_isr, create_topic_plaintext, start_single_broker_plaintext,
        wait_partition_exists,
    },
    configs::drive_incremental_alter_configs_plaintext,
    records::{fetch_plaintext_replica, produce_plaintext},
};

/// Sets a leader throttle rate of 512 bytes/sec and the given
/// `leader.replication.throttled.replicas` list on a topic of one partition,
/// fills the partition with 8 KB, and returns the size of the response to a
/// Fetch with `replica_id=2`, which is a follower outside the ISR, or in it
/// when `follower_in_isr` says so.
///
/// `list` gets the broker id of the leader, the one the topic lists it by.
async fn replica_fetch_bytes(
    list: impl FnOnce(u64) -> String,
    follower_in_isr: bool,
    fetch_again_after_append: bool,
) -> (usize, Option<usize>) {
    let (handle, _dir, addr) = start_single_broker_plaintext().await;
    let node_id = handle.node_id();

    // Create topic rf=1 so this broker is always the leader, then assign
    // replica 2 as a follower so its fetch is served.
    create_topic_plaintext(addr, crate::CLIENT_ID, "bar", 1, 1).await;
    wait_partition_exists(&handle, "bar", 0).await;
    if follower_in_isr {
        add_follower_in_isr(&handle, "bar", 2).await;
    } else {
        add_follower(&handle, "bar", 2).await;
    }

    // The token bucket has a one-second burst capacity at the configured rate.
    let err = drive_incremental_alter_configs_plaintext(
        addr,
        vec![(
            4, // resource_type = Broker
            node_id.to_string(),
            vec![(
                "leader.replication.throttled.rate".into(),
                Some("512".into()),
                0, // OP_SET
            )],
        )],
    )
    .await;
    assert!(err == 0, "broker throttle alter failed: error_code={err}");

    let list = list(node_id);
    let err = drive_incremental_alter_configs_plaintext(
        addr,
        vec![(
            2, // resource_type = Topic
            "bar".into(),
            vec![(
                "leader.replication.throttled.replicas".into(),
                Some(list.clone()),
                0, // OP_SET
            )],
        )],
    )
    .await;
    assert!(err == 0, "topic throttle alter failed: error_code={err}");

    // Wait for the configs to appear in the image before producing (so the
    // throttle enforcement is armed when the Fetch arrives).
    handle
        .wait_for_image(|img| {
            img.broker_throttle_rate(
                krabka_metadata::NodeId(node_id),
                krabka_metadata::ThrottleKind::Leader,
            ) == Some(krabka_units::bytes_per_sec(512))
                && img
                    .topic_config("bar")
                    .and_then(|configs| configs.get("leader.replication.throttled.replicas"))
                    == Some(&list)
        })
        .await;

    // Produce 8 KB of data (8 records of 1 KB each).
    produce_plaintext(addr, "bar", 1024, 8).await;

    // Fetch with replica_id=2 (inter-broker follower path → leader throttle applies).
    let resp_bytes = fetch_plaintext_replica(addr, "bar", 2).await;
    let followup_bytes = if fetch_again_after_append {
        // The first over-burst partition payload is allowed to make progress;
        // append another payload so the next fetch tests the debt it incurred.
        produce_plaintext(addr, "bar", 1024, 8).await;
        Some(fetch_plaintext_replica(addr, "bar", 2).await)
    } else {
        None
    };
    handle.shutdown().await;
    (resp_bytes, followup_bytes)
}

/// Test 3: After setting a very low leader throttle rate (512 bytes/sec) and
/// marking partition 0 as throttled on this broker, a Fetch issued with
/// `replica_id=2`, a follower outside the ISR, must return a response well
/// under 8 KB.
///
/// An 8 KB response must be capped to at most 512 bytes of record data. The
/// list names the leader, as Kafka reads it:
/// `leader.replication.throttled.replicas` is `partition:replica` for the
/// replicas that leader holds (#1210).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throttle_rate_caps_fetch_response_size() {
    let (first_bytes, followup_bytes) =
        replica_fetch_bytes(|leader| format!("0:{leader}"), false, true).await;

    // A single indivisible partition payload larger than the burst progresses
    // once; its bounded debt throttles the next fetch, which must return no
    // records (up to protocol framing overhead).
    assert!(
        first_bytes >= 4096,
        "expected the over-burst payload to make progress, got {first_bytes} bytes"
    );
    let followup_bytes = followup_bytes.expect("requested a follow-up fetch");
    assert!(
        followup_bytes <= 2048,
        "expected the debt-throttled fetch response <= 2048 bytes, got {followup_bytes} bytes"
    );
}

/// Test 4: Without any throttle config, a Fetch with `replica_id >= 0` delivers
/// all 8 KB of data unimpeded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unthrottled_partition_unaffected() {
    let (handle, _dir, addr) = start_single_broker_plaintext().await;

    // Create topic rf=1.
    create_topic_plaintext(addr, crate::CLIENT_ID, "baz", 1, 1).await;
    wait_partition_exists(&handle, "baz", 0).await;
    add_follower(&handle, "baz", 2).await;

    // Produce 8 KB of data (8 records of 1 KB each). No throttle configured.
    produce_plaintext(addr, "baz", 1024, 8).await;

    // Fetch with replica_id=2 (inter-broker path). No throttle → full data.
    let resp_bytes = fetch_plaintext_replica(addr, "baz", 2).await;

    // Full 8 KB data plus framing. The response should be well over 4 KB.
    assert!(
        resp_bytes >= 4096,
        "expected unthrottled fetch response >= 4096 bytes, got {resp_bytes} bytes"
    );

    handle.shutdown().await;
}

/// Test 5: The leader reads its own id from the list, so a list that names
/// only the fetching follower, the reading the old code made, throttles
/// nothing on the leader: `kafka-reassign-partitions --throttle` lists the
/// source replicas on the leader side, and never the destination that fetches
/// (#1210).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_list_naming_only_the_follower_throttles_nothing_on_the_leader() {
    let (resp_bytes, _) = replica_fetch_bytes(|_| "0:2".to_owned(), false, false).await;

    assert!(
        resp_bytes >= 4096,
        "expected an unthrottled fetch response >= 4096 bytes, got {resp_bytes} bytes"
    );
}

/// Test 6: A follower in the ISR is never throttled, whatever the list and the
/// rate say, so a throttle left configured after a reassignment cannot starve
/// the replicas that keep the partition available (#1211).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_follower_in_the_isr_is_not_throttled() {
    let (resp_bytes, _) = replica_fetch_bytes(|_| "*".to_owned(), true, false).await;

    assert!(
        resp_bytes >= 4096,
        "expected an in-sync follower's fetch response >= 4096 bytes, got {resp_bytes} bytes"
    );
}
