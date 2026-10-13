//! A dead broker must stop being advertised as `controller_id`, on every node.
//!
//! `Metadata` and `DescribeCluster` name a registered, unfenced broker rather
//! than the quorum leader (see `handlers::controller_id`), so the id has to be
//! one a client can actually reach. Kafka draws it from
//! `getRandomAliveBrokerId`, and a `KRaft` broker knows who is alive because
//! `BrokerRegistration.fenced` is replicated to it.
//!
//! Only the controller leader holds the heartbeat registry that decides
//! fencing, so it writes the decision into the broker's registration, as
//! Kafka's `BrokerRegistrationChangeRecord` does.
//! This suite asks a node that is *not* the controller: if that node ignored
//! the replicated state it would treat every registration as available, and it
//! would hand a client the dead broker's id — an endpoint nobody answers on,
//! which is the controller-routed dead end the whole feature exists to avoid.
//!
//! `handlers::controller_id`'s own unit test pins the choice given a fenced
//! set. What is unproven there, and proven here, is that a follower's fenced
//! set tracks a death it only learns about through the metadata log.

use {
    crate::support::{
        client::connect_client, discovery::topic_metadata_request, registered_cluster,
        shutdown_cluster,
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::DescribeClusterResponse, metadata_response::MetadataResponse,
    },
    krabka_units::convert::TimeExt as _,
    std::{collections::BTreeSet, time::Duration},
};

mod support;

/// Requests per measurement. The advertised id rotates one step per request,
/// so a run of this length visits every candidate of a three-broker cluster at
/// least twice.
const REQUESTS: usize = 6;

/// A bound on a `heartbeat_timeout` (2s under the test config) for the
/// session to expire, a `liveness_tick_interval` (1s) for the tick that
/// publishes the decision, and the metadata propagation behind it, with room
/// for a loaded runner. Only a stuck cluster reaches it.
const FENCING_DEADLINE: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_non_controller_node_stops_advertising_a_broker_that_died() {
    support::init_tracing();

    let mut cluster = registered_cluster(3).await;

    // The node under test must not be the one holding the heartbeat registry,
    // or the answer could come from local state rather than from the log.
    let (leader, observer_index, victim_index) = support::two_controller_followers(&cluster).await;
    let observer_addr = cluster[observer_index].0.listen_addr();
    let victim_id = cluster[victim_index].0.node_id();
    let everyone: BTreeSet<i32> = cluster.iter().map(|(h, _, _)| node_id_of(h)).collect();
    let survivors: BTreeSet<i32> = everyone
        .iter()
        .copied()
        .filter(|&id| id != node_id_of(&cluster[victim_index].0))
        .collect();

    let client = connect_client(observer_addr.to_string(), None).await;

    let leader_index = (0..cluster.len())
        .find(|&i| cluster[i].0.node_id() == leader.0)
        .expect("the elected leader is one of the nodes under test");
    settle_unfenced(
        &cluster[leader_index].0,
        &cluster[observer_index].0,
        &everyone,
    )
    .await;

    // Every broker is advertised while every broker is alive, so what the
    // assertions after the death measure is the death, and not a rotation that
    // was never going to name the victim.
    assert!(
        advertised_over_a_run(&client).await == everyone,
        "a live cluster should advertise every one of its brokers"
    );

    // The config and the log dir stay alive for the rest of the test: a
    // crashed broker's teardown must not race a deleted directory.
    let (victim, _victim_config, _victim_dir) = cluster.remove(victim_index);
    victim.crash_for_test().await;

    wait_until_fenced_set(&cluster[observer_index].0, &BTreeSet::from([victim_id])).await;

    // From here the observer must name only survivors, on both APIs, for every
    // turn of the rotation, and `Metadata` must stop listing the dead broker,
    // as Kafka's `getAliveBrokerNodes` does, so a client does not dial it.
    let dead_id = i32::try_from(victim_id).expect("node id fits an i32");
    for _ in 0..REQUESTS {
        let resp: MetadataResponse = client
            .send(topic_metadata_request(Some(vec![])))
            .await
            .unwrap();
        assert!(
            resp.brokers.iter().all(|row| row.node_id != dead_id),
            "the dead broker is not listed in Metadata: {:?}",
            resp.brokers
        );
        assert!(
            survivors.contains(&resp.controller_id),
            "Metadata advertised {} as controller_id; only {survivors:?} are alive",
            resp.controller_id
        );

        let resp: DescribeClusterResponse = client
            .send(DescribeClusterRequest::default())
            .await
            .unwrap();
        assert!(
            survivors.contains(&resp.controller_id),
            "DescribeCluster advertised {} as controller_id; only {survivors:?} are alive",
            resp.controller_id
        );
    }

    // The answers above are only worth anything if they came from a node that
    // does not hold the heartbeat registry.
    let observer = &cluster[observer_index].0;
    assert!(
        observer.controller_leader_id() != Some(krabka_raft::NodeId(observer.node_id())),
        "the observer must still be a follower for this test to mean anything"
    );

    shutdown_cluster(cluster).await;
}

fn node_id_of(handle: &BrokerHandle) -> i32 {
    i32::try_from(handle.node_id()).expect("node id fits an i32")
}

/// Block until the observer's image is known to be past the cluster's start-up
/// unfencing, with an empty fenced set.
///
/// A broker registers fenced and is unfenced on its first caught-up heartbeat,
/// so a freshly booted cluster writes an unfencing edge shortly after start. An
/// observer image that has simply not seen that edge reads exactly like one
/// past it: both report nobody fenced. Waiting on the empty set alone can
/// therefore return before the seed lands, and the death this test measures
/// would then race the unfencing that precedes it.
///
/// Watching for the fenced set to go non-empty and then empty would not settle
/// it either. The liveness tick's fence publication is level-triggered: the
/// controller leader compares its registry against the image and writes only
/// the difference, so it may fence a broker that a heartbeat then unfences,
/// or never fence one at all.
///
/// What is observable is the decision, the pass that published it, and the
/// offset it was written at.
///
/// The leader's liveness registry reporting every broker alive means no later
/// pass computes a fence. It does not mean no earlier one is still committing:
/// a pass that read the registry before the first heartbeats decides
/// a fence and then awaits its own `submit_change`, and while
/// that await is outstanding the registry already reads all-alive and the
/// leader's image does not carry the record yet. Reading the image there would
/// find nobody fenced and be wrong about it. So wait for one publication pass
/// to *complete* after the all-alive point: the pass awaits its commit, so its
/// counter moving means any fence decided before that point is applied and
/// visible, not in flight.
///
/// From there the image is honest, and the leader agreeing that nobody is
/// fenced means the tombstone for anything it did write has committed. An
/// offset read after that is past every fencing record the start-up
/// produced, which is what makes the last wait mean something: an observer
/// that has applied up to that offset has applied the fencing too, so an empty
/// fenced set there can no longer be one that has not seen it yet.
async fn settle_unfenced(leader: &BrokerHandle, observer: &BrokerHandle, everyone: &BTreeSet<i32>) {
    for &id in everyone {
        leader
            .wait_until_broker_alive(u64::try_from(id).expect("node id is positive"))
            .await;
    }
    let published = leader.metrics().controller_fencing_publications_total.get();
    leader
        .wait_for_metrics(
            "a fencing publication completing after every broker is alive",
            |m| m.controller_fencing_publications_total.get() > published,
        )
        .await;
    wait_until_fenced_set(leader, &BTreeSet::new()).await;
    let unfenced_at = leader.metadata_offset_for_test();

    observer
        .wait_until_metadata_offset_at_least(unfenced_at)
        .await;
    wait_until_fenced_set(observer, &BTreeSet::new()).await;
}

/// The ids one run of requests advertises as `controller_id`.
async fn advertised_over_a_run(client: &Client) -> BTreeSet<i32> {
    let mut advertised = BTreeSet::new();
    for _ in 0..REQUESTS {
        let resp: MetadataResponse = client
            .send(topic_metadata_request(Some(vec![])))
            .await
            .unwrap();
        advertised.insert(resp.controller_id);
    }
    advertised
}

/// Block until `handle`'s own replicated image marks exactly `expected` fenced.
///
/// This reads the image rather than a response, on purpose: the wait must not
/// run through the handlers under test, or a handler that ignores the
/// replicated state would simply hang here instead of failing on the id it
/// advertises.
async fn wait_until_fenced_set(handle: &BrokerHandle, expected: &BTreeSet<u64>) {
    let deadline = tokio::time::Instant::now() + FENCING_DEADLINE;
    loop {
        let fenced = handle.fenced_broker_ids_for_test();
        if fenced == *expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the observer's image never settled on fenced brokers {expected:?}; it has {fenced:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A broker booted from `BrokerConfig::for_tests` must keep its heartbeat
/// session, as a Kafka broker does, and so must the same broker restarted on
/// its log directory.
///
/// `for_tests` asks for port 0 on the controller listener and names that same
/// `127.0.0.1:0` as its own voter endpoint. The heartbeat client dials the
/// controller leader through the voter endpoint, so if the broker does not
/// publish the port it bound, no heartbeat ever arrives. The controller then
/// fences the broker and expires its session one `heartbeat_timeout` after
/// start, and every single-broker test runs against a fenced broker.
///
/// The restart holds the first controller port, so the second start must bind
/// another one. The port the first start published must not survive in the
/// recovered metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_for_tests_broker_keeps_its_heartbeat_session() {
    support::init_tracing();

    let dir = tempfile::tempdir().expect("log dir");
    let config = krabka_broker::BrokerConfig::for_tests(dir.path().to_path_buf());
    let broker = krabka_broker::Broker::start(config.clone())
        .await
        .expect("start broker");
    let first_port = broker.controller_addr();
    let started = session_state(&broker, &config).await;
    broker.shutdown().await;

    let _held = std::net::TcpListener::bind(first_port).expect("hold the first controller port");
    let mut rejoin = config.clone();
    rejoin.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let broker = krabka_broker::Broker::start(rejoin)
        .await
        .expect("restart broker");
    let restarted = session_state(&broker, &config).await;
    broker.shutdown().await;

    assert!([started, restarted] == [(true, BTreeSet::new()), (true, BTreeSet::new())]);
}

/// Whether the controller holds `broker` alive, and the brokers its image
/// marks fenced, once a session that never heartbeated would have expired.
///
/// It waits for three session timeouts and a liveness tick, and then for one
/// more fencing publication, so an expired session has been decided and
/// published before it reads the state.
async fn session_state(
    broker: &BrokerHandle,
    config: &krabka_broker::BrokerConfig,
) -> (bool, BTreeSet<u64>) {
    tokio::time::sleep(
        config.heartbeat_timeout.to_std() * 3 + config.liveness_tick_interval.to_std(),
    )
    .await;
    let published = broker.metrics().controller_fencing_publications_total.get();
    broker
        .wait_for_metrics("a fencing publication after the session window", |m| {
            m.controller_fencing_publications_total.get() > published
        })
        .await;
    (
        broker.broker_alive_for_test(broker.node_id()).await,
        broker.fenced_broker_ids_for_test(),
    )
}
