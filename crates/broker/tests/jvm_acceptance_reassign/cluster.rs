//! Common cluster mutations used by the reassignment scenarios.
use krabka_broker::BrokerHandle;
use krabka_metadata::PartitionRecord;

pub(crate) fn free_replica(record: &PartitionRecord) -> u64 {
    (1u64..=3)
        .find(|node| !record.replicas.contains(&krabka_metadata::NodeId(*node)))
        .expect("free broker")
}

pub(crate) fn plan_file(
    topic: &str,
    staying: u64,
    new_node: u64,
) -> crate::jvm_acceptance::TempFileMount {
    let json = format!(
        r#"{{"version":1,"partitions":[{{"topic":"{topic}","partition":0,"replicas":[{staying},{new_node}]}}]}}"#
    );
    crate::jvm_acceptance::write_temp_file("reassignment.json", &json)
}

pub(crate) async fn offline_target(
    brokers: [BrokerHandle; 3],
    current: &PartitionRecord,
    controller_leader: u64,
) -> (u64, [Option<BrokerHandle>; 3]) {
    let target = (2_u64..=3)
        .find(|node| {
            *node != controller_leader && !current.replicas.iter().any(|replica| replica.0 == *node)
        })
        .expect("a non-bootstrap target broker");
    let mut handles = brokers.map(Some);
    handles[usize::try_from(target - 1).unwrap()]
        .take()
        .expect("offline target handle")
        .shutdown()
        .await;
    (target, handles)
}

pub(crate) fn assert_reassigned(broker: &BrokerHandle, topic: &str, staying: u64, new_node: u64) {
    let pr = broker
        .partition_record_for_test(topic, 0)
        .expect("partition record after reassignment");
    let got: std::collections::HashSet<u64> = pr.replicas.iter().map(|n| n.0).collect();
    let want: std::collections::HashSet<u64> = maplit::hashset! {staying, new_node};
    assert2::assert!(
        got == want,
        "reassignment completed but replicas mismatch: got={got:?} want={want:?}"
    );
}

/// The replica removed by this move, including the legacy empty-list fallback.
pub(crate) fn removed_replica(
    after: &PartitionRecord,
    initial: &[krabka_metadata::NodeId],
) -> krabka_metadata::NodeId {
    after.removing_replicas.first().copied().unwrap_or_else(|| {
        initial
            .last()
            .copied()
            .unwrap_or(krabka_metadata::NodeId(0))
    })
}

/// Capture the current assignment before stopping its non-controller free target.
pub(crate) async fn offline_for_partition(
    brokers: [BrokerHandle; 3],
    topic: &str,
    context: &str,
) -> (u64, [Option<BrokerHandle>; 3], PartitionRecord) {
    let current = brokers[0]
        .partition_record_for_test(topic, 0)
        .expect(context);
    let controller = brokers[0].wait_until_controller_leader().await.0;
    let (target, handles) = offline_target(brokers, &current, controller).await;
    (target, handles, current)
}

/// Election fixture with caller-supplied leader, replica ordering and ISR ordering.
pub(crate) fn election_partition(
    topic: &str,
    leader: krabka_metadata::NodeId,
    replicas: Vec<krabka_metadata::NodeId>,
    isr: Vec<krabka_metadata::NodeId>,
) -> PartitionRecord {
    PartitionRecord {
        topic: topic.to_string(),
        partition: 0,
        leader,
        replicas,
        isr,
        leader_epoch: krabka_metadata::LeaderEpoch(1),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }
}

/// Voters and their guards, stored in the original local-variable teardown order.
pub(crate) struct RegisteredCluster {
    pub(crate) d3: tempfile::TempDir,
    pub(crate) d2: tempfile::TempDir,
    pub(crate) d1: tempfile::TempDir,
    _cfg3: krabka_broker::BrokerConfig,
    _cfg2: krabka_broker::BrokerConfig,
    _cfg1: krabka_broker::BrokerConfig,
    pub(crate) h3: BrokerHandle,
    pub(crate) h2: BrokerHandle,
    pub(crate) h1: BrokerHandle,
}

async fn registered_admin() -> RegisteredCluster {
    let (h1, h2, h3, cfg1, cfg2, cfg3, d1, d2, d3) =
        Box::pin(crate::jvm_acceptance::start_registered_sasl_cluster(
            crate::jvm_acceptance::SaslClusterSetup::default(),
        ))
        .await;
    RegisteredCluster {
        d3,
        d2,
        d1,
        _cfg3: cfg3,
        _cfg2: cfg2,
        _cfg1: cfg1,
        h3,
        h2,
        h1,
    }
}

/// Registered default-admin cluster and its mounted client configuration.
pub(crate) async fn admin_cluster() -> (RegisteredCluster, crate::jvm_acceptance::ClientPropsFile) {
    let cluster = Box::pin(registered_admin()).await;
    let props = crate::jvm_acceptance::write_plain_props(
        crate::jvm_acceptance::ADMIN,
        crate::jvm_acceptance::ADMIN_PASS,
    );
    (cluster, props)
}

pub(crate) async fn reassignment_cluster(
    topic: &str,
) -> (
    RegisteredCluster,
    crate::jvm_acceptance::ClientPropsFile,
    String,
) {
    let (cluster, props) = Box::pin(admin_cluster()).await;
    let mount = props.mount_str();
    prepare_topic(&cluster.h1, &mount, topic).await;
    (cluster, props, mount)
}

/// Registered voters and caller-owned properties/advertised address for oracle-mounted admin files.
pub(crate) async fn admin_text_cluster() -> (RegisteredCluster, String, String) {
    let brokers = Box::pin(registered_admin()).await;
    let props = crate::jvm_acceptance::plain_client_properties(
        crate::jvm_acceptance::ADMIN,
        crate::jvm_acceptance::ADMIN_PASS,
    );
    let advertised = crate::jvm_acceptance::broker0_advertised().to_owned();
    (brokers, props, advertised)
}

/// Install the rf=2 reassignment topic on the first registered broker.
pub(crate) async fn prepare_topic(broker: &BrokerHandle, admin_mount: &str, topic: &str) {
    crate::jvm_acceptance::create_console_topic(
        crate::jvm_acceptance::KAFKA_IMAGE_TXN,
        &[admin_mount],
        topic,
        1,
        2,
    );
    broker.wait_until_partition_present(topic, 0).await;
}

/// Inspect the assignment and select a free replica, with each scenario's staying-replica rule.
pub(crate) fn assignment(
    broker: &BrokerHandle,
    topic: &str,
    keep_leader: bool,
) -> (Vec<krabka_metadata::NodeId>, u64, u64) {
    let current = broker
        .partition_record_for_test(topic, 0)
        .expect("partition record");
    let initial = current.replicas.clone();
    let new_node = free_replica(&current);
    let staying = if keep_leader {
        current.leader.0
    } else {
        initial.first().unwrap().0
    };
    eprintln!("KRABKA[test] initial replicas={initial:?} staying={staying} new_node={new_node}");
    (initial, new_node, staying)
}

/// Execute a held reassignment document with the scenario's optional throttle.
pub(crate) fn execute_plan(
    admin_mount: &str,
    topic: &str,
    staying: u64,
    new_node: u64,
    throttled: bool,
) -> (crate::jvm_acceptance::TempFileMount, String) {
    let file = plan_file(topic, staying, new_node);
    let mount = format!("{}:/reassignment.json", file.host_path());
    let mut args = vec![
        "kafka-reassign-partitions",
        "--execute",
        "--reassignment-json-file",
        "/reassignment.json",
    ];
    if throttled {
        args.extend(["--throttle", "1024"]);
    }
    args.extend([
        "--bootstrap-server",
        crate::jvm_acceptance::broker0_advertised(),
        "--command-config",
        "/client.properties",
    ]);
    let context = if throttled {
        "spawn kafka-reassign-partitions --execute --throttle"
    } else {
        "spawn kafka-reassign-partitions --execute"
    };
    let label = if throttled {
        "--execute --throttle"
    } else {
        "--execute"
    };
    let out = crate::support::jvm_docker_command(crate::support::JvmDockerSetup {
        image: crate::jvm_acceptance::KAFKA_IMAGE_TXN,
        mounts: &[admin_mount, &mount],
        args: &args,
        ..Default::default()
    })
    .output()
    .expect(context);
    eprintln!(
        "KRABKA[test] {label} status={} stdout={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert2::assert!(
        out.status.success(),
        "kafka-reassign-partitions {label} failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    (file, mount)
}
