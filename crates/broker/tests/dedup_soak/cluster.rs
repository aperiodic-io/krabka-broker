//! The three-broker cluster a soak runs on, and the admin steps of the churn.
//!
//! The brokers run inside the test process and are booted, stopped and
//! restarted with the `dedup_recovery` harness. Every request goes through a
//! client that knows exactly one broker: a `Client` sends to the least loaded
//! broker it learned of from a Metadata response, so these clients are never
//! sent one. Admin requests go to broker 1, which the churn never restarts;
//! produces go to each partition's leader.

use {
    crate::{
        gauges::{DedupGauges, scrape},
        harness::{self, Node},
        settings::{BROKERS, PARTITIONS, PLACEMENT, Settings},
        support::{
            self,
            client::connect_client,
            configs::{ConfigOperation, alter_topic_config},
            topic_on,
            topics::{create_topic_request, topic_configs},
        },
        topics::{ConfigEdit, SoakTopic},
        workload::Target,
    },
    assert2::assert,
    krabka_broker::{BrokerHandle, NodeId},
    krabka_client_core::{Client, Connection},
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

/// How long a deleted topic's partitions may take to leave every broker.
const REMOVAL_TIMEOUT: Duration = Duration::from_secs(30);

/// The running brokers, in node-id order, and a client of each.
pub struct SoakCluster {
    nodes: Vec<Node>,
    clients: Vec<Client>,
    /// A connection to broker 1 for the Metadata requests that resolve topic
    /// ids, so that no client learns a second broker.
    metadata: Connection,
}

impl SoakCluster {
    /// Boot [`BROKERS`] brokers with the run's entry limits and a one-second
    /// retention sweep, and wait until each sees all of them registered.
    pub async fn boot(settings: &Settings) -> Self {
        let per_partition = usize::try_from(settings.max_entries_per_partition).expect("limit");
        let total = usize::try_from(settings.max_entries).expect("limit");
        let nodes = Box::pin(support::start_n_node_customized_with_retry(
            BROKERS,
            |_, config| {
                config.dedup_max_entries_per_partition = per_partition;
                config.dedup_max_entries = total;
                config.log_retention_check_interval = krabka_units::secs(1);
            },
            "dedup soak cluster",
        ))
        .await;
        support::wait_for_all_brokers_registered(
            &nodes,
            usize::try_from(BROKERS).expect("broker count"),
        )
        .await;
        let mut clients = Vec::new();
        for (_, config, _) in &nodes {
            clients.push(client_of(config.listen_addr.to_string()).await);
        }
        let metadata = harness::connect(nodes[0].1.listen_addr).await;
        Self {
            nodes,
            clients,
            metadata,
        }
    }

    /// Every running broker and its node id.
    pub fn brokers(&self) -> impl Iterator<Item = (u64, &BrokerHandle)> {
        self.nodes
            .iter()
            .map(|(broker, config, _)| (config.node_id.0, broker))
    }

    /// The KFC-10 series of every running broker, by node id.
    pub async fn gauges(&self) -> Vec<(u64, DedupGauges)> {
        let mut all = Vec::new();
        for (node, broker) in self.brokers() {
            all.push((node, scrape(broker).await));
        }
        all
    }

    fn admin(&self) -> &Client { &self.clients[0] }

    fn first(&self) -> &BrokerHandle { &self.nodes[0].0 }

    /// Create `topic` with [`PLACEMENT`] and its configs, wait until every
    /// replica is in the ISR and each leader takes produces, and record the
    /// topic id the broker assigned.
    pub async fn create(&self, topic: &mut SoakTopic) {
        let name = topic.name();
        let mut creatable = topic_on(&name, &PLACEMENT);
        let configs = topic.create_configs();
        creatable.configs = topic_configs(configs.iter().map(|(k, v)| (*k, v.as_str())));
        let response = self
            .admin()
            .send(create_topic_request(creatable))
            .await
            .expect("CreateTopics");
        assert!(
            response.topics[0].error_code == 0,
            "CreateTopics: {response:?}"
        );
        self.await_in_sync(std::slice::from_ref(topic)).await;
        topic.id = harness::topic_id_on(&self.metadata, &name).await;
        for (partition, replicas) in (0..PARTITIONS).zip(PLACEMENT) {
            let leader = u64::try_from(replicas[0]).expect("a node id");
            self.broker(leader)
                .wait_until_local_partition_leader(&name, partition, NodeId(leader))
                .await;
        }
    }

    /// Wait until every running broker hosts every partition of `topics`, and
    /// broker 1's image shows each led by its preferred leader with all
    /// replicas in sync.
    async fn await_in_sync(&self, topics: &[SoakTopic]) {
        let names: Vec<String> = topics.iter().map(SoakTopic::name).collect();
        for (_, broker) in self.brokers() {
            for name in &names {
                for partition in 0..PARTITIONS {
                    broker.wait_until_partition_present(name, partition).await;
                }
            }
        }
        self.first()
            .wait_for_image(|image| names.iter().all(|name| in_sync(image, name)))
            .await;
    }

    /// Delete `topic` and wait until every broker has removed its partitions.
    pub async fn delete(&self, topic: &SoakTopic) {
        let name = topic.name();
        let response = self
            .admin()
            .send(DeleteTopicsRequest {
                topics: vec![DeleteTopicState {
                    name: Some(name.clone()),
                    ..Default::default()
                }],
                timeout_ms: 30_000,
                ..Default::default()
            })
            .await
            .expect("DeleteTopics");
        assert!(
            response.responses[0].error_code == 0,
            "DeleteTopics: {response:?}"
        );
        let deadline = Instant::now() + REMOVAL_TIMEOUT;
        while self
            .brokers()
            .any(|(_, broker)| (0..PARTITIONS).any(|p| broker.partition_exists_for_test(&name, p)))
        {
            assert!(
                Instant::now() < deadline,
                "{name} is still hosted after {REMOVAL_TIMEOUT:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Make `edit` to `topic` and wait until every broker's image has it, so
    /// the next produce runs under it everywhere.
    pub async fn edit(&self, topic: &SoakTopic, edit: &ConfigEdit) {
        let name = topic.name();
        let outcome = alter_topic_config()
            .client(self.admin())
            .topic(&name)
            .name(edit.key)
            .operation(if edit.value.is_some() {
                ConfigOperation::Set
            } else {
                ConfigOperation::Delete
            })
            .maybe_value(edit.value.as_deref())
            .call()
            .await;
        assert!(outcome.error_code == 0, "{edit:?} on {name}: {outcome:?}");
        for (_, broker) in self.brokers() {
            broker
                .wait_for_image(|image| {
                    image
                        .topic_config(&name)
                        .and_then(|configs| configs.get(edit.key))
                        .map(String::as_str)
                        == edit.value.as_deref()
                })
                .await;
        }
    }

    /// One produce target for every partition of `topics`, each with a
    /// client of the partition's current leader.
    pub fn targets(&self, topics: &[SoakTopic]) -> Vec<Target> {
        let mut targets = Vec::new();
        for topic in topics {
            let name: Arc<str> = Arc::from(topic.name());
            for partition in 0..PARTITIONS {
                let leader = self
                    .first()
                    .partition_leader_for_test(&name, partition)
                    .unwrap_or_else(|| panic!("{name}-{partition} has no leader"));
                targets.push(Target {
                    topic: Arc::clone(&name),
                    topic_id: topic.id,
                    partition,
                    rule: topic.rule(),
                    leader: self.clients[self.position(leader)].clone(),
                });
            }
        }
        targets
    }

    fn position(&self, node: u64) -> usize { harness::position(&self.nodes, node) }

    fn broker(&self, node: u64) -> &BrokerHandle { &self.nodes[self.position(node)].0 }

    /// Stop broker `node` gracefully, start it again on its own directory and
    /// addresses, and wait until it is back in the ISR of every partition of
    /// `topics`.
    pub async fn restart(&mut self, node: u64, topics: &[SoakTopic]) {
        let (config, dir) = harness::stop(&mut self.nodes, node).await;
        let restarted = harness::restart(config, dir).await;
        let at = self
            .nodes
            .iter()
            .position(|(_, config, _)| config.node_id.0 > node)
            .unwrap_or(self.nodes.len());
        self.nodes.insert(at, restarted);
        self.clients[at] = client_of(self.nodes[at].1.listen_addr.to_string()).await;
        self.await_in_sync(topics).await;
    }

    pub async fn shutdown(self) { harness::shutdown_all(self.nodes).await; }
}

/// Whether every partition of `topic` has its preferred leader and every
/// replica in sync, by `image`.
fn in_sync(image: &MetadataImage, topic: &str) -> bool {
    (0..PARTITIONS).zip(PLACEMENT).all(|(partition, replicas)| {
        image.partition(topic, partition).is_some_and(|record| {
            record.leader == NodeId(u64::try_from(replicas[0]).expect("a node id"))
                && record.isr.len() == replicas.len()
        })
    })
}

/// A client that knows only the broker at `addr`.
async fn client_of(addr: String) -> Client { connect_client(addr, Some("dedup-soak")).await }
