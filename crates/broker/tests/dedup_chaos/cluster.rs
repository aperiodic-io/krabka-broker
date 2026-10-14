//! Three brokers that stop, crash and restart on their own directories, the
//! topics of the run, and the admin requests the faults send.
//!
//! A stopped broker keeps its slot, config and directory, so it restarts on
//! the same addresses and log. Every request the faults send goes to one
//! live broker over its own connection and is bounded in time, so a broker
//! that stops under it fails the request instead of hanging the run.

use {
    crate::{
        disk::{Tail, damage},
        gauges::Meters,
        plan::{ConfigChange, Stop},
        support::{
            self,
            configs::{
                ConfigOperation, incremental_config, incremental_request, incremental_resource,
            },
            dedup::DEDUP_HEADER,
            discovery::topic_metadata_request,
            topic_on,
            topics::{create_topic_request, topic_configs},
        },
        topics::{
            BROKER_LIMIT, LONG_WINDOW_MS, MUTABLE_TOPIC, PER_PARTITION_LIMIT, Rule, TOPICS, Target,
            targets,
        },
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, BrokerConfig, BrokerHandle, NodeId},
    krabka_client_core::{Connection, ConnectionOptions, ProtocolRequest},
    krabka_protocol::{
        owned::{
            alter_partition_reassignments_request::{
                AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
            },
            elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        },
        primitives::uuid::Uuid,
    },
    std::{
        future::Future,
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

/// A bound on one request to one broker.
pub const STEP: Duration = Duration::from_secs(8);

/// `ELECTION_NOT_NEEDED`: the preferred replica already leads.
const ELECTION_NOT_NEEDED: i16 = 84;

/// One broker of the cluster, running or not.
struct Slot {
    config: BrokerConfig,
    /// The log directory, kept across restarts and removed with the slot.
    _dir: TempDir,
    handle: Option<BrokerHandle>,
}

pub struct ChaosCluster {
    slots: Vec<Slot>,
    pub topic_ids: Vec<Uuid>,
    pub meters: Meters,
}

/// Connect to one broker within [`STEP`].
pub async fn dial(addr: SocketAddr, client_id: &str) -> Result<Connection, String> {
    let options = ConnectionOptions {
        client_id: client_id.into(),
        ..Default::default()
    };
    bounded(Connection::connect(addr, options))
        .await?
        .map_err(|error| format!("connect to {addr}: {error}"))
}

/// `work` within [`STEP`].
pub async fn bounded<T>(work: impl Future<Output = T>) -> Result<T, String> {
    tokio::time::timeout(STEP, work)
        .await
        .map_err(|_| format!("no answer within {STEP:?}"))
}

impl ChaosCluster {
    /// Boot three registered brokers with the run's entry limits and create
    /// every topic of [`TOPICS`]. Returns once every partition has its
    /// preferred leader ready to take a produce and a full ISR.
    pub async fn boot() -> Self {
        let cluster = support::start_n_node_customized_with_retry(
            3,
            |_, config| {
                config.dedup_max_entries_per_partition =
                    usize::try_from(PER_PARTITION_LIMIT).expect("a limit");
                config.dedup_max_entries = usize::try_from(BROKER_LIMIT).expect("a limit");
            },
            "dedup-chaos",
        )
        .await;
        support::wait_for_all_brokers_registered(&cluster, 3).await;
        let meters = Meters::default();
        let slots = cluster
            .into_iter()
            .map(|(handle, config, dir)| {
                meters.set(config.node_id.0, Some(handle.metrics().clone()));
                Slot {
                    config,
                    _dir: dir,
                    handle: Some(handle),
                }
            })
            .collect();
        let mut cluster = Self {
            slots,
            topic_ids: Vec::new(),
            meters,
        };
        cluster.create_topics().await;
        cluster
    }

    async fn create_topics(&mut self) {
        let conn = dial(self.addr(1), "dedup-chaos-admin")
            .await
            .expect("connect to broker 1");
        let min_isr = ("min.insync.replicas", "2");
        let unclean = ("unclean.leader.election.enable", "false");
        for spec in &TOPICS {
            let window = spec.window_ms.to_string();
            let mut configs = vec![("dedup.window.ms", window.as_str()), min_isr, unclean];
            if spec.rule == Rule::Header {
                configs.push(("dedup.key.header", DEDUP_HEADER));
            }
            let replicas: Vec<&[i32]> = spec.replicas.iter().map(<[i32; 3]>::as_slice).collect();
            let mut creatable = topic_on(spec.name, &replicas);
            creatable.configs = topic_configs(configs);
            let response = conn
                .send(create_topic_request(creatable))
                .await
                .expect("CreateTopics");
            assert!(
                response.topics[0].error_code == 0,
                "CreateTopics: {response:?}"
            );
        }
        for target in targets() {
            let preferred =
                target.spec().replicas[usize::try_from(target.partition).expect("index")][0];
            let leader = u64::try_from(preferred).expect("a broker id");
            let (topic, partition) = (target.spec().name, target.partition);
            for slot in &self.slots {
                slot.live()
                    .wait_until_partition_present(topic, partition)
                    .await;
            }
            self.slots[0]
                .live()
                .wait_for_image(|image| {
                    image
                        .partition(topic, partition)
                        .is_some_and(|row| row.leader == NodeId(leader) && row.isr.len() == 3)
                })
                .await;
            self.slots[self.index(leader)]
                .live()
                .wait_until_local_partition_leader(topic, partition, NodeId(leader))
                .await;
        }
        let metadata = conn
            .send(topic_metadata_request(None))
            .await
            .expect("Metadata");
        self.topic_ids = TOPICS
            .iter()
            .map(|spec| {
                metadata
                    .topics
                    .iter()
                    .find(|row| row.name.as_deref() == Some(spec.name))
                    .map(|row| row.topic_id)
                    .expect("the topic is in the metadata")
            })
            .collect();
    }

    fn index(&self, node: u64) -> usize {
        self.slots
            .iter()
            .position(|slot| slot.config.node_id == NodeId(node))
            .unwrap_or_else(|| panic!("no broker {node}"))
    }

    /// The client address of broker `node`, the same across restarts.
    pub fn addr(&self, node: u64) -> SocketAddr { self.slots[self.index(node)].config.listen_addr }

    /// Every broker id and its client address.
    pub fn addrs(&self) -> Vec<(u64, SocketAddr)> {
        self.slots
            .iter()
            .map(|slot| (slot.config.node_id.0, slot.config.listen_addr))
            .collect()
    }

    pub fn is_live(&self, node: u64) -> bool { self.slots[self.index(node)].handle.is_some() }

    /// The running brokers, in id order.
    pub fn live(&self) -> Vec<u64> {
        self.slots
            .iter()
            .filter(|slot| slot.handle.is_some())
            .map(|slot| slot.config.node_id.0)
            .collect()
    }

    pub fn down(&self) -> Vec<u64> {
        self.slots
            .iter()
            .filter(|slot| slot.handle.is_none())
            .map(|slot| slot.config.node_id.0)
            .collect()
    }

    pub fn handle(&self, node: u64) -> Option<&BrokerHandle> {
        self.slots[self.index(node)].handle.as_ref()
    }

    /// A running broker whose metadata image the run reads.
    fn observer(&self) -> Option<&BrokerHandle> {
        self.slots.iter().find_map(|slot| slot.handle.as_ref())
    }

    /// The leader of `target` by the observer's image, if it is running.
    pub fn leader_of(&self, target: Target) -> Option<u64> {
        self.observer()?
            .partition_leader_for_test(target.spec().name, target.partition)
            .filter(|leader| self.is_live(*leader))
    }

    /// The leader epoch of `target` by the observer's image.
    pub fn leader_epoch(&self, target: Target) -> Option<i32> {
        let record = self
            .observer()?
            .partition_record_for_test(target.spec().name, target.partition)?;
        Some(record.leader_epoch.0)
    }

    /// The ISR of `target` by the observer's image.
    pub fn isr_of(&self, target: Target) -> Vec<u64> {
        self.observer()
            .and_then(|observer| {
                observer.partition_isr_for_test(target.spec().name, target.partition)
            })
            .unwrap_or_default()
    }

    /// The controller leader by the observer's quorum state, if it runs.
    pub fn controller_leader(&self) -> Option<u64> {
        let leader = (*self.observer()?.watch_leader_for_test().borrow())?;
        Some(leader.0).filter(|node| *node != 0 && self.is_live(*node))
    }

    /// Whether every broker but `victim` runs and is in the ISR of
    /// `target`: then those brokers hold every batch acknowledged on it.
    pub fn others_hold(&self, target: Target, victim: u64) -> bool {
        let isr = self.isr_of(target);
        self.addrs()
            .iter()
            .map(|(node, _)| *node)
            .filter(|node| *node != victim)
            .all(|node| self.is_live(node) && isr.contains(&node))
    }

    /// Whether every broker runs and every partition has all three in its
    /// ISR.
    pub fn whole(&self) -> bool {
        self.down().is_empty()
            && targets()
                .into_iter()
                .all(|target| self.isr_of(target).len() == 3)
    }

    /// Stop broker `node` as `stop` says and damage its log if it says so.
    /// Returns what it did to the log.
    pub async fn stop(&mut self, node: u64, stop: Stop) -> String {
        let index = self.index(node);
        let handle = self.slots[index].handle.take().expect("the broker runs");
        self.meters.set(node, None);
        if stop == Stop::Graceful {
            handle.shutdown().await;
        } else {
            handle.crash_for_test().await;
        }
        let slot = &mut self.slots[index];
        slot.config.bootstrap_mode = BootstrapMode::Rejoin;
        let (target, tail) = match stop {
            Stop::Graceful | Stop::Crash => return String::new(),
            Stop::CrashTornTail(target) => (target, Tail::Tear),
            Stop::CrashCutTail(target) => (target, Tail::Cut),
        };
        let target = targets()[target];
        damage(
            &slot
                .config
                .log_dir
                .join(format!("{}-{}", target.spec().name, target.partition)),
            tail,
        )
    }

    /// Start broker `node` again on its own directory and addresses.
    pub async fn restart(&mut self, node: u64) {
        let index = self.index(node);
        let what = format!("restart broker {node}");
        let handle = Box::pin(support::start_reusing_addrs(
            &self.slots[index].config,
            &what,
        ))
        .await;
        self.meters.set(node, Some(handle.metrics().clone()));
        self.slots[index].handle = Some(handle);
    }

    /// Send `request` to a running broker, the controller leader if it
    /// runs, within [`STEP`].
    async fn ask<R: ProtocolRequest>(&self, request: R) -> Result<R::Response, String> {
        let node = self
            .controller_leader()
            .or_else(|| self.live().first().copied())
            .ok_or("no broker runs")?;
        let conn = dial(self.addr(node), "dedup-chaos-admin").await?;
        bounded(conn.send(request))
            .await?
            .map_err(|error| format!("broker {node}: {error}"))
    }

    /// A preferred-leader election of `only`, or of every partition.
    pub async fn elect_preferred(&self, only: Option<Target>) -> Result<String, String> {
        let chosen: Vec<Target> = targets()
            .into_iter()
            .filter(|target| only.is_none_or(|only| only == *target))
            .collect();
        let request = ElectLeadersRequest {
            election_type: 0,
            topic_partitions: Some(
                TOPICS
                    .iter()
                    .enumerate()
                    .map(|(topic, spec)| TopicPartitions {
                        topic: spec.name.to_owned(),
                        partitions: chosen
                            .iter()
                            .filter(|target| target.topic == topic)
                            .map(|target| target.partition)
                            .collect(),
                        ..Default::default()
                    })
                    .filter(|row| !row.partitions.is_empty())
                    .collect(),
            ),
            timeout_ms: 5_000,
            ..Default::default()
        };
        let response = self.ask(request).await?;
        let mut outcome = Vec::new();
        for row in &response.replica_election_results {
            for partition in &row.partition_result {
                if ![0, ELECTION_NOT_NEEDED].contains(&partition.error_code) {
                    outcome.push(format!(
                        "{}-{}: error {}",
                        row.topic, partition.partition_id, partition.error_code
                    ));
                }
            }
        }
        if response.error_code != 0 {
            outcome.push(format!("error {}", response.error_code));
        }
        Ok(if outcome.is_empty() {
            "elected".to_owned()
        } else {
            outcome.join(", ")
        })
    }

    /// Put broker `to` first in the replica list of `target`, then elect it.
    pub async fn move_leader(&self, target: Target, to: u64) -> Result<String, String> {
        let current = self
            .observer()
            .and_then(|observer| {
                observer.partition_record_for_test(target.spec().name, target.partition)
            })
            .ok_or("the partition is not in the image")?;
        let to_id = i32::try_from(to).expect("a broker id");
        let mut replicas = vec![to_id];
        replicas.extend(
            current
                .replicas
                .iter()
                .map(|node| i32::try_from(node.0).expect("a broker id"))
                .filter(|node| *node != to_id),
        );
        let request = AlterPartitionReassignmentsRequest {
            timeout_ms: 5_000,
            topics: vec![ReassignableTopic {
                name: target.spec().name.to_owned(),
                partitions: vec![ReassignablePartition {
                    partition_index: target.partition,
                    replicas: Some(replicas.clone()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = self.ask(request).await?;
        let codes: Vec<i16> = std::iter::once(response.error_code)
            .chain(
                response
                    .responses
                    .iter()
                    .flat_map(|row| row.partitions.iter().map(|partition| partition.error_code)),
            )
            .filter(|code| *code != 0)
            .collect();
        if !codes.is_empty() {
            return Err(format!("reassignment to {replicas:?}: errors {codes:?}"));
        }
        let elected = self.elect_preferred(Some(target)).await?;
        Ok(format!("replicas {replicas:?}, {elected}"))
    }

    /// Apply `change` to the mutable topic. `Ok` means the controller
    /// accepted it.
    pub async fn change_config(&self, change: ConfigChange) -> Result<(), String> {
        let window = |ms: i64| {
            (
                "dedup.window.ms",
                ConfigOperation::Set,
                Some(ms.to_string()),
            )
        };
        let (name, operation, value) = match change {
            ConfigChange::Window(ms) => window(ms),
            ConfigChange::Enabled(true) => window(LONG_WINDOW_MS),
            ConfigChange::Enabled(false) => window(-1),
            ConfigChange::HeaderRule(true) => (
                "dedup.key.header",
                ConfigOperation::Set,
                Some(DEDUP_HEADER.to_owned()),
            ),
            ConfigChange::HeaderRule(false) => ("dedup.key.header", ConfigOperation::Delete, None),
        };
        let request = incremental_request()
            .resources(vec![incremental_resource(
                2,
                TOPICS[MUTABLE_TOPIC].name,
                vec![incremental_config(name, value, operation)],
            )])
            .call();
        let response = self.ask(request).await?;
        match response.responses.first().map(|row| row.error_code) {
            Some(0) => Ok(()),
            code => Err(format!("IncrementalAlterConfigs: {code:?}")),
        }
    }

    /// Wait until every broker runs and every partition has a full ISR, for
    /// at most `limit`. Returns how long it took.
    pub async fn await_whole(&self, limit: Duration) -> Result<Duration, String> {
        let started = Instant::now();
        while !self.whole() {
            if started.elapsed() > limit {
                let isrs: Vec<String> = targets()
                    .into_iter()
                    .map(|target| format!("{target}: {:?}", self.isr_of(target)))
                    .collect();
                return Err(format!(
                    "no full ISR within {limit:?}, down {:?}: {}",
                    self.down(),
                    isrs.join(", ")
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(started.elapsed())
    }

    pub async fn shutdown(self) {
        for slot in self.slots {
            if let Some(handle) = slot.handle {
                handle.shutdown().await;
            }
        }
    }
}

impl Slot {
    fn live(&self) -> &BrokerHandle { self.handle.as_ref().expect("the broker runs") }
}
