//! KIP-890 part 1: the transaction check that a partition leader asks the
//! transaction coordinator for before a transactional `Produce` starts a
//! transaction on the partition.
//!
//! Kafka's `ReplicaManager.handleProduceAppend` sends the check through
//! `AddPartitionsToTxnManager.addOrVerifyTransaction`. From `Produce` v12
//! (transaction version 2) the check adds the partition to the transaction.
//! Below v12 it only verifies that the client added the partition with its own
//! `AddPartitionsToTxn`. The call always goes to the coordinator's broker, also
//! when that is this broker, so the answer does not depend on where the
//! coordinator lives.

use {
    super::TxnCoordinator,
    crate::{
        broker::registered_listener_endpoint,
        codes,
        config::BrokerConfig,
        txn::{
            bootstrap,
            state::{TopicPartition, TxnState},
            util::ProducerIdentity,
            version::TxnVersion,
        },
    },
    krabka_log::ProducerId,
    krabka_protocol::owned::{
        add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
        common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
    },
};

/// The `AddPartitionsToTxn` request version to use for a registration that
/// does not come from a client's own `AddPartitionsToTxn` request: the
/// `Produce`-path partition check below, and the KIP-890 server-side
/// `TxnOffsetCommit` partition enrollment. Kafka's `AddPartitionsToTxnManager`
/// sends both to the coordinator as an `AddPartitionsToTxn` request of its
/// latest version, which is above 3, so the coordinator records `TV_2` for the
/// transition (`transactionVersionForAddPartitionsToTxn`) and no answer needs
/// the legacy `INVALID_PRODUCER_EPOCH` downgrade.
pub(crate) const INTERNAL_REGISTRATION_VERSION: i16 = 4;

/// Kafka's `TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG`,
/// a dynamic broker config that defaults to `true`.
pub(crate) const PARTITION_VERIFICATION_ENABLE: &str = "transaction.partition.verification.enable";

/// Whether the broker `config` describes verifies that a transaction contains
/// a partition before it appends transactional records to it, as of `image`.
///
/// Kafka's `ReplicaManager` reads `KafkaConfig.transactionPartitionVerificationEnable`,
/// the effective value: the dynamic per-broker config, else the cluster-wide
/// one, else the static `server.properties` value, which is Kafka's default
/// `true` when the operator named none. A dynamic value that is not a boolean
/// does not apply, as Kafka refuses to set it.
///
/// This is read for each request, so a dynamic change takes effect for the
/// next one.
pub(crate) fn partition_verification_enabled(
    image: &krabka_metadata::MetadataImage,
    config: &BrokerConfig,
) -> bool {
    image
        .broker_config(config.node_id)
        .and_then(|configs| configs.get(PARTITION_VERIFICATION_ENABLE))
        .or_else(|| {
            image
                .default_broker_config()?
                .get(PARTITION_VERIFICATION_ENABLE)
        })
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        })
        .unwrap_or(config.transaction_partition_verification_enable)
}

/// Whether the leader skips the coordinator call of a transactional append.
///
/// Kafka's `ReplicaManager.maybeSendPartitionsToTransactionCoordinator` skips
/// it when `transaction.partition.verification.enable` is `false` and the
/// operation only verifies (`Produce` below v12, `TxnOffsetCommit` below v5).
/// An operation that adds the partition still asks the coordinator, because
/// the add is what registers the partition, not a check on top of it. With no
/// call, `UnifiedLog.batchMissingRequiredVerification` does not refuse the
/// append either, whatever the log holds.
pub(crate) fn skips_coordinator_verification(
    supports_epoch_bump: bool,
    verification_enabled: bool,
) -> bool {
    !supports_epoch_bump && !verification_enabled
}

/// The partitions one transactional producer asks its coordinator to add or
/// verify: Kafka's `AddPartitionsToTxnTransaction`.
#[derive(Debug, Clone)]
pub(crate) struct TransactionCheck<'a> {
    pub(crate) transactional_id: &'a str,
    pub(crate) producer_id: ProducerId,
    pub(crate) producer_epoch: i16,
    pub(crate) partitions: Vec<TopicPartition>,
    /// `true` asks only whether the partitions are in the transaction.
    /// `false` adds them.
    pub(crate) verify_only: bool,
}

impl TxnCoordinator {
    /// Ask the coordinator of `check.transactional_id` to add or verify every
    /// partition of `check` in one `AddPartitionsToTxn` call, and return the
    /// coordinator's answer for each partition, in `check.partitions` order.
    ///
    /// This is Kafka's `AddPartitionsToTxnManager.addOrVerifyTransaction`:
    /// one request per coordinator for all the partitions a `Produce` starts a
    /// transaction on. A partition's answer is its partition error code, or
    /// the top-level error code when the response has one. A coordinator this
    /// broker cannot find answers `COORDINATOR_NOT_AVAILABLE`, and a call that
    /// does not complete answers `NETWORK_EXCEPTION`, for every partition.
    pub(crate) async fn add_or_verify_partitions(
        self: &std::sync::Arc<Self>,
        check: TransactionCheck<'_>,
        txnv: TxnVersion,
        version: i16,
    ) -> Vec<(TopicPartition, i16)> {
        let code = match self.add_or_verify_remote(&check).await {
            Remote::Local => return self.add_or_verify_locally(check, txnv, version).await,
            Remote::Answered(answers) => return answers,
            Remote::Failed(code) => code,
        };
        check
            .partitions
            .into_iter()
            .map(|partition| (partition, code))
            .collect()
    }

    /// The remote half of [`Self::add_or_verify_partitions`].
    async fn add_or_verify_remote(
        self: &std::sync::Arc<Self>,
        check: &TransactionCheck<'_>,
    ) -> Remote {
        let Some(transport) = &self.marker_transport else {
            return Remote::Local;
        };
        let image = transport.controller.current_image();
        drop(self.refresh_leader_partitions(&image).await);
        let coordinator_partition = self.partition_for(check.transactional_id);
        let Some(leader) = image
            .partition(bootstrap::TOPIC, coordinator_partition.get())
            .map(|partition| partition.leader)
        else {
            return Remote::Failed(codes::COORDINATOR_NOT_AVAILABLE);
        };
        if leader == self.node_id {
            return Remote::Local;
        }
        let Some(broker) = image.broker(leader) else {
            return Remote::Failed(codes::COORDINATOR_NOT_AVAILABLE);
        };
        let (host, port) = registered_listener_endpoint(broker, &transport.listener_name);
        let host = host.to_owned();
        let mut topics: Vec<AddPartitionsToTxnTopic> = Vec::new();
        for partition in &check.partitions {
            match topics
                .iter_mut()
                .find(|topic| topic.name == partition.topic)
            {
                Some(topic) => topic.partitions.push(partition.partition.get()),
                None => topics.push(AddPartitionsToTxnTopic {
                    name: partition.topic.clone(),
                    partitions: vec![partition.partition.get()],
                    ..Default::default()
                }),
            }
        }
        let request = AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: check.transactional_id.to_string(),
                producer_id: check.producer_id.get(),
                producer_epoch: check.producer_epoch,
                topics: topics.clone(),
                verify_only: check.verify_only,
                ..Default::default()
            }],
            v3_and_below_transactional_id: check.transactional_id.to_string(),
            v3_and_below_producer_id: check.producer_id.get(),
            v3_and_below_producer_epoch: check.producer_epoch,
            v3_and_below_topics: topics,
            ..Default::default()
        };
        let Some(connection) = self
            .verification_connection(transport, leader, &host, port)
            .await
        else {
            return Remote::Failed(codes::NETWORK_EXCEPTION);
        };
        let response = match connection.send(request).await {
            Ok(response) => response,
            Err(error) => {
                self.drop_verification_connection(leader).await;
                tracing::warn!(%error, %host, port, "transaction partition check failed");
                return Remote::Failed(codes::NETWORK_EXCEPTION);
            }
        };
        if response.error_code != codes::NONE {
            return Remote::Failed(response.error_code);
        }
        let transaction = response
            .results_by_transaction
            .iter()
            .find(|transaction| transaction.transactional_id == check.transactional_id);
        Remote::Answered(
            check
                .partitions
                .iter()
                .map(|partition| {
                    let code = transaction
                        .and_then(|transaction| {
                            transaction
                                .topic_results
                                .iter()
                                .find(|topic| topic.name == partition.topic)
                        })
                        .and_then(|topic| {
                            topic
                                .results_by_partition
                                .iter()
                                .find(|answer| answer.partition_index == partition.partition.get())
                        })
                        .map_or(codes::UNKNOWN_SERVER_ERROR, |answer| {
                            answer.partition_error_code
                        });
                    (partition.clone(), code)
                })
                .collect(),
        )
    }

    /// The open connection to the coordinator on `leader`, dialed on first
    /// use and kept for the next check, as Kafka's `AddPartitionsToTxnManager`
    /// keeps its `NetworkClient` connection. `None` when the dial fails.
    async fn verification_connection(
        &self,
        transport: &super::MarkerTransport,
        leader: krabka_metadata::NodeId,
        host: &str,
        port: u16,
    ) -> Option<krabka_client_core::Connection> {
        let mut connections = self.verification_connections.lock().await;
        if let Some(cached) = connections.get(&leader)
            && cached.address.host == host
            && cached.address.port == port
            && !cached.connection.is_closed()
        {
            return Some(cached.connection.clone());
        }
        let options = krabka_client_core::ConnectionOptions {
            client_id: format!("krabka-broker-txn-{}", self.node_id),
            ..Default::default()
        };
        match transport
            .inter_broker_client
            .connect_as_connection(
                host,
                port,
                transport.protocol,
                &transport.server_name,
                options,
            )
            .await
        {
            Ok(connection) => {
                if let Some(stale) = connections.insert(leader, super::VerificationConnection {
                    address: super::VerificationEndpoint {
                        host: host.to_string(),
                        port,
                    },
                    connection: connection.clone(),
                }) {
                    stale.connection.close();
                }
                Some(connection)
            }
            Err(error) => {
                tracing::warn!(%error, %host, port, "transaction partition check connect failed");
                None
            }
        }
    }

    /// Forget and close the connection to `leader` after a failed call.
    async fn drop_verification_connection(&self, leader: krabka_metadata::NodeId) {
        if let Some(stale) = self.verification_connections.lock().await.remove(&leader) {
            stale.connection.close();
        }
    }

    async fn add_or_verify_locally(
        &self,
        check: TransactionCheck<'_>,
        txnv: TxnVersion,
        version: i16,
    ) -> Vec<(TopicPartition, i16)> {
        if check.verify_only {
            let mut answers = Vec::with_capacity(check.partitions.len());
            for partition in &check.partitions {
                let code = self
                    .verify_partition_in_transaction(&check, partition)
                    .await;
                answers.push((partition.clone(), code));
            }
            answers
        } else {
            let code = self
                .register_partitions(
                    check.transactional_id,
                    check.producer_id,
                    check.producer_epoch,
                    check.partitions.clone(),
                    txnv,
                    version,
                )
                .await;
            check
                .partitions
                .into_iter()
                .map(|partition| (partition, code))
                .collect()
        }
    }

    /// Whether the partition is in the producer's transaction. Kafka's
    /// `TransactionCoordinator.handleVerifyPartitionsInTransaction`.
    async fn verify_partition_in_transaction(
        &self,
        check: &TransactionCheck<'_>,
        partition: &TopicPartition,
    ) -> i16 {
        // Kafka's `handleVerifyPartitionsInTransaction` refuses a null or empty
        // transactional id first.
        if check.transactional_id.is_empty() {
            return codes::INVALID_REQUEST;
        }
        if let Some(code) = self.coordinator_error(check.transactional_id).await {
            return code;
        }
        let Some(entry) = self.get(check.transactional_id) else {
            return codes::INVALID_PRODUCER_ID_MAPPING;
        };
        let entry = entry.lock().await;
        verification_code(
            HeldTransaction {
                identity: ProducerIdentity {
                    producer_id: entry.producer_id,
                    producer_epoch: entry.producer_epoch,
                },
                state: entry.state,
            },
            entry.partitions.contains(partition),
            ProducerIdentity {
                producer_id: check.producer_id,
                producer_epoch: check.producer_epoch,
            },
        )
    }
}

/// How the remote half of a check ended.
enum Remote {
    /// This broker is the coordinator, or has no inter-broker transport.
    Local,
    /// The coordinator answered each partition.
    Answered(Vec<(TopicPartition, i16)>),
    /// Every partition gets this code.
    Failed(i16),
}

/// The producer identity and state of the transaction a verify-only check
/// compares against.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HeldTransaction {
    pub(crate) identity: ProducerIdentity,
    pub(crate) state: TxnState,
}

/// The verify-only answer for one partition, from the transaction's identity,
/// state and partition set. Kafka's
/// `TransactionCoordinator.handleVerifyPartitionsInTransaction`.
pub(crate) fn verification_code(
    held: HeldTransaction,
    contains_partition: bool,
    requested: ProducerIdentity,
) -> i16 {
    let HeldTransaction {
        identity: ProducerIdentity {
            producer_id,
            producer_epoch,
        },
        state,
    } = held;
    if producer_id != requested.producer_id {
        codes::INVALID_PRODUCER_ID_MAPPING
    } else if producer_epoch != requested.producer_epoch {
        codes::PRODUCER_FENCED
    } else if matches!(state, TxnState::PrepareCommit | TxnState::PrepareAbort) {
        codes::CONCURRENT_TRANSACTIONS
    } else if contains_partition {
        codes::NONE
    } else {
        codes::TRANSACTION_ABORTABLE
    }
}

#[cfg(test)]
mod tests;
