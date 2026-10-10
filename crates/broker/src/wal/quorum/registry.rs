//! WAL shard registry.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use krabka_verified::wal::{WalFetchAdmission, wal_fetch_admission};

use super::{
    engine::WalShardEngine,
    wire::{
        FENCED_LEADER_EPOCH, OFFSET_OUT_OF_RANGE, QuorumGroup, UNKNOWN_LEADER_EPOCH,
        WalFetchRequest, decode_fetch, decode_fetch_request, encode_fetch_response_struct,
        fetch_response, unknown_shard_fetch_response,
    },
};

/// Per-partition WAL shard identity for distributed WAL registry work stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ShardId {
    pub(crate) topic_id: uuid::Uuid,
    pub(crate) partition: PartitionIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalPlacement {
    pub(crate) voters: Vec<krabka_raft::NodeId>,
    pub(crate) leader_epoch: i32,
}

/// Registry from shard identity to its in-process WAL engine.
#[derive(Debug)]
pub(crate) struct WalShardRegistry {
    local_node_id: krabka_raft::NodeId,
    principal_node_ids: HashMap<String, krabka_raft::NodeId>,
    engines: DashMap<ShardId, Arc<WalShardEngine>>,
    placements: RwLock<HashMap<ShardId, WalPlacement>>,
    metrics: crate::metrics::BrokerMetrics,
    #[cfg(any(test, feature = "test-helpers"))]
    follower_fetchers: DashMap<ShardId, std::collections::BTreeSet<krabka_raft::NodeId>>,
}

impl WalShardRegistry {
    #[must_use]
    pub(crate) fn new(local_node_id: krabka_raft::NodeId) -> Self {
        Self {
            local_node_id,
            principal_node_ids: HashMap::new(),
            engines: DashMap::new(),
            placements: RwLock::new(HashMap::new()),
            metrics: crate::metrics::BrokerMetrics::new(),
            #[cfg(any(test, feature = "test-helpers"))]
            follower_fetchers: DashMap::new(),
        }
    }

    #[must_use]
    pub(crate) fn with_metrics(mut self, metrics: crate::metrics::BrokerMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    #[must_use]
    pub(crate) fn with_principal_node_ids(
        mut self,
        principal_node_ids: HashMap<String, krabka_raft::NodeId>,
    ) -> Self {
        self.principal_node_ids = principal_node_ids;
        self
    }

    /// Maps an authenticated principal to the WAL node id it may fetch as.
    ///
    /// `inter_broker_principal_node_ids` is authoritative: when it is
    /// populated, only the names it lists map to a node id. The built-in
    /// `broker-<id>` / `CN=broker-<id>` convention is a fallback for an empty
    /// map, and only on the inter-broker listener -- otherwise a principal
    /// created on a client listener could name itself into a voter slot.
    pub(crate) fn authenticated_node_id(
        &self,
        principal: &krabka_security::Principal,
        on_inter_broker_listener: bool,
    ) -> Option<krabka_raft::NodeId> {
        if principal.auth_method == krabka_security::AuthMethod::Anonymous {
            return None;
        }
        if let Some(node_id) = self.principal_node_ids.get(&principal.name) {
            return Some(*node_id);
        }
        if !on_inter_broker_listener || !self.principal_node_ids.is_empty() {
            return None;
        }
        super::wire::conventional_node_id(&principal.name)
    }

    pub(crate) fn insert(&self, shard_id: ShardId, engine: Arc<WalShardEngine>) {
        let placements = self
            .placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let voters = placements
            .get(&shard_id)
            .map_or(&[][..], |placement| placement.voters.as_slice());
        engine.configure_distributed(self.local_node_id, voters);
        engine.attach_observability(shard_id, self.metrics.clone());
        self.engines.insert(shard_id, engine);
    }

    /// Atomically install the voter placement derived from one metadata image.
    /// Replacing the map also removes deleted topics and superseded topic IDs.
    pub(crate) fn replace_placements(&self, placements: &HashMap<ShardId, WalPlacement>) {
        let mut current = self
            .placements
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        current.clone_from(placements);
        for entry in &self.engines {
            let voters = current
                .get(entry.key())
                .map_or(&[][..], |placement| placement.voters.as_slice());
            entry
                .value()
                .configure_distributed(self.local_node_id, voters);
        }
    }

    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub(crate) fn placement(&self, shard_id: ShardId) -> Option<Vec<krabka_raft::NodeId>> {
        self.placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard_id)
            .map(|placement| placement.voters.clone())
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn follower_fetcher_count(&self, shard_id: ShardId) -> usize {
        self.follower_fetchers
            .get(&shard_id)
            .map_or(0, |fetchers| fetchers.len())
    }

    pub(crate) fn get(&self, shard_id: ShardId) -> Option<Arc<WalShardEngine>> {
        self.engines
            .get(&shard_id)
            .map(|entry| entry.value().clone())
    }

    #[must_use]
    pub(crate) fn local_node_id(&self) -> krabka_raft::NodeId {
        self.local_node_id
    }

    #[must_use]
    pub(crate) fn local_is_leader(&self, shard_id: ShardId) -> bool {
        self.placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard_id)
            .and_then(|placement| placement.voters.first())
            == Some(&self.local_node_id)
    }

    #[must_use]
    pub(crate) fn local_is_voter(&self, shard_id: ShardId) -> bool {
        self.placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard_id)
            .is_some_and(|placement| placement.voters.contains(&self.local_node_id))
    }

    pub(crate) fn remove(&self, shard_id: ShardId) -> Option<Arc<WalShardEngine>> {
        let mut placements = self
            .placements
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        placements.remove(&shard_id);
        self.engines.remove(&shard_id).map(|(_, engine)| {
            engine.clear_observability();
            engine
        })
    }

    pub(crate) fn route_fetch_request(
        &self,
        request: &krabka_protocol::owned::fetch_request::FetchRequest,
        authenticated_from: krabka_raft::NodeId,
    ) -> Option<Result<krabka_protocol::owned::fetch_response::FetchResponse, crate::BrokerError>>
    {
        self.route_decoded_fetch(decode_fetch_request(request)?, Some(authenticated_from))
    }

    fn route_decoded_fetch(
        &self,
        request: WalFetchRequest,
        authenticated_from: Option<krabka_raft::NodeId>,
    ) -> Option<Result<krabka_protocol::owned::fetch_response::FetchResponse, crate::BrokerError>>
    {
        let QuorumGroup::DisklessWal {
            topic_id,
            partition,
        } = request.group
        else {
            return None;
        };
        let shard = ShardId {
            topic_id,
            partition,
        };
        let placement = self
            .placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard)
            .cloned();
        let voters = placement.as_ref().map_or_else(Vec::new, |placement| {
            placement.voters.iter().map(|voter| voter.0).collect()
        });
        let admission = wal_fetch_admission(
            authenticated_from.map(|node| node.0),
            request.from.0,
            self.local_node_id.0,
            &voters,
            request.current_leader_epoch,
            placement
                .as_ref()
                .map_or(0, |placement| placement.leader_epoch),
        );
        if admission == WalFetchAdmission::Denied {
            return Some(Ok(unknown_shard_fetch_response(request.group)));
        }
        #[cfg(any(test, feature = "test-helpers"))]
        self.follower_fetchers
            .entry(shard)
            .or_default()
            .insert(request.from);
        let Some(engine) = self.get(shard) else {
            return Some(Ok(unknown_shard_fetch_response(request.group)));
        };
        let epoch_error = match admission {
            WalFetchAdmission::FencedLeaderEpoch => Some(FENCED_LEADER_EPOCH),
            WalFetchAdmission::UnknownLeaderEpoch => Some(UNKNOWN_LEADER_EPOCH),
            WalFetchAdmission::Denied | WalFetchAdmission::Serve => None,
        };
        if let Some(error_code) = epoch_error {
            return Some(Ok(fetch_response(
                request.group,
                0,
                0,
                0,
                bytes::Bytes::new(),
                error_code,
                None,
            )));
        }
        Some(
            engine
                .serve_fetch(
                    krabka_ids::Offset(request.fetch_offset),
                    request.last_fetched_epoch,
                    request.max_size,
                )
                .map(|fetch| {
                    if !fetch.offset_out_of_range && fetch.diverging_epoch.is_none() {
                        engine.record_follower_ack(
                            request.from,
                            krabka_ids::Offset(request.fetch_offset),
                        );
                    }
                    let error_code = if fetch.offset_out_of_range {
                        OFFSET_OUT_OF_RANGE
                    } else {
                        0
                    };
                    fetch_response(
                        request.group,
                        fetch.high_watermark.0,
                        fetch.log_end_offset.0,
                        fetch.log_start_offset.0,
                        fetch.records,
                        error_code,
                        fetch
                            .diverging_epoch
                            .map(|(epoch, offset)| (epoch.0, offset.0)),
                    )
                }),
        )
    }
}

/// Routes shard-addressed KIP-595 Fetch requests to the registered diskless
/// WAL engines.
#[derive(Debug, Clone)]
pub(crate) struct WalShardRouter {
    registry: Arc<WalShardRegistry>,
}

impl WalShardRouter {
    #[must_use]
    pub(crate) fn new(registry: Arc<WalShardRegistry>) -> Self {
        Self { registry }
    }
}

impl krabka_raft::RaftShardRouter for WalShardRouter {
    fn route(
        &self,
        api_key: i16,
        body: bytes::Bytes,
        principal: Option<&krabka_security::Principal>,
    ) -> krabka_raft::ShardRouteFuture<'_> {
        // The raft transport only ever carries inter-broker traffic, so the
        // `broker-<id>` convention stays available on it.
        let authenticated_from =
            principal.and_then(|principal| self.registry.authenticated_node_id(principal, true));
        Box::pin(async move {
            if api_key != krabka_raft::kraft::transport::api_key::FETCH {
                return Ok(None);
            }
            let Some(request) = decode_fetch(&body) else {
                return Ok(None);
            };
            let Some(response) = self
                .registry
                .route_decoded_fetch(request, authenticated_from)
            else {
                return Ok(None);
            };
            let response =
                response.map_err(|err| krabka_raft::RaftError::ChangeRejected(err.to_string()))?;
            Ok(Some(encode_fetch_response_struct(&response)))
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use krabka_ids::Offset;
    use krabka_protocol::{
        Decode,
        owned::fetch_response::FetchResponse,
        records::{Record, RecordBatch},
    };
    use krabka_raft::RaftShardRouter;
    use tempfile::tempdir;

    use super::*;
    use crate::wal::quorum::{
        engine::WalShardEngine,
        test_support::open_log,
        wire::{encode_fetch_for_group, fetch_request},
    };

    fn value_batch() -> RecordBatch {
        RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"a")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        }
    }

    fn placement(voters: Vec<krabka_raft::NodeId>, leader_epoch: i32) -> WalPlacement {
        WalPlacement {
            voters,
            leader_epoch,
        }
    }

    async fn route_fetch(
        router: &WalShardRouter,
        body: Bytes,
        principal: &krabka_security::Principal,
    ) -> FetchResponse {
        let response = router
            .route(
                krabka_raft::kraft::transport::api_key::FETCH,
                body,
                Some(principal),
            )
            .await
            .unwrap()
            .expect("diskless WAL fetch response");
        FetchResponse::decode(&mut response.as_ref(), 17).unwrap()
    }

    fn broker_principal(id: u64) -> krabka_security::Principal {
        crate::test_support::sasl_principal(&format!("broker-{id}"))
    }

    #[tokio::test]
    async fn wal_shard_router_serves_registered_fetch() {
        let dir = tempdir().unwrap();
        let source = open_log(dir.path());
        let mut batch = value_batch();
        source
            .lock()
            .unwrap()
            .append_at(&mut batch, Offset(0))
            .unwrap();
        source.lock().unwrap().sync().unwrap();
        let engine = Arc::new(WalShardEngine::for_logs(
            maplit::btreemap! {krabka_raft::NodeId(1) => source.clone()},
        ));
        engine.replicate_and_sync(&source, Offset(1)).await.unwrap();

        let registry = Arc::new(
            WalShardRegistry::new(krabka_raft::NodeId(9)).with_principal_node_ids(
                maplit::hashmap! {"admin".to_string() => krabka_raft::NodeId(9)},
            ),
        );
        let topic_id = uuid::Uuid::from_u128(17);
        let partition = PartitionIndex(2);
        registry.insert(
            ShardId {
                topic_id,
                partition,
            },
            engine,
        );
        registry.replace_placements(&maplit::hashmap! {ShardId {
            topic_id,
            partition,
        } => placement(vec![krabka_raft::NodeId(9)], 0)});
        let router = WalShardRouter::new(registry);
        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(topic_id, partition),
            krabka_raft::NodeId(9),
            0,
            0,
        );
        let principal = crate::test_support::sasl_principal("admin");

        let decoded = route_fetch(&router, body, &principal).await;
        let partition = &decoded.responses[0].partitions[0];
        assert2::assert!((partition.high_watermark) == (1));
        assert2::assert!((partition.last_stable_offset) == (1));
        assert2::assert!((partition.log_start_offset) == (0));
        assert2::assert!(
            partition
                .records
                .as_ref()
                .is_some_and(|records| records.payload_len() > 0)
        );
    }

    #[tokio::test]
    async fn wal_shard_router_reports_offset_out_of_range_with_log_bounds() {
        let dir = tempdir().unwrap();
        let source = open_log(dir.path());
        {
            let mut log = source.lock().unwrap();
            for offset in 0..6 {
                let mut batch = value_batch();
                log.append_at(&mut batch, Offset(offset)).unwrap();
            }
            log.sync().unwrap();
            log.trim_to_offset(Offset(5)).unwrap();
        }
        let engine = Arc::new(WalShardEngine::for_logs(
            maplit::btreemap! {krabka_raft::NodeId(1) => source},
        ));

        let registry = Arc::new(WalShardRegistry::new(krabka_raft::NodeId(9)));
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(18),
            partition: PartitionIndex(3),
        };
        registry.insert(shard, engine);
        registry.replace_placements(
            &maplit::hashmap! {shard => placement(vec![krabka_raft::NodeId(9)], 0)},
        );
        let router = WalShardRouter::new(registry);
        let principal = broker_principal(9);
        for fetch_offset in [4, 7] {
            let body = encode_fetch_for_group(
                QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
                krabka_raft::NodeId(9),
                0,
                fetch_offset,
            );
            let response = router
                .route(
                    krabka_raft::kraft::transport::api_key::FETCH,
                    body,
                    Some(&principal),
                )
                .await
                .unwrap()
                .unwrap();
            let decoded = FetchResponse::decode(&mut response.as_ref(), 17).unwrap();
            let partition = &decoded.responses[0].partitions[0];
            assert2::assert!((partition.error_code) == (OFFSET_OUT_OF_RANGE));
            assert2::assert!((partition.log_start_offset) == (5));
            assert2::assert!((partition.last_stable_offset) == (6));
            assert2::assert!(
                partition
                    .records
                    .as_ref()
                    .is_some_and(|records| records.payload_len() == 0)
            );
        }
    }

    #[tokio::test]
    async fn wal_shard_router_rejects_a_broker_outside_the_placement() {
        let registry = Arc::new(WalShardRegistry::new(krabka_raft::NodeId(2)));
        let topic_id = uuid::Uuid::from_u128(17);
        let partition = PartitionIndex(2);
        let shard = ShardId {
            topic_id,
            partition,
        };
        let dir = tempdir().unwrap();
        let log = open_log(dir.path());
        registry.insert(
            shard,
            Arc::new(WalShardEngine::for_logs(
                maplit::btreemap! {krabka_raft::NodeId(1) => log},
            )),
        );
        registry.replace_placements(
            &maplit::hashmap! {shard => placement(vec![krabka_raft::NodeId(2)], 0)},
        );
        let router = WalShardRouter::new(registry);
        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(topic_id, partition),
            krabka_raft::NodeId(9),
            0,
            0,
        );
        let principal = broker_principal(9);

        let decoded = route_fetch(&router, body, &principal).await;
        let partition = &decoded.responses[0].partitions[0];
        assert2::assert!((partition.error_code) == (3));
        assert2::assert!(
            partition
                .records
                .as_ref()
                .is_some_and(|records| records.payload_len() == 0)
        );
    }

    #[test]
    fn claimed_voter_must_match_authenticated_node() {
        let dir = tempdir().unwrap();
        let source = open_log(dir.path());
        let mut batch = RecordBatch {
            records: vec![Record::default()],
            ..Default::default()
        };
        source.lock().unwrap().append(&mut batch).unwrap();
        let engine = Arc::new(WalShardEngine::new_distributed(source, 3).unwrap());
        let registry = WalShardRegistry::new(krabka_raft::NodeId(1));
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(19),
            partition: PartitionIndex(0),
        };
        registry.insert(shard, Arc::clone(&engine));
        registry.replace_placements(&maplit::hashmap! {shard => placement(
            vec![krabka_raft::NodeId(1), krabka_raft::NodeId(2), krabka_raft::NodeId(3)],
            0,
        )});
        let request = fetch_request(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            krabka_raft::NodeId(2),
            0,
            0,
            1,
            krabka_units::mebibytes(1),
        );

        let response = registry
            .route_fetch_request(&request, krabka_raft::NodeId(3))
            .unwrap()
            .unwrap();

        assert2::assert!((response.responses[0].partitions[0].error_code) == (3));
        assert2::assert!((engine.durable_watermark()) == (Offset(0)));
    }

    #[test]
    fn wal_shard_registry_fences_mismatched_leader_epochs_before_acknowledging() {
        let dir = tempdir().unwrap();
        let source = open_log(dir.path());
        source
            .lock()
            .unwrap()
            .append(&mut RecordBatch {
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .unwrap();
        let engine = Arc::new(WalShardEngine::new_distributed(source, 3).unwrap());
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(19),
            partition: PartitionIndex(4),
        };
        let registry = WalShardRegistry::new(krabka_raft::NodeId(1));
        registry.replace_placements(&maplit::hashmap! {shard => placement(
            vec![
                krabka_raft::NodeId(1),
                krabka_raft::NodeId(2),
                krabka_raft::NodeId(3),
            ],
            8,
        )});
        registry.insert(shard, Arc::clone(&engine));
        for (epoch, expected) in [(7, FENCED_LEADER_EPOCH), (9, UNKNOWN_LEADER_EPOCH)] {
            let request = fetch_request(
                QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
                krabka_raft::NodeId(2),
                epoch,
                epoch,
                1,
                krabka_units::mebibytes(1),
            );
            let response = registry
                .route_fetch_request(&request, krabka_raft::NodeId(2))
                .unwrap()
                .unwrap();

            assert2::assert!((response.responses[0].partitions[0].error_code) == (expected));
            assert2::assert!((engine.durable_watermark()) == (Offset(0)));
        }
    }

    #[test]
    fn replacing_placements_removes_stale_shards() {
        let registry = WalShardRegistry::new(krabka_raft::NodeId(1));
        let stale = ShardId {
            topic_id: uuid::Uuid::from_u128(1),
            partition: PartitionIndex(0),
        };
        let current = ShardId {
            topic_id: uuid::Uuid::from_u128(2),
            partition: PartitionIndex(0),
        };
        registry.replace_placements(
            &maplit::hashmap! {stale => placement(vec![krabka_raft::NodeId(1)], 1)},
        );

        registry.replace_placements(
            &maplit::hashmap! {current => placement(vec![krabka_raft::NodeId(2)], 2)},
        );

        assert2::assert!(registry.placement(stale).is_none());
        assert2::assert!((registry.placement(current)) == (Some(vec![krabka_raft::NodeId(2)])));
    }

    #[test]
    fn authenticated_node_id_honors_listener_and_map() {
        use krabka_security::{AuthMethod, Principal};

        let node3 = krabka_raft::NodeId(3);
        let mapped = maplit::hashmap! {"wal-fetcher".to_string() => node3};
        let scram = AuthMethod::SaslScramSha256;
        // (configured map, principal name, auth method, inter-broker listener, expected node)
        let cases = vec![
            (HashMap::new(), "broker-3", scram, false, None),
            (HashMap::new(), "broker-3", scram, true, Some(node3)),
            (HashMap::new(), "CN=broker-3,O=krabka", scram, false, None),
            (
                HashMap::new(),
                "CN=broker-3,O=krabka",
                scram,
                true,
                Some(node3),
            ),
            (
                HashMap::new(),
                "broker-3",
                AuthMethod::Anonymous,
                true,
                None,
            ),
            (mapped.clone(), "wal-fetcher", scram, false, Some(node3)),
            (mapped.clone(), "wal-fetcher", scram, true, Some(node3)),
            (mapped, "broker-3", scram, true, None),
        ];

        for (principal_node_ids, name, auth_method, inter_broker, expected) in cases {
            let registry = WalShardRegistry::new(krabka_raft::NodeId(1))
                .with_principal_node_ids(principal_node_ids);
            let principal = Principal {
                name: name.to_string(),
                auth_method,
                groups: Vec::new(),
            };

            let actual = registry.authenticated_node_id(&principal, inter_broker);

            assert2::check!(actual == expected, "{name} inter_broker={inter_broker}");
        }
    }
}
