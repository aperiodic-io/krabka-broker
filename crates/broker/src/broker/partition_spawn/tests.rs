use {
    super::*,
    crate::{
        config::BrokerConfig,
        log_dir::partition_dir,
        log_dir_status::LogDirRegistry,
        producer_state::ProducerState,
        wal::quorum::{
            registry::{ShardId, WalShardRegistry},
            wire::{QuorumGroup, fetch_request},
        },
    },
    assert2::assert,
    tempfile::tempdir,
};

#[tokio::test]
async fn nondefault_partition_writer_queue_depth_backpressures_at_bound() {
    let dir = tempdir().expect("tempdir");
    let partition = try_spawn_partition_with_sequencer(PartitionSpawnConfig {
        topic: "queue-bound".to_string(),
        topic_id: None,
        partition_id: PartitionIndex(0),
        log_dir: dir.path().to_path_buf(),
        log: krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).expect("open log"),
        log_dir_status: LogDirRegistry::default(),
        producer_state: Arc::new(ProducerState::new()),
        runtime: PartitionRuntimeConfig::new(
            (BrokerConfig::default().max_produce_group, 2, 3),
            false,
            (None, None, None),
        ),
    })
    .expect("spawn partition");

    for _ in 0..2 {
        let (ack, _ack_rx) = tokio::sync::oneshot::channel();
        assert!(
            partition
                .writer_tx
                .try_send(WriterMessage::Compact { ack })
                .is_ok()
        );
    }
    let (ack, _ack_rx) = tokio::sync::oneshot::channel();
    assert!(matches!(
        partition.writer_tx.try_send(WriterMessage::Compact { ack }),
        Err(tokio::sync::mpsc::error::TrySendError::Full(_))
    ));

    partition
        .take_writer_handle()
        .expect("partition writer handle")
        .abort();
}

#[test]
fn diskless_partition_requires_distributed_identity_and_registry() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).expect("open log"),
    ));

    assert!(
        partition_wal(
            PartitionIdentity {
                topic: "topic",
                topic_id: None,
                partition_id: PartitionIndex(0),
            },
            log.clone(),
            false,
            None,
            None,
            3,
        )
        .expect("partition wal")
        .wal
        .is_none()
    );
    let error = partition_wal(
        PartitionIdentity {
            topic: "topic",
            topic_id: None,
            partition_id: PartitionIndex(0),
        },
        log.clone(),
        true,
        None,
        None,
        3,
    )
    .err()
    .expect("missing topic id must fail");
    assert!(matches!(
        error,
        BrokerError::Replication(message)
            if message == "diskless WAL topic id is not available for topic-0"
    ));

    let error = partition_wal(
        PartitionIdentity {
            topic: "topic",
            topic_id: Some(uuid::Uuid::new_v4()),
            partition_id: PartitionIndex(0),
        },
        log,
        true,
        None,
        None,
        3,
    )
    .err()
    .expect("missing shard registry must fail");
    assert!(matches!(
        error,
        BrokerError::Replication(message)
            if message == "diskless WAL shard registry is not available for topic-0"
    ));
}

#[tokio::test]
async fn distributed_wal_ack_restores_the_partition_watermark() {
    let dir = tempdir().expect("tempdir");
    let topic_id = uuid::Uuid::new_v4();
    let shard = ShardId {
        topic_id,
        partition: PartitionIndex(0),
    };
    let registry = Arc::new(WalShardRegistry::new(krabka_raft::NodeId(1)));
    registry.replace_placements(
        &maplit::hashmap! {shard => crate::wal::quorum::registry::WalPlacement {
            voters: vec![
                krabka_raft::NodeId(1),
                krabka_raft::NodeId(2),
                krabka_raft::NodeId(3),
            ],
            leader_epoch: 0,
        }},
    );
    let partition_dir = partition_dir(dir.path(), "recovered", 0);
    std::fs::create_dir_all(&partition_dir).expect("partition directory");
    let mut log = krabka_log::Log::open(&partition_dir, krabka_log::LogConfig::default()).unwrap();
    let mut batch = krabka_protocol::records::RecordBatch {
        last_offset_delta: 1,
        records: vec![
            krabka_protocol::records::Record::default(),
            krabka_protocol::records::Record {
                offset_delta: 1,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    log.append(&mut batch).expect("append recovered records");
    let partition = try_spawn_partition_with_sequencer(PartitionSpawnConfig {
        topic: "recovered".into(),
        topic_id: Some(topic_id),
        partition_id: PartitionIndex(0),
        log_dir: dir.path().to_path_buf(),
        log,
        log_dir_status: LogDirRegistry::default(),
        producer_state: Arc::new(ProducerState::new()),
        runtime: PartitionRuntimeConfig::new(
            (1_024, 64, 3),
            true,
            (None, Some(registry.clone()), None),
        ),
    })
    .expect("spawn recovered partition");
    assert!(partition.high_watermark().await == krabka_log::Offset(0));

    let acknowledgement = fetch_request(
        QuorumGroup::diskless_wal(topic_id, PartitionIndex(0)),
        krabka_raft::NodeId(2),
        0,
        0,
        2,
        krabka_units::mebibytes(1),
    );
    registry
        .route_fetch_request(&acknowledgement, krabka_raft::NodeId(2))
        .expect("WAL route")
        .expect("WAL response");

    partition
        .await_hw_at_least(
            krabka_log::Offset(2),
            std::time::Instant::now() + std::time::Duration::from_secs(2),
        )
        .await
        .expect("restored WAL watermark");
    partition
        .take_writer_handle()
        .expect("partition writer handle")
        .abort();
}
