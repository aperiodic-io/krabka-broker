//! Committed `__cluster_metadata` reads for observers: the local record-batch
//! slice this node serves out of its own log, and the one-shot outbound fetch a
//! broker-only observer issues against a controller listener.

use {
    super::{ControllerHandle, connection_options},
    crate::{
        error::RaftError,
        kraft::MetadataFetchSlice,
        types::NodeId,
        wire::{
            API_KEY_METADATA_FETCH, KrabkaMetadataFetchRequest, KrabkaMetadataFetchResponse,
            PRIVATE_UNSUPPORTED_VERSION, private_request_version,
        },
    },
    krabka_units::prelude::{ByteSize, ByteSizeExt as _},
    std::net::SocketAddr,
};

impl ControllerHandle {
    /// Read committed `__cluster_metadata` entries starting at `fetch_offset`,
    /// encoded as Kafka record batches for an observer.
    #[must_use]
    pub async fn metadata_records(
        &self,
        fetch_offset: u64,
        max_size: ByteSize,
    ) -> MetadataFetchSlice {
        let off = i64::try_from(fetch_offset).unwrap_or(i64::MAX);
        self.engine
            .metadata_fetch(off, max_size, None)
            .await
            .unwrap_or(MetadataFetchSlice {
                records: bytes::Bytes::new(),
                snapshot_id: None,
                log_start_offset: 0,
                high_watermark: 0,
                quorum_high_watermark: 0,
            })
    }

    /// Dial a controller-listener `addr` and issue one `API_KEY_METADATA_FETCH`.
    /// Used by broker-only observers to pull committed `__cluster_metadata`.
    ///
    /// # Errors
    /// - [`RaftError::Network`] if the dial or request fails.
    /// - [`RaftError::Protocol`] if the response cannot be decoded.
    pub async fn fetch_metadata_from(
        &self,
        addr: SocketAddr,
        fetch_offset: u64,
        max_size: ByteSize,
    ) -> Result<KrabkaMetadataFetchResponse, RaftError> {
        let req = KrabkaMetadataFetchRequest {
            fetch_offset: i64::try_from(fetch_offset).unwrap_or(i64::MAX),
            // `max_bytes` is the KIP-595-shaped `int32` on the Krabka observer
            // wire; the quantity converts here and nowhere deeper.
            max_bytes: max_size.bytes_i32(),
            // A one-shot read, not a replica: the leader tracks nothing for it.
            replica_id: -1,
            replica_directory_id: uuid::Uuid::nil(),
        };
        // Negotiated from `krabka.version`; every level this build knows
        // gives v0, the body below.
        let version = private_request_version(
            krabka_metadata::PrivateRpc::MetadataFetch,
            Some(&self.current_image()),
        );
        let mut body = Vec::with_capacity(32);
        req.encode_v0(&mut body);

        let opts = connection_options(
            &self.client_id,
            self.client_dispatch_queue_capacity,
            self.client_frame_max,
        );
        let conn = self
            .dialer
            .dial(NodeId(1), &addr.to_string(), opts)
            .await
            .map_err(RaftError::Network)?;
        let resp_body = conn
            .raw_request(API_KEY_METADATA_FETCH, version, bytes::Bytes::from(body))
            .await
            .map_err(RaftError::Network)?;
        conn.close();

        let mut cur: &[u8] = &resp_body;
        let response =
            KrabkaMetadataFetchResponse::decode_v0(&mut cur).map_err(RaftError::Protocol)?;
        if response.error_code == PRIVATE_UNSUPPORTED_VERSION {
            return Err(RaftError::UnsupportedPrivateVersion {
                api_key: API_KEY_METADATA_FETCH,
                version,
            });
        }
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            config::ControllerConfig,
            controller::{
                Controller,
                test_support::{
                    FAST_ELECTION_TIMEOUT, RecordingDialer, TEST_OP_TIMEOUT, UNBOUNDED_FETCH,
                    bootstrap_controller, committable_topic_record, submit_change_with_timeout,
                    wait_for_leader,
                },
            },
        },
        krabka_metadata::{MetadataImage, MetadataRecord, from_kraft_value},
        krabka_protocol::records::RecordBatch,
        krabka_units::prelude::{TimeExt as _, mebibytes},
        std::sync::Arc,
        tempfile::TempDir,
    };

    fn contains_topic(records: &[u8], name: &str) -> bool {
        let image = MetadataImage::new(uuid::Uuid::nil());
        let mut buf = records;
        let mut found = false;
        while !buf.is_empty() {
            let batch = RecordBatch::decode(&mut buf).expect("decode");
            if batch.attributes.is_control_batch() {
                continue;
            }
            for r in &batch.records {
                let Some(value) = r.value.as_ref() else {
                    continue;
                };
                if let Ok(MetadataRecord::V1Topic(t)) = from_kraft_value(value, &image)
                    && t.name == name
                {
                    found = true;
                }
            }
        }
        found
    }

    /// A bootstrapped controller with one committed topic, and the data
    /// directory it must outlive.
    struct SeededController {
        dir: TempDir,
        ctrl: ControllerHandle,
    }

    async fn committed_topic(name: &str, label: &str) -> SeededController {
        let (dir, ctrl) = bootstrap_controller("bootstrap").await;
        wait_for_leader(&ctrl).await;
        submit_change_with_timeout(&ctrl, vec![committable_topic_record(name)], label)
            .await
            .expect("submit");
        SeededController { dir, ctrl }
    }

    #[tokio::test]
    async fn metadata_records_serves_committed_topic() {
        let SeededController { dir: _dir, ctrl } =
            committed_topic("t", "metadata_records seed").await;

        let slice = tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.metadata_records(0, UNBOUNDED_FETCH),
        )
        .await
        .expect("metadata_records timed out");
        assert2::assert!(slice.high_watermark >= 1);
        assert2::assert!(contains_topic(&slice.records, "t"));
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_metadata_from_returns_committed_records() {
        let SeededController { dir: _dir, ctrl } =
            committed_topic("fetched", "fetch_metadata seed").await;

        let addr = ctrl.controller_bound_addr();
        let resp = tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.fetch_metadata_from(addr, 0, mebibytes(1)),
        )
        .await
        .expect("fetch_metadata_from timed out")
        .expect("fetch");
        assert2::assert!(resp.error_code == 0);
        assert2::assert!(resp.high_watermark >= 1);

        assert2::assert!(contains_topic(&resp.records, "fetched"));
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_metadata_from_passes_configured_client_id_to_dialer() {
        let dir = TempDir::new().unwrap();
        let client_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded_options = Arc::new(std::sync::Mutex::new(Vec::new()));
        let dialer = RecordingDialer {
            client_ids: Arc::clone(&client_ids),
            options: Arc::clone(&recorded_options),
        };
        let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        cfg.election_timeout = FAST_ELECTION_TIMEOUT;
        cfg.client_id = "metadata-fetch-client".into();
        cfg.client_dispatch_queue_capacity =
            krabka_client_core::ConnectionDispatchQueueCapacity::new(7).unwrap();
        cfg.client_frame_max = krabka_client_core::ClientFrameMax::try_from(mebibytes(8)).unwrap();
        cfg.dialer = Some(Arc::new(dialer));

        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        wait_for_leader(&ctrl).await;
        submit_change_with_timeout(
            &ctrl,
            vec![committable_topic_record("client-id-check")],
            "client-id fetch seed",
        )
        .await
        .expect("submit");

        let resp = tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.fetch_metadata_from(ctrl.controller_bound_addr(), 0, mebibytes(1)),
        )
        .await
        .expect("fetch_metadata_from timed out")
        .expect("fetch");

        assert2::assert!(resp.error_code == 0);
        assert2::assert!(client_ids.lock().unwrap().as_slice() == ["metadata-fetch-client"]);
        {
            let opts = recorded_options.lock().unwrap();
            let first_opt = &opts[0];
            assert2::assert!(opts.len() == 1);
            let expected_cap = krabka_client_core::ConnectionDispatchQueueCapacity::new(7).unwrap();
            assert2::assert!(first_opt.dispatch_queue_capacity == expected_cap);
            let expected_frame =
                krabka_client_core::ClientFrameMax::try_from(mebibytes(8)).unwrap();
            assert2::assert!(first_opt.frame_max == expected_frame);
        }
        ctrl.shutdown().await;
    }
}
