//! Fixtures shared by the controller unit tests: the metadata records they
//! submit, the waits for an elected leader and for a committed change, the
//! retrying listener bind that proves a port was released, and the recording
//! dialer that observes the client id an outbound fetch goes out with.

use {
    crate::{
        SubmitChangeResult,
        config::{BootstrapMode, ControllerConfig},
        controller::{Controller, ControllerHandle},
        error::RaftError,
        network::OutboundDialer,
        test_support::single_partition_topic,
        types::NodeId,
        wire::KrabkaSubmitChangeResponse,
    },
    krabka_units::prelude::{ByteSize, Time, TimeExt as _, gibibytes, millis, secs},
    std::sync::Arc,
    uuid::Uuid,
};

pub(super) const TEST_OP_TIMEOUT: Time = secs(2);

/// Election timeout used by tests that want a leader elected promptly.
pub(super) const FAST_ELECTION_TIMEOUT: Time = millis(200);

/// A fetch budget large enough that no test log is truncated by it.
pub(super) const UNBOUNDED_FETCH: ByteSize = gibibytes(1);

pub(super) fn topic_record(name: &str) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1Topic(single_partition_topic(name, Uuid::nil()))
}

pub(super) fn committable_topic_record(name: &str) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1Topic(single_partition_topic(name, Uuid::new_v4()))
}

pub(super) async fn bootstrap_controller(context: &str) -> (tempfile::TempDir, ControllerHandle) {
    let dir = tempfile::TempDir::new().unwrap();
    let config = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
    let controller = Controller::start(config).await.expect(context);
    (dir, controller)
}

pub(super) async fn joining_controller(context: &str) -> (tempfile::TempDir, ControllerHandle) {
    let dir = tempfile::TempDir::new().unwrap();
    let cfg = ControllerConfig {
        bootstrap_mode: BootstrapMode::Join,
        initial_voters: krabka_metadata::VoterSet::from_voters(std::iter::empty()),
        ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
    };
    let ctrl = Controller::start(cfg).await.expect(context);
    (dir, ctrl)
}

pub(super) async fn wait_for_leader(ctrl: &ControllerHandle) {
    let mut leader_rx = ctrl.watch_leader();
    tokio::time::timeout(
        TEST_OP_TIMEOUT.to_std(),
        leader_rx.wait_for(Option::is_some),
    )
    .await
    .expect("no leader elected within 2s")
    .expect("leader watch channel closed");
}

pub(super) async fn submit_change_with_timeout(
    ctrl: &ControllerHandle,
    records: Vec<krabka_metadata::MetadataRecord>,
    context: &str,
) -> Result<(), RaftError> {
    tokio::time::timeout(TEST_OP_TIMEOUT.to_std(), ctrl.submit_change(records))
        .await
        .unwrap_or_else(|_| panic!("{context} submit_change timed out"))
        .map(|_| ())
}

krabka_macros::bind_retry_fixture!(bind_eventually, ::std::time::Duration::from_secs(5));

#[derive(Clone, Default)]
pub(super) struct RecordingDialer {
    pub(super) client_ids: Arc<std::sync::Mutex<Vec<String>>>,
    pub(super) options: Arc<std::sync::Mutex<Vec<krabka_client_core::ConnectionOptions>>>,
}

#[async_trait::async_trait]
impl OutboundDialer for RecordingDialer {
    async fn dial(
        &self,
        _target: NodeId,
        addr: &str,
        options: krabka_client_core::ConnectionOptions,
    ) -> Result<krabka_client_core::Connection, krabka_client_core::ClientError> {
        self.client_ids
            .lock()
            .unwrap()
            .push(options.client_id.clone());
        self.options.lock().unwrap().push(options.clone());
        let sock = tokio::net::lookup_host(addr)
            .await
            .map_err(krabka_client_core::ClientError::Io)?
            .next()
            .ok_or_else(|| {
                krabka_client_core::ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "test address resolved to no sockets",
                ))
            })?;
        krabka_client_core::Connection::connect(sock, options).await
    }
}

pub(super) fn submit_change_response_bytes(error_code: i16, leader_hint: i64) -> bytes::Bytes {
    let mut out = Vec::new();
    let result = <serde_wincode::SerdeCompat<SubmitChangeResult> as wincode::Serialize>::serialize(
        &SubmitChangeResult::default(),
    )
    .expect("serialize result");
    KrabkaSubmitChangeResponse {
        error_code,
        leader_hint,
        result: bytes::Bytes::from(result),
    }
    .encode_v0(&mut out)
    .unwrap();
    bytes::Bytes::from(out)
}
