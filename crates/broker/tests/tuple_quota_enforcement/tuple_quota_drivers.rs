//! The two typed request drivers the test needs on top of the raw wire
//! exchange: `AlterClientQuotas`, which installs the (user, client-id) tuple
//! quota, and `Produce`, which carries an explicit on-wire `client_id` so one
//! test can send the same payload as two different clients.
//!
//! `await_authorized_produce` lives with them because the retry it wraps is a
//! property of the produce driver: a freshly seeded ACL can still be absent
//! from the handler's image snapshot when the first request arrives.

pub use crate::kafka_wire::quotas::drive_alter_client_quotas_sasl;
use {
    crate::{
        CLIENT_ID,
        kafka_wire::{
            self, PlainAuthenticationSetup, ProduceRecordCount, SaslProduceSetup,
            ZeroFilledRecordsSetup,
        },
    },
    assert2::assert,
    krabka_protocol::owned::produce_response::ProduceResponse,
    std::{
        net::SocketAddr,
        time::{Duration, Instant},
    },
};

// ─────────────────────────────────────────────────────────────────────────────
// Wire driver for Produce with explicit on-wire client_id
// ─────────────────────────────────────────────────────────────────────────────

pub(crate) async fn await_authorized_produce(
    addr: SocketAddr,
    password: &[u8],
    client_id: &str,
) -> ProduceResponse {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = kafka_wire::produce_sasl_with_ids(addr, SaslProduceSetup {
            authentication: PlainAuthenticationSetup {
                client_id: CLIENT_ID,
                password: (password).to_vec(),
                ..Default::default()
            },
            produce_client_id: client_id,
            records: ZeroFilledRecordsSetup {
                topic: ("tuple-quota-topic").to_string(),
                count: ProduceRecordCount(4),
                ..Default::default()
            },
        })
        .await;
        let error_code = response
            .responses
            .first()
            .and_then(|topic| topic.partition_responses.first())
            .map_or(-1, |partition| partition.error_code);
        if error_code != 29 {
            return response;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
