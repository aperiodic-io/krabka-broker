//! The SASL/PLAIN half of the wire layer: the `CreateTopics` and
//! `ElectLeaders` drivers that run over an authenticated stream.
//!
//! Only the authorization test uses these. It needs a named principal for the
//! authorizer to deny, so its listener is `SASL_PLAINTEXT` rather than the
//! PLAINTEXT one the election tests use.

pub use crate::kafka_wire::create_configured_topic_sasl as create_topic_sasl_plain;
use {
    crate::{
        kafka_wire::{self, PlainAuthenticationSetup},
        support::admin::{ElectionSetup, election_partition_errors, election_request},
    },
    std::net::SocketAddr,
};

/// Drives `ElectLeaders` over a SASL/PLAIN authenticated connection.
///
/// This function returns the per-partition `(partition_id, error_code)` rows
/// for the given topic. It does **not** assert the top-level `error_code`, so
/// the caller can examine per-row auth failures.
pub async fn drive_elect_leaders_sasl_plain(
    addr: SocketAddr,
    election: ElectionSetup,
    authentication: &PlainAuthenticationSetup<'_>,
) -> Vec<(i32, i16)> {
    let mut stream = kafka_wire::sasl_plain_authenticate(addr, authentication)
        .await
        .expect("SASL authenticate for ElectLeaders");
    let topic = election.topic.clone();
    let req = election_request()
        .topic(election.topic)
        .partitions(election.partitions)
        .kind(election.kind)
        .call();
    let resp =
        kafka_wire::elect_leaders_response(&mut stream, &req, authentication.client_id).await;

    election_partition_errors(resp, &topic)
}
