//! The coordinator lookup that a Kafka client makes.
//!
//! No broker creates `__consumer_offsets`, `__transaction_state` or
//! `__share_group_state` when it starts. The first `FindCoordinator` that
//! needs one of them asks for it, and the lookup answers
//! `COORDINATOR_NOT_AVAILABLE` until the topic exists and has leaders
//! (Kafka's `KafkaApis.getCoordinator`). A Kafka client retries that answer,
//! as `AbstractCoordinator.FindCoordinatorResponseHandler` and
//! `TransactionManager.FindCoordinatorHandler` do. A suite that sends raw
//! requests uses [`find_coordinator`] to make the same lookup.

use {
    crate::support::discovery::coordinator_lookup_request,
    assert2::assert,
    krabka_client_core::Client,
    krabka_protocol::owned::find_coordinator_response::Coordinator,
    std::time::{Duration, Instant},
};

/// `FindCoordinator` key type of a consumer group.
pub const KEY_TYPE_GROUP: i8 = 0;
/// `FindCoordinator` key type of a transactional id.
pub const KEY_TYPE_TRANSACTION: i8 = 1;
/// `FindCoordinator` key type of a share-group state key.
pub const KEY_TYPE_SHARE: i8 = 2;

/// `COORDINATOR_LOAD_IN_PROGRESS`.
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
/// `COORDINATOR_NOT_AVAILABLE`.
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// `NOT_COORDINATOR`.
const NOT_COORDINATOR: i16 = 16;

/// The time a lookup may take to see its coordinator topic created.
const DEADLINE: Duration = Duration::from_secs(30);

/// Looks `key` up until a broker answers as its coordinator, and returns
/// that row.
///
/// A retriable coordinator error is retried, as a Kafka client retries it.
/// Any other error panics.
pub async fn find_coordinator(client: &Client, key_type: i8, key: &str) -> Coordinator {
    let deadline = Instant::now() + DEADLINE;
    loop {
        let response = client
            .send(coordinator_lookup_request(key, key_type, vec![key.into()]))
            .await
            .expect("FindCoordinator");
        let [row] = response.coordinators.as_slice() else {
            panic!("one coordinator row: {response:?}");
        };
        match row.error_code {
            0 => return row.clone(),
            COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR => {
                assert!(
                    Instant::now() < deadline,
                    "no coordinator for {key:?} within {DEADLINE:?}: {row:?}"
                );
                // intentional: a client has no awaiter for the creation; it
                // retries after a backoff, as Kafka's clients do.
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            code => panic!("FindCoordinator({key:?}) answered {code}: {row:?}"),
        }
    }
}
