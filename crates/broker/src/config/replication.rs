//! The runtime policy a follower replication task reads: the shape of each
//! replication fetch and the backoffs between them.

use krabka_units::{ByteSize, Time, bytes, mebibytes, millis, secs};

/// Runtime policy for follower replication tasks.
///
/// It sets the size and the maximum wait of each replication fetch, and the
/// backoffs the follower loop applies between fetches.
///
/// This type is not `Eq`: every value here is a quantity, and its `f64`
/// storage is only `PartialEq`. Three of the fields reach the wire, as
/// `FetchRequest`'s `max_bytes`, `min_bytes`, and `max_wait_ms`.
#[derive(Debug, Clone, PartialEq, krabka_macros::FieldDefaults)]
pub struct ReplicationRuntimeConfig {
    /// How many fetchers this broker runs per leader it follows.
    ///
    /// Kafka's `num.replica.fetchers`. Every partition this broker follows
    /// from one leader is hashed onto one of that leader's fetchers, and each
    /// fetcher holds one connection and sends one `Fetch` per round however
    /// many partitions it carries. Raising it spreads a very large follower
    /// set across more connections and more concurrent rounds; the default of
    /// one is Kafka's, and is what keeps a follower of ten thousand
    /// partitions at three connections rather than ten thousand.
    ///
    /// Zero is treated as one: a leader with no fetcher would never be
    /// followed at all.
    #[default(1)]
    pub fetchers: usize,

    /// Maximum bytes requested for one partition in a replication fetch,
    /// Kafka's `replica.fetch.max.bytes`.
    #[default(mebibytes(1))]
    pub fetch_max: ByteSize,
    /// Maximum bytes of one whole replication fetch response, Kafka's
    /// `replica.fetch.response.max.bytes`.
    ///
    /// A follower asks for every partition it follows from a leader in one
    /// fetch, so this caps how much it can copy per round trip. Kafka's
    /// default is ten partitions' worth; the per-partition cap alone would let
    /// one partition's backlog hold the whole follower to a megabyte a round.
    #[default(mebibytes(10))]
    pub fetch_response_max: ByteSize,
    /// Maximum leader wait for a replication fetch.
    #[default(millis(500))]
    pub fetch_max_wait: Time,
    /// Minimum bytes that satisfy a replication fetch.
    ///
    /// It reaches the leader as the request's `min_bytes`, and a krabka leader
    /// honours it as a floor the way Kafka does: the fetch is held until that
    /// many bytes are readable across its partitions or `fetch_max_wait`
    /// expires, however many appends it takes to get there.
    #[default(bytes(1))]
    pub fetch_min: ByteSize,
    /// Delay after a replication throttle budget is exhausted.
    #[default(millis(100))]
    pub throttle_exhausted_backoff: Time,
    /// Retry delay after sending a replication request fails.
    #[default(secs(1))]
    pub send_error_backoff: Time,
    /// Retry delay when the leader does not yet know the topic.
    #[default(millis(100))]
    pub unknown_topic_retry_delay: Time,
    /// Retry delay after a leader-epoch fence.
    #[default(millis(200))]
    pub epoch_fence_backoff: Time,
    /// Retry delay after an unexpected replication error.
    #[default(millis(500))]
    pub unexpected_error_backoff: Time,
    /// Initial delay before reconnecting to a leader.
    #[default(millis(100))]
    pub reconnect_initial_delay: Time,
    /// Maximum delay between leader reconnection attempts.
    #[default(secs(5))]
    pub reconnect_delay_cap: Time,
}
