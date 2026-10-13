//! [`ThrottleState`], the broker-wide bundle of [`TokenBucket`]s that meters
//! replica traffic and intra-broker log directory moves.
//!
//! It is its own module because it is a container of buckets rather than part
//! of the bucket itself.

use {super::TokenBucket, std::sync::Arc};

/// Broker-wide throttle state for replica traffic and intra-broker log moves.
#[derive(Debug)]
pub struct ThrottleState {
    pub leader_out: Arc<TokenBucket>,
    pub follower_in: Arc<TokenBucket>,
    pub alter_log_dirs: Arc<TokenBucket>,
}

impl ThrottleState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            leader_out: Arc::new(TokenBucket::new()),
            follower_in: Arc::new(TokenBucket::new()),
            alter_log_dirs: Arc::new(TokenBucket::new()),
        }
    }
}

impl Default for ThrottleState {
    fn default() -> Self { Self::new() }
}
