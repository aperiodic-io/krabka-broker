//! The witness bitset that records which interleavings the search actually
//! reached, and the bit for each one.
//!
//! A `sometimes` property passes only when its bit is set, so these bits are
//! what keeps a vacuous run out of reach. They are a concern of their own,
//! separate from the state the model checks.

pub const WITNESS_DUPLICATE_RESPONSE: u16 = 1 << 0;
pub const WITNESS_FAILOVER: u16 = 1 << 1;
pub const WITNESS_RETRY: u16 = 1 << 2;
pub const WITNESS_RETRY_AFTER_FAILOVER: u16 = 1 << 3;
pub const WITNESS_PREPARED_RETRY: u16 = 1 << 4;
pub const WITNESS_ACKED_BEFORE_FAILOVER: u16 = 1 << 5;
pub const WITNESS_NOT_LEADER: u16 = 1 << 8;
pub const WITNESS_TIMED_OUT_UNKNOWN: u16 = 1 << 9;
pub const WITNESS_APPENDED_UNACKED: u16 = 1 << 10;
pub const WITNESS_DUPLICATE_AFTER_UNKNOWN: u16 = 1 << 11;
pub const WITNESS_UNKNOWN_RETRY_AFTER_FAILOVER: u16 = 1 << 12;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Witnesses(pub u16);

impl Witnesses {
    pub fn mark(&mut self, bit: u16) { self.0 |= bit; }

    pub fn seen(self, bit: u16) -> bool { self.0 & bit != 0 }
}
