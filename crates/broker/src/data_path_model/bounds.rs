//! The bounds of the modelled cluster and the total conversions those bounds
//! justify.
//!
//! Every cast in this model is infallible only because the cluster is tiny, so
//! the constants that make it tiny and the helpers that rely on them live in
//! one file. `NB` brokers, a log of at most `MAX_LEN` records and at most
//! `MAX_EPOCH` leader epochs bound the search; `node` and `has` are the
//! bit-mask vocabulary that the broker sets are written in.

// All arithmetic here is bounded to a tiny cluster (≤ 3 brokers, log-len ≤ 3),
// so the offset/length/id casts below can never wrap or truncate.

pub(super) const NB: usize = 3; // brokers 0,1,2
pub(super) const NB_U8: u8 = 3;
pub(super) const MAX_LEN: usize = 4; // max log length (offsets 0..4)
pub(super) const MAX_EPOCH: u8 = 3;

pub(super) fn model_offset(value: usize) -> i64 {
    i64::try_from(value).expect("bounded model offset fits in i64")
}

pub(super) fn model_index(value: i64) -> usize {
    usize::try_from(value).expect("model offsets are non-negative and bounded")
}

pub(super) fn model_broker(value: u64) -> u8 {
    u8::try_from(value).expect("model broker id fits in u8")
}

pub(super) fn node(b: u8) -> u64 { u64::from(b) }
pub(super) fn has(mask: u8, b: u8) -> bool { mask & (1 << b) != 0 }
