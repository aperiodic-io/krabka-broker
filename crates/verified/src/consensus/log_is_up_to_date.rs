#[cfg(creusot)]
use super::{count_ge, count_ge_prefix, hwm_member_at, valid_hwm_majority};
use creusot_std::prelude::*;

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= limit && limit <= s.len() + 1)]
#[requires(low <= high)]
#[ensures(count_ge_prefix(log_end, s, low, limit, leader_counts)
    >= count_ge_prefix(log_end, s, high, limit, leader_counts))]
#[variant(limit)]
pub fn lemma_count_ge_prefix_monotone(
    log_end: Int,
    s: Seq<i64>,
    low: Int,
    high: Int,
    limit: Int,
    leader_counts: bool,
) {
    if limit > 0 {
        lemma_count_ge_prefix_monotone(log_end, s, low, high, limit - 1, leader_counts);
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= limit && limit <= s.len() + 1)]
#[ensures(count_ge_prefix(log_end, s, v, limit, leader_counts) >= 0)]
#[variant(limit)]
pub fn lemma_count_ge_prefix_nonnegative(
    log_end: Int,
    s: Seq<i64>,
    v: Int,
    limit: Int,
    leader_counts: bool,
) {
    if limit > 0 {
        lemma_count_ge_prefix_nonnegative(log_end, s, v, limit - 1, leader_counts);
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 < limit && limit <= s.len() + 1)]
#[requires(count_ge_prefix(log_end, s, v, limit, leader_counts) >= 1)]
#[ensures(0 <= result && result < limit)]
#[ensures(hwm_member_at(log_end, s, result) >= v)]
#[ensures(count_ge_prefix(log_end, s, hwm_member_at(log_end, s, result), limit, leader_counts)
    >= count_ge_prefix(log_end, s, v, limit, leader_counts))]
#[variant(limit)]
pub fn least_hwm_member_ge_index(
    log_end: Int,
    s: Seq<i64>,
    v: Int,
    limit: Int,
    leader_counts: bool,
) -> Int {
    // Every step below unfolds `count_ge_prefix` once at `limit`, where the
    // last member always counts because `limit > 1` puts it past the leader.
    if limit <= 1 {
        0
    } else {
        let last_index = limit - 1;
        let last_member = hwm_member_at(log_end, s, last_index);
        let previous_count = count_ge_prefix(log_end, s, v, last_index, leader_counts);
        if last_member >= v {
            if previous_count >= 1 {
                let previous_index =
                    least_hwm_member_ge_index(log_end, s, v, last_index, leader_counts);
                let previous_member = hwm_member_at(log_end, s, previous_index);
                if last_member <= previous_member {
                    // The last member is the smaller witness. Monotonicity
                    // carries the earlier witness's count down to it.
                    lemma_count_ge_prefix_monotone(
                        log_end,
                        s,
                        last_member,
                        previous_member,
                        last_index,
                        leader_counts,
                    );
                    last_index
                } else {
                    // The earlier witness stays smaller; the last member
                    // reaches it, so both counts gain exactly one at `limit`.
                    previous_index
                }
            } else {
                // No earlier member reaches `v`, so `v`'s count at `limit` is
                // exactly the last member's one, and the last member's own
                // count is at least that.
                lemma_count_ge_prefix_nonnegative(log_end, s, v, last_index, leader_counts);
                lemma_count_ge_prefix_nonnegative(
                    log_end,
                    s,
                    last_member,
                    last_index,
                    leader_counts,
                );
                last_index
            }
        } else {
            // The last member misses `v`, so the witness is an earlier one.
            least_hwm_member_ge_index(log_end, s, v, last_index, leader_counts)
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[requires(valid_hwm_majority(majority@, s@.len()))]
#[ensures(forall<v: Int> count_ge(log_end@, s@, v, leader_counts) >= majority@
    ==> exists<k: Int> hwm_member_witness(log_end@, s@, k, v, majority@, leader_counts))]
pub fn lemma_hwm_threshold_has_member(
    log_end: i64,
    s: &[i64],
    majority: usize,
    leader_counts: bool,
) {
    proof_assert!(forall<v: Int> count_ge(log_end@, s@, v, leader_counts) >= majority@ ==>
        exists<k: Int> k == least_hwm_member_ge_index(log_end@, s@, v, s@.len() + 1, leader_counts)
            && hwm_member_witness(log_end@, s@, k, v, majority@, leader_counts));
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[requires(valid_hwm_majority(majority@, s@.len()))]
#[requires(forall<k: Int> 0 <= k && k < s@.len() + 1
    && count_ge(log_end@, s@, hwm_member_at(log_end@, s@, k), leader_counts) >= majority@
    ==> hwm_member_at(log_end@, s@, k) <= best@)]
#[ensures(forall<v: Int> count_ge(log_end@, s@, v, leader_counts) >= majority@ ==> v <= best@)]
pub fn lemma_hwm_member_maximal(
    log_end: i64,
    s: &[i64],
    majority: usize,
    best: i64,
    leader_counts: bool,
) {
    lemma_hwm_threshold_has_member(log_end, s, majority, leader_counts);
}

/// Deterministic per-`(node, epoch)` election-timeout jitter in `[0, base_ms)`.
///
/// This is Raft's randomized backoff, made reproducible for the deterministic
/// sims: the jitter is a fixed hash of the node ID and the epoch, so the same
/// run replays the same way. The contract proves only the range, `0` for a
/// zero base and below `base_ms` otherwise. It does not prove that two nodes,
/// or two epochs of one node, get different values; the hash usually spreads
/// them, and the unit tests pin a few spread values, but a collision is
/// possible and nothing here rules it out.
#[ensures(base_ms@ == 0 ==> result@ == 0)]
#[ensures(base_ms@ > 0 ==> result@ < base_ms@)]
#[must_use]
pub fn election_jitter_ms(me: u64, epoch: u32, base_ms: u64) -> u64 {
    if base_ms == 0 {
        return 0;
    }
    // Cheap integer hash of (node id, epoch); avoids any RNG so the sims stay
    // deterministic.
    let mix = me.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ u64::from(epoch).wrapping_mul(0xD1B5_4A32_D192_ED03);
    mix % base_ms
}

/// `true` if the candidate's log is at least as up-to-date as ours.
///
/// KIP-595: the higher last epoch wins. On a tie, the higher or equal offset
/// wins.
#[ensures(result == (cand_epoch@ > my_epoch@
    || (cand_epoch@ == my_epoch@ && cand_offset@ >= my_end@)))]
#[must_use]
pub const fn log_is_up_to_date(
    my_epoch: u32,
    my_end: i64,
    cand_epoch: u32,
    cand_offset: i64,
) -> bool {
    cand_epoch > my_epoch || (cand_epoch == my_epoch && cand_offset >= my_end)
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// This actual high-watermark member reaches the threshold with a majority.
fn hwm_member_witness(
    end: Int,
    offsets: Seq<i64>,
    index: Int,
    value: Int,
    majority: Int,
    leader_counts: bool,
) -> bool {
    pearlite! { 0 <= index && index < offsets.len() + 1 && hwm_member_at(end, offsets, index) >= value
    && count_ge(end, offsets, hwm_member_at(end, offsets, index), leader_counts) >= majority }
}
