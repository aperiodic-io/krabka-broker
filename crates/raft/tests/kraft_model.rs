//! Exhaustive stateright checks of the `KRaft` consensus core. See
//! `model/mod.rs`.
//!
//! Memory safety: the stateright BFS keeps every visited unique state resident.
//! Each checker run is therefore fenced with a `target_state_count` backstop,
//! on top of the model's tight `within_boundary`, so a runaway space cannot
//! exhaust the host RAM.
//!
//! The configs differ in their bounds because the linearizability tester keeps
//! its history in the fingerprinted state and so blows the space up by about
//! 30x wherever client appends are enabled:
//! - `three_voters_election_safety`: 3 voters and NO client appends. This covers
//!   election and log-matching safety over the small, fast space.
//! - `two_voters_linearizable`: 2 voters with client appends. This covers
//!   committed-log linearizability over a tightly-bounded space.
//! - `three_voters_faults`: 3 voters, no appends, message loss and duplication,
//!   and one crash at a time.
//! - `three_voters_append`: 3 voters WITH client appends and one crash at a
//!   time, which is the only config in which a committed entry can rest on a
//!   bare majority while the third voter falls behind. That is what makes the
//!   KIP-595 log-recency test in `handle_vote_request` observable: without it a
//!   stale voter wins an election and `leader_completeness` fails.
//! - `two_voters_append_via_linearizable`: 2 voters and stateless appenders.
mod model;

use {krabka_ids::NodeId, model::ConsensusModel, stateright::Checker};

/// Hard backstop on the explored, that is generated, states. It bounds memory
/// even if `within_boundary` is looser than intended. It is set well above the
/// true bounded count of each config, so it never truncates a real check. Such
/// a truncation would spuriously fail a `sometimes` witness, or leave an
/// `always` only partially verified.
const MAX_STATES: usize = 8_000_000;
/// Depth backstop. It must exceed the reachable-graph diameter of each config,
/// or the search is depth-truncated and therefore incomplete. The configs below
/// are bounded, so their diameter sits well under this value.
const MAX_DEPTH: usize = 60;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
//
// Two configs grew when the model began to record, for one transition, that a
// vote was refused for log recency alone (`three_voters_append` only) and that
// the production `TruncateTo` cut a divergent log (both crash configs): 779,078
// and 445,169 before. Replicating only onto a log the leader's extends, rather
// than overwriting any shorter log, left every count unchanged: within these
// bounds no follower is ever shorter than the leader and divergent when it
// fetches, so the overwrite had never fired.
//
// The same two shrank, from 779,582 and 457,697, when the production
// `handle_fetch_response` began to fetch again right after it truncates on a
// diverging epoch, as Kafka's follower does. The truncating step now also
// sends that fetch, which the model's replication abstraction answers at once:
// the truncated log takes the leader's records in the same transition, and the
// fetch replaces the node's in-flight one. The states in which a truncated
// voter sat with no fetch in flight, able to move on only through a timeout,
// are gone, and so are the states reachable only through them. The step
// cannot leave the in-flight bound: it consumes the response and adds at most
// one fetch.
//
// Every count moved again when vote and pre-vote began to follow
// `KafkaRaftClient` (#1243): a higher-epoch pre-vote now advances the epoch
// before the voter-key check, and a follower grants a pre-vote only once it has
// stopped fetching from its leader. The election-safety config lost states (a
// follower no longer grants while its leader is live) and the others gained the
// epoch advance. The `always` properties are unchanged and still hold.
//
// Three counts moved again, and the generated count of `three_voters_faults`
// passed the old 6M cap, when a replica that follows the leader of its epoch
// began to keep the vote it cast in that epoch, as Kafka's
// `QuorumState.transitionToFollower` does, and a `Prospective` replica began to
// remember the leader it abandoned. A replica that votes for one candidate and
// then follows another used to forget the vote, and so it could vote twice in
// one epoch after a fetch timeout. Every state in which a follower still holds
// its vote is new, so the three-voter configs grew by 40 to 80 percent (the
// cap rose from 6M to 8M with them). The two-voter configs never split a vote
// from a leader, and did not move. The `always` properties, election safety
// among them, still hold.
const PINNED_UNIQUE_STATES_THREE_VOTERS_ELECTION_SAFETY: usize = 10_750;
const PINNED_UNIQUE_STATES_TWO_VOTERS_LINEARIZABLE: usize = 46_521;
const PINNED_UNIQUE_STATES_THREE_VOTERS_FAULTS: usize = 1_128_704;
const PINNED_UNIQUE_STATES_THREE_VOTERS_APPEND: usize = 839_339;
const PINNED_UNIQUE_STATES_TWO_VOTERS_APPEND_VIA: usize = 256_973;

krabka_macros::bounded_bfs!(bounded_bfs);

fn run(model: ConsensusModel, label: &str, pinned_unique_states: usize) {
    // Both truncation caps must remain strictly above the completed search.
    let checker = bounded_bfs(model, label, MAX_DEPTH, MAX_STATES);
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

#[test]
fn three_voters_election_safety() {
    run(
        ConsensusModel::elections(&[NodeId(1), NodeId(2), NodeId(3)]),
        "three_voters_election_safety",
        PINNED_UNIQUE_STATES_THREE_VOTERS_ELECTION_SAFETY,
    );
}

#[test]
fn two_voters_linearizable() {
    run(
        ConsensusModel::linearizable(&[NodeId(1), NodeId(2)], 2),
        "two_voters_linearizable",
        PINNED_UNIQUE_STATES_TWO_VOTERS_LINEARIZABLE,
    );
}

#[test]
fn three_voters_faults() {
    // Election + log-matching safety under an adversarial network: message
    // loss, duplication, and a single crash/recover. 3 voters so a crash leaves
    // a majority that can still make progress.
    run(
        ConsensusModel::faults(&[NodeId(1), NodeId(2), NodeId(3)]),
        "three_voters_faults",
        PINNED_UNIQUE_STATES_THREE_VOTERS_FAULTS,
    );
}

#[test]
fn three_voters_append() {
    // Leader completeness under a stale majority: three voters, client appends,
    // and one crash at a time, so a committed prefix can live on a bare
    // majority while the crashed voter misses it. The `leader_completeness`
    // property then holds every elected leader to that prefix, and the
    // `stale_candidate_refused` witness proves the refusal is actually reached.
    run(
        ConsensusModel::three_voters_append(&[NodeId(1), NodeId(2), NodeId(3)], 1),
        "three_voters_append",
        PINNED_UNIQUE_STATES_THREE_VOTERS_APPEND,
    );
}

#[test]
fn two_voters_append_via_linearizable() {
    run(
        // The diskless linearizability leg deliberately uses the design's
        // exhaustive tiny bound: two voters, two stateless appenders, and two
        // appends. The separate crash model and 3-broker black-box gate cover
        // minority WAL-node loss.
        ConsensusModel::append_via(&[NodeId(1), NodeId(2)], 2),
        "two_voters_append_via_linearizable",
        PINNED_UNIQUE_STATES_TWO_VOTERS_APPEND_VIA,
    );
}
