//! The session handler's contract: the first request is full and opens a
//! session, later requests carry only what changed, a partition the follower
//! dropped is forgotten once, and a leader that refuses the session sends the
//! follower back to a full request.

use assert2::check;

use super::*;

fn key(topic: &str, partition: i32) -> SessionKey {
    SessionKey {
        topic: topic.to_string(),
        topic_id: WireUuid([7; 16]),
        partition,
    }
}

fn row(fetch_offset: i64) -> FetchPartition {
    FetchPartition {
        fetch_offset,
        ..FetchPartition::default()
    }
}

fn established_session(rows: &[(&str, i32, i64)]) -> FollowerFetchSession {
    let mut session = FollowerFetchSession::default();
    session.build(wanted(rows));
    check!(session.handle_response(codes::NONE, 42) == SessionOutcome::Usable);
    session
}

fn wanted(rows: &[(&str, i32, i64)]) -> WantedRows {
    rows.iter()
        .map(|(topic, partition, offset)| (key(topic, *partition), row(*offset)))
        .collect()
}

/// The partitions one request names, flattened for comparison.
fn sent_rows(request: &SessionRequest) -> Vec<(String, i32, i64)> {
    request
        .topics
        .iter()
        .flat_map(|topic| {
            topic.partitions.iter().map(move |partition| {
                (
                    topic.topic.clone(),
                    partition.partition,
                    partition.fetch_offset,
                )
            })
        })
        .collect()
}

#[test]
fn the_first_request_names_every_partition_and_asks_for_a_session() {
    let mut session = FollowerFetchSession::default();

    let request = session.build(wanted(&[("a", 0, 10), ("a", 1, 20), ("b", 0, 30)]));

    check!(request.session_id == INVALID_SESSION_ID);
    check!(request.session_epoch == INITIAL_EPOCH);
    check!(
        sent_rows(&request)
            == vec![
                ("a".to_string(), 0, 10),
                ("a".to_string(), 1, 20),
                ("b".to_string(), 0, 30),
            ]
    );
    check!(request.forgotten_topics_data.is_empty());
    // One topic per name, not one per partition.
    check!(request.topics.len() == 2);
}

#[test]
fn a_later_request_carries_only_the_partitions_whose_offset_moved() {
    let mut session = established_session(&[("a", 0, 10), ("a", 1, 20)]);

    // Partition 1 appended; partition 0 is caught up and unchanged.
    let request = session.build(wanted(&[("a", 0, 10), ("a", 1, 25)]));

    check!(request.session_id == 42);
    check!(request.session_epoch == 1);
    check!(sent_rows(&request) == vec![("a".to_string(), 1, 25)]);
    check!(request.forgotten_topics_data.is_empty());
}

#[test]
fn a_round_that_changed_nothing_sends_no_partition_rows_at_all() {
    let mut session = established_session(&[("a", 0, 10)]);

    let request = session.build(wanted(&[("a", 0, 10)]));

    check!(request.topics.is_empty());
    check!(request.session_id == 42);
}

#[test]
fn a_partition_this_follower_stopped_following_is_forgotten_once() {
    let mut session = established_session(&[("a", 0, 10), ("a", 1, 20), ("b", 0, 30)]);

    let dropped = session.build(wanted(&[("a", 0, 10)]));
    let after = session.build(wanted(&[("a", 0, 10)]));

    check!(dropped.forgotten_topics_data.len() == 2);
    check!(
        dropped
            .forgotten_topics_data
            .iter()
            .map(|topic| (topic.topic.clone(), topic.partitions.clone()))
            .collect::<Vec<_>>()
            == vec![("a".to_string(), vec![1]), ("b".to_string(), vec![0]),]
    );
    // Forgotten is a delta, not a standing list.
    check!(after.forgotten_topics_data.is_empty());
}

#[test]
fn a_partition_the_follower_added_is_sent_even_when_the_round_is_incremental() {
    let mut session = established_session(&[("a", 0, 10)]);

    let request = session.build(wanted(&[("a", 0, 10), ("a", 1, 0)]));

    check!(sent_rows(&request) == vec![("a".to_string(), 1, 0)]);
}

#[test]
fn the_epoch_advances_once_per_answered_round() {
    let mut session = FollowerFetchSession::default();
    let mut epochs = Vec::new();
    for offset in 0..4 {
        let request = session.build(wanted(&[("a", 0, offset)]));
        epochs.push(request.session_epoch);
        session.handle_response(codes::NONE, 42);
    }

    check!(epochs == vec![INITIAL_EPOCH, 1, 2, 3]);
}

#[test]
fn a_refused_session_drops_the_round_and_makes_the_next_request_full() {
    for refusal in [
        codes::FETCH_SESSION_ID_NOT_FOUND,
        codes::INVALID_FETCH_SESSION_EPOCH,
    ] {
        let mut session = established_session(&[("a", 0, 10), ("a", 1, 20)]);
        session.build(wanted(&[("a", 0, 11), ("a", 1, 20)]));

        check!(session.handle_response(refusal, 0) == SessionOutcome::SessionLost);
        check!(session.is_full());

        let recovered = session.build(wanted(&[("a", 0, 11), ("a", 1, 20)]));
        check!(recovered.session_id == INVALID_SESSION_ID);
        check!(recovered.session_epoch == INITIAL_EPOCH);
        check!(sent_rows(&recovered) == vec![("a".to_string(), 0, 11), ("a".to_string(), 1, 20)]);
    }
}

#[test]
fn a_leader_that_grants_no_session_keeps_every_request_full() {
    let mut session = FollowerFetchSession::default();
    session.build(wanted(&[("a", 0, 10)]));

    check!(session.handle_response(codes::NONE, INVALID_SESSION_ID) == SessionOutcome::Usable);
    check!(session.is_full());

    let request = session.build(wanted(&[("a", 0, 10)]));
    check!(request.session_epoch == INITIAL_EPOCH);
    check!(sent_rows(&request) == vec![("a".to_string(), 0, 10)]);
}

#[test]
fn a_reset_after_a_lost_request_re_sends_everything() {
    let mut session = established_session(&[("a", 0, 10), ("a", 1, 20)]);

    // The next request never reached the leader, so what it holds is unknown.
    session.reset();

    let request = session.build(wanted(&[("a", 0, 10), ("a", 1, 20)]));
    check!(request.session_epoch == INITIAL_EPOCH);
    check!(sent_rows(&request).len() == 2);
}

/// A per-partition error is the leader answering about that partition, not
/// refusing the session, so the round is applied and the session survives.
#[test]
fn a_top_level_error_that_is_not_a_session_error_leaves_the_session_alone() {
    let mut session = established_session(&[("a", 0, 10)]);
    session.build(wanted(&[("a", 0, 11)]));

    check!(session.handle_response(codes::UNKNOWN_SERVER_ERROR, 42) == SessionOutcome::Usable);
    check!(!session.is_full());
}
