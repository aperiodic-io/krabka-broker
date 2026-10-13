//! The share-session state machine that sits in front of the acquire path.
//! The epoch checks reject a stale epoch and a member with no live session,
//! and an incremental request with no topic rows keeps fetching the cached
//! partitions until the client forgets one.

use {
    crate::{
        INVALID_SHARE_SESSION_EPOCH, NONE, ONE_MB, SHARE_SESSION_NOT_FOUND,
        harness::{ConsumptionFixture, join, produce_n, single_record_consumption_fixture, wire},
        share_rpc::{acquired_count, fetch_until_acquired, share_fetch_req},
        support::share::{
            ShareAck, ShareFetchSetup, ShareSessionEpoch, ShareSessionSetup, acknowledge_success,
            empty_session_request,
        },
    },
    assert2::assert,
    krabka_protocol::owned::{
        share_fetch_request::ForgottenTopic, share_fetch_response::ShareFetchResponse,
    },
};

/// The share-session epoch state machine rejects stale and unknown epochs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_epoch_validation() {
    let ConsumptionFixture {
        _permit,
        broker: _broker,
        client,
        _dir,
        tid,
        member,
    } = single_record_consumption_fixture().await;

    // Open (epoch 0) succeeds: top-level error_code 0.
    let opened: ShareFetchResponse = client
        .send(
            share_fetch_req()
                .session(ShareFetchSetup::from(ShareSessionSetup::opening(&member, tid)).session)
                .max_wait(ShareFetchSetup::from(ShareSessionSetup::opening(&member, tid)).max_wait)
                .acks(ShareFetchSetup::from(ShareSessionSetup::opening(&member, tid)).acks)
                .call(),
        )
        .await
        .expect("ShareFetch open");
    assert!(
        opened.error_code == NONE,
        "open (epoch 0) must succeed, got {}",
        opened.error_code
    );

    // The stored epoch is now 1. A non-matching positive epoch (say 9) →
    // INVALID_SHARE_SESSION_EPOCH (123) at the top level.
    let stale: ShareFetchResponse = client
        .send(
            share_fetch_req()
                .session(ShareFetchSetup::at_epoch(&member, tid, ShareSessionEpoch(9)).session)
                .max_wait(ShareFetchSetup::at_epoch(&member, tid, ShareSessionEpoch(9)).max_wait)
                .acks(ShareFetchSetup::at_epoch(&member, tid, ShareSessionEpoch(9)).acks)
                .call(),
        )
        .await
        .expect("ShareFetch stale");
    assert!(
        stale.error_code == INVALID_SHARE_SESSION_EPOCH,
        "stale epoch must be 123 (INVALID_SHARE_SESSION_EPOCH), got {}",
        stale.error_code
    );

    // A member with no live session sending a non-zero epoch →
    // SHARE_SESSION_NOT_FOUND (122).
    let (ghost, _) = join(&client, "g1", "t").await;
    let not_found: ShareFetchResponse = client
        .send(
            share_fetch_req()
                .session(ShareFetchSetup::at_epoch(&ghost, tid, ShareSessionEpoch(5)).session)
                .max_wait(ShareFetchSetup::at_epoch(&ghost, tid, ShareSessionEpoch(5)).max_wait)
                .acks(ShareFetchSetup::at_epoch(&ghost, tid, ShareSessionEpoch(5)).acks)
                .call(),
        )
        .await
        .expect("ShareFetch unknown session");
    assert!(
        not_found.error_code == SHARE_SESSION_NOT_FOUND,
        "unknown session must be 122 (SHARE_SESSION_NOT_FOUND), got {}",
        not_found.error_code
    );
}

/// An incremental request with no topic rows continues to fetch every cached
/// partition. A forgotten partition is removed from the session and stays
/// removed on later empty incremental requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_session_uses_cached_and_forgotten_partitions() {
    let ConsumptionFixture {
        _permit,
        broker: _broker,
        client,
        _dir,
        tid,
        member,
    } = single_record_consumption_fixture().await;

    let first = fetch_until_acquired(&client, ShareSessionSetup::opening(&member, tid)).await;
    assert!(acquired_count(&first) == 1);
    acknowledge_success(
        &client,
        ShareAck::prefix_for(&member, tid, krabka_ids::Offset(0)),
    )
    .await;

    produce_n().client(&client).topic_id(tid).call().await;
    let cached: ShareFetchResponse = client
        .send(
            empty_session_request()
                .member(member.clone())
                .epoch(2)
                .max_bytes(ONE_MB)
                .call(),
        )
        .await
        .expect("incremental ShareFetch");
    assert!(cached.error_code == NONE);
    assert!(cached.responses.len() == 1);
    let cached_partition = &cached.responses[0].partitions[0];
    assert!(cached_partition.partition_index == 0);
    assert!(acquired_count(cached_partition) == 1);
    acknowledge_success(
        &client,
        ShareAck::prefix()
            .member(&member)
            .topic_id(tid)
            .epoch(3)
            .last(1)
            .call()
            .starting_at(krabka_ids::Offset(1)),
    )
    .await;

    let forgotten: ShareFetchResponse = client
        .send(
            empty_session_request()
                .member(member.clone())
                .epoch(4)
                .max_bytes(ONE_MB)
                .forgotten_topics_data(vec![ForgottenTopic {
                    topic_id: wire(tid),
                    partitions: vec![0],
                    ..Default::default()
                }])
                .call(),
        )
        .await
        .expect("forget partition");
    assert!(forgotten.error_code == NONE);
    assert!(forgotten.responses.is_empty());

    produce_n().client(&client).topic_id(tid).call().await;
    let after_forget: ShareFetchResponse = client
        .send(
            empty_session_request()
                .member(member)
                .epoch(5)
                .max_bytes(ONE_MB)
                .call(),
        )
        .await
        .expect("ShareFetch after forget");
    assert!(after_forget.error_code == NONE);
    assert!(after_forget.responses.is_empty());
}
