//! The `ShareFetch` and `ShareAcknowledge` calls every consume test in this
//! binary drives, in one place. It builds a single-partition fetch request at
//! a given share-session epoch, sends a per-offset acknowledgement, sends a
//! renew-ack that extends a lock without changing record state, and retries
//! the first fetch until the acquire pass returns records.

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::owned::share_acknowledge_response::ShareAcknowledgeResponse;

pub use crate::support::share::{acquired_count, share_fetch_req};
use crate::{
    NONE, RENEW,
    harness::wire,
    support::share::{
        acknowledge_partition, acknowledge_request, acknowledge_topic, acknowledgement,
    },
};

/// `ShareFetch`. This helper retries while the share-state leadership and
/// acquisition are still settling. The first acquire pass after topic creation
/// can briefly find the `__share_group_state` partition still materializing, so
/// this helper mirrors the retry-on-not-ready loop in `share_state.rs`. Returns
/// the (single) partition row.
pub async fn share_fetch(
    client: &Client,
    setup: crate::support::share::ShareFetchSetup<'_>,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    crate::support::share::fetch_row(
        client,
        share_fetch_req(setup),
        crate::support::share::FetchSessionMode::Incremental,
    )
    .await
}

#[derive(Clone, Copy)]
pub struct AcquiredRecordCount(pub i64);

pub async fn fetch_count(
    client: &Client,
    setup: crate::support::share::ShareFetchSetup<'_>,
    expected: AcquiredRecordCount,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    let row = share_fetch(client, setup).await;
    assert!(
        acquired_count(&row) == expected.0,
        "unexpected acquisition: {row:?}"
    );
    row
}

pub async fn acquire_count(
    client: &Client,
    session: crate::support::share::ShareSessionSetup<'_>,
    expected: AcquiredRecordCount,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    let row = fetch_until_acquired(
        client,
        session.with_epoch(crate::support::share::ShareSessionEpoch(0)),
    )
    .await;
    assert!(
        acquired_count(&row) == expected.0,
        "unexpected initial acquisition: {row:?}"
    );
    row
}

pub async fn acquire_three_and_acknowledge<'a>(
    client: &Client,
    setup: crate::support::share::ShareAck<'a>,
) -> crate::support::share::ShareSessionSetup<'a> {
    acquire_count(client, setup.session, AcquiredRecordCount(3)).await;
    crate::support::share::acknowledge_success(client, setup).await;
    setup.session
}

pub async fn fetch_empty(
    client: &Client,
    setup: crate::support::share::ShareFetchSetup<'_>,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    fetch_count(client, setup, AcquiredRecordCount(0)).await
}

pub async fn fetch_redelivered(client: &Client, setup: crate::support::share::ShareFetchSetup<'_>) {
    let row = fetch_count(client, setup, AcquiredRecordCount(1)).await;
    assert!(
        row.acquired_records[0].delivery_count == 2,
        "unexpected redelivery: {row:?}"
    );
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct ShareRenewSetup<'a> {
    pub session: crate::support::share::ShareSessionSetup<'a>,
    pub first: krabka_ids::Offset,
    pub last: krabka_ids::Offset,
}

/// A renew-ack `ShareAcknowledge` (`is_renew_ack = true`) over `[first, last]`
/// with *empty* ack types. The broker renew path extends each batch's lock
/// without changing record state. Returns the partition row.
pub async fn share_renew(
    client: &Client,
    setup: ShareRenewSetup<'_>,
) -> krabka_protocol::owned::share_acknowledge_response::PartitionData {
    let req = acknowledge_request(crate::support::share::AcknowledgeRequestSetup {
        group_id: Some(setup.session.group.into()),
        member_id: Some(setup.session.member.into()),
        epoch: setup.session.epoch,
        mode: crate::support::share::RenewalMode::Renew,
        topics: vec![acknowledge_topic(
            wire(setup.session.topic_id),
            vec![acknowledge_partition(
                setup.session.partition.0,
                vec![acknowledgement(setup.first.0, setup.last.0, vec![RENEW])],
            )],
        )],
    });
    let resp: ShareAcknowledgeResponse = client.send(req).await.expect("ShareAcknowledge renew");
    assert!(
        resp.error_code == NONE,
        "ShareAcknowledge(renew) top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

// Do the very first `ShareFetch` for a freshly-created topic. This helper
// retries until the acquire pass actually returns records. Leadership and
// materialization of both the data partition and `__share_group_state` may
// still be settling. Asserts the supplied invariant on the resulting row.
crate::share_first_fetch_fixture!(fetch_until_acquired, Incremental);
