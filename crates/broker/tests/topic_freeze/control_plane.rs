//! The two RPCs that carry the freeze registry, and the waits a case needs
//! before it reads the result of one.
//!
//! `SetTopicFreeze` reports a refusal in its own `error_code` and never as a
//! transport failure, so [`set_freeze`] hands the whole response back and lets
//! the case read one shape whatever the outcome.
//! [`wait_for_registry_len`] polls `DescribeTopicFreezes` rather than sleeping:
//! that handler and the produce gate both read the controller's current image,
//! so a registry that answers over the wire is a produce path that answers too.

use {
    assert2::assert,
    krabka_broker::codes,
    krabka_client_core::Client,
    krabka_protocol::{
        krabka::freeze::{
            DescribeTopicFreezesRequest, DescribedTopicFreeze, SetTopicFreezeRequest,
            SetTopicFreezeResponse,
        },
        owned::describe_cluster_request::DescribeClusterRequest,
    },
    std::time::{Duration, Instant},
};

/// How long a wire-visible state change gets before a case gives up on it.
const SETTLE: Duration = Duration::from_secs(10);

/// Send one `SetTopicFreeze` (api key 1015) and hand back the whole response.
///
/// A refusal rides the response's own `error_code`, never a transport failure,
/// so every case reads one shape whatever the outcome.
pub(super) async fn set_freeze(
    client: &Client,
    request: SetTopicFreezeRequest,
) -> SetTopicFreezeResponse {
    client.send(request).await.expect("SetTopicFreeze")
}

/// The unsigned freeze an operator reaches for in one command.
pub(super) fn freeze_request(pattern_type: i8, scope: &str, reason: &str) -> SetTopicFreezeRequest {
    SetTopicFreezeRequest {
        scope: scope.to_owned(),
        pattern_type,
        frozen: true,
        reason: reason.to_owned(),
        ..SetTopicFreezeRequest::default()
    }
}

/// Freeze `scope`, assert the broker took it, and wait until the registry the
/// wire serves shows it.
///
/// The wait is on `DescribeTopicFreezes` rather than on a sleep. Both that
/// handler and the produce gate read the controller's current image, so a
/// registry that answers over the wire is a produce path that answers too.
pub(super) async fn freeze_scope(client: &Client, pattern_type: i8, scope: &str, reason: &str) {
    let before = describe_freezes(client).await.len();
    let response = set_freeze(client, freeze_request(pattern_type, scope, reason)).await;
    assert!(
        response.error_code == codes::NONE,
        "freeze {scope}: {response:?}"
    );
    wait_for_registry_len(client, before + 1).await;
}

/// Read the whole registry through `DescribeTopicFreezes` (api key 1016).
async fn describe_freezes(client: &Client) -> Vec<DescribedTopicFreeze> {
    let response = client
        .send(DescribeTopicFreezesRequest::default())
        .await
        .expect("DescribeTopicFreezes");
    assert!(
        response.error_code == codes::NONE,
        "DescribeTopicFreezes: {response:?}"
    );
    response.freezes
}

/// Wait until the registry holds exactly `want` entries, and return them.
pub(super) async fn wait_for_registry_len(
    client: &Client,
    want: usize,
) -> Vec<DescribedTopicFreeze> {
    let deadline = Instant::now() + SETTLE;
    loop {
        let entries = describe_freezes(client).await;
        if entries.len() == want {
            return entries;
        }
        assert!(
            Instant::now() < deadline,
            "the freeze registry never reached {want} entries; it holds {entries:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The cluster id, read the way `krabka-guard` reads it before it signs.
///
/// It is inside the signed bytes, which is what stops a signature made for one
/// cluster from being replayed into another.
pub(super) async fn cluster_id(client: &Client) -> String {
    let response = client
        .send(DescribeClusterRequest::default())
        .await
        .expect("DescribeCluster");
    assert!(
        response.error_code == codes::NONE,
        "DescribeCluster: {response:?}"
    );
    response.cluster_id
}
