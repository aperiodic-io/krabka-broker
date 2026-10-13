//! A proposal past its lifetime, on the approve path and on the consume path.
//!
//! `proposal_ttl` is the only bound on an approval given by a principal who
//! has since left the approver set, because the broker does not re-check that
//! set when it spends one. The case waits the window out on a real clock, the
//! same clock the broker stamps the proposal against.

use {
    crate::{
        cluster::{ClusterClients, boot, client_fixture},
        principals::{ALICE, BOB, CAROL},
        proposals::{ACTION_DELETE_TOPIC, approve, now_ms, propose, stored},
        topics::{delete_topic, topic_exists},
    },
    assert2::{assert, check},
    krabka_broker::codes,
    std::time::Duration,
};

/// Sleep until the wall clock is past `expires_at_ms`.
///
/// A real sleep, because the thing under test is a wall-clock expiry that the
/// broker reads from its own clock. There is no image change and no metric to
/// await: the proposal does not move, the clock does.
async fn sleep_past(expires_at_ms: i64) {
    let remaining = expires_at_ms.saturating_sub(now_ms()).max(0);
    let wait = u64::try_from(remaining).expect("a non-negative remainder") + 400;
    tokio::time::sleep(Duration::from_millis(wait)).await;
}

/// A proposal past its lifetime authorizes nothing, on either end.
///
/// `proposal_ttl` is the whole safety bound on an approver who was removed from
/// the set: the design says explicitly that the broker does not re-check the
/// approver set when it spends an approval, and that waiting out the lifetime
/// is what kills a pending approval by a person who has since gone. If an
/// expired proposal could still be approved, or a fully approved one could
/// still be spent after its expiry, that bound would not exist and a removed
/// approver's agreement would stay live for as long as the proposal sat in the
/// image.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_proposal_authorizes_nothing() {
    let ClusterClients {
        cluster,
        clients: [alice, bob, carol],
    } = client_fixture(boot().await, [ALICE, BOB, CAROL], "doomed", 1).await;

    // The approve path. Nobody approved it before it ran out.
    let short = propose(&alice, ACTION_DELETE_TOPIC, "doomed", 500).await;
    assert!(short.error_code == codes::NONE, "{short:?}");
    sleep_past(short.expires_at_ms).await;
    let late = approve(&bob, short.proposal_id).await;
    check!(late.error_code == codes::POLICY_VIOLATION, "{late:?}");
    check!(
        late.error_message
            .as_deref()
            .is_some_and(|message| message.contains("expired")),
        "{late:?}"
    );

    // The consume path. Two people agreed in time, and the window closed
    // before anybody spent the agreement.
    let ready = propose(&alice, ACTION_DELETE_TOPIC, "doomed", 3_000).await;
    assert!(ready.error_code == codes::NONE, "{ready:?}");
    check!(approve(&bob, ready.proposal_id).await.error_code == codes::NONE);
    check!(approve(&carol, ready.proposal_id).await.error_code == codes::NONE);
    check!(
        stored(&alice, ready.proposal_id).await.approvals.len() == 2,
        "both approvals landed inside the window"
    );

    sleep_past(ready.expires_at_ms).await;
    check!(delete_topic(&alice, "doomed").await == codes::POLICY_VIOLATION);
    check!(topic_exists(&alice, "doomed").await, "the topic survives");
    check!(
        stored(&alice, ready.proposal_id).await.consumed_at_ms == 0,
        "a refused transition spends nothing"
    );

    cluster.broker.shutdown().await;
}
