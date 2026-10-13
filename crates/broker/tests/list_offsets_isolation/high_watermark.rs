//! `LATEST` against a partition whose tail the ISR has not acknowledged.
//!
//! The bound this suite is about is not only a transactional one. A leader
//! whose follower is behind holds records that are durable locally and that no
//! `Fetch` will serve, and the answer to `LATEST` has to stop at the high
//! watermark for both isolation levels rather than at the log end offset. This
//! module is the case that holds the watermark back on purpose and reads the
//! partition's end on either side of that.

use {
    crate::{
        support,
        wire::{
            EndOfPartition, create_topic, end_of_partition, latest_row, send_ok,
            wait_for_settled_log,
        },
    },
    assert2::check,
    krabka_client_producer::Producer,
};

/// `LATEST` under `read_uncommitted` answers the high watermark, not the log
/// end offset, so a consumer cannot seek past what the ISR acknowledged.
///
/// This is the older of the two divergences the bound closed, and the one with
/// the wider reach: it applies to every consumer on every topic, transactions
/// or not. A leader whose follower is behind holds records that are durable
/// locally and that no `Fetch` will serve. Answering `LATEST` from the log end
/// offset hands a plain `read_uncommitted` consumer a position inside that
/// unreplicated tail, where it waits for records that may yet be truncated away
/// by a leader election.
///
/// Both isolation levels are asked, because the last stable offset is capped at
/// the high watermark too -- a `read_committed` client must not read past it
/// either, and a bound that fenced only the transactional path would leave the
/// larger hole open.
/// How often the watermark is held and read before the test gives up on a
/// reading no reconcile interrupted.
const HOLD_ATTEMPTS: usize = 20;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn latest_answers_the_high_watermark_not_the_unreplicated_log_end() {
    const TOPIC: &str = "list-offsets-isolation-unreplicated";

    let p = support::start().await;
    let producer = Producer::builder()
        .bootstrap(p.broker.listen_addr().to_string())
        .build()
        .await
        .expect("producer build");

    create_topic(&p.client, TOPIC).await;
    p.broker.wait_until_partition_present(TOPIC, 0).await;
    for value in ["a", "b", "c", "d"] {
        send_ok(&producer, TOPIC, value).await;
    }
    wait_for_settled_log(&p.broker, TOPIC, 4).await;

    // The control. Every record is acknowledged, so the high watermark and the
    // log end offset are the same number and no bound can be told from the
    // other. Without this reading a broker that answered a constant 2 below
    // would look correct.
    check!(
        end_of_partition(&p.client, TOPIC).await
            == EndOfPartition {
                read_uncommitted: latest_row(4),
                read_committed: latest_row(4),
            },
        "a fully replicated partition ends at its log end offset"
    );

    // Now the last two records are unreplicated: durable on the leader, not yet
    // acknowledged by the ISR, exactly as they are while a follower catches up.
    // A replicator reconcile can raise the watermark back to the log end at any
    // moment, so the reading counts only when the hold is still in place after
    // both isolation levels were asked; only the hold lowers the watermark, so
    // then both answers were given under it.
    let mut held = None;
    for _ in 0..HOLD_ATTEMPTS {
        check!(
            p.broker.hold_high_watermark_for_test(TOPIC, 0, 2).await,
            "the partition is hosted here"
        );
        let reading = end_of_partition(&p.client, TOPIC).await;
        if p.broker.high_watermark_for_test(TOPIC, 0).await == Some(2) {
            held = Some(reading);
            break;
        }
    }
    check!(
        held == Some(EndOfPartition {
            read_uncommitted: latest_row(2),
            read_committed: latest_row(2),
        }),
        "neither isolation level may seek into the unreplicated tail"
    );

    producer.close().await.expect("producer close");
    p.broker.shutdown().await;
}
