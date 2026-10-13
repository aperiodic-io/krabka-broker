//! KFC-10 produce-time deduplication under adversarial and heavy load: the
//! inputs and operating conditions that could slow a broker down, crash it, or
//! grow its memory until the kernel kills it.
//!
//! The acceptance, recovery and bounds suites prove the contract on ordinary
//! traffic. This suite looks for the cliffs: a batch header that lies about
//! its record count, a small compressed request that decodes to millions of
//! records, a batch larger than the rebuild's read, an idle partition that
//! keeps its share of the broker budget, a truncation that removes nothing, a
//! rebuild of one partition next to produces to another, and a scheduled
//! topic whose delivery time a partial drop moves.
//!
//! A case that could abort or exhaust the test process runs in a child: the
//! test binary runs itself again with [`child::SCENARIO_ENV`] naming the
//! scenario, under `prlimit --as` where a cap matters, and reports what it
//! measured on standard output. A crash is then a failed test, not a dead
//! test runner. Memory is read from `/proc/self/status` in the child, whose
//! peak is reset just before the measured step.
//!
//! Each timed or measured case prints its numbers; run with
//! `cargo test -p krabka-broker --test dedup_stress -- --include-ignored
//! --nocapture` to see them. Cases marked `product bug` fail on the current
//! broker and stay ignored until the bug they name is fixed.

mod support;

#[path = "dedup_stress/amplification.rs"]
mod amplification;
#[path = "dedup_stress/child.rs"]
mod child;
#[path = "dedup_stress/expiry.rs"]
mod expiry;
#[path = "dedup_stress/malformed.rs"]
mod malformed;
#[path = "dedup_stress/rebuild.rs"]
mod rebuild;
#[path = "dedup_stress/scheduled.rs"]
mod scheduled;

use {
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::{
        owned::produce_response::PartitionProduceResponse, primitives::uuid::Uuid,
        records::RecordBatch,
    },
    support::dedup::{partition_metric, produce},
};

/// One partition of a topic, as Produce v13 and Fetch v13 address it.
#[derive(Clone, Copy, Debug)]
struct Target<'a> {
    topic: &'a str,
    topic_id: Uuid,
    partition: i32,
}

impl Target<'_> {
    /// Produce `batch` with `acks=all` and return the partition row.
    async fn send(self, client: &Client, batch: RecordBatch) -> PartitionProduceResponse {
        produce()
            .client(client)
            .topic(self.topic)
            .topic_id(self.topic_id)
            .partition(self.partition)
            .batch(batch)
            .call()
            .await
    }

    /// One per-partition KFC-10 series, 0 when it is absent.
    async fn metric(self, broker: &BrokerHandle, name: &str) -> u64 {
        whole(partition_metric(broker, name, self.topic, self.partition).await)
    }
}

/// A gauge or counter value. The broker sets these to whole numbers only.
fn whole(value: f64) -> u64 {
    value
        .to_string()
        .parse()
        .expect("a whole, non-negative metric value")
}
