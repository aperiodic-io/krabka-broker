//! Reading the broker's audit topic back out over the Kafka wire.
//!
//! Both helpers here fetch `AUDIT_TOPIC` partition 0 from offset zero and
//! decode the record batches, one into the `seq` header of every
//! non-checkpoint record and the other into the JSON body of every record.
//! They live beside the polling wrappers in the parent module rather than in
//! it, because between them they are the only code in `support` that speaks
//! `FetchRequest` directly.

use krabka_broker::coordinator::AUDIT_TOPIC;
use krabka_protocol::{owned::fetch_response::FetchResponse, records::Record};

use crate::support::fetch::single_partition_fetch;

/// Fetch the audit topic and return the `seq` header value (parsed as `u64`)
/// from each non-checkpoint record, in order.
pub async fn audit_record_seqs(client: &krabka_client_core::Client) -> Vec<u64> {
    let fr = fetch_audit(client).await;

    let mut seqs = Vec::new();
    for rec in audit_records(&fr) {
        // Skip checkpoint records — they have no `seq` header.
        let is_checkpoint = rec
            .headers
            .iter()
            .any(|h| h.key == "event_class" && h.value.as_deref() == Some(b"checkpoint"));
        if is_checkpoint {
            continue;
        }
        if let Some(seq_val) = rec
            .headers
            .iter()
            .find(|h| h.key == "seq")
            .and_then(|h| h.value.as_ref())
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|s| s.parse::<u64>().ok())
        {
            seqs.push(seq_val);
        }
    }
    seqs
}

pub async fn consume_audit_records(client: &krabka_client_core::Client) -> Vec<serde_json::Value> {
    let fr = fetch_audit(client).await;

    let mut records = Vec::new();
    for rec in audit_records(&fr) {
        if let Some(value) = &rec.value
            && let Ok(j) = serde_json::from_slice::<serde_json::Value>(value)
        {
            records.push(j);
        }
    }
    records
}

async fn fetch_audit(
    client: &krabka_client_core::Client,
) -> krabka_protocol::owned::fetch_response::FetchResponse {
    let topic_id = super::topic_id_for(client, AUDIT_TOPIC).await;
    client
        .send(single_partition_fetch(
            crate::support::fetch::SinglePartitionFetchSetup {
                topic: AUDIT_TOPIC.into(),
                topic_id,
                ..Default::default()
            },
        ))
        .await
        .expect("FetchRequest for audit topic")
}

fn audit_records(response: &FetchResponse) -> impl Iterator<Item = &Record> {
    response
        .responses
        .first()
        .and_then(|topic| topic.partitions.first())
        .and_then(|partition| partition.records.as_ref())
        .and_then(|records| records.as_v2())
        .into_iter()
        .flatten()
        .flat_map(|batch| &batch.records)
}
