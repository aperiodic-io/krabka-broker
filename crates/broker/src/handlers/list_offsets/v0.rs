//! The legacy `ListOffsets` v0 wire shape and segment-boundary lookup.

use bytes::{Bytes, BytesMut};
use krabka_protocol::primitives::{
    array::{get_array_len, put_array_len},
    fixed::{get_i32, get_i64, put_i16, put_i32, put_i64},
    string_bytes::{get_string_owned, put_string},
};

use super::{
    bound::{fetch_bound, last_fetchable_offset},
    leadership::resolve_for_broker,
    remote::concurrently,
    resolve::{LatestContext, resolve_latest},
};
use crate::{
    broker::Broker, codes, config_keys::resolve_remote_list_offsets_timeout, error::BrokerError,
    handlers::RequestContext,
};

const LATEST_TIMESTAMP: i64 = -1;

struct Request {
    replica_id: i32,
    topics: Vec<TopicRequest>,
}

struct TopicRequest {
    name: String,
    partitions: Vec<PartitionRequest>,
}

struct PartitionRequest {
    index: i32,
    timestamp: i64,
    max_num_offsets: i32,
}

struct TopicResponse {
    name: String,
    partitions: Vec<PartitionResponse>,
}

struct PartitionResponse {
    index: i32,
    error_code: i16,
    offsets: Vec<i64>,
}

fn error_response(index: i32, error_code: i16) -> PartitionResponse {
    PartitionResponse {
        index,
        error_code,
        offsets: Vec::new(),
    }
}

fn decode(mut bytes: &[u8]) -> Result<Request, BrokerError> {
    let replica_id = get_i32(&mut bytes)?;
    let topic_count = get_array_len(&mut bytes, false)?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = get_string_owned(&mut bytes)?;
        let partition_count = get_array_len(&mut bytes, false)?;
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(PartitionRequest {
                index: get_i32(&mut bytes)?,
                timestamp: get_i64(&mut bytes)?,
                max_num_offsets: get_i32(&mut bytes)?,
            });
        }
        topics.push(TopicRequest { name, partitions });
    }
    Ok(Request { replica_id, topics })
}

fn encode(topics: &[TopicResponse]) -> Bytes {
    let mut buf = BytesMut::new();
    put_array_len(&mut buf, topics.len(), false);
    for topic in topics {
        put_string(&mut buf, &topic.name);
        put_array_len(&mut buf, topic.partitions.len(), false);
        for partition in &topic.partitions {
            put_i32(&mut buf, partition.index);
            put_i16(&mut buf, partition.error_code);
            put_array_len(&mut buf, partition.offsets.len(), false);
            for offset in &partition.offsets {
                put_i64(&mut buf, *offset);
            }
        }
    }
    buf.freeze()
}

async fn resolve_partition(
    broker: &Broker,
    topic: &str,
    request: PartitionRequest,
    replica_id: i32,
) -> PartitionResponse {
    if krabka_verified::list_offsets_kind(request.timestamp, 0)
        == krabka_verified::ListOffsetsKind::Unsupported
    {
        return error_response(request.index, codes::UNSUPPORTED_VERSION);
    }
    let Ok(max_num_offsets) = usize::try_from(request.max_num_offsets) else {
        return error_response(request.index, codes::INVALID_REQUEST);
    };
    let partition = match resolve_for_broker(broker, topic, request.index, replica_id, -1) {
        Ok(partition) => partition,
        Err(error_code) => return error_response(request.index, error_code),
    };

    let (local_end, local_start, deleted_below, config, offsets) = {
        let log = partition.log.lock().expect("log mutex poisoned");
        (
            log.log_end_offset().0,
            log.local_log_start_offset().0,
            log.established_log_start(),
            log.config_snapshot(),
            log.legacy_offsets_before(request.timestamp, max_num_offsets),
        )
    };
    let Ok(mut offsets) = offsets else {
        return error_response(request.index, codes::KAFKA_STORAGE_ERROR);
    };
    let Some(mut upper) = last_fetchable_offset(&partition, fetch_bound(replica_id, 0)).await
    else {
        return error_response(request.index, codes::KAFKA_STORAGE_ERROR);
    };
    if request.timestamp == LATEST_TIMESTAMP {
        let image = broker.controller.current_image();
        let remote_topic_id = image
            .topic(topic)
            .filter(|_| config.remote_storage_enable)
            .map(|topic| topic.topic_id);
        let timeout = resolve_remote_list_offsets_timeout(&image, broker.config.node_id);
        match resolve_latest(LatestContext {
            broker,
            partition: &partition,
            topic: (topic, request.index),
            remote_timeout: timeout,
            remote_topic_id,
            snapshot: (&config, local_end, local_start, deleted_below),
        })
        .await
        {
            Ok(latest) => upper = upper.min(latest),
            Err(error) => return error_response(request.index, error),
        }
    }
    if offsets.iter().any(|offset| offset.0 > upper) {
        offsets = std::iter::once(krabka_ids::Offset(upper))
            .chain(offsets.into_iter().skip_while(|offset| offset.0 > upper))
            .collect();
    }

    PartitionResponse {
        index: request.index,
        error_code: codes::NONE,
        offsets: offsets.into_iter().map(|offset| offset.0).collect(),
    }
}

pub(super) async fn handle(
    broker: &Broker,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let request = decode(req_bytes)?;
    let acl_image = broker.controller.current_image();
    let duplicates = super::duplicate_partition_keys(request.topics.iter().map(|topic| {
        (
            topic.name.as_str(),
            topic.partitions.iter().map(|part| part.index),
        )
    }));

    let (authorized_topics, denied_topics) = super::partition_topics_by_describe(
        request.topics,
        |topic| &topic.name,
        broker,
        &acl_image,
        ctx,
    );

    let mut topics = concurrently(authorized_topics.into_iter().map(|(topic_index, topic)| {
        let duplicates = &duplicates;
        async move {
            let name = topic.name;
            let partitions = concurrently(topic.partitions.into_iter().map(|partition| {
                let is_duplicate = duplicates.contains(&(topic_index, partition.index));
                let name = name.clone();
                async move {
                    if is_duplicate {
                        error_response(partition.index, codes::INVALID_REQUEST)
                    } else {
                        resolve_partition(broker, &name, partition, request.replica_id).await
                    }
                }
            }))
            .await;
            TopicResponse { name, partitions }
        }
    }))
    .await;

    topics.extend(denied_topics.into_iter().map(|(_, topic)| {
        let partitions = topic
            .partitions
            .into_iter()
            .map(|partition| error_response(partition.index, codes::TOPIC_AUTHORIZATION_FAILED))
            .collect();
        TopicResponse {
            name: topic.name,
            partitions,
        }
    }));

    Ok(encode(&topics))
}

#[cfg(test)]
pub(super) async fn latest_offsets_for_test(broker: &Broker, topic: &str) -> (i16, Vec<i64>) {
    let response = resolve_partition(
        broker,
        topic,
        PartitionRequest {
            index: 0,
            timestamp: LATEST_TIMESTAMP,
            max_num_offsets: 1,
        },
        -1,
    )
    .await;
    (response.error_code, response.offsets)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert2::assert;
    use krabka_protocol::primitives::{
        array::{get_array_len, put_array_len},
        fixed::{get_i16, get_i32, get_i64, put_i32, put_i64},
        string_bytes::{get_string_owned, put_string},
    };
    use krabka_units::prelude::bytes;

    use super::*;
    use crate::{
        authorizer::AllowAllAuthorizer,
        broker::BrokerHandle,
        handlers::list_offsets::test_support::{client_for, create_topic, test_context},
        test_support::{peer, principal, start_broker_with_authorizer_no_audit},
    };

    macro_rules! topic_header {
        ($response:ident, $topic:expr, $partitions:expr) => {
            assert!(get_array_len(&mut $response, false).unwrap() == 1);
            assert!(get_string_owned(&mut $response).unwrap() == $topic);
            assert!(get_array_len(&mut $response, false).unwrap() == $partitions);
        };
    }

    #[tokio::test]
    async fn max_num_offsets_caps_legacy_segment_boundaries() {
        const TOPIC: &str = "list-offsets-v0-max";
        let (broker_handle, _dir) =
            start_broker_with_authorizer_no_audit(Arc::new(AllowAllAuthorizer)).await;
        let client = client_for(&broker_handle).await;
        create_topic(
            &client,
            TOPIC,
            vec![
                krabka_protocol::owned::create_topics_request::CreatableTopicConfig {
                    name: "internal.segment.bytes".into(),
                    value: Some("1".into()),
                    ..Default::default()
                },
            ],
        )
        .await;
        broker_handle.wait_until_partition_present(TOPIC, 0).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if broker_handle
                    .partition_log_config_for_test(TOPIC, 0)
                    .is_some_and(|config| config.segment_size == bytes(1))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("topic config propagated");
        broker_handle
            .produce_records_for_test(TOPIC, 0, 4)
            .await
            .expect("produce");

        let mut request = BytesMut::new();
        put_i32(&mut request, -2); // debugging replica: no high-watermark cap
        put_array_len(&mut request, 1, false);
        put_string(&mut request, TOPIC);
        put_array_len(&mut request, 1, false);
        put_i32(&mut request, 0);
        put_i64(&mut request, LATEST_TIMESTAMP);
        put_i32(&mut request, 3);

        let response = answer(&broker_handle, &request).await;
        let mut response: &[u8] = &response;
        topic_header!(response, TOPIC, 1);
        assert!(get_i32(&mut response).unwrap() == 0);
        assert!(get_i16(&mut response).unwrap() == codes::NONE);
        let count = get_array_len(&mut response, false).unwrap();
        let offsets: Vec<i64> = (0..count)
            .map(|_| get_i64(&mut response).unwrap())
            .collect();
        assert!(offsets == vec![4, 3, 2], "{offsets:?}");
        assert!(response.is_empty());

        broker_handle.shutdown().await;
    }

    /// Same ordering guarantee as the v1+ path (`super::tests::
    /// denied_topic_rows_are_appended_after_authorized_rows_regardless_of_request_order`):
    /// a denied topic's row always lands last, regardless of where it sat in
    /// the v0 request.
    use crate::handlers::list_offsets::test_support::DenyNamed;

    #[tokio::test]
    async fn denied_topic_row_is_appended_last_regardless_of_request_order() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            start_broker_with_authorizer_no_audit(Arc::new(DenyNamed(
                std::collections::HashSet::from(["denied"]),
            )))
        );
        request_identity!((admin, socket), principal("admin"));

        let mut request = BytesMut::new();
        put_i32(&mut request, -1);
        put_array_len(&mut request, 3, false);
        for name in ["a", "denied", "c"] {
            put_string(&mut request, name);
            put_array_len(&mut request, 1, false);
            put_i32(&mut request, 0);
            put_i64(&mut request, LATEST_TIMESTAMP);
            put_i32(&mut request, 1);
        }

        let response = handle(&broker, &request, &test_context(&admin, &socket))
            .await
            .expect("ListOffsets v0");
        let mut response: &[u8] = &response;
        assert!(get_array_len(&mut response, false).unwrap() == 3);
        let mut names = Vec::new();
        for _ in 0..3 {
            names.push(get_string_owned(&mut response).unwrap());
            let partition_count = get_array_len(&mut response, false).unwrap();
            for _ in 0..partition_count {
                get_i32(&mut response).unwrap();
                let error_code = get_i16(&mut response).unwrap();
                let offset_count = get_array_len(&mut response, false).unwrap();
                for _ in 0..offset_count {
                    get_i64(&mut response).unwrap();
                }
                if names.last().map(String::as_str) == Some("denied") {
                    assert!(error_code == codes::TOPIC_AUTHORIZATION_FAILED);
                }
            }
        }
        assert!(names == vec!["a", "c", "denied"], "{names:?}");

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn duplicate_partition_gets_invalid_request_on_every_row() {
        const TOPIC: &str = "list-offsets-v0-duplicate";
        let (broker_handle, _dir) =
            start_broker_with_authorizer_no_audit(Arc::new(AllowAllAuthorizer)).await;
        let client = client_for(&broker_handle).await;
        create_topic(&client, TOPIC, vec![]).await;
        broker_handle.wait_until_partition_present(TOPIC, 0).await;

        let mut request = BytesMut::new();
        put_i32(&mut request, -1);
        put_array_len(&mut request, 1, false);
        put_string(&mut request, TOPIC);
        put_array_len(&mut request, 2, false);
        // The same partition index named twice: every row of it must come
        // back INVALID_REQUEST, matching Kafka's duplicatePartitions check.
        put_i32(&mut request, 0);
        put_i64(&mut request, LATEST_TIMESTAMP);
        put_i32(&mut request, 1);
        put_i32(&mut request, 0);
        put_i64(&mut request, LATEST_TIMESTAMP);
        put_i32(&mut request, 1);

        let response = answer(&broker_handle, &request).await;
        let mut response: &[u8] = &response;
        topic_header!(response, TOPIC, 2);
        for _ in 0..2 {
            assert!(get_i32(&mut response).unwrap() == 0);
            assert!(get_i16(&mut response).unwrap() == codes::INVALID_REQUEST);
            assert!(get_array_len(&mut response, false).unwrap() == 0);
        }
        assert!(response.is_empty());

        broker_handle.shutdown().await;
    }
    async fn answer(broker: &BrokerHandle, request: &[u8]) -> Bytes {
        let shared = broker.broker_arc_for_test();
        request_identity!((admin, socket), principal("admin"));
        handle(&shared, request, &test_context(&admin, &socket))
            .await
            .expect("ListOffsets v0")
    }
}
