//! The deadline and the fan-out that bound one request's remote-tier work.
//!
//! KIP-1075 gives a `ListOffsets` request its own timeout for the lookups that
//! reach the remote tier, so [`remote_timeout`] picks the requested deadline
//! over the broker default, [`await_remote`] applies it to a single lookup, and
//! [`concurrently`] resolves the requested topics and partitions together
//! instead of one after another.

use std::{future::Future, time::Duration};

pub(super) async fn concurrently<F>(futures: impl IntoIterator<Item = F>) -> Vec<F::Output>
where
    F: Future,
{
    futures_util::future::join_all(futures).await
}

pub(super) async fn await_remote<T>(
    timeout: Duration,
    future: impl Future<Output = Result<T, krabka_remote_storage::RemoteStorageError>>,
) -> Option<Result<T, krabka_remote_storage::RemoteStorageError>> {
    tokio::time::timeout(timeout, future).await.ok()
}

pub(super) fn remote_timeout(version: i16, timeout_ms: i32, server_timeout: Duration) -> Duration {
    if version >= 10 && timeout_ms > 0 {
        Duration::from_millis(u64::from(timeout_ms.unsigned_abs()))
    } else {
        server_timeout
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes,
            config_keys::REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS,
            handlers::list_offsets::{
                response::error_response,
                sentinels::{UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP},
            },
        },
        assert2::assert,
        krabka_protocol::owned::list_offsets_response::ListOffsetsPartitionResponse,
        std::sync::Arc,
    };

    #[test]
    fn remote_timeout_uses_v10_positive_value_or_server_default() {
        let server_timeout = Duration::from_millis(123);
        let cases = [
            (9, 7, server_timeout),
            (10, -1, server_timeout),
            (10, 0, server_timeout),
            (10, 7, Duration::from_millis(7)),
            (11, 19, Duration::from_millis(19)),
        ];
        for (version, timeout_ms, expected) in cases {
            assert!(remote_timeout(version, timeout_ms, server_timeout) == expected);
        }
    }

    #[test]
    fn remote_timeout_resolves_dynamic_broker_over_cluster_default() {
        use krabka_metadata::{BrokerConfigRecord, MetadataRecord, NodeId};

        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        for (node_id, value) in [
            (krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID, "400"),
            (NodeId(1), "250"),
        ] {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id,
                config_name: REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS.to_owned(),
                config_value: Some(value.to_owned()),
            }));
        }

        assert!(
            crate::config_keys::resolve_remote_list_offsets_timeout(&image, NodeId(1))
                == Duration::from_millis(250)
        );
        assert!(
            crate::config_keys::resolve_remote_list_offsets_timeout(&image, NodeId(2))
                == Duration::from_millis(400)
        );
    }

    #[tokio::test]
    async fn remote_work_expires_at_the_request_deadline() {
        let result = await_remote(
            Duration::from_millis(1),
            std::future::pending::<Result<(), krabka_remote_storage::RemoteStorageError>>(),
        )
        .await;
        assert!(result.is_none());
        assert!(
            error_response(3, codes::REQUEST_TIMED_OUT)
                == ListOffsetsPartitionResponse {
                    partition_index: 3,
                    error_code: codes::REQUEST_TIMED_OUT,
                    timestamp: UNKNOWN_TIMESTAMP,
                    offset: UNKNOWN_OFFSET,
                    ..Default::default()
                }
        );

        let retry = await_remote(Duration::from_secs(1), async { Ok(7_u8) }).await;
        assert!(matches!(retry, Some(Ok(7))));
    }

    #[tokio::test]
    async fn partition_futures_are_polled_concurrently() {
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let work = (0..2).map(|value| {
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                value
            }
        });
        let output = tokio::time::timeout(Duration::from_secs(1), concurrently(work))
            .await
            .expect("both futures must start before either can finish");
        assert!(output == vec![0, 1]);
    }
}
