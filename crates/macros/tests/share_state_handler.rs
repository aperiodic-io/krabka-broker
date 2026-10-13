//! ACL denial preserves request rows; allowed requests reach the operation once.

use {
    assert2::assert,
    std::sync::atomic::{AtomicUsize, Ordering},
};

mod codes {
    pub(crate) const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
}

mod error {
    pub(crate) type BrokerError = &'static str;
}

mod broker {
    use super::{AtomicUsize, Ordering};

    #[derive(Default)]
    pub(crate) struct Coordinator {
        pub(crate) calls: AtomicUsize,
    }

    #[derive(Default)]
    pub(crate) struct Controller {
        pub(crate) snapshots: AtomicUsize,
    }

    pub(crate) struct Image {
        pub(crate) generation: usize,
    }

    impl Controller {
        pub(crate) fn current_image(&self) -> Image {
            Image {
                generation: self.snapshots.fetch_add(1, Ordering::SeqCst) + 1,
            }
        }
    }

    #[derive(Default)]
    pub(crate) struct Broker {
        pub(crate) share_coordinator: Coordinator,
        pub(crate) controller: Controller,
    }
}

mod protocol {
    pub(crate) struct Request {
        pub(crate) group_id: String,
        pub(crate) topics: Vec<RequestTopic>,
    }

    pub(crate) struct RequestTopic {
        pub(crate) topic_id: u8,
        pub(crate) partitions: Vec<RequestPartition>,
    }

    pub(crate) struct RequestPartition {
        pub(crate) partition: i32,
    }

    #[derive(Debug, PartialEq, Eq, krabka_macros::FieldDefaults)]
    pub(crate) struct Response {
        pub(crate) results: Vec<TopicRow>,
        pub(crate) group_id: String,
        pub(crate) observed_snapshot: usize,
        #[default(91)]
        pub(crate) retained: i32,
    }

    #[derive(Debug, PartialEq, Eq, krabka_macros::FieldDefaults)]
    pub(crate) struct TopicRow {
        pub(crate) topic_id: u8,
        pub(crate) partitions: Vec<PartitionRow>,
        #[default(73)]
        pub(crate) retained: i32,
    }

    #[derive(Debug, PartialEq, Eq, krabka_macros::FieldDefaults)]
    pub(crate) struct PartitionRow {
        pub(crate) partition: i32,
        pub(crate) error_code: i16,
        #[default(Some("default message".into()))]
        pub(crate) error_message: Option<String>,
        #[default(19)]
        pub(crate) retained: i32,
    }
}

mod handlers {
    use {crate::broker::Broker, assert2::assert};

    pub(crate) struct RequestContext<'a> {
        pub(crate) principal: &'a str,
        pub(crate) denied: bool,
    }

    const CLUSTER_AUTHORIZATION_FAILED_MESSAGE: &str = "Cluster authorization failed.";

    fn cluster_action_denied(broker: &Broker, ctx: &RequestContext<'_>) -> bool {
        let _image = broker.controller.current_image();
        assert!(ctx.principal == "alice");
        ctx.denied
    }

    pub(crate) mod operation {
        use crate::{
            Ordering,
            broker::{Coordinator, Image},
            protocol,
        };

        krabka_macros::share_state_handler!(
            protocol::Request,
            protocol::Response,
            protocol::TopicRow,
            protocol::PartitionRow,
            serve
        );

        async fn serve(
            coordinator: &Coordinator,
            image: &Image,
            request: protocol::Request,
        ) -> protocol::Response {
            tokio::task::yield_now().await;
            coordinator.calls.fetch_add(1, Ordering::SeqCst);
            protocol::Response {
                group_id: request.group_id,
                observed_snapshot: image.generation,
                ..Default::default()
            }
        }
    }
}

fn request() -> protocol::Request {
    protocol::Request {
        group_id: "group".into(),
        topics: [(7, vec![3, 1]), (2, vec![]), (7, vec![4])]
            .into_iter()
            .map(|(topic_id, partitions)| protocol::RequestTopic {
                topic_id,
                partitions: partitions
                    .into_iter()
                    .map(|partition| protocol::RequestPartition { partition })
                    .collect(),
            })
            .collect(),
    }
}

#[tokio::test]
async fn denied_requests_keep_rows_defaults_and_order_without_running_the_operation() {
    let broker = broker::Broker::default();
    let ctx = handlers::RequestContext {
        principal: "alice",
        denied: true,
    };
    let response = handlers::operation::handle(&broker, request(), 0, &ctx)
        .await
        .unwrap();
    assert!(broker.controller.snapshots.load(Ordering::SeqCst) == 1);
    assert!(broker.share_coordinator.calls.load(Ordering::SeqCst) == 0);
    assert!(
        response.retained == 91 && response.group_id.is_empty() && response.observed_snapshot == 0
    );
    let rows: Vec<_> = response
        .results
        .iter()
        .map(|topic| {
            (
                topic.topic_id,
                topic
                    .partitions
                    .iter()
                    .map(|partition| partition.partition)
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    assert!(rows == vec![(7, vec![3, 1]), (2, vec![]), (7, vec![4])]);
    for topic in response.results {
        assert!(topic.retained == 73);
        for partition in topic.partitions {
            assert!(partition.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
            assert!(partition.error_message.as_deref() == Some("Cluster authorization failed."));
            assert!(partition.retained == 19);
        }
    }
}

#[tokio::test]
async fn allowed_requests_use_a_fresh_snapshot_and_return_the_operation_response() {
    let broker = broker::Broker::default();
    let ctx = handlers::RequestContext {
        principal: "alice",
        denied: false,
    };
    let response = handlers::operation::handle(&broker, request(), 4, &ctx)
        .await
        .unwrap();
    assert!(broker.controller.snapshots.load(Ordering::SeqCst) == 2);
    assert!(broker.share_coordinator.calls.load(Ordering::SeqCst) == 1);
    assert!(
        response
            == protocol::Response {
                group_id: "group".into(),
                observed_snapshot: 2,
                ..Default::default()
            }
    );
}
