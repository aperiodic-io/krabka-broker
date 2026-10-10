//! The two setup steps every admin test in this suite repeats: building a
//! `krabka-client-core` client against a started broker, and creating the topic
//! whose configuration, partitions, or records the test then drives.

use assert2::assert;

use crate::support::{
    client::connect_owned,
    topics::{creatable_topic, create_topic_request},
};

pub(crate) async fn build_client(addr: std::net::SocketAddr) -> krabka_client_core::Client {
    connect_owned(
        format!("127.0.0.1:{}", addr.port()),
        "admin-handlers-test",
        "client build",
    )
    .await
}

pub(crate) async fn create_topic_helper(
    client: &krabka_client_core::Client,
    name: &str,
    partitions: i32,
) {
    let req = create_topic_request(creatable_topic(
        crate::support::topics::ConfiguredTopicSetup {
            name: (name).into(),
            partitions: crate::support::topics::TopicPartitionCount(partitions),
            ..Default::default()
        },
    ));
    let resp = client.send(req).await.expect("create_topics");
    let result = &resp.topics[0];
    assert!(
        result.error_code == 0,
        "create_topics failed: {:?}",
        result.error_message
    );
}
