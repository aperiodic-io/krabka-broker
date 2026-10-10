mod support;

use assert2::check;
use krabka_client_admin::{AdminClient, AdminError, ConfigResource, DescribeConfigsOptions};

krabka_macros::bound_start_fixture!(config, bound_config, ::krabka_broker);
krabka_macros::bound_start_fixture!(start, start_bound, ::krabka_broker, unwrap, bound_config);

async fn start_broker() -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    let (broker, _controller_addr, dir) = start_bound(|_| {}).await;
    (broker, dir)
}

async fn describe_topic(
    controller_admin: &AdminClient,
    topic: &ConfigResource,
) -> krabka_client_admin::DescribeConfigsResults {
    controller_admin
        .describe_configs(
            std::slice::from_ref(topic),
            DescribeConfigsOptions::default(),
        )
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_bootstrap_routes_supported_and_rejects_unsupported_admin_rpc() {
    let (broker, _dir) = start_broker().await;
    let broker_bootstrap = broker.listen_addr().to_string();
    let mut broker_admin = AdminClient::connect(std::slice::from_ref(&broker_bootstrap))
        .await
        .unwrap();
    let created = broker_admin
        .create_topics(
            &[crate::support::admin::topic_spec("controller-admin", 1, 1)],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(5)),
        )
        .await
        .unwrap();
    check!(created[0].error.is_none());

    let controller_bootstrap = broker.controller_addr().to_string();
    let mut controller_admin =
        AdminClient::connect_controller(std::slice::from_ref(&controller_bootstrap))
            .await
            .unwrap();
    let topic = ConfigResource::topic("controller-admin");
    let configs = describe_topic(&controller_admin, &topic).await;

    check!(configs.keys().collect::<Vec<_>>() == vec![&topic]);
    check!(configs[&topic].is_ok());

    let unsupported_reconciliation = controller_admin
        .reconcile_topic_replication_factor("controller-admin", 1, krabka_units::secs(5))
        .await;
    check!(matches!(
        unsupported_reconciliation,
        Err(AdminError::Broker {
            api: "ControllerEndpoint",
            code: 115,
            name: "UNSUPPORTED_ENDPOINT_TYPE",
            ..
        })
    ));

    // KIP-919 puts the topic lifecycle on the controller listener, so this is
    // routed rather than refused: `CreateTopics` is tagged `controller` in
    // Kafka's own request schema.
    let through_controller = controller_admin
        .create_topics(
            &[crate::support::admin::topic_spec(
                "created-through-controller",
                1,
                1,
            )],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(5)),
        )
        .await
        .unwrap();
    check!(through_controller[0].error.is_none());

    // `DescribeClientQuotas` is tagged `broker` only, so no Kafka controller
    // advertises it and the AdminClient refuses it before the wire.
    let unsupported = controller_admin.describe_user_quotas("alice").await;
    check!(matches!(
        unsupported,
        Err(AdminError::Broker {
            api: "ControllerEndpoint",
            code: 115,
            name: "UNSUPPORTED_ENDPOINT_TYPE",
            ..
        })
    ));

    // Kafka's KIP-919 error 115 is a local AdminClient preflight failure. The
    // same controller connection therefore remains usable after rejection.
    let configs = describe_topic(&controller_admin, &topic).await;
    check!(configs.keys().collect::<Vec<_>>() == vec![&topic]);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_bootstrap_rejects_broker_endpoint() {
    let (broker, _dir) = start_broker().await;
    let result = AdminClient::connect_controller(&[broker.listen_addr().to_string()]).await;

    check!(matches!(
        result,
        Err(AdminError::Broker {
            api: "DescribeCluster",
            code: 114,
            name: "MISMATCHED_ENDPOINT_TYPE",
            ..
        })
    ));
    broker.shutdown().await;
}
