//! Shared live-broker and local-partition fixtures for handler tests.

use {
    crate::{
        api_catalog::UnstableApiVersions,
        authorizer::AllowAllAuthorizer,
        broker::{Broker, BrokerHandle, spawn_partition as spawn_broker_partition},
        codes,
        config::BrokerConfig,
        config_keys::broker_dynamic::BrokerConfigContext,
        error::BrokerError,
        features::{METADATA_VERSION, STREAMS_VERSION, require_feature},
        handlers::{
            acl_wire::CLUSTER_RESOURCE_NAME, decode_request, encode_response, produce::handle,
        },
        log_dir::partition_dir,
        log_dir_status::LogDirRegistry,
        partition::Partition,
        producer_state::ProducerState,
        share_partition::state::RecordState,
        test_support::{
            StandalonePartitionSetup, StorageMode, broker_registration, decode_response,
            encode_request, initialize_share_state, peer, principal, request_context,
            sasl_principal, start_broker_no_audit_with,
            start_broker_with as start_test_broker_with, try_dispatch_context,
        },
        topic_policy::TopicPolicy,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_metadata::MetadataRecord,
    krabka_protocol::{
        Decode, Encode,
        owned::{
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            produce_response::ProduceResponse,
        },
        primitives::uuid::Uuid as WireUuid,
        records::{Record, RecordBatch, RecordsPayload},
    },
    std::{future::Future, path::Path, sync::Arc},
};

pub(crate) mod transaction_admin_tests {
    #[allow(unused_imports)]
    pub(crate) use assert2::assert;
    pub(crate) use {
        crate::{
            test_support::{
                lead_transaction_state_partitions,
                start_broker_with_authorizer_no_audit as start_broker, test_ctx,
            },
            txn::version::TxnVersion,
        },
        std::sync::Arc,
    };
}

#[bon::builder]
pub(crate) fn registered_broker_image(
    #[builder(default = krabka_raft::NodeId(7))] node_id: krabka_raft::NodeId,
    #[builder(default = 42)] broker_epoch: i64,
) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&krabka_metadata::MetadataRecord::V1BrokerRegistration(
        krabka_metadata::BrokerRegistrationRecord {
            broker_epoch,
            host: "localhost".into(),
            ..broker_registration(node_id)
        },
    ));
    image
}

pub(crate) struct OrderedRows<Request, Response> {
    pub(crate) requests: Vec<Request>,
    pub(crate) responses: Vec<Response>,
}

pub(crate) fn ordered_rows<Request: Clone, Response: Clone>(
    rows: &[(Request, Response)],
    order: &[usize],
) -> OrderedRows<Request, Response> {
    let mut ordered = OrderedRows {
        requests: Vec::with_capacity(order.len()),
        responses: Vec::with_capacity(order.len()),
    };
    for &index in order {
        let (request, response) = &rows[index];
        ordered.requests.push(request.clone());
        ordered.responses.push(response.clone());
    }
    ordered
}

#[cfg(test)]
pub(crate) fn duplicate_config_resource_response(
    resource_type: i8,
    resource_name: &str,
) -> krabka_protocol::owned::alter_configs_response::AlterConfigsResourceResponse {
    tagged_wire!(
        krabka_protocol::owned::alter_configs_response::AlterConfigsResourceResponse {
            resource_type,
            resource_name: resource_name.to_owned(),
            error_code: codes::INVALID_REQUEST,
            error_message: Some("Each resource must appear at most once.".into()),
        }
    )
}

pub(crate) async fn assert_dispatch_response<Request, Response, Handler, HandlerFuture>(
    request: &Request,
    expected: &Response,
    version: i16,
    handler: Handler,
) where
    Request: Encode,
    Response: for<'body> Decode<'body> + PartialEq + std::fmt::Debug,
    Handler: FnOnce(Bytes) -> HandlerFuture,
    HandlerFuture: Future<Output = Result<Bytes, BrokerError>>,
{
    let body = encode_response(request, version).expect("encode request");
    let body = handler(body).await.expect("dispatch request");
    let actual: Response = decode_request(&body, version).expect("decode response");
    assert!(&actual == expected);
}

/// Retains a broker's handle, directory, and shared runtime in their original order.
/// The calling test supplies its own start expression, including authorizer and config.
macro_rules! broker_fixture {
    ($bindings:tt, $setup:ident, context($context:ident, $user:expr) $(, $ready:ident)?) => {
        broker_fixture!($bindings, $setup $(, $ready)?);
        test_ctx!($context, $user);
    };
    (($handle:ident, $directory:ident, $broker:ident, $partition:ident), local_follower_partition($topic:expr)) => {
        broker_fixture!(($handle, $directory, $broker, $partition), local_partition($topic));
        $partition.install_replication_target(None, $broker.config.node_id.0, 0).await;
    };
    (($handle:ident, $directory:ident, $broker:ident, $persister:ident), share_persister($authorizer:expr, $enabled:expr)) => {
        broker_fixture!(
            ($handle, $directory, $broker),
            crate::test_support::start_share_broker()
                .authorizer($authorizer)
                .support($enabled)
                .call()
        );
        let $persister = $broker.group_coordinator.share_persister().cloned().expect("share persister");
    };
    ($bindings:tt, extra_log_dir($extra:ident)) => {
        broker_fixture!($bindings, crate::test_support::start_broker_with({
            let extra_dir = $extra.clone();
            move |config| config.extra_log_dirs = vec![extra_dir]
        }));
    };
    ($bindings:tt, local_remote_storage($object_dir:ident)) => {
        broker_fixture!($bindings, crate::handlers::test_support::start_broker_with(|config| {
            config.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Local {
                dir: $object_dir.path().to_path_buf(),
            });
            config.remote_log_metadata = crate::config::RlmmKind::InMemory;
        }));
    };
    (($handle:ident, $directory:ident, $broker:ident, $partition:ident), local_partition($topic:expr)) => {
        broker_fixture!(
            ($handle, $directory, $broker),
            crate::handlers::test_support::start_broker()
        );
        let $partition = crate::handlers::test_support::local_partition(&$broker, $directory.path(), $topic);
    };
    ($bindings:tt, local_object_store($object_dir:ident)) => {
        broker_fixture!($bindings, crate::test_support::start_broker_no_audit_with(|config| {
            config.authorizer = std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer);
            config.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Local {
                dir: $object_dir.path().to_path_buf(),
            });
        }));
    };
    ($bindings:tt, controller_peer_acls) => {
        broker_fixture!($bindings, start_broker(std::sync::Arc::new(crate::test_support::ControllerPeerAllowed(
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new()),
        ))));
    };
    ($bindings:tt, break_glass($config:expr)) => {
        broker_fixture!($bindings, crate::test_support::start_broker_no_audit_with(|config| {
            config.authorizer = std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer);
            config.break_glass = $config;
        }));
    };
    ($bindings:tt, principal_grants) => {
        broker_fixture!($bindings, crate::test_support::start_broker_no_audit_with(|config| {
            config.authorizer = std::sync::Arc::new(crate::test_support::GrantsInPrincipalName);
        }));
    };
    ($bindings:tt, group_controller_peer($authorizer:expr)) => {
        broker_fixture!($bindings, crate::test_support::start_group_broker_no_audit(
            std::sync::Arc::new(crate::test_support::ControllerPeerAllowed($authorizer)),
        ));
    };
    ($bindings:tt, group_allow_all) => {
        broker_fixture!($bindings, crate::test_support::start_group_broker_no_audit(
            std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer)
        ));
    };
    ($bindings:tt, share_allow_all) => {
        broker_fixture!(
            $bindings,
            crate::test_support::start_share_broker()
                .authorizer(std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer))
                .call()
        );
    };
    ($bindings:tt, allow_all $(, $ready:ident)?) => {
        broker_fixture!(
            $bindings,
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer))
            $(, $ready)?
        );
    };
    ($bindings:tt, deny_all $(, $ready:ident)?) => {
        broker_fixture!($bindings, start_broker(Arc::new(DenyAll)) $(, $ready)?);
    };
    (($handle:ident, $directory:ident, $broker:ident), $start:expr $(, $ready:ident)?) => {
        let ($handle, $directory) = $start.await;
        let $broker = $handle.broker_arc_for_test();
        $(broker_fixture!(@$ready $broker);)?
    };
    (@controller_leader $broker:ident) => {
        crate::test_support::wait_for_controller_leader(&$broker).await;
    };
}

/// Define config-image input fixtures with their record fields and starting image explicit.
macro_rules! config_image_fixture {
    ($(#[$attr:meta])* $vis:vis fn $name:ident($key:ident, $pairs:ident)
     from $initial:expr; $variant:ident($record:ident { $key_field:ident, $map_field:ident })) => {
        $(#[$attr])*
        $vis fn $name($key: &str, $pairs: &[(&str, &str)]) -> krabka_metadata::MetadataImage {
            let mut image = $initial;
            image.apply(&krabka_metadata::MetadataRecord::$variant(krabka_metadata::$record {
                $key_field: $key.into(),
                $map_field: crate::test_support::string_pairs($pairs),
            }));
            image
        }
    };
}

/// Run the share-offset refusal matrices with identical resource and assertion order.
macro_rules! share_refusal_cases {
    (($case:ident, $authorizer:ident, $enabled:ident, [$($input:ident),*], $expected:ident) in $cases:expr;
     ($handle:ident, $directory:ident, $broker:ident, $ctx:ident, $response:ident);
     $handler:ident($request:expr, $version:expr)) => {
        for ($case, $authorizer, $enabled, $($input,)* $expected) in $cases {
            broker_fixture!(($handle, $directory, $broker),
                crate::test_support::start_share_broker()
                    .authorizer($authorizer)
                    .support($enabled)
                    .call());
            test_ctx!($ctx, "alice");
            let $response = $handler(&$broker, $request, $version, &$ctx)
                .await.expect("handle");
            assert!($response == $expected, "case: {}", $case);
            $handle.shutdown().await;
        }
    };
}

/// Bind a request principal, peer, and optional context in the caller's scope.
/// Explicit expressions retain the caller's authentication and context policy.
macro_rules! request_identity {
    (
        ($principal:ident, $peer:ident, $context:ident), $identity:expr,client_id = $client_id:expr
    ) => {
        request_identity!(
            ($principal, $peer, $context),
            $identity,
            client_id = $client_id,
            address = peer()
        );
    };
    (
        ($principal:ident, $peer:ident, $context:ident),
        $identity:expr,client_id =
        $client_id:expr,address =
        $address:expr
    ) => {
        request_identity!(($principal, $peer), $identity, $address);
        let $context = crate::test_support::request_context(&$principal, &$peer, $client_id);
    };
    (($principal:ident, $peer:ident), $identity:expr) => {
        request_identity!(($principal, $peer), $identity, peer());
    };
    (($principal:ident, $peer:ident), $identity:expr, $address:expr) => {
        let $principal = $identity;
        let $peer = $address;
    };
    (($principal:ident, $peer:ident, $context:ident), $identity:expr, $builder:path) => {
        request_identity!(($principal, $peer, $context), $identity, $builder, peer());
    };
    (
        ($principal:ident, $peer:ident, $context:ident),
        $identity:expr,
        $builder:path,
        $address:expr
    ) => {
        request_identity!(($principal, $peer), $identity, $address);
        let $context = $builder(&$principal, &$peer);
    };
}

/// Empty ACL metadata and a bound caller for synchronous authorization checks.
macro_rules! empty_acl_fixture {
    (($authorizer:ident, $image:ident), ($principal:ident, $peer:ident, $context:ident), $identity:expr, client_id = $client_id:expr $(, connection_id = $connection_id:expr)?) => {
        let $authorizer = crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let $image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        request_identity!(($principal, $peer), $identity);
        empty_acl_fixture!(@context $context, $principal, $peer, $client_id $(, $connection_id)?);
    };
    (@context $context:ident, $principal:ident, $peer:ident, $client_id:expr) => {
        let $context = crate::test_support::request_context(&$principal, &$peer, $client_id);
    };
    (@context $context:ident, $principal:ident, $peer:ident, $client_id:expr, $connection_id:expr) => {
        let $context = crate::handlers::RequestContext::new(&$principal, &$peer, $client_id, $connection_id, false, "PLAINTEXT");
    };
}

/// Stamp a voter RPC with the broker's current cluster and optional Raft term.
macro_rules! stamp_voter_request {
    ($request:ident, $broker:ident $(, $epoch:ident)?) => {
        $request.cluster_id = Some($broker.controller.current_image().cluster_id().to_string());
        $(stamp_voter_request!(@$epoch $request, $broker);)?
    };
    (@leader_epoch $request:ident, $broker:ident) => {
        $request.current_leader_epoch = i32::try_from($broker.controller.quorum_state().current_term).unwrap_or(i32::MAX);
    };
}

/// Authorizer fixtures state their complete policy while sharing the trait signature.
macro_rules! test_authorizer {
    ($ty:ident, ($this:ident, $source:ident, $request:ident), $decision:block) => {
        impl crate::authorizer::Authorizer for $ty {
            fn authorize(
                &$this,
                $source: &dyn crate::authorizer::AclSource,
                $request: &crate::authorizer::AuthorizationRequest<'_>,
            ) -> crate::authorizer::AuthorizationResult $decision
        }
    };
}

/// Refuses only topic reads while allowing setup and group operations.
#[derive(Debug)]
pub(crate) struct DenyTopicRead;

test_authorizer!(DenyTopicRead, (self, _source, request), {
    if request.resource_type == krabka_metadata::ResourceType::Topic
        && request.operation == krabka_metadata::AclOperation::Read
    {
        crate::authorizer::AuthorizationResult::Deny
    } else {
        crate::authorizer::AuthorizationResult::Allow
    }
});

/// Allows group operations but refuses every topic operation.
#[derive(Debug)]
pub(crate) struct DenyTopics;

test_authorizer!(DenyTopics, (self, _source, request), {
    if request.resource_type == krabka_metadata::ResourceType::Topic {
        crate::authorizer::AuthorizationResult::Deny
    } else {
        crate::authorizer::AuthorizationResult::Allow
    }
});

/// The kinds of ids exercised by share API topic-resolution tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TopicIdRef {
    /// The id of a topic that exists.
    Known,
    /// A non-zero id that no topic has.
    Unknown,
    /// The zero id.
    Zero,
}

/// Topic references used by the independent Fetch and Produce wire models.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TopicRef {
    KnownName,
    UnknownName,
    KnownId,
    UnknownId,
    ZeroId,
}

impl TopicRef {
    pub(crate) fn wire_reference<'a>(
        self,
        known: (&'a str, WireUuid),
        unknown: (&'a str, WireUuid),
    ) -> (&'a str, WireUuid) {
        match self {
            Self::KnownName => (known.0, WireUuid::ZERO),
            Self::UnknownName => (unknown.0, WireUuid::ZERO),
            Self::KnownId => ("", known.1),
            Self::UnknownId => ("", unknown.1),
            Self::ZeroId => ("", WireUuid::ZERO),
        }
    }
}

/// A pinned request version, topic reference, and expected partition error.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TopicResolutionCase {
    pub(crate) version: i16,
    pub(crate) topic: TopicRef,
    pub(crate) error_code: i16,
}

impl TopicResolutionCase {
    pub(crate) const fn new(version: i16, topic: TopicRef, error_code: i16) -> Self {
        Self {
            version,
            topic,
            error_code,
        }
    }
}

/// A denied name reaches authorization, while unknown ids fail resolution first.
pub(crate) const fn unauthorized_topic_cases(
    named_version: i16,
    id_version: i16,
) -> [TopicResolutionCase; 3] {
    [
        TopicResolutionCase::new(
            named_version,
            TopicRef::UnknownName,
            codes::TOPIC_AUTHORIZATION_FAILED,
        ),
        TopicResolutionCase::new(id_version, TopicRef::UnknownId, codes::UNKNOWN_TOPIC_ID),
        TopicResolutionCase::new(id_version, TopicRef::ZeroId, codes::UNKNOWN_TOPIC_ID),
    ]
}

/// Adapt typed acknowledgement ranges to each share API's concrete wire batch.
macro_rules! acknowledgement_batches {
    ($ty:ident, $batches:expr) => {
        $batches
            .into_iter()
            .map(|batch| $ty {
                first_offset: batch.first_offset.0,
                last_offset: batch.last_offset.0,
                acknowledge_types: batch.types.into_iter().map(|kind| kind.0).collect(),
                ..Default::default()
            })
            .collect()
    };
}

krabka_macros::create_topic_fixture!(configured_topic_request);
krabka_macros::single_replica_partition_fixture!(single_replica_partition);

/// A partition with the supplied leader and replicas, initially all in ISR.
/// Callers keep differing ISR, epoch, directory, and reassignment values explicit.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct ReplicatedPartitionSetup<'a> {
    #[default("orders")]
    pub topic: &'a str,
    pub partition: krabka_ids::PartitionIndex,
    #[default(&[krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)])]
    pub replicas: &'a [krabka_metadata::NodeId],
    #[default(krabka_metadata::NodeId(1))]
    pub leader: krabka_metadata::NodeId,
}

#[bon::builder]
pub(crate) fn replicated_partition(
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(into, default)] partition: krabka_ids::PartitionIndex,
    #[builder(into, default = krabka_metadata :: NodeId (1))] leader: krabka_metadata::NodeId,
    #[builder(default = & [krabka_metadata :: NodeId (1) , krabka_metadata :: NodeId (2)])]
    replicas: &'_ [krabka_metadata::NodeId],
) -> krabka_metadata::PartitionRecord {
    let setup = ReplicatedPartitionSetup {
        topic,
        partition,
        replicas,
        leader,
    };
    let ReplicatedPartitionSetup {
        topic,
        partition,
        leader,
        replicas,
    } = setup;
    krabka_metadata::PartitionRecord {
        replicas: replicas.to_vec(),
        isr: replicas.to_vec(),
        ..single_replica_partition(topic, partition, leader)
    }
}

pub(crate) fn replica_nodes_from_partitions(replicas: &[i32]) -> Vec<krabka_metadata::NodeId> {
    replicas
        .iter()
        .map(|&id| krabka_metadata::NodeId(u64::try_from(id).expect("broker id")))
        .collect()
}

#[cfg(test)]
pub(crate) const ALL_BROKERS_FENCED_PARTITION_ERROR: &str = "All brokers specified in the manual partition assignment for partition 1 are fenced or in controlled shutdown.";

#[cfg(test)]
pub(crate) fn assigned_partition_record(
    topic: &str,
    partition: i32,
    leader: krabka_metadata::NodeId,
    replicas: &[i32],
    isr: Vec<krabka_metadata::NodeId>,
    leader_epoch: krabka_metadata::LeaderEpoch,
) -> krabka_metadata::PartitionRecord {
    krabka_metadata::PartitionRecord {
        topic: topic.to_owned(),
        partition,
        leader,
        replicas: replica_nodes_from_partitions(replicas),
        isr,
        leader_epoch,
        adding_replicas: Vec::new(),
        removing_replicas: Vec::new(),
        directories: Vec::new(),
        partition_epoch: 0,
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct AclSetup<'a> {
    #[default(krabka_metadata::ResourceType::Topic)]
    pub resource_type: krabka_metadata::ResourceType,
    #[default("orders")]
    pub resource_name: &'a str,
    #[default(krabka_metadata::PatternType::Literal)]
    pub pattern_type: krabka_metadata::PatternType,
    #[default(krabka_metadata::AclOperation::Read)]
    pub operation: krabka_metadata::AclOperation,
}

impl AclSetup<'_> {
    pub fn cluster(operation: krabka_metadata::AclOperation) -> Self {
        Self {
            resource_type: krabka_metadata::ResourceType::Cluster,
            resource_name: CLUSTER_RESOURCE_NAME,
            operation,
            ..Default::default()
        }
    }
}

#[bon::builder]
pub(crate) fn acl(
    #[builder(default = krabka_metadata :: ResourceType :: Topic)]
    resource_type: krabka_metadata::ResourceType,
    #[builder(default = "orders")] resource_name: &'_ str,
    #[builder(default = krabka_metadata :: PatternType :: Literal)]
    pattern_type: krabka_metadata::PatternType,
    #[builder(default = krabka_metadata :: AclOperation :: Read)]
    operation: krabka_metadata::AclOperation,
) -> krabka_metadata::AclEntry {
    let setup = AclSetup {
        resource_type,
        resource_name,
        pattern_type,
        operation,
    };
    let AclSetup {
        resource_type,
        resource_name,
        pattern_type,
        operation,
    } = setup;
    krabka_metadata::AclEntry {
        resource_type,
        resource_name: resource_name.into(),
        pattern_type,
        principal: "User:alice".into(),
        host: "*".into(),
        operation,
        permission_type: krabka_metadata::PermissionType::Allow,
    }
}

pub(crate) fn cluster_acl(operation: krabka_metadata::AclOperation) -> krabka_metadata::AclEntry {
    let setup = AclSetup::cluster(operation);
    acl()
        .resource_type(setup.resource_type)
        .resource_name(setup.resource_name)
        .pattern_type(setup.pattern_type)
        .operation(setup.operation)
        .call()
}

pub(crate) async fn start_allow_all_no_audit() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|config| {
        config.authorizer = Arc::new(AllowAllAuthorizer);
    })
    .await
}

pub(crate) async fn start_broker() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|_| {}).await
}

pub(crate) async fn start_broker_with(
    configure: impl FnOnce(&mut BrokerConfig),
) -> (BrokerHandle, tempfile::TempDir) {
    start_test_broker_with(configure).await
}

pub(crate) async fn produce_wire(
    broker: &BrokerHandle,
    version: i16,
    request: &ProduceRequest,
) -> ProduceResponse {
    let shared = broker.broker_arc_for_test();
    let user = principal("producer");
    let peer = peer();
    let context = request_context(&user, &peer, "producer-client");
    let request_bytes = encode_request(request, version);
    let response_bytes = handle(
        &shared,
        version,
        &request_bytes,
        request_bytes.clone(),
        &context,
    )
    .await
    .expect("handle produce");
    decode_response(&response_bytes, version)
}

/// Build the shared envelope for tests that Produce one partition directly.
#[bon::builder]
pub(crate) fn produce_partition_request(
    topic: String,
    partition: i32,
    records: RecordsPayload,
    #[builder(default = 1)] acks: i16,
) -> ProduceRequest {
    ProduceRequest {
        acks,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic,
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(records),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Send a fixture acknowledgement using the current wire version and default principal.
#[bon::builder]
pub(crate) async fn send_acknowledgements(
    broker: &BrokerHandle,
    request: &krabka_protocol::owned::share_acknowledge_request::ShareAcknowledgeRequest,
) -> krabka_protocol::owned::share_acknowledge_response::ShareAcknowledgeResponse {
    share_acknowledge_wire()
        .broker(broker)
        .request(request)
        .version(krabka_protocol::owned::share_acknowledge_request::MAX_VERSION)
        .call()
        .await
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct ShareWireSetup<'a> {
    #[default(krabka_ids::ApiVersion(2))]
    pub version: krabka_ids::ApiVersion,
    #[default("share-consumer")]
    pub user: &'a str,
}

#[bon::builder]
pub(crate) async fn share_acknowledge_wire(
    broker: &BrokerHandle,
    request: &krabka_protocol::owned::share_acknowledge_request::ShareAcknowledgeRequest,
    #[builder(into, default = krabka_ids :: ApiVersion (2))] version: krabka_ids::ApiVersion,
    #[builder(default = "share-consumer")] user: &'_ str,
) -> krabka_protocol::owned::share_acknowledge_response::ShareAcknowledgeResponse {
    let setup = ShareWireSetup { version, user };
    context_wire(broker, request, ContextWireSetup {
        operation: ShareOperation::Acknowledge,
        wire: setup,
    })
    .await
}

/// Compare each table case's complete actual and expected outcome.
pub(crate) async fn check_cases<C, T: std::fmt::Debug + PartialEq>(
    cases: impl IntoIterator<Item = C>,
    mut drive: impl AsyncFnMut(C) -> (T, T),
) {
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases {
        let (got, want) = drive(case).await;
        actual.push(got);
        expected.push(want);
    }
    assert!(actual == expected);
}

/// Run an independent denied-topic model against one broker, comparing every full outcome.
pub(crate) async fn check_denied_topic_cases<T: std::fmt::Debug + PartialEq>(
    cases: impl IntoIterator<Item = TopicResolutionCase>,
    start: impl std::future::Future<Output = (BrokerHandle, tempfile::TempDir)>,
    mut drive: impl AsyncFnMut(&BrokerHandle, TopicResolutionCase) -> (T, T),
) {
    let (broker, _dir) = start.await;
    check_cases(cases, async |case| drive(&broker, case).await).await;
    broker.shutdown().await;
}

/// Submit the topic before its replicated partition, retaining each fixture's epochs.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct ReplicatedTopicSetup<'a> {
    #[default("orders")]
    pub topic: &'a str,
    #[default(uuid::Uuid::from_u128(1))]
    pub topic_id: uuid::Uuid,
    #[default(krabka_metadata::NodeId(1))]
    pub leader: krabka_metadata::NodeId,
    #[default(&[krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)])]
    pub replicas: &'a [krabka_metadata::NodeId],
    pub leader_epoch: krabka_ids::LeaderEpoch,
}

#[bon::builder]
pub(crate) async fn seed_partition_replicas(
    broker: &BrokerHandle,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(default = uuid :: Uuid :: from_u128 (1))] topic_id: uuid::Uuid,
    #[builder(into, default = krabka_metadata :: NodeId (1))] leader: krabka_metadata::NodeId,
    #[builder(default = & [krabka_metadata :: NodeId (1) , krabka_metadata :: NodeId (2)])]
    replicas: &'_ [krabka_metadata::NodeId],
    #[builder(into, default)] leader_epoch: krabka_ids::LeaderEpoch,
) {
    use krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};

    let setup = ReplicatedTopicSetup {
        topic,
        topic_id,
        leader,
        replicas,
        leader_epoch,
    };
    let ReplicatedTopicSetup {
        topic,
        topic_id,
        leader,
        replicas,
        leader_epoch,
    } = setup;
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1Topic(TopicRecord {
            name: topic.to_owned(),
            topic_id,
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).unwrap(),
        }))
        .await
        .expect("submit topic record");
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
            leader_epoch,
            directories: vec![uuid::Uuid::nil(); replicas.len()],
            ..replicated_partition()
                .topic(topic)
                .leader(leader)
                .replicas(replicas)
                .call()
        }))
        .await
        .expect("submit partition record");
}

pub(crate) use crate::test_support::default_value_records_batch as default_records_batch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::From, derive_more::Into)]
pub(crate) struct ShareSessionEpoch(pub i32);

impl Default for ShareSessionEpoch {
    fn default() -> Self { Self(1) }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum AcknowledgementMode {
    #[default]
    Settle,
    Renew,
}

/// A partition index and its ordered acknowledgement ranges, with independent
/// lifetimes for the range list and its acknowledgement-type slices.
/// Wire acknowledgement codes include malformed values in validation cases.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
pub(crate) struct AcknowledgementCode(pub i8);

#[derive(Clone, Default)]
pub(crate) struct AcknowledgementBatchSetup {
    pub first_offset: krabka_log::Offset,
    pub last_offset: krabka_log::Offset,
    pub types: Vec<AcknowledgementCode>,
}

#[derive(Clone, Default)]
pub(crate) struct AcknowledgementPartitionSetup {
    pub index: krabka_ids::PartitionIndex,
    pub batches: Vec<AcknowledgementBatchSetup>,
}

type WirePartitionAcknowledgements<'a, 'b> = (i32, &'a [(i64, i64, &'b [i8])]);

impl AcknowledgementPartitionSetup {
    /// One acknowledgement code covering an inclusive offset range on partition zero.
    pub(crate) fn single_batch(
        offsets: std::ops::RangeInclusive<krabka_log::Offset>,
        code: AcknowledgementCode,
    ) -> Self {
        Self {
            batches: vec![AcknowledgementBatchSetup {
                first_offset: *offsets.start(),
                last_offset: *offsets.end(),
                types: vec![code],
            }],
            ..Default::default()
        }
    }

    /// Adapt the literal wire tables used by malformed-request cases.
    pub(crate) fn from_wire((index, batches): WirePartitionAcknowledgements<'_, '_>) -> Self {
        Self {
            index: krabka_ids::PartitionIndex(index),
            batches: batches
                .iter()
                .map(|&(first, last, types)| AcknowledgementBatchSetup {
                    first_offset: krabka_log::Offset(first),
                    last_offset: krabka_log::Offset(last),
                    types: types.iter().copied().map(AcknowledgementCode).collect(),
                })
                .collect(),
        }
    }
}

#[derive(Clone, krabka_macros::FieldDefaults)]
pub(crate) struct AcknowledgementSetup<'a> {
    #[default("g")]
    pub group: &'a str,
    #[default("member")]
    pub member: &'a str,
    pub epoch: ShareSessionEpoch,
    pub topic_id: WireUuid,
    pub partition: AcknowledgementPartitionSetup,
    pub mode: AcknowledgementMode,
}

impl<'a> AcknowledgementSetup<'a> {
    /// The default member and acknowledgement mode for one topic's session.
    pub(crate) fn for_topic_session(
        group: &'a str,
        epoch: ShareSessionEpoch,
        topic_id: WireUuid,
    ) -> Self {
        Self {
            group,
            epoch,
            topic_id,
            ..Default::default()
        }
    }
}

#[bon::builder]
pub(crate) fn acknowledge_batches_request(
    #[builder(default = "g")] group: &'_ str,
    #[builder(default = "member")] member: &'_ str,
    #[builder(into, default)] epoch: ShareSessionEpoch,
    #[builder(default)] topic_id: WireUuid,
    #[builder(default)] partition: AcknowledgementPartitionSetup,
    #[builder(default)] mode: AcknowledgementMode,
) -> krabka_protocol::owned::share_acknowledge_request::ShareAcknowledgeRequest {
    use krabka_protocol::owned::share_acknowledge_request::{
        AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch, ShareAcknowledgeRequest,
    };

    let setup = AcknowledgementSetup {
        group,
        member,
        epoch,
        topic_id,
        partition,
        mode,
    };
    let AcknowledgementSetup {
        group,
        member,
        epoch,
        topic_id,
        partition,
        mode,
    } = setup;
    ShareAcknowledgeRequest {
        group_id: Some(group.into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch.0,
        is_renew_ack: mode == AcknowledgementMode::Renew,
        topics: vec![AcknowledgeTopic {
            topic_id,
            partitions: vec![AcknowledgePartition {
                partition_index: partition.index.0,
                acknowledgement_batches: acknowledgement_batches!(
                    AcknowledgementBatch,
                    partition.batches
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Return the incremental rows for the first topic, or no rows when it is omitted.
pub(crate) fn share_fetch_partition_rows(
    response: krabka_protocol::owned::share_fetch_response::ShareFetchResponse,
) -> Vec<krabka_protocol::owned::share_fetch_response::PartitionData> {
    response
        .responses
        .into_iter()
        .next()
        .map(|topic| topic.partitions)
        .unwrap_or_default()
}

#[bon::builder]
pub(crate) async fn share_fetch_wire(
    broker: &BrokerHandle,
    request: &krabka_protocol::owned::share_fetch_request::ShareFetchRequest,
    #[builder(into, default = krabka_ids :: ApiVersion (2))] version: krabka_ids::ApiVersion,
    #[builder(default = "share-consumer")] user: &'_ str,
) -> krabka_protocol::owned::share_fetch_response::ShareFetchResponse {
    let setup = ShareWireSetup { version, user };
    context_wire(broker, request, ContextWireSetup {
        operation: ShareOperation::Fetch,
        wire: setup,
    })
    .await
}

#[derive(Clone, Copy)]
enum ShareOperation {
    Fetch,
    Acknowledge,
}

#[derive(Clone, Copy)]
struct ContextWireSetup<'a> {
    operation: ShareOperation,
    wire: ShareWireSetup<'a>,
}

/// Encodes and decodes one contextual share dispatch using the fixture's identity.
async fn context_wire<R: Decode<'static>>(
    broker: &BrokerHandle,
    request: &impl Encode,
    setup: ContextWireSetup<'_>,
) -> R {
    let (api_key, expectation) = match setup.operation {
        ShareOperation::Fetch => (
            krabka_protocol::owned::share_fetch_request::API_KEY,
            "handle share fetch",
        ),
        ShareOperation::Acknowledge => (
            krabka_protocol::owned::share_acknowledge_request::API_KEY,
            "handle share acknowledge",
        ),
    };
    let shared = broker.broker_arc_for_test();
    let user = principal(setup.wire.user);
    let peer = peer();
    let context = request_context(&user, &peer, "share-client");
    let request_bytes = encode_request(request, setup.wire.version.0);
    let response = try_dispatch_context(
        &shared,
        api_key,
        setup.wire.version.0,
        &request_bytes,
        &context,
    )
    .await
    .expect(expectation);
    decode_response(&response, setup.wire.version.0)
}

/// Reads the ordered record states of partition zero without changing its locks.
pub(crate) async fn share_record_states(
    broker: &BrokerHandle,
    group: &str,
    topic_id: WireUuid,
) -> Vec<RecordState> {
    let cell = broker
        .broker_arc_for_test()
        .share_partition_leaders
        .peek_for_test(group, uuid::Uuid::from_bytes(topic_id.0), 0)
        .expect("a loaded share partition");
    let state = cell.lock().await;
    state
        .record_states()
        .into_iter()
        .map(|(_, state)| state)
        .collect()
}

/// Bind the client's response before waiting or inspecting subsequent metadata.
macro_rules! created_topic_fixture {
    (($client:ident, $response:ident), $broker:expr, $client_id:expr, $request:expr) => {
        let $client = krabka_client_core::Client::builder()
            .bootstrap($broker.listen_addr().to_string())
            .client_id($client_id)
            .build()
            .await
            .expect("client build");
        let $response = $client.send($request).await.expect("CreateTopics");
        assert!(
            $response.topics[0].error_code == crate::codes::NONE,
            "{:?}",
            $response
        );
    };
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct ClientTopicSetup<'a> {
    #[default("admin-client")]
    pub client_id: &'a str,
    #[default("orders")]
    pub name: &'a str,
    #[default(TopicPartitionCount(1))]
    pub partitions: TopicPartitionCount,
}

pub(crate) fn metadata_topic_record(
    name: &str,
    topic_id: uuid::Uuid,
    partitions: i32,
    replication_factor: i16,
) -> krabka_metadata::TopicRecord {
    krabka_metadata::TopicRecord {
        name: name.to_owned(),
        topic_id,
        partitions,
        replication_factor,
    }
}

#[cfg(test)]
pub(crate) fn build_topic_config_record_with_default_policy<Resource>(
    resource: &Resource,
    image: &krabka_metadata::MetadataImage,
    build: impl FnOnce(
        &Resource,
        &krabka_metadata::MetadataImage,
        &TopicPolicy,
        bool,
        UnstableApiVersions,
    ) -> Result<krabka_metadata::MetadataRecord, (i16, String)>,
) -> Result<krabka_metadata::MetadataRecord, (i16, String)> {
    build(
        resource,
        image,
        &TopicPolicy::default(),
        true,
        UnstableApiVersions::Disabled,
    )
}

macro_rules! topic_config_record_with_default_policy {
    ($resource:expr, $image:expr, $build:path $(,)?) => {
        $crate::handlers::test_support::build_topic_config_record_with_default_policy(
            $resource, $image, $build,
        )
    };
}
#[bon::builder]
pub(crate) async fn create_topic(
    broker: &BrokerHandle,
    #[builder(default = "admin-client")] client_id: &'_ str,
    #[builder(default = "orders")] name: &'_ str,
    #[builder(into, default = TopicPartitionCount (1))] partitions: TopicPartitionCount,
) -> WireUuid {
    let setup = ClientTopicSetup {
        client_id,
        name,
        partitions,
    };
    let ClientTopicSetup {
        client_id,
        name,
        partitions,
    } = setup;
    created_topic_fixture!(
        (client, response),
        broker,
        client_id,
        configured_topic_request()
            .topic(name)
            .num_partitions(partitions)
            .call()
    );
    for partition in 0..partitions.0 {
        broker.wait_until_partition_present(name, partition).await;
    }
    WireUuid(
        broker
            .controller_image_for_test()
            .topic(name)
            .expect("created topic")
            .topic_id
            .into_bytes(),
    )
}

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    derive_more::Display,
    derive_more::From,
    derive_more::Into,
)]
pub(crate) struct RecordCount(pub i32);

/// Appends one v2 batch of `count` records through the v12 Produce handler.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct ProduceRecordsSetup<'a> {
    #[default("orders")]
    pub topic: &'a str,
    pub partition_index: krabka_ids::PartitionIndex,
    #[default(RecordCount(3))]
    pub count: RecordCount,
}

#[bon::builder]
pub(crate) async fn produce_records(
    broker: &BrokerHandle,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(into, default)] partition_index: krabka_ids::PartitionIndex,
    #[builder(into, default = RecordCount (3))] count: RecordCount,
) {
    let setup = ProduceRecordsSetup {
        topic,
        partition_index,
        count,
    };
    let ProduceRecordsSetup {
        topic,
        partition_index,
        count,
    } = setup;
    let request = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition_index.0,
                records: Some(RecordsPayload::V2(vec![record_batch(count.0)])),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let response = produce_wire(broker, 12, &request).await;
    assert!(
        response.responses[0].partition_responses[0].error_code == codes::NONE,
        "{response:?}"
    );
}

pub(crate) fn record_batch(count: i32) -> RecordBatch {
    RecordBatch {
        last_offset_delta: count - 1,
        records: (0..count)
            .map(|offset_delta| Record {
                offset_delta,
                value: Some(Bytes::from_static(b"v")),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

pub(crate) fn local_partition(broker: &Broker, root: &Path, topic: &str) -> Arc<Partition> {
    partition(broker, root, StandalonePartitionSetup {
        topic,
        ..Default::default()
    })
}

pub(crate) fn partition(
    broker: &Broker,
    root: &Path,
    setup: StandalonePartitionSetup<'_>,
) -> Arc<Partition> {
    let StandalonePartitionSetup {
        topic,
        partition: index,
        storage,
    } = setup;
    spawn_partition()
        .root(root)
        .topic(topic)
        .index(index)
        .log_dir_status(broker.log_dir_status.clone())
        .producer_state(Arc::clone(&broker.producer_state))
        .storage(storage)
        .call()
}

#[derive(krabka_macros::FieldDefaults)]
pub(crate) struct PartitionSpawnSetup<'a> {
    #[default("orders")]
    pub topic: &'a str,
    pub index: krabka_ids::PartitionIndex,
    pub log_dir_status: LogDirRegistry,
    #[default(Arc::new(crate::producer_state::ProducerState::new()))]
    pub producer_state: Arc<ProducerState>,
    pub storage: StorageMode,
    pub log_config: krabka_log::LogConfig,
}

#[bon::builder]
pub(crate) fn spawn_partition(
    root: &Path,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(into, default)] index: krabka_ids::PartitionIndex,
    #[builder(default)] log_dir_status: LogDirRegistry,
    #[builder(default = Arc :: new (crate :: producer_state :: ProducerState :: new ()))]
    producer_state: Arc<ProducerState>,
    #[builder(default)] storage: StorageMode,
    #[builder(default)] log_config: krabka_log::LogConfig,
) -> Arc<Partition> {
    let setup = PartitionSpawnSetup {
        topic,
        index,
        log_dir_status,
        producer_state,
        storage,
        log_config,
    };
    let PartitionSpawnSetup {
        topic,
        index,
        log_dir_status,
        producer_state,
        storage,
        log_config,
    } = setup;
    let partition_dir = partition_dir(root, topic, index.0);
    std::fs::create_dir_all(&partition_dir).expect("partition directory");
    spawn_broker_partition(
        topic.to_string(),
        index,
        root.to_path_buf(),
        krabka_log::Log::open(&partition_dir, log_config).expect("open partition log"),
        log_dir_status,
        producer_state,
        storage == StorageMode::Diskless,
    )
}

pub(crate) async fn seed_controller_quota(handle: &BrokerHandle, rate: f64) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1ClientQuota(
            krabka_metadata::ClientQuotaRecord {
                entity: vec![
                    krabka_metadata::QuotaEntity {
                        entity_type: "user".into(),
                        entity_name: Some("admin".into()),
                    },
                    krabka_metadata::QuotaEntity {
                        entity_type: "client-id".into(),
                        entity_name: Some("admin-client".into()),
                    },
                ],
                config_key: "controller_mutation_rate".into(),
                config_value: Some(rate),
            },
        )])
        .await
        .expect("seed quota");
}

pub(crate) async fn set_streams_version(broker: &Broker, level: i16) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: crate::features::STREAMS_VERSION.into(),
                level,
            },
        )])
        .await
        .expect("submit streams.version");
    let want = (level != 0).then_some(level);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if broker
                .controller
                .current_image()
                .finalized_feature(STREAMS_VERSION)
                == want
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("streams.version visible");
}

pub(crate) async fn wait_until_creation_ends(broker: &Broker, topic: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while broker.auto_topic_creation.is_in_flight(topic) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the creation ends");
}

pub(crate) fn acquired_share_records(
    row: &krabka_protocol::owned::share_fetch_response::PartitionData,
) -> Vec<(i64, i64)> {
    row.acquired_records
        .iter()
        .map(|range| (range.first_offset, range.last_offset))
        .collect()
}

pub(crate) fn metadata_version_gated(level: Option<i16>, minimum: i16) -> bool {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    if let Some(level) = level {
        image.apply(&MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: METADATA_VERSION.to_string(),
                level,
            },
        ));
    }
    require_feature(&image, METADATA_VERSION, minimum).is_err()
}

/// The independent placement model: preserve replica order and exclude fenced ISR members.
pub(crate) fn replica_availability(
    replicas: &[krabka_metadata::NodeId],
    fenced: &[u64],
) -> (Vec<bool>, Vec<krabka_metadata::NodeId>) {
    let flags = replicas
        .iter()
        .map(|node| fenced.contains(&node.0))
        .collect();
    let isr = replicas
        .iter()
        .copied()
        .filter(|node| !fenced.contains(&node.0))
        .collect();
    (flags, isr)
}

/// The SASL operator identity used to exercise request and ACL contexts.
pub(crate) fn operators_principal() -> krabka_security::Principal {
    krabka_security::Principal {
        groups: vec!["operators".to_string()],
        ..sasl_principal("alice")
    }
}

/// The inputs and independent outcomes of manual-assignment availability tables.
pub(crate) type ManualAssignmentRow = (
    &'static [u64],
    &'static [u64],
    &'static [&'static [i32]],
    i16,
    Option<&'static str>,
    Vec<(krabka_raft::NodeId, Vec<krabka_raft::NodeId>)>,
);

#[cfg(test)]
pub(crate) fn invalid_manual_assignment(
    fenced: &'static [u64],
    assignments: &'static [&'static [i32]],
    error_message: &'static str,
) -> ManualAssignmentRow {
    (
        fenced,
        &[],
        assignments,
        codes::INVALID_REPLICA_ASSIGNMENT,
        Some(error_message),
        Vec::new(),
    )
}

/// Start the canonical `ListOffsets` client, create its topic, then await the log.
macro_rules! list_offsets_topic_fixture {
    (($broker:ident, $directory:ident, $client:ident), $topic:expr) => {
        let ($broker, $directory) = crate::test_support::start_broker_no_audit().await;
        let $client = client_for(&$broker).await;
        create_topic(&$client, $topic, Vec::new()).await;
        $broker.wait_until_partition_present($topic, 0).await;
    };
}

/// A config refusal must retain its Kafka error and name the rejected key.
pub(crate) fn check_config_result<T>(
    result: Result<T, (i16, String)>,
    want_ok: bool,
    label: &str,
    key: &str,
) {
    assert2::check!(result.is_ok() == want_ok, "{label}");
    if let Err((code, message)) = result {
        assert2::check!(code == crate::codes::INVALID_CONFIG, "{label}");
        assert2::check!(message.contains(key), "{label}: {message}");
    }
}

/// Collect actual and pinned expected outcomes for distinct per-case topic names.
/// Comparisons and broker shutdown remain at each caller's original boundary.
macro_rules! topic_case_outcomes {
    (($actual:ident, $expected:ident), ($index:ident, $case:ident, $label:ident, $topic:ident),
        $prefix:expr, $cases:expr, { $($body:tt)* }) => {
        let mut $actual = Vec::new();
        let mut $expected = Vec::new();
        for ($index, $case) in (0_u128..).zip($cases) {
            let $label = format!("{:?}", $case);
            let $topic = format!("{}-{}", $prefix, $index);
            $($body)*
        }
    };
}

/// Create the share topic before loading group/partition zero's persisted state.
macro_rules! initialized_share_topic {
    (($handle:ident, $directory:ident, $topic_id:ident), $start:expr, $topic:expr, $group:expr) => {
        let ($handle, $directory) = $start.await;
        let $topic_id = create_topic(&$handle, $topic).await;
        crate::test_support::initialize_share_state(
            &$handle,
            $group,
            uuid::Uuid::from_bytes($topic_id.0),
            0,
        )
        .await;
    };
}

/// Project a replica list into sorted declared sites for independent test expectations.
pub(crate) fn sorted_replica_sites(
    replicas: &[krabka_raft::NodeId],
    site_of: impl Fn(krabka_raft::NodeId) -> String,
) -> Vec<String> {
    let mut sites: Vec<_> = replicas.iter().map(|node| site_of(*node)).collect();
    sites.sort();
    sites
}

/// Set earliest-offset policy before initializing the requested share partitions.
pub(crate) async fn initialize_earliest_share(
    broker: &BrokerHandle,
    group: &str,
    topic_id: WireUuid,
    partitions: std::ops::Range<i32>,
) {
    broker
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1GroupConfig(
            krabka_metadata::GroupConfigRecord {
                group_id: group.to_string(),
                configs: crate::test_support::string_pairs(&[(
                    "share.auto.offset.reset",
                    "earliest",
                )]),
            },
        )])
        .await
        .expect("set the group config");
    for partition in partitions {
        initialize_share_state(broker, group, uuid::Uuid::from_bytes(topic_id.0), partition).await;
    }
}

/// A fixed cluster id used to pin Kafka's independent base64-form expectations.
macro_rules! known_cluster_fixture {
    (($id:ident, $handle:ident, $directory:ident, $broker:ident)) => {
        let $id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        broker_fixture!(
            ($handle, $directory, $broker),
            crate::test_support::start_broker_with(|config| config.cluster_id = Some($id))
        );
    };
}

/// Bind the shared runtime before waiting for a candidate that satisfies the caller's predicate.
macro_rules! wait_for_local_partition {
    (
        ($shared:ident, $partition:ident),
        $broker:expr,
        $topic:expr,
        $candidate:ident,
        $condition:expr,
        $expect:literal
    ) => {
        let $shared = $broker.broker_arc_for_test();
        wait_for_bound_partition!(
            ($shared, $partition),
            $topic,
            $candidate,
            $condition,
            $expect
        );
    };
}

macro_rules! wait_for_bound_partition {
    (
        ($shared:ident, $partition:ident),
        $topic:expr,
        $candidate:ident,
        $condition:expr,
        $expect:literal
    ) => {
        let $partition = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some($candidate) = $shared
                    .partitions
                    .get($topic, krabka_ids::PartitionIndex(0))
                    && $condition
                {
                    return $candidate;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect($expect);
    };
}

pub(crate) fn share_fetch_topics(
    topic_id: WireUuid,
    indices: &[i32],
) -> Vec<krabka_protocol::owned::share_fetch_request::FetchTopic> {
    use krabka_protocol::owned::share_fetch_request::{FetchPartition, FetchTopic};
    vec![FetchTopic {
        topic_id,
        partitions: indices
            .iter()
            .map(|&partition_index| FetchPartition {
                partition_index,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }]
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct BrokerConfigMutationSetup<'a> {
    #[default(krabka_metadata::NodeId(1))]
    pub serving: krabka_metadata::NodeId,
    #[default(&[])]
    pub log_dirs: &'a [std::path::PathBuf],
    #[default(UnstableApiVersions::Disabled)]
    pub unstable: UnstableApiVersions,
}

#[bon::builder]
pub(crate) fn broker_config_context<'a>(
    image: &'a krabka_metadata::MetadataImage,
    #[builder(into, default = krabka_metadata :: NodeId (1))] serving: krabka_metadata::NodeId,
    #[builder(default = & [])] log_dirs: &'a [std::path::PathBuf],
    #[builder(default = UnstableApiVersions :: Disabled)] unstable: UnstableApiVersions,
) -> BrokerConfigContext<'a> {
    let setup = BrokerConfigMutationSetup {
        serving,
        log_dirs,
        unstable,
    };
    BrokerConfigContext {
        image,
        serving: setup.serving,
        log_dirs: setup.log_dirs,
        unstable: setup.unstable,
    }
}
