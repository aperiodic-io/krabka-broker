//! What `dispatch_table!` generates, driven through stand-ins for the broker
//! types it names: every section registers its entry under the right kind,
//! api key and `FLEXIBLE_MIN`, and each adapter hands its handler what that
//! section promises.

use {
    assert2::assert,
    std::{
        collections::BTreeMap,
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll, Waker},
    },
    tracing_subscriber::layer::SubscriberExt as _,
};

// These adapters share tracing callsites. Serialize calls while capture tests install scoped subscribers.
static ADAPTER_TESTS: Mutex<()> = Mutex::new(());

fn registered_dispatch_table() -> DispatchRegistry {
    let mut registry = DispatchRegistry::default();
    register_dispatch_table(&mut registry);
    registry
}

type ApiVersion = i16;
type CorrelationId = i32;
type Bytes = Vec<u8>;
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

struct Broker {
    name: &'static str,
}

struct RequestContext<'a> {
    client_id: &'a str,
    correlation_id: CorrelationId,
}

struct TelemetryContext<'a> {
    client_instance: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BrokerError {
    Decode,
    EmptyBody,
    /// `decode_group_request` refused a record string over its bound.
    RecordString,
    /// `encode_response` refused the response.
    Encode,
}

/// What the adapter's `ERROR` event records as `error`.
impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "broker error: {self:?}")
    }
}

impl From<krabka_protocol::DecodeError> for BrokerError {
    fn from(_: krabka_protocol::DecodeError) -> Self { Self::Decode }
}

#[derive(Debug, Clone, Copy)]
enum ApiKey {
    Metadata = 3,
    DescribeConfigs = 32,
    AddPartitionsToTxn = 24,
    CreateDelegationToken = 38,
    PushTelemetry = 72,
    ListGroups = 16,
    Heartbeat = 12,
    ListConfigResources = 74,
    UpdateFeatures = 57,
    DescribeAcls = 29,
}

type ContextHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    &'a [u8],
    &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

type TelemetryHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a TelemetryContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

type AuthHandler = for<'a> fn(
    &'a Broker,
    ApiVersion,
    CorrelationId,
    &'a [u8],
    &'a str,
) -> BoxFuture<'a, Result<Bytes, BrokerError>>;

#[derive(Clone, Copy)]
enum Handler {
    Context(ContextHandler),
    Telemetry(TelemetryHandler),
    Auth(AuthHandler),
}

#[derive(Clone, Copy)]
struct DispatchEntry {
    api_key: i16,
    flexible_min: ApiVersion,
    handler: Handler,
}

impl DispatchEntry {
    fn context(api_key: i16, flexible_min: ApiVersion, handler: ContextHandler) -> Self {
        Self {
            api_key,
            flexible_min,
            handler: Handler::Context(handler),
        }
    }

    fn telemetry(api_key: i16, flexible_min: ApiVersion, handler: TelemetryHandler) -> Self {
        Self {
            api_key,
            flexible_min,
            handler: Handler::Telemetry(handler),
        }
    }

    fn auth(api_key: i16, flexible_min: ApiVersion, handler: AuthHandler) -> Self {
        Self {
            api_key,
            flexible_min,
            handler: Handler::Auth(handler),
        }
    }
}

#[derive(Default)]
struct DispatchRegistry {
    entries: BTreeMap<i16, DispatchEntry>,
}

impl DispatchRegistry {
    fn register(&mut self, entry: DispatchEntry) -> bool {
        self.entries.insert(entry.api_key, entry).is_none()
    }
}

/// The request schemas the table names: a `FLEXIBLE_MIN` per api, and a
/// decodable request type for the `typed` sections.
mod krabka_protocol {
    pub struct DecodeError;

    pub trait Decode: Sized {
        fn decode(buf: &mut &[u8], version: i16) -> Result<Self, DecodeError>;
    }

    /// A request whose body is its UTF-8 text, prefixed with the version it
    /// was decoded at.
    pub struct Text(pub String);

    impl Decode for Text {
        fn decode(buf: &mut &[u8], version: i16) -> Result<Self, DecodeError> {
            let text = std::str::from_utf8(buf).map_err(|_| DecodeError)?;
            *buf = &[];
            Ok(Self(format!("v{version}:{text}")))
        }
    }

    pub mod owned {
        pub mod metadata_request {
            pub const FLEXIBLE_MIN: i16 = 9;
        }
        pub mod describe_configs_request {
            pub const FLEXIBLE_MIN: i16 = 4;
        }
        pub mod list_groups_request {
            pub use crate::krabka_protocol::Text as ListGroupsRequest;
            pub const FLEXIBLE_MIN: i16 = 3;
        }
        pub mod heartbeat_request {
            pub use crate::krabka_protocol::Text as HeartbeatRequest;
            pub const FLEXIBLE_MIN: i16 = 4;
        }
        pub mod list_config_resources_request {
            pub use crate::krabka_protocol::Text as ListConfigResourcesRequest;
            pub const FLEXIBLE_MIN: i16 = 0;
        }
        pub mod describe_acls_request {
            pub use crate::krabka_protocol::Text as DescribeAclsRequest;
            pub const FLEXIBLE_MIN: i16 = 2;
        }
        pub mod update_features_request {
            pub use crate::krabka_protocol::Text as UpdateFeaturesRequest;
            pub const FLEXIBLE_MIN: i16 = 0;
        }
        pub mod add_partitions_to_txn_request {
            pub const FLEXIBLE_MIN: i16 = 3;
        }
        pub mod create_delegation_token_request {
            pub const FLEXIBLE_MIN: i16 = 2;
        }
        pub mod push_telemetry_request {
            pub const FLEXIBLE_MIN: i16 = 0;
        }
    }
}

/// Renders what a handler received, or fails on an empty body so that the
/// tests can see an error pass through the adapter unchanged.
fn reply(parts: &[&dyn std::fmt::Display]) -> Result<Bytes, BrokerError> {
    let text = parts
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    if text.ends_with(' ') || text.ends_with(':') {
        Err(BrokerError::EmptyBody)
    } else {
        Ok(text.into_bytes())
    }
}

fn text(body: &[u8]) -> String { String::from_utf8_lossy(body).into_owned() }

#[derive(Clone, Copy)]
enum ContextReply {
    Metadata,
    Transaction,
}

const fn context_handler(kind: ContextReply) -> ContextHandler {
    match kind {
        ContextReply::Metadata => context_bytes_handler::<false>,
        ContextReply::Transaction => context_bytes_handler::<true>,
    }
}

fn context_bytes_handler<'a, const TRANSACTION: bool>(
    broker: &'a Broker,
    version: ApiVersion,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    let name = if TRANSACTION {
        "txn::add_partitions_to_txn"
    } else {
        "metadata"
    };
    Box::pin(std::future::ready(reply(&[
        &name,
        &broker.name,
        &version,
        &ctx.correlation_id,
        &ctx.client_id,
        &text(body),
    ])))
}
mod handlers {
    use {
        super::reply,
        crate::{
            ApiVersion, Broker, BrokerError, Bytes, RequestContext,
            krabka_protocol::{Decode, Text},
        },
    };

    /// The `typed` adapters' encoder: the response, then the version it was
    /// encoded at. It refuses a response that says `nope`.
    pub fn encode_response<R: std::fmt::Display>(
        resp: &R,
        version: ApiVersion,
    ) -> Result<Bytes, BrokerError> {
        let text = resp.to_string();
        if text.contains("nope") {
            return Err(BrokerError::Encode);
        }
        Ok(format!("{text} @v{version}").into_bytes())
    }

    /// The `typed_group` adapters' decoder, which refuses a body longer than
    /// eight bytes as the broker's refuses an over-long record string.
    pub fn decode_group_request<R: Decode>(
        buf: &mut &[u8],
        version: ApiVersion,
    ) -> Result<R, BrokerError> {
        if buf.len() > 8 {
            return Err(BrokerError::RecordString);
        }
        Ok(R::decode(buf, version)?)
    }

    /// Renders what a typed handler received, failing on an empty body like
    /// [`crate::reply`].
    fn typed_reply(parts: &[&dyn std::fmt::Display]) -> Result<String, BrokerError> {
        reply(parts).map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
    }

    fn typed_context_reply(
        name: &str,
        broker: &Broker,
        request: Text,
        version: ApiVersion,
        ctx: &RequestContext<'_>,
    ) -> std::future::Ready<Result<String, BrokerError>> {
        let Text(text) = request;
        std::future::ready(typed_reply(&[
            &name,
            &broker.name,
            &version,
            &ctx.client_id,
            &text,
        ]))
    }
    pub mod list_groups {
        use crate::{
            ApiVersion, Broker, BrokerError, RequestContext,
            krabka_protocol::owned::list_groups_request::ListGroupsRequest,
        };

        pub fn handle(
            broker: &Broker,
            request: ListGroupsRequest,
            version: ApiVersion,
            ctx: &RequestContext<'_>,
        ) -> std::future::Ready<Result<String, BrokerError>> {
            super::typed_context_reply("list_groups", broker, request, version, ctx)
        }
    }
    pub mod heartbeat {
        use crate::{
            ApiVersion, Broker, BrokerError, RequestContext,
            krabka_protocol::owned::heartbeat_request::HeartbeatRequest,
        };

        pub fn handle(
            broker: &Broker,
            request: HeartbeatRequest,
            version: ApiVersion,
            ctx: &RequestContext<'_>,
        ) -> std::future::Ready<Result<String, BrokerError>> {
            super::typed_context_reply("heartbeat", broker, request, version, ctx)
        }
    }
    pub mod update_features {
        use crate::{
            ApiVersion, Broker, BrokerError, RequestContext,
            krabka_protocol::owned::update_features_request::UpdateFeaturesRequest,
        };

        pub fn handle(
            broker: &Broker,
            request: UpdateFeaturesRequest,
            version: ApiVersion,
            ctx: &RequestContext<'_>,
        ) -> std::future::Ready<Result<String, BrokerError>> {
            super::typed_context_reply("update_features", broker, request, version, ctx)
        }
    }
    pub mod list_config_resources {
        use crate::{
            ApiVersion, Broker, BrokerError, RequestContext,
            krabka_protocol::owned::list_config_resources_request::ListConfigResourcesRequest,
        };

        pub fn handle(
            broker: &Broker,
            request: &ListConfigResourcesRequest,
            version: ApiVersion,
            ctx: &RequestContext<'_>,
        ) -> Result<String, BrokerError> {
            let ListConfigResourcesRequest(text) = request;
            super::typed_reply(&[
                &"list_config_resources",
                &broker.name,
                &version,
                &ctx.client_id,
                &text,
            ])
        }
    }

    pub mod describe_acls {
        use crate::{
            ApiVersion, Broker, RequestContext,
            krabka_protocol::owned::describe_acls_request::DescribeAclsRequest,
        };

        /// Answers every request, so an empty body reaches the encoder.
        pub fn handle(
            broker: &Broker,
            request: &DescribeAclsRequest,
            version: ApiVersion,
            ctx: &RequestContext<'_>,
        ) -> String {
            let DescribeAclsRequest(text) = request;
            format!(
                "describe_acls {} {version} {} {text}",
                broker.name, ctx.client_id
            )
        }
    }

    pub mod metadata {
        use crate::{ContextHandler, ContextReply, context_handler};
        pub const HANDLER: ContextHandler = context_handler(ContextReply::Metadata);
        pub use HANDLER as handle;
    }

    pub mod describe_configs {
        use crate::{ApiVersion, Broker, BrokerError, Bytes, RequestContext, reply, text};

        pub fn handle(
            broker: &Broker,
            version: ApiVersion,
            body: &[u8],
            ctx: &RequestContext<'_>,
        ) -> Result<Bytes, BrokerError> {
            reply(&[
                &"describe_configs",
                &broker.name,
                &version,
                &ctx.correlation_id,
                &ctx.client_id,
                &text(body),
            ])
        }
    }

    pub mod push_telemetry {
        use crate::{
            ApiVersion, Broker, BrokerError, Bytes, CorrelationId, TelemetryContext, reply, text,
        };

        pub fn handle(
            broker: &Broker,
            version: ApiVersion,
            correlation_id: CorrelationId,
            body: &[u8],
            ctx: &TelemetryContext<'_>,
        ) -> Result<Bytes, BrokerError> {
            reply(&[
                &"push_telemetry",
                &broker.name,
                &version,
                &correlation_id,
                &ctx.client_instance,
                &text(body),
            ])
        }
    }
}

/// The `=> path` override: a handler outside `crate::handlers`.
mod txn {
    pub mod add_partitions_to_txn {
        use crate::{ContextHandler, ContextReply, context_handler};
        pub const HANDLER: ContextHandler = context_handler(ContextReply::Transaction);
        pub use HANDLER as handle;
    }
}

/// The hand-written adapter an `auth` entry registers.
fn create_delegation_token_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    principal: &'a str,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(std::future::ready(reply(&[
        &"create_delegation_token_adapter",
        &broker.name,
        &version,
        &correlation_id,
        &principal,
        &text(body),
    ])))
}

krabka_macros::dispatch_table! {
    context: Metadata, AddPartitionsToTxn => crate::txn::add_partitions_to_txn::handle;
    sync_context: DescribeConfigs;
    typed: ListGroups;
    typed_own_span: UpdateFeatures;
    typed_group: Heartbeat;
    typed_sync: ListConfigResources;
    typed_infallible: DescribeAcls;
    auth: CreateDelegationToken;
    telemetry: PushTelemetry;
}

/// Polls a future that is ready on its first poll, which every handler here
/// is.
fn ready<T>(mut future: BoxFuture<'_, T>) -> T {
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("the adapter's future was not ready"),
    }
}

/// Calls `entry`'s handler at version 5 with correlation id 7.
fn call(entry: DispatchEntry, body: &[u8]) -> Result<String, BrokerError> {
    let broker = Broker { name: "b1" };
    let bytes = match entry.handler {
        Handler::Context(handler) => {
            let ctx = RequestContext {
                client_id: "c1",
                correlation_id: 7,
            };
            ready(handler(&broker, 5, body, &ctx))
        }
        Handler::Telemetry(handler) => {
            let ctx = TelemetryContext {
                client_instance: "i1",
            };
            ready(handler(&broker, 5, 7, body, &ctx))
        }
        Handler::Auth(handler) => ready(handler(&broker, 5, 7, body, "User:alice")),
    }?;
    Ok(String::from_utf8(bytes).expect("handlers reply in UTF-8"))
}

#[derive(Debug, PartialEq, Eq)]
struct Observed {
    kind: &'static str,
    flexible_min: ApiVersion,
    reply: Result<String, BrokerError>,
    empty_body: Result<String, BrokerError>,
}

fn observe(registry: &DispatchRegistry, api: ApiKey) -> Option<Observed> {
    let entry = *registry.entries.get(&(api as i16))?;
    Some(Observed {
        kind: match entry.handler {
            Handler::Context(_) => "context",
            Handler::Telemetry(_) => "telemetry",
            Handler::Auth(_) => "auth",
        },
        flexible_min: entry.flexible_min,
        reply: call(entry, b"body"),
        empty_body: call(entry, b""),
    })
}

#[yare::parameterized(
    metadata = {
        ApiKey::Metadata,
        "context",
        9,
        Ok("metadata b1 5 7 c1 body".to_owned()),
        Err(BrokerError::EmptyBody),
    },
    add_partitions_to_txn = {
        ApiKey::AddPartitionsToTxn,
        "context",
        3,
        Ok("txn::add_partitions_to_txn b1 5 7 c1 body".to_owned()),
        Err(BrokerError::EmptyBody),
    },
    describe_configs = {
        ApiKey::DescribeConfigs,
        "context",
        4,
        Ok("describe_configs b1 5 7 c1 body".to_owned()),
        Err(BrokerError::EmptyBody),
    },
    list_groups = {
        ApiKey::ListGroups,
        "context",
        3,
        Ok("list_groups b1 5 c1 v5:body @v5".to_owned()),
        Err(BrokerError::EmptyBody),
    },
    heartbeat = {
        ApiKey::Heartbeat,
        "context",
        4,
        Ok("heartbeat b1 5 c1 v5:body @v5".to_owned()),
        Err(BrokerError::EmptyBody),
    },
    list_config_resources = {
        ApiKey::ListConfigResources,
        "context",
        0,
        Ok("list_config_resources b1 5 c1 v5:body @v5".to_owned()),
        Err(BrokerError::EmptyBody),
    },
    describe_acls = {
        ApiKey::DescribeAcls,
        "context",
        2,
        Ok("describe_acls b1 5 c1 v5:body @v5".to_owned()),
        Ok("describe_acls b1 5 c1 v5: @v5".to_owned()),
    },
    update_features = {
        ApiKey::UpdateFeatures,
        "context",
        0,
        Ok("update_features b1 5 c1 v5:body @v5".to_owned()),
        Err(BrokerError::EmptyBody),
    },
    create_delegation_token = {
        ApiKey::CreateDelegationToken,
        "auth",
        2,
        Ok("create_delegation_token_adapter b1 5 7 User:alice body".to_owned()),
        Err(BrokerError::EmptyBody),
    },
    push_telemetry = {
        ApiKey::PushTelemetry,
        "telemetry",
        0,
        Ok("push_telemetry b1 5 7 i1 body".to_owned()),
        Err(BrokerError::EmptyBody),
    },
)]
fn every_section_registers_an_adapter_that_reaches_its_handler(
    api: ApiKey,
    kind: &'static str,
    flexible_min: ApiVersion,
    reply: Result<String, BrokerError>,
    empty_body: Result<String, BrokerError>,
) {
    let _serial = ADAPTER_TESTS.lock().expect("adapter test lock");
    let registry = registered_dispatch_table();
    assert2::assert!(registry.entries.len() == 10);
    let expected = Observed {
        kind,
        flexible_min,
        reply,
        empty_body,
    };
    assert2::assert!(observe(&registry, api) == Some(expected), "{api:?}");
}

fn check_typed_adapter_error(body: &[u8], error: &BrokerError, api: ApiKey) {
    let registry = registered_dispatch_table();
    let entry = registry.entries[&(api as i16)];
    assert2::assert!(call(entry, body).as_ref() == Err(error), "{api:?}");
}

#[yare::parameterized(
    decode_list_groups = { &[0xff][..], BrokerError::Decode, ApiKey::ListGroups },
    decode_heartbeat = { &[0xff][..], BrokerError::Decode, ApiKey::Heartbeat },
    decode_config_resources = { &[0xff][..], BrokerError::Decode, ApiKey::ListConfigResources },
    decode_acls = { &[0xff][..], BrokerError::Decode, ApiKey::DescribeAcls },
    encode_list_groups = { &b"nope"[..], BrokerError::Encode, ApiKey::ListGroups },
    encode_heartbeat = { &b"nope"[..], BrokerError::Encode, ApiKey::Heartbeat },
    encode_config_resources = { &b"nope"[..], BrokerError::Encode, ApiKey::ListConfigResources },
    encode_acls = { &b"nope"[..], BrokerError::Encode, ApiKey::DescribeAcls },
)]
fn typed_adapters_map_failures_to_broker_errors(body: &[u8], error: BrokerError, api: ApiKey) {
    let _serial = ADAPTER_TESTS.lock().expect("adapter test lock");
    check_typed_adapter_error(body, &error, api);
}

#[test]
fn only_a_typed_group_adapter_decodes_through_decode_group_request() {
    let _serial = ADAPTER_TESTS.lock().expect("adapter test lock");
    let registry = registered_dispatch_table();

    let long = b"longer-than-eight";
    let replies = [ApiKey::ListGroups, ApiKey::Heartbeat]
        .map(|api| call(registry.entries[&(api as i16)], long));
    assert!(
        replies
            == [
                Ok("list_groups b1 5 c1 v5:longer-than-eight @v5".to_owned()),
                Err(BrokerError::RecordString),
            ]
    );
}

#[test]
#[should_panic(expected = "assertion failed")]
fn registering_the_table_twice_panics() {
    let mut registry = DispatchRegistry::default();
    register_dispatch_table(&mut registry);
    register_dispatch_table(&mut registry);
}

/// One span opened or event emitted while an adapter ran.
#[derive(Debug, PartialEq, Eq)]
enum Traced {
    Span {
        name: &'static str,
        level: tracing::Level,
        fields: BTreeMap<&'static str, String>,
    },
    Event {
        level: tracing::Level,
        /// The span the event was emitted in.
        span: Option<&'static str>,
        fields: BTreeMap<&'static str, String>,
    },
}

/// Collects each recorded field's `Debug` rendering, or a `str` as is.
#[derive(Default)]
struct Fields {
    values: BTreeMap<&'static str, String>,
}

impl tracing::field::Visit for Fields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.values.insert(field.name(), value.to_owned());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.values.insert(field.name(), format!("{value:?}"));
    }
}

/// A layer that records every span it sees open and every event.
#[derive(Clone, Default)]
struct Capture {
    traced: Arc<Mutex<Vec<Traced>>>,
}

impl<S> tracing_subscriber::Layer<S> for Capture
where
    S: tracing::Subscriber + for<'l> tracing_subscriber::registry::LookupSpan<'l>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _: &tracing::span::Id,
        _: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        self.traced
            .lock()
            .expect("capture lock")
            .push(Traced::Span {
                name: attrs.metadata().name(),
                level: *attrs.metadata().level(),
                fields: fields.values,
            });
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.traced
            .lock()
            .expect("capture lock")
            .push(Traced::Event {
                level: *event.metadata().level(),
                span: ctx.event_span(event).map(|span| span.name()),
                fields: fields.values,
            });
    }
}

/// Calls `api`'s adapter on `body` and returns what it traced.
fn trace(registry: &DispatchRegistry, api: ApiKey, body: &[u8]) -> Vec<Traced> {
    let capture = Capture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());
    let entry = registry.entries[&(api as i16)];
    tracing::subscriber::with_default(subscriber, || drop(call(entry, body)));
    std::mem::take(&mut *capture.traced.lock().expect("capture lock"))
}

#[yare::parameterized(
    metadata = {
        ApiKey::Metadata,
        "handle_metadata",
        "Metadata",
        true,
        &b""[..],
        BrokerError::EmptyBody,
    },
    add_partitions_to_txn = {
        ApiKey::AddPartitionsToTxn,
        "handle_add_partitions_to_txn",
        "AddPartitionsToTxn",
        true,
        &b""[..],
        BrokerError::EmptyBody,
    },
    describe_configs = {
        ApiKey::DescribeConfigs,
        "handle_describe_configs",
        "DescribeConfigs",
        true,
        &b""[..],
        BrokerError::EmptyBody,
    },
    list_groups = {
        ApiKey::ListGroups,
        "handle_list_groups",
        "ListGroups",
        false,
        &b"nope"[..],
        BrokerError::Encode,
    },
    heartbeat = {
        ApiKey::Heartbeat,
        "handle_heartbeat",
        "Heartbeat",
        false,
        &b"longer-than-eight"[..],
        BrokerError::RecordString,
    },
    list_config_resources = {
        ApiKey::ListConfigResources,
        "handle_list_config_resources",
        "ListConfigResources",
        false,
        &[0xff][..],
        BrokerError::Decode,
    },
    describe_acls = {
        ApiKey::DescribeAcls,
        "handle_describe_acls",
        "DescribeAcls",
        false,
        &b"nope"[..],
        BrokerError::Encode,
    },
    push_telemetry = {
        ApiKey::PushTelemetry,
        "handle_push_telemetry",
        "PushTelemetry",
        true,
        &b""[..],
        BrokerError::EmptyBody,
    },
)]
fn a_generated_adapter_runs_its_handler_in_a_handle_span_and_records_its_error(
    api: ApiKey,
    name: &'static str,
    api_field: &'static str,
    raw: bool,
    refused: &'static [u8],
    refusal: BrokerError,
) {
    let _serial = ADAPTER_TESTS.lock().expect("adapter test lock");
    let registry = registered_dispatch_table();

    let span = |name, api: &str, req_bytes: Option<usize>| {
        let mut fields = BTreeMap::from([("api", api.to_owned()), ("version", "5".to_owned())]);
        if let Some(req_bytes) = req_bytes {
            fields.insert("req_bytes", req_bytes.to_string());
        }
        Traced::Span {
            name,
            level: tracing::Level::INFO,
            fields,
        }
    };
    let error = |name, error: BrokerError| Traced::Event {
        level: tracing::Level::ERROR,
        span: Some(name),
        fields: BTreeMap::from([("error", error.to_string())]),
    };
    let ok = trace(&registry, api, b"body");
    let expected_ok = vec![span(name, api_field, raw.then_some(4))];
    assert2::assert!(ok == expected_ok, "{api:?}");

    let err = trace(&registry, api, refused);
    let expected_err = vec![
        span(name, api_field, raw.then_some(refused.len())),
        error(name, refusal),
    ];
    assert2::assert!(err == expected_err, "{api:?}");
}

#[yare::parameterized(
    update_features = { ApiKey::UpdateFeatures },
    delegation_token = { ApiKey::CreateDelegationToken },
)]
fn an_own_span_or_hand_written_adapter_opens_no_span_and_records_no_error(api: ApiKey) {
    let _serial = ADAPTER_TESTS.lock().expect("adapter test lock");
    let registry = registered_dispatch_table();

    let traced = [b"body".as_slice(), b""].map(|body| trace(&registry, api, body));
    assert2::assert!(traced == [vec![], vec![]], "{api:?}");
}
