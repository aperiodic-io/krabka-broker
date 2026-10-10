//! `dispatch_table!`: see the crate documentation.

use moxy::{
    ast::ParseError,
    token::{Ident, LitStr, TokenStream},
};

use crate::api_names::{Entry, Section, Suffixed, parse_sections, section};

/// What the adapter of a section does with the request before it reaches the
/// handler.
#[derive(Clone, Copy)]
enum Adapter {
    /// Hands the handler the raw body and awaits it.
    Context,
    /// Hands the handler the raw body and wraps its result in a ready future.
    SyncContext,
    /// Decodes the body into the request type, awaits the handler's response
    /// struct and encodes it. `group` decodes through
    /// `decode_group_request`, which also refuses a record string over the
    /// coordinator bound.
    Typed { group: bool },
    /// Decodes the body into the request type, calls the handler with a
    /// reference to it, encodes its response struct and wraps the result in a
    /// ready future. A `fallible` handler returns a `Result` the adapter
    /// propagates; any other returns the response itself.
    TypedSync { fallible: bool },
    /// Hands a telemetry handler the raw body and wraps its result in a ready
    /// future.
    Telemetry,
    /// The adapter is hand-written; the table only registers it.
    HandWritten,
}

/// One section the table accepts: its label, its adapter, and the
/// `DispatchEntry` constructor that registers it.
struct Kind {
    label: &'static str,
    adapter: Adapter,
    /// Whether the generated adapter opens the `handle_<snake>` span. A
    /// section whose handler opens a span of its own sets this false, so the
    /// request is not traced twice.
    traced: bool,
    constructor: &'static str,
}

impl Kind {
    const fn new(
        label: &'static str,
        adapter: Adapter,
        traced: bool,
        constructor: &'static str,
    ) -> Self {
        Self {
            label,
            adapter,
            traced,
            constructor,
        }
    }
}

const KINDS: [Kind; 9] = [
    Kind::new("context", Adapter::Context, true, "context"),
    Kind::new("sync_context", Adapter::SyncContext, true, "context"),
    Kind::new("typed", Adapter::Typed { group: false }, true, "context"),
    Kind::new(
        "typed_own_span",
        Adapter::Typed { group: false },
        false,
        "context",
    ),
    Kind::new(
        "typed_group",
        Adapter::Typed { group: true },
        true,
        "context",
    ),
    Kind::new(
        "typed_sync",
        Adapter::TypedSync { fallible: true },
        true,
        "context",
    ),
    Kind::new(
        "typed_infallible",
        Adapter::TypedSync { fallible: false },
        true,
        "context",
    ),
    Kind::new("auth", Adapter::HandWritten, false, "auth"),
    Kind::new("telemetry", Adapter::Telemetry, true, "telemetry"),
];

/// The adapter function for one generated entry.
///
/// Unless the entry's section says the handler opens its own span, the adapter
/// runs the decode, the handler and the encode inside an `info` span named
/// `handle_<snake_name>` with `api`, `version` and, for an adapter that hands
/// the handler the raw body, `req_bytes`. On `Err` it emits the `ERROR` event
/// `#[tracing::instrument(err)]` would: `error` set to the error's `Display`,
/// inside the span.
fn adapter(
    kind: Adapter,
    traced: bool,
    adapter: &Ident,
    handler: &TokenStream,
    entry: &Entry,
) -> TokenStream {
    let request_module = &entry.names.request_module;
    let request_type = &entry.names.request_type;
    let (raw_body, body) = match kind {
        Adapter::Context => (
            true,
            Body::Future(moxy::template! { {{ handler }}(broker, version, body, ctx) }),
        ),
        Adapter::SyncContext => (
            true,
            Body::Ready(moxy::template! { {{ handler }}(broker, version, body, ctx) }),
        ),
        Adapter::Typed { group } => {
            let decode = if group {
                moxy::template! {
                    crate::handlers::decode_group_request::<
                        krabka_protocol::owned::{{ request_module }}::{{ request_type }},
                    >(&mut cur, version)?
                }
            } else {
                moxy::template! {
                    {
                        use krabka_protocol::Decode as _;

                        krabka_protocol::owned::{{ request_module }}::{{ request_type }}::decode(
                            &mut cur, version,
                        )?
                    }
                }
            };
            (
                false,
                Body::Future(moxy::template! {
                    async move {
                        let mut cur = body;
                        let req = {{ decode }};
                        let resp = {{ handler }}(broker, req, version, ctx).await?;
                        crate::handlers::encode_response(&resp, version)
                    }
                }),
            )
        }
        Adapter::TypedSync { fallible } => {
            let call = moxy::template! { {{ handler }}(broker, &req, version, ctx) };
            let resp = if fallible {
                moxy::template! { {{ call }}? }
            } else {
                call
            };
            (
                false,
                Body::Ready(moxy::template! {
                    (|| {
                        use krabka_protocol::Decode as _;

                        let mut cur = body;
                        let req = krabka_protocol::owned::{{ request_module }}::{{ request_type }}::decode(
                            &mut cur, version,
                        )?;
                        let resp = {{ resp }};
                        crate::handlers::encode_response(&resp, version)
                    })()
                }),
            )
        }
        Adapter::Telemetry => (
            true,
            Body::Ready(moxy::template! {
                {{ handler }}(broker, version, correlation_id, body, ctx)
            }),
        ),
        Adapter::HandWritten => return TokenStream::new(),
    };
    let telemetry = matches!(kind, Adapter::Telemetry);
    let context = if telemetry {
        moxy::template! { TelemetryContext }
    } else {
        moxy::template! { RequestContext }
    };
    let signature = moxy::template! {
        fn {{ adapter }}<'a>(
            broker: &'a Broker,
            version: ApiVersion,
            @if telemetry { correlation_id: CorrelationId, }
            body: &'a [u8],
            ctx: &'a {{ context }}<'a>,
        ) -> BoxFuture<'a, Result<Bytes, BrokerError>>
    };
    let block = if traced {
        traced_block(entry, raw_body, body)
    } else {
        match body {
            Body::Future(future) => moxy::template! { Box::pin({{ future }}) },
            Body::Ready(result) => moxy::template! { Box::pin(::std::future::ready({{ result }})) },
        }
    };
    moxy::template! {
        {{ signature }} {
            {{ block }}
        }
    }
}

/// What an adapter computes before it boxes the result.
enum Body {
    /// A future of the handler's encoded result.
    Future(TokenStream),
    /// The handler's encoded result, computed when the adapter is called.
    Ready(TokenStream),
}

/// An adapter's block that runs `body` inside the entry's `handle_<snake>`
/// span and emits an `ERROR` event with the error's `Display` on `Err`.
fn traced_block(entry: &Entry, raw_body: bool, body: Body) -> TokenStream {
    let names = &entry.names;
    let name = LitStr::new(
        &format!("handle_{}", names.snake.text()),
        names.snake.span(),
    );
    let api = LitStr::new(names.api.text(), names.api.span());
    let req_bytes = if raw_body {
        moxy::template! { req_bytes = body.len(), }
    } else {
        TokenStream::new()
    };
    let span = moxy::template! {
        ::tracing::info_span!({{ name }}, api = {{ api }}, version, {{ req_bytes }})
    };
    match body {
        Body::Future(future) => moxy::template! {
            Box::pin(::tracing::Instrument::instrument(
                async move {
                    ({{ future }})
                        .await
                        .inspect_err(|error| ::tracing::error!(error = %error))
                },
                {{ span }},
            ))
        },
        Body::Ready(result) => moxy::template! {
            let span = {{ span }};
            let _entered = span.enter();
            Box::pin(::std::future::ready(
                ({{ result }}).inspect_err(|error| ::tracing::error!(error = %error)),
            ))
        },
    }
}

/// One registration call of the generated `register_dispatch_table`.
struct Registration {
    constructor: Ident,
    api: Ident,
    request_module: Ident,
    adapter: Ident,
}

/// Expands `dispatch_table! { ... }`.
pub(crate) fn expand(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let labels = KINDS.map(|kind| kind.label);
    let sections: Vec<Section> = parse_sections(tokens, "=>", &labels)?;

    let mut adapters = Vec::new();
    let mut registrations = Vec::new();
    for kind in &KINDS {
        for entry in section(&sections, kind.label) {
            let names = &entry.names;
            let adapter_ident = names.snake.suffixed("_adapter");
            match (kind.adapter, &entry.value) {
                (Adapter::HandWritten, Some(value)) => {
                    return Err(ParseError::new(
                        value.span(),
                        format!(
                            "`{}` entries name no handler: the table registers the \
                             hand-written `{}`",
                            kind.label,
                            adapter_ident.text()
                        ),
                    ));
                }
                (Adapter::HandWritten, None) => {}
                (generated, value) => {
                    let handler = value.clone().unwrap_or_else(|| {
                        let snake = &names.snake;
                        moxy::template! { crate::handlers::{{ snake }}::handle }
                    });
                    adapters.push(adapter(
                        generated,
                        kind.traced,
                        &adapter_ident,
                        &handler,
                        entry,
                    ));
                }
            }
            registrations.push(Registration {
                constructor: Ident::new(kind.constructor).with_span(names.api.span()),
                api: names.api.clone(),
                request_module: names.request_module.clone(),
                adapter: adapter_ident,
            });
        }
    }

    Ok(moxy::template! {
        @for adapter in &adapters {
            {{ adapter }}
        }

        fn register_dispatch_table(registry: &mut DispatchRegistry) {
            @for r in &registrations {
                assert2::assert!(
                    registry.register(DispatchEntry::{{ r.constructor }}(
                        ApiKey::{{ r.api }} as i16,
                        krabka_protocol::owned::{{ r.request_module }}::FLEXIBLE_MIN,
                        {{ r.adapter }},
                    )),
                    "duplicate dispatch registration for {:?}",
                    ApiKey::{{ r.api }}
                );
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use moxy::token::TokenStream;

    use super::expand;
    use crate::meta::compact;

    fn expanded(table: &str) -> Result<String, String> {
        let table: TokenStream = table.parse().expect("table tokenizes");
        expand(table)
            .map(|tokens| compact(&tokens))
            .map_err(|error| error.to_string())
    }

    /// The `register_dispatch_table` the expansion of a one-entry table ends
    /// with.
    fn registration(constructor: &str, api: &str, snake: &str) -> String {
        format!(
            "fn register_dispatch_table(registry: &mut DispatchRegistry) {{
                assert2::assert!(
                    registry.register(DispatchEntry::{constructor}(
                        ApiKey::{api} as i16,
                        krabka_protocol::owned::{snake}_request::FLEXIBLE_MIN,
                        {snake}_adapter,
                    )),
                    \"duplicate dispatch registration for {{:?}}\",
                    ApiKey::{api}
                );
            }}"
        )
    }

    /// The signature of an adapter that receives a `RequestContext`.
    fn context_signature(snake: &str) -> String {
        format!(
            "fn {snake}_adapter<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                body: &'a [u8],
                ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>>"
        )
    }

    /// The decode of a request through its owned codec.
    fn decode(snake: &str, request: &str) -> String {
        format!("krabka_protocol::owned::{snake}_request::{request}::decode(&mut cur, version,)?")
    }

    /// Synchronous typed adapters share a decode/encode envelope in the golden expansion.
    fn sync_adapter(snake: &str, api: &str, request: &str, fallible: bool) -> String {
        let error_event = ".inspect_err(|error| ::tracing::error!(error = %error))";
        let fallibility = if fallible { "?" } else { "" };
        format!(
            "{signature} {{
                let span = ::tracing::info_span!(\"handle_{snake}\", api = \"{api}\", version,);
                let _entered = span.enter();
                Box::pin(::std::future::ready(
                    ((|| {{
                        use krabka_protocol::Decode as _;
                        let mut cur = body;
                        let req = {decode};
                        let resp = crate::handlers::{snake}::handle(broker, &req, version, ctx){fallibility};
                        crate::handlers::encode_response(&resp, version)
                    }})()){error_event},
                ))
            }}
            {registration}",
            signature = context_signature(snake),
            decode = decode(snake, request),
            registration = registration("context", api, snake),
        )
    }

    /// Every section kind's expansion of a one-entry table: its adapter, with
    /// or without the `handle_<snake>` span and its `ERROR` event, and its
    /// registration.
    #[test]
    fn each_section_expands_to_its_adapter_and_registration() {
        let error_event = ".inspect_err(|error| ::tracing::error!(error = %error))";
        let cases = [
            (
                "context: Metadata;",
                format!(
                    "{signature} {{
                        Box::pin(::tracing::Instrument::instrument(
                            async move {{
                                (crate::handlers::metadata::handle(broker, version, body, ctx))
                                    .await{error_event}
                            }},
                            ::tracing::info_span!(\"handle_metadata\", api = \"Metadata\", version,
                                req_bytes = body.len(),),
                        ))
                    }}
                    {registration}",
                    signature = context_signature("metadata"),
                    registration = registration("context", "Metadata", "metadata"),
                ),
            ),
            (
                "sync_context: DescribeConfigs => x::describe;",
                format!(
                    "{signature} {{
                        let span = ::tracing::info_span!(\"handle_describe_configs\",
                            api = \"DescribeConfigs\", version, req_bytes = body.len(),);
                        let _entered = span.enter();
                        Box::pin(::std::future::ready(
                            (x::describe(broker, version, body, ctx)){error_event},
                        ))
                    }}
                    {registration}",
                    signature = context_signature("describe_configs"),
                    registration = registration("context", "DescribeConfigs", "describe_configs"),
                ),
            ),
            (
                "typed: ListGroups;",
                format!(
                    "{signature} {{
                        Box::pin(::tracing::Instrument::instrument(
                            async move {{
                                (async move {{
                                    let mut cur = body;
                                    let req = {{
                                        use krabka_protocol::Decode as _;
                                        {decode}
                                    }};
                                    let resp = crate::handlers::list_groups::handle(
                                        broker, req, version, ctx).await?;
                                    crate::handlers::encode_response(&resp, version)
                                }}).await{error_event}
                            }},
                            ::tracing::info_span!(\"handle_list_groups\", api = \"ListGroups\",
                                version,),
                        ))
                    }}
                    {registration}",
                    signature = context_signature("list_groups"),
                    decode = decode("list_groups", "ListGroupsRequest"),
                    registration = registration("context", "ListGroups", "list_groups"),
                ),
            ),
            (
                "typed_own_span: UpdateFeatures;",
                format!(
                    "{signature} {{
                        Box::pin(async move {{
                            let mut cur = body;
                            let req = {{
                                use krabka_protocol::Decode as _;
                                {decode}
                            }};
                            let resp = crate::handlers::update_features::handle(
                                broker, req, version, ctx).await?;
                            crate::handlers::encode_response(&resp, version)
                        }})
                    }}
                    {registration}",
                    signature = context_signature("update_features"),
                    decode = decode("update_features", "UpdateFeaturesRequest"),
                    registration = registration("context", "UpdateFeatures", "update_features"),
                ),
            ),
            (
                "typed_group: Heartbeat;",
                format!(
                    "{signature} {{
                        Box::pin(::tracing::Instrument::instrument(
                            async move {{
                                (async move {{
                                    let mut cur = body;
                                    let req = crate::handlers::decode_group_request::<
                                        krabka_protocol::owned::heartbeat_request::HeartbeatRequest,
                                    >(&mut cur, version)?;
                                    let resp = crate::handlers::heartbeat::handle(
                                        broker, req, version, ctx).await?;
                                    crate::handlers::encode_response(&resp, version)
                                }}).await{error_event}
                            }},
                            ::tracing::info_span!(\"handle_heartbeat\", api = \"Heartbeat\",
                                version,),
                        ))
                    }}
                    {registration}",
                    signature = context_signature("heartbeat"),
                    registration = registration("context", "Heartbeat", "heartbeat"),
                ),
            ),
            (
                "typed_sync: DescribeAcls;",
                sync_adapter("describe_acls", "DescribeAcls", "DescribeAclsRequest", true),
            ),
            (
                "typed_infallible: ListConfigResources;",
                sync_adapter(
                    "list_config_resources",
                    "ListConfigResources",
                    "ListConfigResourcesRequest",
                    false,
                ),
            ),
            (
                "auth: CreateDelegationToken;",
                registration("auth", "CreateDelegationToken", "create_delegation_token"),
            ),
            (
                "telemetry: PushTelemetry;",
                format!(
                    "fn push_telemetry_adapter<'a>(
                        broker: &'a Broker,
                        version: ApiVersion,
                        correlation_id: CorrelationId,
                        body: &'a [u8],
                        ctx: &'a TelemetryContext<'a>,
                    ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {{
                        let span = ::tracing::info_span!(\"handle_push_telemetry\",
                            api = \"PushTelemetry\", version, req_bytes = body.len(),);
                        let _entered = span.enter();
                        Box::pin(::std::future::ready(
                            (crate::handlers::push_telemetry::handle(
                                broker, version, correlation_id, body, ctx)){error_event},
                        ))
                    }}
                    {registration}",
                    registration = registration("telemetry", "PushTelemetry", "push_telemetry"),
                ),
            ),
        ];

        for (table, expected) in cases {
            check!(expanded(table) == Ok(compact(&expected)), "{table}");
        }
    }

    #[test]
    fn an_empty_table_registers_nothing() {
        assert!(
            expanded("")
                == Ok(compact(
                    "fn register_dispatch_table(registry: &mut DispatchRegistry) {}"
                ))
        );
    }

    /// The table refuses what it cannot register, with the error that names
    /// the mistake.
    #[test]
    fn a_malformed_table_is_refused() {
        let labels = "context, sync_context, typed, typed_own_span, typed_group, typed_sync, \
                      typed_infallible, auth, telemetry";
        for (table, expected) in [
            (
                "typed_sync_own_span: DescribeAcls;",
                format!("unknown section `typed_sync_own_span`; expected one of: {labels}"),
            ),
            (
                "typed: ListGroups; context: ListGroups;",
                "`ListGroups` appears twice in the table".to_owned(),
            ),
            (
                "typed: ListGroups =>;",
                "expected tokens after `=>`".to_owned(),
            ),
            (
                "auth: CreateDelegationToken => x::handle;",
                "`auth` entries name no handler: the table registers the hand-written \
                 `create_delegation_token_adapter`"
                    .to_owned(),
            ),
        ] {
            check!(expanded(table) == Err(expected), "{table}");
        }
    }
}
