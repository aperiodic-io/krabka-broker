//! Derive macros for the krabka broker.
//!
//! `krabka-macros` turns one annotated declaration into code that the broker
//! would otherwise write out by hand two or three times. It is built on
//! [`moxy`], which supplies the token, syntax-tree, attribute-parsing and
//! template layers.

//!
//! # `RegisterMetrics`
//!
//! `#[derive(RegisterMetrics)]` goes on a struct of `prometheus-client` metric
//! handles. It adds two private methods to the struct:
//!
//! - `fn unregistered() -> Self` constructs every field: with
//!   `Default::default()`, with a histogram over `buckets`, or with the `new`
//!   expression.
//! - `fn register(&self, registry: &mut Registry)` registers a clone of every
//!   field that is not `skip`, in field order. The order of the fields is the
//!   order of the families in the text exposition.
//!
//! Each field takes one optional `#[metric(...)]` attribute:
//!
//! - `help = "..."` â€” the help text. Every registered field needs one.
//! - `name = "..."` â€” the registered name. The default is the field name with
//!   a trailing `_total` removed, because `prometheus-client` appends `_total`
//!   to the name of a counter itself.
//! - `buckets = EXPR` â€” construct a `Histogram` over `EXPR`. On a
//!   `Family<_, Histogram>` field, construct the family so that each of its
//!   histograms uses `EXPR`.
//! - `new = EXPR` â€” construct the field with `EXPR`.
//! - `skip` â€” construct the field, but do not register it.
//!
//! ```ignore
//! #[derive(krabka_macros::RegisterMetrics)]
//! struct Metrics {
//!     #[metric(help = "Records received")]
//!     records_total: Counter,
//!     #[metric(help = "Request latency in seconds", buckets = [0.001, 0.01, 0.1])]
//!     latency_seconds: Family<ApiLabel, Histogram>,
//! }
//! ```
//!
//! # `human_units`
//!
//! `#[human_units]` goes on a file-config struct, above its
//! `#[derive(Deserialize, Serialize, JsonSchema)]`. Each field typed
//! `Option<Time>`, `Option<ByteSize>` or `Option<Ratio>` (under any path
//! prefix) gets two attributes:
//!
//! - `#[serde(default, with = "krabka_units::serde_units::human::option_X")]`,
//!   so that the TOML file writes the value as a string with a unit, such as
//!   `"5s"` or `"1MiB"`. `default` is left out when the field already has a
//!   serde `default`.
//! - `#[schemars(with = "Option<crate::file_config::schema_units::X>")]`, so
//!   that the JSON Schema names the unit in its `format`.
//!
//! A field with its own serde `with`, `deserialize_with` or `serialize_with`
//! keeps its codec and gets neither attribute. Every other field passes through
//! unchanged.
//!
//! ```ignore
//! #[krabka_macros::human_units]
//! #[derive(Deserialize, Serialize, JsonSchema)]
//! struct FileTimeouts {
//!     request_timeout: Option<Time>,
//!     segment_bytes: Option<ByteSize>,
//! }
//! ```
//!
//! # `dispatch_table!`
//!
//! `dispatch_table! { ... }` goes in the broker's dispatch registry module. It
//! reads sections of `CamelCase` Kafka api names, each a label, a colon, and
//! comma-separated entries closed by `;`:
//!
//! ```ignore
//! krabka_macros::dispatch_table! {
//!     context: Metadata, AddPartitionsToTxn => crate::txn::handlers::add_partitions_to_txn::handle;
//!     sync_context: DescribeConfigs;
//!     typed: ListGroups;
//!     typed_group: Heartbeat;
//!     typed_sync: DescribeAcls;
//!     typed_infallible: ListConfigResources;
//!     auth: CreateDelegationToken;
//!     telemetry: PushTelemetry;
//! }
//! ```
//!
//! From each name, such as `CreateAcls`, it derives the adapter
//! `create_acls_adapter`, the `ApiKey::CreateAcls` variant, the request module
//! `krabka_protocol::owned::create_acls_request` and its `CreateAclsRequest`
//! type. The handler is `crate::handlers::create_acls::handle` unless the entry
//! names another with `=> path`.
//!
//! - `context` and `sync_context` adapters call
//!   `handler(broker, version, body, ctx)` on the raw body; a `sync_context`
//!   handler returns its result instead of a future. A handler that needs the
//!   request's correlation id reads it from the context.
//! - `typed` adapters decode the request, await
//!   `handler(broker, request, version, ctx)` for a
//!   `Result<Response, BrokerError>`, and encode the response with
//!   `crate::handlers::encode_response`. `typed_group` adapters do the same
//!   but decode through `crate::handlers::decode_group_request`.
//! - `typed_sync` adapters decode the request, call
//!   `handler(broker, &request, version, ctx)` for a
//!   `Result<Response, BrokerError>` without awaiting it, encode the response,
//!   and wrap the result in a ready future. `typed_infallible` adapters do the
//!   same for a handler that returns the `Response` itself.
//! - `telemetry` adapters pass a `TelemetryContext` and wrap a synchronous
//!   result.
//! - `auth` entries generate no adapter: the hand-written `<name>_adapter`
//!   must be in scope.
//! - `typed_own_span` generates the `typed` adapter without the span below,
//!   for a handler that keeps a `#[tracing::instrument]` of its own.
//!
//! Every other generated adapter runs the decode, the handler and the encode
//! inside an `info` span named `handle_<snake_name>`, with `api = "<Name>"`,
//! `version` and, for the `context`, `sync_context` and `telemetry` sections,
//! `req_bytes = body.len()`. On `Err` it emits, inside that span, the event
//! `#[tracing::instrument(err)]` would: `ERROR` with `error` set to the
//! error's `Display`. The expansion calls `::tracing`, so the calling crate
//! depends on it, and `BrokerError` implements `Display`.
//!
//! It then emits `fn register_dispatch_table(registry: &mut DispatchRegistry)`,
//! which registers every entry at the request schema's `FLEXIBLE_MIN` and
//! panics on a duplicate registration. The expansion names `Broker`,
//! `ApiVersion`, `CorrelationId`, `RequestContext`, `TelemetryContext`,
//! `BoxFuture`, `Bytes`, `BrokerError`, `ApiKey`, `DispatchEntry` and
//! `DispatchRegistry` unqualified, so the calling module imports them.
//!
//! # `throttle_probes!`
//!
//! `throttle_probes! { ... }` is an expression for the KIP-219 throttle-echo
//! audit: an array of `(API_KEY, probe as Probe)`, one per entry, keyed by the
//! response module's generated `API_KEY`. Its sections are `throttled` (the
//! response has `throttle_time_ms`, which the probe sets to `SENTINEL`),
//! `unthrottled` (it has none), and `legacy_split: Name = version`, whose
//! probe encodes the `kafka_3_6_2` response below `version`. The probes call
//! `position`, `ApiVersion`, `ThrottlePosition`, `SENTINEL` and `Probe` from
//! the calling module.
//!
//! ```ignore
//! let probes: BTreeMap<_, _> = krabka_macros::throttle_probes! {
//!     throttled: Metadata, ListOffsets;
//!     unthrottled: SaslHandshake;
//!     legacy_split: Produce = 3, Fetch = 4;
//! }
//! .into_iter()
//! .collect();
//! ```
//!
//! # `RuntimeOverlay`
//!
//! `#[derive(RuntimeOverlay)]` goes on a clap argument group whose fields
//! overlay the same-named fields of a config struct. The struct names that
//! config with `#[overlay(target = Type)]`. The derive adds
//! `pub(crate) fn copy_into(&self, target: &mut Type)`, which assigns every
//! field in field order:
//!
//! - by default, `target.f = self.f`, so the field must be `Copy`;
//! - with `#[overlay(refined)]`, `target.f = self.f.map(|value| value.into_value())`,
//!   for an `Option` of a refined newtype;
//! - with `#[overlay(clone)]`, `target.f.clone_from(&self.f)`;
//! - with `#[overlay(skip)]`, not at all.
//!
//! A field that the target lacks, or whose type differs, fails to compile.
//!
//! ```ignore
//! #[derive(clap::Args, krabka_macros::RuntimeOverlay)]
//! #[overlay(target = RuntimeFileConfig)]
//! struct RuntimeArgs {
//!     cleaner_interval: Option<Time>,
//!     #[overlay(refined)]
//!     num_partitions: Option<PositiveI32>,
//! }
//! ```
//!
//! # `krabka_env`
//!
//! `#[krabka_env]` goes on a clap argument struct, above its
//! `#[derive(clap::Args)]`. Each field without an `#[arg(...)]` of its own gets
//! `#[arg(long, env = "KRABKA_<FIELD>", ...)]`, where `<FIELD>` is the field
//! name in upper case and the rest follows from the field type:
//!
//! | Field type | Added argument |
//! |---|---|
//! | `Option<Time>` | `value_parser = krabka_units::parse::positive_time` |
//! | `Option<ByteSize>` | `value_parser = krabka_units::parse::positive_byte_size` |
//! | `Option<Ratio>` | `value_parser = krabka_units::parse::positive_ratio` |
//! | `Option<PositiveCount>` | `value_parser = krabka_broker::config_value::parse_positive_count` |
//! | `Option<PositiveI16>`, `I32`, `I64` | the matching `parse_positive_*` |
//! | `Option<u32>` | `value_parser = clap::value_parser!(u32).range(1..)` |
//! | `Option<i64>` | `value_parser = clap::value_parser!(i64).range(0..)` |
//! | `Option<bool>` | `action = clap::ArgAction::Set` |
//! | `Option<i16>`, `Option<i32>`, `Option<String>`, `Option<PathBuf>` | none |
//!
//! The type is matched as written, so a field of any other type, or of one
//! of these that needs another parser, keeps its own `#[arg(...)]`. Doc
//! comments stay where they are and remain the help text.
//!
//! `#[krabka_env(prefix = "BENCH_")]` puts every variable under that prefix in
//! place of `KRABKA_`, so the field `tls_ca_path` reads `BENCH_TLS_CA_PATH`.
//! A field whose variable is not its name under the prefix keeps its own
//! `#[arg(...)]`.
//!
//! # `RefinedNewtype`
//!
//! `#[derive(RefinedNewtype)]` goes on a tuple struct with one field, the value
//! a `refined_type` rule checks. It adds `new(value) -> Result<Self, E>`, which
//! validates through the rule, and a `#[must_use] const` getter that returns
//! the value. Both take the struct's visibility. One `#[refined(...)]`
//! attribute configures it:
//!
//! - `rule(<type>)` â€” the `refined_type` rule, such as `GreaterU32<0>`.
//!   Required. It goes in parentheses because a generic type does not parse
//!   after `=`.
//! - `getter = name` â€” the getter's name. The default is `into_value`.
//! - `string_error` â€” `new` returns `String`, the rule's error text, rather
//!   than `refined_type::result::Error<T>`.
//! - `label = "..."` â€” with `string_error`, `new` returns
//!   `"<label>: <rule error>"`.
//! - `default = EXPR` â€” implement `Default` as `Self::new(EXPR)`, which panics
//!   when `EXPR` breaks the rule.
//! - `from_str` â€” implement `FromStr` with `Err = String`: the field type's
//!   own `parse`, then `new`, each error as its text.
//! - `display` â€” implement `Display` as the field's.
//! - `parse_fn = name` â€” add a free `fn name(&str) -> Result<Self, String>`
//!   that parses the way `from_str` does.
//! - `quantity = ByteSize`, `Time` or `Frequency` â€” the field is a whole
//!   number of bytes, milliseconds or Hz, and `new` takes the `krabka_units`
//!   quantity. It refuses a fraction, an infinity, or a count the field type
//!   cannot hold with `"<label>: must be a whole number of <unit> that fits in
//!   <field type>"`, then hands the count to the rule. It implies
//!   `string_error`, adds `TryFrom<quantity>` through `new`, makes `from_str`
//!   parse with `krabka_units::parse` and `display` print the quantity's
//!   `human()` text, and makes `default` take a quantity.
//! - `quantity_getter = name` â€” with `quantity`, add a getter that returns the
//!   value as its quantity.
//!
//! ```ignore
//! #[derive(Clone, Copy, krabka_macros::RefinedNewtype)]
//! #[refined(rule(GreaterU32<0>), string_error, label = "fetch miss limit", getter = get,
//!           default = 3, from_str, display)]
//! pub struct FetchMissLimit(u32);
//!
//! #[derive(Clone, Copy, krabka_macros::RefinedNewtype)]
//! #[refined(rule(GreaterI32<0>), quantity = ByteSize, label = "fetch max", getter = bytes,
//!           quantity_getter = size, default = mebibytes(8), from_str, display)]
//! pub struct FetchMax(i32);
//! ```
//!
//! # `PrimitiveCmp`
//!
//! `#[derive(PrimitiveCmp)]` goes on a tuple struct with one field. It
//! implements `PartialEq` and `PartialOrd` between the struct and the field's
//! type in both directions, so that `Seq(3) == 3_u64` and `2_u64 < Seq(3)`
//! compile.
//!
//! # `EnumStr`
//!
//! `#[derive(EnumStr)]` goes on an enum whose variants each stand for one
//! fixed text, such as a metric label value or a config value. It adds a
//! `#[must_use] const fn as_str(self) -> &'static str` with the enum's
//! visibility. The method takes `&self` when any variant has fields, which
//! its arm matches with `{ .. }` or `(..)`. One optional `#[enum_str(...)]`
//! attribute on the enum configures it:
//!
//! - `case = "..."` â€” how a variant name becomes its text: `"snake_case"`,
//!   `"kebab-case"`, `"lowercase"` or `"UPPERCASE"`. Without it the text is
//!   the variant name unchanged.
//! - `as_str = name` â€” the method's name.
//! - `parse` or `parse = name` â€” add `fn parse(&str) -> Option<Self>`, under
//!   that name, over the unit variants.
//! - `all` â€” add `const ALL: [Self; N]`, every variant in declaration order.
//!   Every variant must be a unit variant.
//! - `label_value` â€” implement `prometheus_client`'s `EncodeLabelValue` as the
//!   text. The crate must depend on `prometheus-client`.
//!
//! A variant's own `#[enum_str(...)]` takes `name = "..."`, its text in place
//! of the cased name, and `alias = "..."` or `alias("...", "...")`, further
//! text that `parse` accepts.
//!
//! ```ignore
//! #[derive(Clone, Copy, krabka_macros::EnumStr)]
//! #[enum_str(case = "snake_case", as_str = config_name, parse = from_config_name)]
//! pub enum AssignorKind {
//!     Auto,
//!     #[enum_str(alias = "highly-available")]
//!     HighlyAvailable,
//! }
//! ```
//!
//! # `FieldDefaults`
//!
//! `#[derive(FieldDefaults)]` goes on a struct with named fields in place of a
//! hand-written `impl Default` that only fills each field with a fixed
//! expression. Each field takes one optional `#[default(EXPR)]` attribute and
//! starts as `EXPR`; a field without one starts as `Default::default()`. The
//! expression is evaluated each time `default()` runs, so it may call
//! functions, and it is written out as given, so it must name what it uses in
//! the scope of the struct. The derive adds no bounds: a generic struct
//! writes the bounds its fields need on the struct itself.
//!
//! The name differs from `Default` because the standard derive owns the bare
//! name and its own `#[default]` attribute on enum variants. Do not derive
//! both on one struct.
//!
//! ```ignore
//! #[derive(krabka_macros::FieldDefaults)]
//! pub struct FreezeConfig {
//!     #[default(Duration::from_secs(30))]
//!     pub poll_interval: Duration,
//!     #[default(true)]
//!     pub enabled: bool,
//!     pub owners: Vec<String>,
//! }
//! ```
//!
//! # `cli_main!`
//!
//! `cli_main!(path)` goes at the top level of an operator binary's
//! `main.rs`, in place of its `main`. It expands to a `#[tokio::main]`
//! `async fn main()` that installs a `tracing_subscriber::fmt()` subscriber
//! filtered by `RUST_LOG`, or at `info` when `RUST_LOG` is unset or does not
//! parse, then awaits `path::run_from_args(std::env::args_os())` and exits the
//! process with the `i32` it returns. Any arguments after the path, such as
//! `flavor = "multi_thread"`, go to `#[tokio::main(...)]` unchanged. The
//! binary depends on `tokio` with its `macros` feature and on
//! `tracing-subscriber`.
//!
//! ```ignore
//! krabka_macros::cli_main!(krabka_restore, flavor = "multi_thread");
//! ```
//!
//! # Group timings and epoch values
//!
//! `#[derive(GroupTimingConfig)]` adds `for_group` and the test-only
//! `assigning_at_once` constructor to a membership config. Its named timing
//! fields and bounds remain visible on the struct. Select the stored keys with
//! `#[group_timing(session = KEY, heartbeat = KEY, assignment = KEY)]`.
//! The implementation uses the broker's existing group timing parser and bounds.
//!
//! `#[derive(GroupLifecycle)]` adds epoch changes, session expiry queries, and
//! member eviction to a group state. Select its existing policy with
//! `#[group_lifecycle(protocol = "consumer" | "share" | "streams")]`.
//! Consumer and share groups get assignment delay checks; consumer and streams
//! groups get rebalance fencing. A successful streams epoch change marks the
//! group dirty. All removals call the state's own `remove_member` method.
//!
//! `#[derive(FlexibleEpochValue)]` adds the version-zero epoch codec to a
//! named struct with an `i32` epoch followed by an `i64` tagged value.
//! `#[epoch_value(record = "RecordName")]` supplies the decoding error name.
//! A zero tagged value is omitted on encode and restored on decode.
//!
//! `#[flexible_value(record = "KafkaRecord")]` goes on an inherent impl with
//! private `encode_fields(self or &self, buf: &mut BytesMut)` and
//! `decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError>` methods.
//! It retains those methods and adds the public framed codecs. The field
//! methods keep wire order explicit; the encoder retains its receiver ownership.
//!
//! `#[derive(KafkaUuid)]` adds display, parsing, and string serde to a UUID
//! wrapper. It calls the declaration's existing `encode`, `decode`, and
//! `KafkaUuidError` helpers. Display retains the formatter's padding rules.
//!
//! # Handler admission
//!
//! `#[voter_admission(request = TYPE, api = KEY, gate = FUNCTION, refusals = EXPR)]`
//! goes on an ordinary async `handle(broker, version, req_bytes, ctx)` function.
//! It runs the shared voter prelude before the operation body and returns early
//! for an encoded refusal. The body retains its original tokens and scopes.
//!
//! `share_state_handler!(Request, Response, TopicRow, PartitionRow, serve)`
//! declares a share-state handler. A denied request returns an error row for
//! every requested partition in request order. An allowed request calls `serve`
//! with the share coordinator and a fresh metadata snapshot.
//!
//! # Storage methods
//!
//! `#[derive(SparseIndex)]` adds the existing loader and file operations to a
//! sparse index. `#[sparse_index(kind = "offset" | "time")]` selects its raw
//! entry layout and I/O target. The loader retains its monotonic-entry checks.
//! `#[index_open]` adds the same tracing span to other ordinary index loaders.
//!
//! `#[remote_operation]` traces an ordinary remote-storage SPI method. Its
//! existing method name selects the span level and fields. It retains the
//! native method body, parameter names, and synchronous signature.
//!
//! `#[model_type]` attaches the existing proof and runtime derives to an
//! ordinary struct or enum. `proof(...)` and `runtime(...)` replace only the
//! selected derive list, preserving its path order and the declaration tokens.
//!
//! `#[sendfile_cfg]` gates one ordinary item with the fetch path's shared
//! platform predicate. Apply it separately to imports, exports and methods.
//!
//! `#[decode_dispatch(request = TYPE, binding(req))]` decodes a request before
//! the handler's native body. `binding(mut req)` retains an owned mutable
//! request. `decoded_dispatch!` accepts the same explicit function, with
//! `[request = TYPE, binding(req)]` after its parameters and before its return type.
//!
//! `#[extension_wire(name = "SPAN", api = "API", level = "info")]` adapts an
//! ordinary operation with broker, owned request, version and context parameters.
//! It emits the raw five-argument entry point and traces both decoding and the
//! native operation. `"debug"` retains the marker API's tracing level.
//!
//! `worm_object_set!` emits one ordered admission rule for its `Model` and
//! `Runtime` declarations. Function headers and count observations stay native;
//! proof projections remain inside the caller's Pearlite scope.
//!
//! # Direct runtime and response tables
//!
//! `set_runtime!` applies optional fields in table order. Each group names a
//! validator. `plain` assigns directly; `duration` validates positive time
//! and converts it to `Duration`. A mapping selects a different target field,
//! and `in` supplies the validator's range argument. An absent field retains
//! the target value. A validation error stops later assignments.
//!
//! ```ignore
//! krabka_macros::set_runtime! {
//!     runtime => config;
//!     plain: enabled;
//!     duration: heartbeat_interval;
//!     in_range: max_size => group.max_size in 1..=1000;
//! }
//! ```
//!
//! `response_errors!` implements the caller's error traits for external
//! protocol types. Its four sections are `response`, `code_response`, `row`
//! and `resource_row`. Each section contains type paths separated by commas
//! inside braces. A resource row also implements `ErrorRow`, so
//! each resource type is listed once. The traits remain in the caller's scope.
//!
//! `metric_registry_fixture!(metrics, output, guard; expect("encode"))`
//! renders directly into the named bindings. The caller retains the registry
//! lock until it drops `guard`. Use `unwrap()` to select that result policy.
//!
//! `broker_config! { pub struct BrokerConfig { ... } }` constructs the broker
//! config in one expansion. The fields stay in declaration order with their
//! original tokens, documentation and attributes. It adds `Debug` and `Clone`.
//!
//! `#[membership_defaults]` goes before `FieldDefaults` on a membership config.
//! It assigns the shared defaults to the six named session and heartbeat
//! fields. An existing default on one of those fields is an input error.
//!
//! `#[derive(ShareStateValue)]` writes scalar fields in declaration order,
//! followed by state batches and the delivery-count tag. The existing batch,
//! version and tag helpers retain their byte layouts and validation. Supply
//! method documentation with `#[share_state(encode_doc = "...", decode_doc = "...")]`.
//!
//! `#[derive(FlexibleArrayValue)]` wraps one `Vec` field in version-zero
//! framing. `#[array_value(record = "RecordName", encode = writer, decode = reader)]`
//! selects the existing array callbacks and decoding error name.
//!
//! `tagged_wire!` and `unthrottled_wire!` construct exhaustive struct values.
//! Both supply an empty tagged-field set; the second supplies zero throttle.
//! Every remaining field stays required. Struct updates with `..` are refused,
//! and field expressions retain their original tokens and evaluation order.

use {
    moxy::{
        ast::{ItemEnum, ItemStruct, ParseError},
        token::TokenStream,
    },
    proc_macro::TokenStream as NativeTokenStream,
};

mod api_names;
mod async_write_delegate;
mod auth_fixtures;
mod background_controllers;
mod bound_start_fixture;
mod broker_config;
mod cli_main;
mod cli_support;
mod config_table;
mod connection_policy;
mod container_fixtures;
mod cross_storage_fixtures;
mod decode_dispatch;
mod deterministic_random;
mod dispatch;
mod enum_str;
mod extension_wire;
mod field_defaults;
mod fixtures;
mod flexible_array_value;
mod flexible_epoch_value;
mod flexible_value;
mod gcs_fields;
mod group_lifecycle;
mod group_timing_config;
mod human_units;
mod kafka_uuid;
mod krabka_env;
mod legacy_fixtures;
mod membership_defaults;
mod meta;
mod metrics;
mod model_type;
mod named_acl_gates;
mod network_fixtures;
mod newtype_styles;
mod node_endpoints;
mod object_ops;
mod object_store_config;
mod object_store_delegate;
mod otlp_exporters;
mod prelude;
mod primitive_cmp;
mod produce_fixtures;
mod projection_identity;
mod raft_fixtures;
mod refined_newtype;
mod remote_metadata_fixtures;
mod remote_operation;
mod replay_member_maps;
mod response_encoding;
mod runtime_assignments;
mod runtime_overlay;
mod runtime_policy_fields;
mod s3_fields;
mod sendfile_cfg;
mod sendfile_match;
mod share_state_handler;
mod share_state_value;
mod sparse_index;
mod throttle_probes;
mod timer_hooks;
mod timestamped_batch;
mod topic_filter;
mod transport_errors;
mod value_framing;
mod voter_admission;
mod wire_codec_helpers;
mod wire_codes;
mod wire_fixtures;
mod wire_state_batch;
mod wire_values;
mod worm_object_set;

/// Append the shared topic, topic-id and partition setters of Produce fixtures.
#[moxy::attribute]
pub fn share_produce_parameters(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    wire_fixtures::share_produce_parameters(meta, item)
}

/// Share sparse-window and nonnegative base/floor preconditions between timestamp kernels.
#[moxy::attribute]
pub fn retained_timestamp_bounds(
    floor: TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let expanded = wire_fixtures::retained_timestamp_bounds(floor, item.into())?;
    // Creusot captures and MIR bindings must share one identifier context.
    Ok(expanded
        .to_string()
        .parse()
        .expect("valid timestamp bounds expansion"))
}

/// Derives `unregistered` and `register` for a struct of metric handles. The
/// crate documentation lists the field attributes.
#[moxy::derive(RegisterMetrics, attributes(metric))]
pub fn register_metrics(item: ItemStruct) -> Result<TokenStream, ParseError> {
    metrics::expand(item)
}

// Keep implementations in their own modules and document each adapter below.

/// Give a scalar id transparent JSON, primitive comparisons and integer conversions.
#[moxy::attribute]
pub fn scalar_id(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    newtype_styles::scalar_id(meta, item)
}

/// Gives a test scalar the shared display, conversion, value, and default traits.
#[moxy::attribute]
pub fn test_scalar(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    newtype_styles::test_scalar(meta, item)
}

/// Share the lowercase hex display and serde identity of byte newtypes.
#[moxy::attribute]
pub fn hex_bytes_identity(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    newtype_styles::hex_bytes_identity(meta, item)
}

/// Gives every `Option<Time>`, `Option<ByteSize>` and `Option<Ratio>` field of
/// a file-config struct its human-unit serde codec and schema marker. The
/// crate documentation describes the expansion.
#[moxy::attribute(name = "human_units")]
pub fn human_units(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    human_units::expand(meta, item)
}

/// Derives `new`, a getter, and optionally `Default`, `FromStr`, `Display`, a
/// free parse function and whole-unit quantity conversions for a tuple newtype
/// over a `refined_type` rule. The crate documentation lists the
/// `#[refined(...)]` arguments.
#[moxy::derive(RefinedNewtype, attributes(refined))]
pub fn refined_newtype(item: ItemStruct) -> Result<TokenStream, ParseError> {
    refined_newtype::expand(item)
}

/// Derives `PartialEq` and `PartialOrd` in both directions between a tuple
/// newtype and the type of its one field.
#[moxy::derive(PrimitiveCmp)]
pub fn primitive_cmp(item: ItemStruct) -> Result<TokenStream, ParseError> {
    primitive_cmp::expand(item)
}

/// Generates the broker's dispatch adapters and `register_dispatch_table`
/// from one table of api names. The crate documentation describes the table.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn dispatch_table(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    dispatch::expand(tokens)
}

/// Expands to the throttle-echo audit's `[(API_KEY, Probe); N]` array from one
/// table of api names. The crate documentation describes the table.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn throttle_probes(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    throttle_probes::expand(tokens)
}

/// Derives `copy_into`, which copies each field of a clap argument group onto
/// the same-named field of its `#[overlay(target = Type)]`. The crate
/// documentation lists the field attributes.
#[moxy::derive(RuntimeOverlay, attributes(overlay))]
pub fn runtime_overlay(item: ItemStruct) -> Result<TokenStream, ParseError> {
    runtime_overlay::expand(item)
}

/// Gives every field of a clap argument struct without an `#[arg(...)]` a
/// `--long` flag, a `KRABKA_<FIELD>` environment variable (under another
/// prefix with `prefix = "..."`), and the value parser its type implies. The
/// crate documentation lists the types.
#[moxy::attribute(name = "krabka_env")]
pub fn krabka_env(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    krabka_env::expand(meta, item)
}

/// Derives `as_str`, and optionally a parse function, `ALL` and
/// `EncodeLabelValue`, for an enum whose variants each stand for one text.
/// The crate documentation lists the `#[enum_str(...)]` arguments.
#[moxy::derive(EnumStr, attributes(enum_str))]
pub fn enum_str(item: ItemEnum) -> Result<TokenStream, ParseError> { enum_str::expand(item) }

/// Derives `Default` from each field's `#[default(EXPR)]`, or
/// `Default::default()` for a field without one. The crate documentation
/// describes the expansion.
#[moxy::derive(FieldDefaults, attributes(default))]
pub fn field_defaults(item: ItemStruct) -> Result<TokenStream, ParseError> {
    field_defaults::expand(item)
}

/// Derive clamped per-group timings and immediate-assignment test defaults.
#[moxy::derive(GroupTimingConfig, attributes(group_timing))]
pub fn group_timing_config(item: ItemStruct) -> Result<TokenStream, ParseError> {
    group_timing_config::expand(item)
}

/// Derive group transitions with the selected protocol's existing lifecycle policy.
#[moxy::derive(GroupLifecycle, attributes(group_lifecycle))]
pub fn group_lifecycle(item: ItemStruct) -> Result<TokenStream, ParseError> {
    group_lifecycle::expand(item)
}

/// Derive display, parsing, and string serde using the caller's Kafka UUID codec.
#[moxy::derive(KafkaUuid)]
pub fn kafka_uuid(item: ItemStruct) -> Result<TokenStream, ParseError> { kafka_uuid::expand(item) }

/// Derive the sparse index's existing loader and file operations.
#[moxy::derive(SparseIndex, attributes(sparse_index))]
pub fn sparse_index(item: ItemStruct) -> Result<TokenStream, ParseError> {
    sparse_index::expand(item)
}

/// Apply the shared index-open tracing span to an ordinary loader method.
#[moxy::attribute]
pub fn index_open(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    sparse_index::index_open(meta, item)
}

/// Trace an ordinary remote-storage operation with its segment identity.
#[moxy::attribute]
pub fn remote_operation(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    remote_operation::expand(meta, item)
}

/// Attach the selected proof and runtime derives to an ordinary model declaration.
#[moxy::attribute]
pub fn model_type(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    model_type::expand(meta, item)
}

/// Keep an ordinary item on the platforms supported by the sendfile fetch path.
#[moxy::attribute]
pub fn sendfile_cfg(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    sendfile_cfg::expand(meta, item)
}

/// Decode the selected request before an ordinary raw dispatch function's body.
#[moxy::attribute]
pub fn decode_dispatch(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    decode_dispatch::expand(&meta.into(), item)
}

/// Decode a request using bracketed metadata after the function parameters.
#[moxy::function]
pub fn decoded_dispatch(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    decode_dispatch::expand_function(tokens)
}

/// Emit the ordered WORM object-set rule around an explicit runtime or proof header.
#[moxy::function]
pub fn worm_object_set(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    worm_object_set::expand(tokens)
}

/// Adapt an explicitly typed extension operation to its decoded and traced raw entry point.
#[moxy::attribute]
pub fn extension_wire(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    extension_wire::expand(&meta.into(), item)
}

/// Derive the controller adapter's three background-task trait implementations.
/// The struct has `handle` and `node_id` fields, plus `holds_leadership` and
/// `submit` methods. The derive delegates to the existing metadata source.
#[moxy::derive(BackgroundControllers)]
pub fn background_controllers(item: ItemStruct) -> Result<TokenStream, ParseError> {
    background_controllers::expand(item)
}

/// Apply the existing retry and client options to an object-store builder expression.
/// Accepts `connection_policy!(builder_expression, config_identifier)` and uses
/// the caller's `retry_config`, `client_options`, and `ClientOptions` helpers.
#[moxy::function]
pub fn connection_policy(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    connection_policy::expand(tokens)
}

/// Declare wire constants and their ordered release, trunk, and private guard tables.
/// Accepts `release TABLE { NAME = -1; } trunk TABLE { ... } private TABLE { ... }`.
/// Each section emits public i16 constants and a private test-only `NamedCode` table.
#[moxy::function]
pub fn wire_codes(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    wire_codes::expand(&tokens.into())
}

/// Implement transport selection for a typed OTLP exporter method declaration.
/// Accepts an ordinary `fn method(&self) -> Result<SpanExporter, TelemetryError>;`
/// declaration, or the same signature with `LogExporter`, inside `OtlpConfig`.
#[moxy::function]
pub fn otlp_exporter(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    otlp_exporters::expand(tokens)
}

/// Emit fixed ACL gates from `name: Group(operation, parameter)` or
/// `name: Cluster(operation)` declarations, using the caller's audited ACL helper.
#[moxy::function]
pub fn named_acl_gates(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    named_acl_gates::expand(&tokens.into())
}

/// Supply `Allow` or `Deny` to the shared topic-selection helper while keeping
/// the function's five parameter types and names explicit.
#[moxy::attribute]
pub fn topic_filter(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    topic_filter::expand(meta.into(), item)
}

/// Emit explicit request/response codec and context helper declarations directly.
/// For example: `encode_request(Request); decode_response(Response); test_context ["test"];`.
/// Add `version [EXPR];` to pin the codecs, or omit it to accept a version per call.
#[moxy::function]
pub fn wire_codec_helpers(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    wire_codec_helpers::expand(tokens)
}

/// Derive endpoint projections into the wire types listed by `node_endpoints(targets(...))`.
#[moxy::derive(NodeEndpoints, attributes(node_endpoints))]
pub fn node_endpoints(item: ItemStruct) -> Result<TokenStream, ParseError> {
    node_endpoints::expand(item)
}

/// Derive read/write share-state batch conversions with their existing ownership directions.
/// `#[wire_state_batch(read = ReadBatch, write = WriteBatch)]` selects both target types.
#[moxy::derive(WireStateBatch, attributes(wire_state_batch))]
pub fn wire_state_batch(item: ItemStruct) -> Result<TokenStream, ParseError> {
    wire_state_batch::expand(item)
}

/// Derive member-map replay methods for the selected consumer, share, or streams protocol.
/// Select the protocol with `#[replay_member_maps(protocol = "consumer")]`, `"share"`, or `"streams"`.
#[moxy::derive(ReplayMemberMaps, attributes(replay_member_maps))]
pub fn replay_member_maps(item: ItemStruct) -> Result<TokenStream, ParseError> {
    replay_member_maps::expand(item)
}

/// Add flexible value framing around explicitly ordered private field codecs.
#[moxy::attribute]
pub fn flexible_value(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    flexible_value::expand(&meta.into(), item)
}

/// Add the shared timing defaults before `FieldDefaults` derives the config constructor.
#[moxy::attribute]
pub fn membership_defaults(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    membership_defaults::expand(meta, item)
}

/// Derive the version-zero epoch codec with an optional i64 value in tag zero.
#[moxy::derive(FlexibleEpochValue, attributes(epoch_value))]
pub fn flexible_epoch_value(item: ItemStruct) -> Result<TokenStream, ParseError> {
    flexible_epoch_value::expand(item)
}

/// Derive the existing share-state value codecs from their ordered fields.
#[moxy::derive(ShareStateValue, attributes(share_state))]
pub fn share_state_value(item: ItemStruct) -> Result<TokenStream, ParseError> {
    share_state_value::expand(item)
}

/// Derive version-zero framing around one array field's existing leaf codecs.
#[moxy::derive(FlexibleArrayValue, attributes(array_value))]
pub fn flexible_array_value(item: ItemStruct) -> Result<TokenStream, ParseError> {
    flexible_array_value::expand(item)
}

/// Implement the shared error traits for external protocol response and row types.
#[moxy::function]
pub fn response_errors(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    response_encoding::expand(&tokens.into())
}

/// Check share-state admission and construct denial rows in request order.
#[moxy::function]
pub fn share_state_handler(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    share_state_handler::expand(&tokens.into())
}

/// Run voter admission before the explicit handler's operation body.
#[moxy::attribute]
pub fn voter_admission(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    voter_admission::expand(meta, item)
}

/// Generate the flat broker configuration declaration with its shared derives.
#[moxy::function]
pub fn broker_config(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    broker_config::expand(&tokens.into())
}

/// Construct an exhaustive wire value with an empty tagged-field set.
#[moxy::function]
pub fn tagged_wire(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    wire_values::expand_native(tokens, false)
}

/// Construct an exhaustive response with zero throttle and empty tagged fields.
#[moxy::function]
pub fn unthrottled_wire(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    wire_values::expand_native(tokens, true)
}

/// Apply optional runtime fields in table order through their named validators.
#[moxy::function]
pub fn set_runtime(tokens: NativeTokenStream) -> Result<TokenStream, ParseError> {
    runtime_assignments::expand(&tokens.into())
}

/// Expands to an operator binary's `main`: tracing setup, then
/// `std::process::exit` with what the named crate's `run_from_args` returns.
/// The crate documentation describes the expansion.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn cli_main(tokens: TokenStream) -> Result<TokenStream, ParseError> { cli_main::expand(tokens) }

/// Generate the bounded exhaustive Stateright runner under the supplied function name.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn bounded_bfs(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::bounded_bfs(tokens)
}

/// Generate the single-topic delete request fixture with a five-second timeout.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn delete_topic_request(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::delete_topic_request(tokens)
}

/// Generate the compacted-batch test projection under the supplied struct name.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn compacted_batch(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::compacted_batch(tokens)
}

/// Generate the benchmark's Kafka record-batch builder under the supplied function name.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn record_batch_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::record_batch(tokens)
}

/// Prepend the common bucket, prefix, redacted credentials and endpoint fields to a GCS config.
/// Select `file` or `runtime` to preserve that configuration's field documentation.
#[moxy::attribute(name = "gcs_fields")]
pub fn gcs_fields(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    gcs_fields::expand(meta, item)
}

/// Generate action-to-message/log/timer adaptation inside a simulation impl.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn simulation_actions(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::simulation_actions(tokens)
}
/// Generate the shared simulation bus routing and replication preconditions.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn simulation_transport(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::simulation_transport(tokens)
}
/// Generate simulation leader heartbeats with the caller's interval.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn simulation_heartbeat(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::simulation_heartbeat(tokens)
}
/// Generate a saturating CI benchmark scenario with a caller-supplied name and size.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn benchmark_scenario_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::benchmark_scenario(tokens)
}

/// Generate the benchmark report's representative latency percentiles.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn benchmark_latency_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::benchmark_latency(tokens)
}

/// Generate the synthetic S3 archive config used by argument-mapping assertions.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn s3_archive_config_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::s3_archive_config(tokens)
}

/// Delegate unchanged metadata-log operations through `self.inner`, before `async_trait`.
#[moxy::attribute(name = "metadata_log_delegate")]
pub fn metadata_log_delegate(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    fixtures::metadata_log_delegate(meta, item)
}

/// Generate the finished remote-segment lookup assertions shared by manager fixtures.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn remote_segment_check(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::remote_segment_check(tokens)
}

/// Add the shared runtime policy fields before TOML or CLI field derives.
#[moxy::attribute(name = "runtime_policy_fields")]
pub fn runtime_policy_fields(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    runtime_policy_fields::expand(meta, item)
}

/// Generate the shared simulation event adapter inside a harness impl.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn simulation_step(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::simulation_step(tokens)
}

/// Generate the shared two-partition metadata topic fixture.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn snapshot_topic_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::snapshot_topic(tokens)
}

/// Generate the timestamped record-batch fixture used by local and remote read tests.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn timestamped_batch(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    timestamped_batch::expand(tokens)
}

/// Generate a producer batch builder with the caller's exact record value expression.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn producer_batch_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::producer_batch(tokens)
}

/// Generate a one-topic `CreateTopics` request builder with explicit sizes and timeout.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn create_topic_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::create_topic(tokens)
}

/// Generate the consumer Fetch request fixture for partition zero with a 1 MiB limit.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn consumer_fetch_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::consumer_fetch(tokens)
}

/// Generate the initial single-replica metadata partition record builder.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn single_replica_partition_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::single_replica_partition(tokens)
}

/// Generate the version-zero control-marker key and value fixture encoders.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn control_marker_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::control_marker(tokens)
}

/// Generate a topic record builder for one partition and one replica.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn topic_record_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::topic_record(tokens)
}

/// Generate the loopback ephemeral-port helper used by container tests.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn ephemeral_port_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    wire_fixtures::ephemeral_port(tokens)
}

/// Generate projection-based Eq and Hash, and optionally Debug, for model state.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn projection_identity(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    projection_identity::expand(tokens)
}

/// Generate the shared `MinIO` process/readiness helpers under the supplied module name.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn minio_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    container_fixtures::expand(tokens)
}

/// Start a SCRAM client exchange while preserving its private handshake phase type.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn scram_client_first(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    auth_fixtures::scram_client_first(tokens)
}

/// Generate the independent four-state remote-segment transition oracle.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn remote_segment_transition_matrix(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::remote_segment_transition_matrix(tokens)
}

/// Generate a tracing capture layer with an explicit mutex poison policy.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn capture_layer_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::capture_layer(tokens)
}

/// Add the selected direct `ObjectStore` delegates to an implementation.
#[moxy::attribute(name = "object_store_delegate")]
pub fn object_store_delegate(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    object_store_delegate::expand(meta, item)
}

/// Generate remote metadata lifecycle fixtures with an explicit timestamp policy.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn remote_segment_fixtures(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    remote_metadata_fixtures::segment(tokens)
}

/// Generate the independent missing-partition metadata assertions.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn remote_metadata_missing(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    remote_metadata_fixtures::missing(tokens)
}

/// Generate the direct flush and shutdown delegates for a tuple write wrapper.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn async_write_delegate(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    async_write_delegate::expand(tokens)
}

/// Generate a model transition's signature and cloned state before its body.
#[moxy::function(name = "model_transition")]
pub fn model_transition(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    fixtures::model_transition(tokens)
}

/// Append the shared object-store retry and HTTP timeout fields.
#[moxy::attribute(name = "object_store_config")]
pub fn object_store_config(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    object_store_config::expand(meta, item)
}

/// Expand the object-store file-upload method before async trait processing.
#[moxy::attribute(name = "object_ops")]
pub fn object_ops(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    object_ops::expand(meta, item)
}

/// Match inline writes and conditionally available sendfile regions.
#[moxy::function(name = "sendfile_match")]
pub fn sendfile_match(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    sendfile_match::expand_native(tokens)
}

/// Generate timer registration and completion helpers with caller-specific messages.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn timer_hooks(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    timer_hooks::expand(tokens)
}

/// Generate the SCRAM proof XOR fixture without imposing extra length checks.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn scram_client_proof_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    auth_fixtures::scram_client_proof(tokens)
}

/// Generate a single-partition Produce fixture with explicit request settings.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn single_partition_produce_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    produce_fixtures::single_partition_produce(tokens)
}

/// Generate a manual Kafka request-frame fixture with explicit header inputs.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn request_frame_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::request_frame(tokens)
}

/// Generate deterministic patterned bytes for framing tests and benchmarks.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn patterned_bytes_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::patterned_bytes(tokens)
}

/// Generate the distinctive proposal UUID shared by signing golden fixtures.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn break_glass_golden_id(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::break_glass_golden_id(tokens)
}

/// Literal Kafka 4.3.1 principal bytes: alice or bob, token flag.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn jvm_principal_golden(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::jvm_principal_golden(tokens)
}

/// Generate a response frame prefix independently of production framing.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn frame_prefix_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::frame_prefix(tokens)
}

/// Generate the independent file-region byte reader used by wire comparisons.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn file_region_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::file_region(tokens)
}

/// Expand the common I/O, TLS and SASL error variants at their original position.
#[moxy::attribute(name = "transport_errors")]
pub fn transport_errors(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    transport_errors::expand(meta, item)
}

/// Generate a voter-set fixture whose endpoints and directory IDs remain empty.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn empty_endpoint_voters(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    raft_fixtures::empty_endpoint_voters(tokens)
}

/// Generate the one-record leader-epoch batch fixture.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn epoch_record_batch_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    raft_fixtures::epoch_record_batch(tokens)
}

/// Generate the epoch-cache `LogView` delegation used by Raft simulations.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn epoch_log_view(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    raft_fixtures::epoch_log_view(tokens)
}

/// Derive a TOML table's schema and value traits with an explicit unknown-key policy.
#[moxy::attribute(name = "config_table")]
pub fn config_table(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    config_table::expand(meta, item)
}

/// Match record payloads with file-region arms only on supported platforms.
#[moxy::function(name = "records_payload_match")]
pub fn records_payload_match(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    sendfile_match::records_payload_native(tokens)
}

/// Generate the canonical compressed legacy record used by policy tests.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn legacy_policy_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    legacy_fixtures::policy(tokens)
}

/// Generate keyed fixture records with explicit count and value size.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn keyed_record_batch_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::keyed_record_batch(tokens)
}

/// Add the shared generic argv signature to a command-line entry point.
#[moxy::attribute(name = "argv_entrypoint")]
pub fn argv_entrypoint(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::argv_entrypoint(meta, item)
}

/// Add bootstrap-server and subcommand fields before deriving a CLI parser.
#[moxy::attribute(name = "bootstrap_cli_fields")]
pub fn bootstrap_cli_fields(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    cli_support::bootstrap_cli_fields(meta, item)
}

/// Generate a duration-parser check over each caller's literal cases.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn duration_cases_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::duration_cases(tokens)
}

/// Generate a sorted directory-tree fixture with the caller's path projection.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn directory_tree_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::directory_tree(tokens)
}

/// Generate a CLI entry point with the caller's documentation.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn parsed_cli_entrypoint(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::parsed_cli_entrypoint(tokens)
}

/// Generate a duration-parser test from explicit literal cases.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn duration_parser_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::duration_parser_fixture(tokens)
}

/// Generate bootstrap parser tests from accepted and refused arguments.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn bootstrap_parser_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::bootstrap_parser_fixture(tokens)
}

/// Generate the shared snapshot fixture over an on-disk metadata log.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn snapshot_node_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::snapshot_node_fixture(tokens)
}

/// Connect a CLI client with the caller's identity and refusal exit code.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn connect_cli_client(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::connect_cli_client(tokens)
}

/// Generate the temporary metadata and data directories used by format tests.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn format_directories_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::format_directories_fixture(tokens)
}

/// Generate the explicit topic map used by WAL capture fixtures.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn wal_capture_topic_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    remote_metadata_fixtures::wal_capture_topic_fixture(tokens)
}

/// Generate a CLI flag's name, environment and default-value projection.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn flag_metadata_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::flag_metadata_fixture(tokens)
}

/// Generate a listener bind retry with an explicit caller timeout.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn bind_retry_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::bind_retry_fixture(tokens)
}

/// Generate bound-listener configuration or startup with the caller's diagnostics.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn bound_start_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::expand(tokens)
}

/// Generate directory-assignment requests with explicit broker and partition identities.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn assignment_dirs_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::assignment_dirs(tokens)
}

/// Render a metric registry into caller-named bindings while retaining its lock guard.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn metric_registry_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::metric_registry(tokens)
}

/// Generate indexed WORM segment metadata with an explicit segment-id namespace.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn worm_segment_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    remote_metadata_fixtures::worm_segment(tokens)
}

/// Generate the started remote-segment fixture with an explicit metadata crate.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn remote_started_segment(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    remote_metadata_fixtures::remote_started_segment(tokens)
}

/// Generate a SCRAM user request preserving optional and ordered user lists.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn scram_users_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::scram_users(tokens)
}

/// Generate the first matching record header decoder with an explicit UTF-8 policy.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn record_header_text(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::record_header_text(tokens)
}

/// Generate the shared dead-letter metric observations with caller-selected types.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn share_dlq_meters(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    fixtures::share_dlq_meters(tokens)
}

/// Generate exported segment paths with an explicit snapshot and lazy epoch projection.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn export_segment_data_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::export_segment_data(tokens)
}

/// Keep partition-spawn wrappers on one shared parameter list.
#[moxy::attribute(name = "partition_spawn_parameters")]
pub fn partition_spawn_parameters(
    meta: TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    network_fixtures::partition_spawn_parameters(meta, item)
}

/// Generate a checkpoint writer with a caller-selected failure kind.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn epoch_checkpoint_failure(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::epoch_checkpoint_failure(tokens)
}

/// Generate a JVM checkpoint decoder fixture with explicit bytes, name and image.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn jvm_checkpoint_dump_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::jvm_checkpoint_dump(tokens)
}

/// Compose a compile-time Java string-producer probe with shared imports and setup.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn java_string_producer_source(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::java_string_producer(tokens)
}

/// Define a wall-clock reader preserving caller attributes and overflow behavior.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn epoch_millis_fn(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    network_fixtures::epoch_millis(tokens)
}

/// Gate original items or expressions on the supported sendfile platforms.
#[moxy::function(name = "sendfile_platform")]
#[expect(
    clippy::unnecessary_wraps,
    reason = "Moxy's proc_macro adapter requires a Result-returning entry point"
)]
pub fn sendfile_platform(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    Ok(network_fixtures::sendfile_platform(tokens))
}

/// Generate a test authorizer while retaining the original policy body tokens.
#[moxy::function(name = "test_authorizer")]
pub fn test_authorizer(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    network_fixtures::test_authorizer(tokens)
}

/// Generate checked Unix-millisecond fixtures with explicit panic contexts.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn unix_millis_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::unix_millis(tokens)
}

/// Generate a Vec request frame with the caller's checked length-prefix type.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn vector_request_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    bound_start_fixture::vector_request(tokens)
}
/// Generate the independent eight-row supported-feature oracle.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn supported_features_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::supported_features_fixture(tokens)
}

/// Generate supported-feature rows from explicit wire bounds.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn supported_feature_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::supported_feature_fixture(tokens)
}
/// Generate finalized-feature rows from explicit wire levels.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn finalized_feature_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::finalized_feature_fixture(tokens)
}
/// Generate the record input used by byte-limit tests.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn record_limit_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::record_limit_fixture(tokens)
}
/// Generate keyed compaction batches with explicit producer state and timestamp.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn compaction_record_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::compaction_record_fixture(tokens)
}
/// Project registration feature bounds into an ordered map.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn registration_feature_projection(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::registration_feature_projection(tokens)
}
/// Generate the validated connection dispatch-capacity parser.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn dispatch_capacity_parser(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cli_support::dispatch_capacity_parser(tokens)
}

/// Share S3 endpoint and credential fields with the original configuration documentation.
#[moxy::attribute]
pub fn s3_fields(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    s3_fields::expand(meta, item)
}

/// Define a named `SplitMix64` step that advances the caller's stream state.
/// Placement shuffles and benchmark byte streams share the exact same arithmetic.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn splitmix64_step(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    deterministic_random::splitmix64_step(tokens)
}

/// Generate an independent compression-byte oracle with a named codec level.
///
/// # Errors
///
/// Rejects input outside this macro's syntax.
#[cfg_attr(not(test), moxy::function)]
pub fn compression_bytes_fixture(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    cross_storage_fixtures::compression_bytes_fixture(tokens)
}

mod reaper_task;
mod record_strings;
mod seed_member_maps;
mod sweep_loop;

/// Snapshot member metadata and assignments using the protocol's explicit target body.
#[moxy::function]
pub fn snapshot_member_maps(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    seed_member_maps::snapshot(tokens)
}

/// Restore member epochs before the protocol's explicit assignment body.
#[moxy::function]
pub fn hydrate_member_epochs(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    seed_member_maps::hydrate(tokens)
}

/// Declare request string traversal in field order, with optional native traversal bodies.
#[moxy::function]
pub fn record_strings(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    record_strings::expand(tokens)
}

/// Run an explicit sweep body with the selected cadence and shutdown body.
#[moxy::function]
pub fn sweep_loop(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    sweep_loop::expand(tokens)
}

/// Schedule an ordinary transaction sweep and retain its caller's shutdown event.
#[moxy::attribute]
pub fn reaper_task(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    reaper_task::expand(meta, item)
}

mod archive_rows;
mod authorization_refusal;
mod broker_test_setup;
mod described_group_rows;
mod forwardable_requests;
mod freeze_refusal_rules;
mod group_record_key;
mod handler_wire_fixtures;
mod model_properties;
mod model_runner;
mod quorum_epoch_wire;
mod reconciliation_fields;
mod reconciliation_operations;
mod reconciliation_projection;
mod request_test_identity;
mod scheduled_loads;
mod share_fixture_schemas;
mod share_refusal_matrix;
mod share_state_constructors;
mod share_state_operation;
mod share_state_results;
mod simulation_message;

/// Declare forwardable API keys and decode each embedded body with its selected request type.
#[moxy::function]
pub fn forwardable_requests(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    forwardable_requests::expand(&input.into()).map(Into::into)
}
/// Add the selected common projection fields around the visible protocol-specific fields.
#[moxy::attribute]
pub fn reconciliation_fields(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    reconciliation_fields::expand(&meta.into(), item)
}
/// Run share-offset refusal cases with direct startup, identity, assertions and shutdown.
#[moxy::function]
pub fn share_refusal_matrix(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_refusal_matrix::expand(input)
}
/// Derive projection construction from the member declaration's visible fields.
#[moxy::derive(ReconciliationMember, attributes(reconciliation))]
pub fn reconciliation_member(item: ItemStruct) -> Result<TokenStream, ParseError> {
    reconciliation_projection::member(item)
}

/// Encode and parse a visible group-record key enum with explicit version constants.
#[moxy::derive(GroupRecordKey, attributes(group_key))]
pub fn group_record_key(item: ItemEnum) -> Result<TokenStream, ParseError> {
    group_record_key::expand(item)
}

/// Implement partition routing from a visible simulation message declaration.
#[moxy::derive(PartitionMessage)]
pub fn partition_message(item: ItemStruct) -> Result<TokenStream, ParseError> {
    simulation_message::expand(item)
}

/// Adapt the external quorum-epoch request types listed at the caller.
#[moxy::function]
pub fn quorum_epoch_requests(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    quorum_epoch_wire::requests(&input.into()).map(Into::into)
}

/// Map native voter listeners to the selected quorum wire endpoint type.
#[moxy::function]
pub fn voter_endpoints(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    quorum_epoch_wire::voter_endpoints(input)
}

/// Supply the common quorum-epoch response layout under an explicit function signature.
#[moxy::function]
pub fn quorum_epoch_encoder(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    quorum_epoch_wire::encoder(input)
}

/// Encode a typed authorization refusal with its optional native message expression.
#[moxy::function]
pub fn authorization_refusal(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    authorization_refusal::expand(input)
}

/// Share an exhaustive freeze mutation inventory under explicit runtime and logical headers.
#[moxy::function]
pub fn freeze_refusal_rules(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    freeze_refusal_rules::expand(input)
}

/// Construct the fixed empty ACL matching filter for the selected external request type.
#[moxy::function]
pub fn empty_match_acl_filter(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_fixtures::empty_match_acl_filter(&input.into()).map(Into::into)
}

/// Compare a delegation-token refusal with the full selected wire response.
#[moxy::function]
pub fn token_refusal_result(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_fixtures::token_refusal_result(&input.into()).map(Into::into)
}

/// Construct an external share-state request from visible field expressions.
#[moxy::function]
pub fn share_state_request(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_state_constructors::expand(input)
}

/// Associate share-state response schemas with the listed request types.
#[moxy::function]
pub fn share_fixture_schemas(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_fixture_schemas::expand(&input.into()).map(Into::into)
}

/// Adapt the listed external response rows to the partition-result trait.
#[moxy::function]
pub fn share_partition_results(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    described_group_rows::partition_results(&input.into()).map(Into::into)
}

/// Run native share-state partition operations concurrently while preserving request order.
#[moxy::function]
pub fn share_state_results(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_state_results::expand(input)
}

/// Add shared active-state admission and reply handling to a visible async method.
#[moxy::attribute]
pub fn share_state_operation(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    share_state_operation::expand(&meta.into(), item)
}

/// Derive group projection construction and epoch checks from its visible fields.
#[moxy::derive(ReconciliationGroup, attributes(reconciliation))]
pub fn reconciliation_group(item: ItemStruct) -> Result<TokenStream, ParseError> {
    reconciliation_projection::group(item)
}

/// Derive heartbeat conversion from a visible reconciliation action enum.
#[moxy::derive(ReconciliationAction)]
pub fn reconciliation_action(item: ItemEnum) -> Result<TokenStream, ParseError> {
    reconciliation_projection::action(item)
}

/// Observe a projected model epoch using its visible epoch field and violation ledger.
#[moxy::derive(ModelEpochLedger)]
pub fn model_epoch_ledger(item: ItemStruct) -> Result<TokenStream, ParseError> {
    reconciliation_projection::epoch(item)
}

/// Build ordered model properties around their original native oracle blocks.
#[moxy::function]
pub fn model_properties(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    model_properties::expand(input)
}

/// Run a model with explicit search bounds and pinned state counts.
#[moxy::function]
pub fn model_runner(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    model_runner::expand(input)
}

/// Rebuild a reconciliation group around explicit native setup and projection operations.
#[moxy::function]
pub fn rebuild_reconciliation_group(
    input: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    reconciliation_operations::rebuild(input)
}

/// Apply an explicit member action and project the resulting reconciliation state.
#[moxy::function]
pub fn reconciliation_next_state(
    input: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    reconciliation_operations::next_state(input)
}

/// Emit reconciliation action scheduling with explicit caller predicates and extensions.
#[moxy::function]
pub fn reconciliation_actions(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    reconciliation_operations::actions(input)
}

/// Retain the broker handle and directory before native readiness operations.
#[moxy::function]
pub fn broker_test_setup(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    broker_test_setup::expand(input)
}

/// Retain principal and peer values for an explicitly selected borrowed request context.
#[moxy::function]
pub fn request_test_identity(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    request_test_identity::expand(input)
}

/// Join the visible task collection in order and log its selected failure message.
#[moxy::derive(ScheduledLoads, attributes(scheduled_loads))]
pub fn scheduled_loads(item: ItemStruct) -> Result<TokenStream, ParseError> {
    scheduled_loads::expand(item)
}

/// Implement group-describe row access for an explicit table of external response types.
#[moxy::function]
pub fn described_group_rows(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    described_group_rows::expand(&input.into()).map(Into::into)
}

/// Generate one annotated archive-validation walk with coherent proof bindings.
#[moxy::function(name = "validate_archive_rows")]
pub fn validate_archive_rows(tokens: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let expanded = archive_rows::expand(tokens.into())?;
    // Creusot matches logical captures to MIR locals by identifier context.
    // Reparse the complete function together so generated and supplied annotations
    // share the same context as the local bindings they describe.
    Ok(expanded
        .to_string()
        .parse()
        .expect("valid archive walk expansion"))
}

#[cfg(test)]
mod export_contract_tests {
    use {super::*, assert2::assert};

    type Expand = fn(TokenStream) -> Result<TokenStream, ParseError>;
    struct FunctionMacro {
        name: &'static str,
        expand: Expand,
    }
    const FUNCTION_MACROS: [FunctionMacro; 81] = [
        FunctionMacro {
            name: "dispatch_table",
            expand: dispatch_table,
        },
        FunctionMacro {
            name: "throttle_probes",
            expand: throttle_probes,
        },
        FunctionMacro {
            name: "cli_main",
            expand: cli_main,
        },
        FunctionMacro {
            name: "bounded_bfs",
            expand: bounded_bfs,
        },
        FunctionMacro {
            name: "delete_topic_request",
            expand: delete_topic_request,
        },
        FunctionMacro {
            name: "compacted_batch",
            expand: compacted_batch,
        },
        FunctionMacro {
            name: "record_batch_fixture",
            expand: record_batch_fixture,
        },
        FunctionMacro {
            name: "simulation_actions",
            expand: simulation_actions,
        },
        FunctionMacro {
            name: "simulation_transport",
            expand: simulation_transport,
        },
        FunctionMacro {
            name: "simulation_heartbeat",
            expand: simulation_heartbeat,
        },
        FunctionMacro {
            name: "benchmark_scenario_fixture",
            expand: benchmark_scenario_fixture,
        },
        FunctionMacro {
            name: "benchmark_latency_fixture",
            expand: benchmark_latency_fixture,
        },
        FunctionMacro {
            name: "s3_archive_config_fixture",
            expand: s3_archive_config_fixture,
        },
        FunctionMacro {
            name: "remote_segment_check",
            expand: remote_segment_check,
        },
        FunctionMacro {
            name: "simulation_step",
            expand: simulation_step,
        },
        FunctionMacro {
            name: "snapshot_topic_fixture",
            expand: snapshot_topic_fixture,
        },
        FunctionMacro {
            name: "timestamped_batch",
            expand: timestamped_batch,
        },
        FunctionMacro {
            name: "producer_batch_fixture",
            expand: producer_batch_fixture,
        },
        FunctionMacro {
            name: "create_topic_fixture",
            expand: create_topic_fixture,
        },
        FunctionMacro {
            name: "consumer_fetch_fixture",
            expand: consumer_fetch_fixture,
        },
        FunctionMacro {
            name: "single_replica_partition_fixture",
            expand: single_replica_partition_fixture,
        },
        FunctionMacro {
            name: "control_marker_fixture",
            expand: control_marker_fixture,
        },
        FunctionMacro {
            name: "topic_record_fixture",
            expand: topic_record_fixture,
        },
        FunctionMacro {
            name: "ephemeral_port_fixture",
            expand: ephemeral_port_fixture,
        },
        FunctionMacro {
            name: "projection_identity",
            expand: projection_identity,
        },
        FunctionMacro {
            name: "minio_fixture",
            expand: minio_fixture,
        },
        FunctionMacro {
            name: "scram_client_first",
            expand: scram_client_first,
        },
        FunctionMacro {
            name: "remote_segment_transition_matrix",
            expand: remote_segment_transition_matrix,
        },
        FunctionMacro {
            name: "capture_layer_fixture",
            expand: capture_layer_fixture,
        },
        FunctionMacro {
            name: "remote_segment_fixtures",
            expand: remote_segment_fixtures,
        },
        FunctionMacro {
            name: "remote_metadata_missing",
            expand: remote_metadata_missing,
        },
        FunctionMacro {
            name: "async_write_delegate",
            expand: async_write_delegate,
        },
        FunctionMacro {
            name: "timer_hooks",
            expand: timer_hooks,
        },
        FunctionMacro {
            name: "scram_client_proof_fixture",
            expand: scram_client_proof_fixture,
        },
        FunctionMacro {
            name: "single_partition_produce_fixture",
            expand: single_partition_produce_fixture,
        },
        FunctionMacro {
            name: "request_frame_fixture",
            expand: request_frame_fixture,
        },
        FunctionMacro {
            name: "patterned_bytes_fixture",
            expand: patterned_bytes_fixture,
        },
        FunctionMacro {
            name: "break_glass_golden_id",
            expand: break_glass_golden_id,
        },
        FunctionMacro {
            name: "jvm_principal_golden",
            expand: jvm_principal_golden,
        },
        FunctionMacro {
            name: "frame_prefix_fixture",
            expand: frame_prefix_fixture,
        },
        FunctionMacro {
            name: "file_region_fixture",
            expand: file_region_fixture,
        },
        FunctionMacro {
            name: "empty_endpoint_voters",
            expand: empty_endpoint_voters,
        },
        FunctionMacro {
            name: "epoch_record_batch_fixture",
            expand: epoch_record_batch_fixture,
        },
        FunctionMacro {
            name: "epoch_log_view",
            expand: epoch_log_view,
        },
        FunctionMacro {
            name: "legacy_policy_fixture",
            expand: legacy_policy_fixture,
        },
        FunctionMacro {
            name: "keyed_record_batch_fixture",
            expand: keyed_record_batch_fixture,
        },
        FunctionMacro {
            name: "duration_cases_fixture",
            expand: duration_cases_fixture,
        },
        FunctionMacro {
            name: "directory_tree_fixture",
            expand: directory_tree_fixture,
        },
        FunctionMacro {
            name: "parsed_cli_entrypoint",
            expand: parsed_cli_entrypoint,
        },
        FunctionMacro {
            name: "duration_parser_fixture",
            expand: duration_parser_fixture,
        },
        FunctionMacro {
            name: "bootstrap_parser_fixture",
            expand: bootstrap_parser_fixture,
        },
        FunctionMacro {
            name: "snapshot_node_fixture",
            expand: snapshot_node_fixture,
        },
        FunctionMacro {
            name: "connect_cli_client",
            expand: connect_cli_client,
        },
        FunctionMacro {
            name: "format_directories_fixture",
            expand: format_directories_fixture,
        },
        FunctionMacro {
            name: "wal_capture_topic_fixture",
            expand: wal_capture_topic_fixture,
        },
        FunctionMacro {
            name: "flag_metadata_fixture",
            expand: flag_metadata_fixture,
        },
        FunctionMacro {
            name: "bind_retry_fixture",
            expand: bind_retry_fixture,
        },
        FunctionMacro {
            name: "bound_start_fixture",
            expand: bound_start_fixture,
        },
        FunctionMacro {
            name: "assignment_dirs_fixture",
            expand: assignment_dirs_fixture,
        },
        FunctionMacro {
            name: "metric_registry_fixture",
            expand: metric_registry_fixture,
        },
        FunctionMacro {
            name: "worm_segment_fixture",
            expand: worm_segment_fixture,
        },
        FunctionMacro {
            name: "remote_started_segment",
            expand: remote_started_segment,
        },
        FunctionMacro {
            name: "scram_users_fixture",
            expand: scram_users_fixture,
        },
        FunctionMacro {
            name: "record_header_text",
            expand: record_header_text,
        },
        FunctionMacro {
            name: "share_dlq_meters",
            expand: share_dlq_meters,
        },
        FunctionMacro {
            name: "export_segment_data_fixture",
            expand: export_segment_data_fixture,
        },
        FunctionMacro {
            name: "epoch_checkpoint_failure",
            expand: epoch_checkpoint_failure,
        },
        FunctionMacro {
            name: "jvm_checkpoint_dump_fixture",
            expand: jvm_checkpoint_dump_fixture,
        },
        FunctionMacro {
            name: "java_string_producer_source",
            expand: java_string_producer_source,
        },
        FunctionMacro {
            name: "epoch_millis_fn",
            expand: epoch_millis_fn,
        },
        FunctionMacro {
            name: "unix_millis_fixture",
            expand: unix_millis_fixture,
        },
        FunctionMacro {
            name: "vector_request_fixture",
            expand: vector_request_fixture,
        },
        FunctionMacro {
            name: "supported_features_fixture",
            expand: supported_features_fixture,
        },
        FunctionMacro {
            name: "supported_feature_fixture",
            expand: supported_feature_fixture,
        },
        FunctionMacro {
            name: "finalized_feature_fixture",
            expand: finalized_feature_fixture,
        },
        FunctionMacro {
            name: "record_limit_fixture",
            expand: record_limit_fixture,
        },
        FunctionMacro {
            name: "compaction_record_fixture",
            expand: compaction_record_fixture,
        },
        FunctionMacro {
            name: "registration_feature_projection",
            expand: registration_feature_projection,
        },
        FunctionMacro {
            name: "dispatch_capacity_parser",
            expand: dispatch_capacity_parser,
        },
        FunctionMacro {
            name: "splitmix64_step",
            expand: splitmix64_step,
        },
        FunctionMacro {
            name: "compression_bytes_fixture",
            expand: compression_bytes_fixture,
        },
    ];

    #[test]
    fn function_macros_keep_their_empty_input_contract() {
        let accepted: Vec<_> = FUNCTION_MACROS
            .into_iter()
            .filter_map(|entry| {
                (entry.expand)(TokenStream::new())
                    .is_ok()
                    .then_some(entry.name)
            })
            .collect();
        // Dispatch and throttle tables permit zero entries; fixtures need arguments.
        assert!(accepted == ["dispatch_table", "throttle_probes"]);
    }
}

mod verified_index;

/// Declare a verified binary-search cursor over named index entries.
#[moxy::function]
pub fn sorted_index_cursor(input: TokenStream) -> Result<TokenStream, ParseError> {
    verified_index::cursor(input)
}

/// Declare a verified last-predecessor lookup over named index entries.
#[moxy::function]
pub fn index_floor_lookup(input: TokenStream) -> Result<TokenStream, ParseError> {
    verified_index::floor(input)
}

/// Add the contract of a remote finished-segment selection.
#[moxy::attribute]
pub fn finished_selection(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    verified_index::finished(meta, item)
}

/// Add the shared domain and proof annotations of a round-robin site lemma.
#[moxy::attribute]
pub fn round_robin_site_lemma(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    verified_index::site(meta, item)
}

mod fixture_methods;
mod group_record_flush;
mod unit_schema;

/// Describe a visible human-unit schema marker.
#[moxy::derive(UnitSchema, attributes(unit_schema))]
pub fn unit_schema(item: ItemStruct) -> Result<TokenStream, ParseError> {
    unit_schema::expand(item)
}

/// Append the shared metadata forwarding methods to an authorizer implementation.
#[moxy::attribute]
pub fn delegate_authorizer(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    fixture_methods::authorizer(meta, item)
}

/// Append missing-read behavior to an explicit remote-log implementation.
#[moxy::attribute]
pub fn missing_remote_reads(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    fixture_methods::remote(meta, item)
}

/// Preserve the visible flush body inside the shared group-log append boundary.
#[moxy::attribute]
pub fn flush_group_records(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    group_record_flush::expand(meta, item)
}

mod handler_wire_rows;

/// Build offset-fetch rows for an explicit table of wire schemas.
#[moxy::function]
pub fn offset_fetch_rows(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_rows::offset_fetch_rows(&input.into()).map(Into::into)
}

/// Build a share-group refusal with the caller's owned fields.
#[moxy::function]
pub fn share_group_denial(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_rows::share_group_denial(&input.into()).map(Into::into)
}

/// Project acknowledgement batches while preserving the native expression.
#[moxy::function]
pub fn acknowledgement_batches(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_rows::acknowledgement_batches(input)
}

mod coordinator_fixtures;

/// Declare heartbeat fixtures with named status observations.
#[moxy::function]
pub fn heartbeat_status(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    coordinator_fixtures::heartbeat(&input.into()).map(Into::into)
}

/// Preserve native key-boundary observations in generated coordinator tests.
#[moxy::function]
pub fn member_key_boundaries(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    coordinator_fixtures::key_boundaries(input)
}

/// Schedule explicit commit changes and preserve caller-owned async actions.
#[moxy::function]
pub fn schedule_commit_changes(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    coordinator_fixtures::commit_changes(input)
}

mod bounded_unit_modules;

/// Declare concrete bounded human-unit serde modules from one flat table.
#[moxy::function]
pub fn bounded_unit_modules(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    bounded_unit_modules::expand(&input.into()).map(Into::into)
}

mod fixture_constructors;

/// Declare fixture constructors from the existing election, supervisor and quota domains.
#[moxy::function]
pub fn fixture_constructors(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    fixture_constructors::expand(&input.into()).map(Into::into)
}

mod wire_test_driver;

/// Declare wire drivers that call the existing socket and framing helpers.
#[moxy::function]
pub fn wire_test_driver(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    wire_test_driver::expand(&input.into()).map(Into::into)
}

mod replication_target_guard;

/// Acquire the local replication target with the original caller-level drop behavior.
#[moxy::function]
pub fn replication_target_guard(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    replication_target_guard::expand(&input.into()).map(Into::into)
}

mod share_test_drivers;

/// Declare a first share fetch through the existing client helper.
#[moxy::function]
pub fn share_first_fetch(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_test_drivers::acquisition(&input.into()).map(Into::into)
}

/// Declare a share-consumption driver with native caller operations.
#[moxy::function]
pub fn share_consumption_driver(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    share_test_drivers::consumption(input)
}

mod index_seed_helpers;

/// Declare typed sparse-index fixture helpers with explicit index and key types.
#[moxy::function]
pub fn index_seed_helpers(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    index_seed_helpers::expand(&input.into()).map(Into::into)
}

/// Route `(broker, context, request, version; operation, refusal)` through the
/// controller listener, continuing with the request or returning its completed dispatch.
#[moxy::function]
pub fn controller_request(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    handler_wire_rows::controller_request(input)
}

mod config_alter_handler;

/// Declare the shared configuration handler with explicit wire and audit policies.
#[moxy::function]
pub fn config_alter_handler(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    config_alter_handler::expand(&input.into()).map(Into::into)
}

mod fixture_expressions;

/// Verify a guard response and return its typed value.
#[moxy::function]
pub fn guard_response(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    fixture_expressions::guard_response(&input.into()).map(Into::into)
}

/// Read the evicted local segment state with the declared retention policy.
#[moxy::function]
pub fn evicted_local_segments(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    fixture_expressions::evicted_segments(&input.into()).map(Into::into)
}

/// Apply reassignment fields in the caller's explicit expression order.
#[moxy::function]
pub fn reassignment_fields(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    fixture_expressions::reassignment(input)
}

mod share_request_identity;

/// Resolve a share request identity before entering the native handler body.
#[moxy::attribute]
pub fn share_request_identity(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    share_request_identity::expand(meta.into(), item)
}

mod group_fixture_tables;
mod partition_gate;
mod private_dispatch_table;
mod string_mutations;

/// Declare partition authorization gates with explicit actions.
#[moxy::function]
pub fn partition_gate(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    partition_gate::expand(&input.into()).map(Into::into)
}

/// Declare private request adapters and their registration table.
#[moxy::function]
pub fn private_dispatch_table(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    private_dispatch_table::expand(&input.into()).map(Into::into)
}

/// Declare required and optional string mutation cases.
#[moxy::function]
pub fn string_mutations(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    string_mutations::expand(&input.into()).map(Into::into)
}

/// Declare member identity fixtures with explicit schema fields.
#[moxy::function]
pub fn member_identity_fixtures(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    group_fixture_tables::member_identity(input)
}

/// Declare required-topic traversal for a topology schema.
#[moxy::function]
pub fn required_topics(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    group_fixture_tables::required_topics(&input.into()).map(Into::into)
}

mod token_dispatch;

/// Adapt an owned delegation-token handler to negotiated wire dispatch.
#[moxy::attribute]
pub fn token_dispatch(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    token_dispatch::expand(meta.into(), item)
}
