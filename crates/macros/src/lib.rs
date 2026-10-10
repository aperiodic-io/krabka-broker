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
//! - `help = "..."` — the help text. Every registered field needs one.
//! - `name = "..."` — the registered name. The default is the field name with
//!   a trailing `_total` removed, because `prometheus-client` appends `_total`
//!   to the name of a counter itself.
//! - `buckets = EXPR` — construct a `Histogram` over `EXPR`. On a
//!   `Family<_, Histogram>` field, construct the family so that each of its
//!   histograms uses `EXPR`.
//! - `new = EXPR` — construct the field with `EXPR`.
//! - `skip` — construct the field, but do not register it.
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
//! - `rule(<type>)` — the `refined_type` rule, such as `GreaterU32<0>`.
//!   Required. It goes in parentheses because a generic type does not parse
//!   after `=`.
//! - `getter = name` — the getter's name. The default is `into_value`.
//! - `string_error` — `new` returns `String`, the rule's error text, rather
//!   than `refined_type::result::Error<T>`.
//! - `label = "..."` — with `string_error`, `new` returns
//!   `"<label>: <rule error>"`.
//! - `default = EXPR` — implement `Default` as `Self::new(EXPR)`, which panics
//!   when `EXPR` breaks the rule.
//! - `from_str` — implement `FromStr` with `Err = String`: the field type's
//!   own `parse`, then `new`, each error as its text.
//! - `display` — implement `Display` as the field's.
//! - `parse_fn = name` — add a free `fn name(&str) -> Result<Self, String>`
//!   that parses the way `from_str` does.
//! - `quantity = ByteSize`, `Time` or `Frequency` — the field is a whole
//!   number of bytes, milliseconds or Hz, and `new` takes the `krabka_units`
//!   quantity. It refuses a fraction, an infinity, or a count the field type
//!   cannot hold with `"<label>: must be a whole number of <unit> that fits in
//!   <field type>"`, then hands the count to the rule. It implies
//!   `string_error`, adds `TryFrom<quantity>` through `new`, makes `from_str`
//!   parse with `krabka_units::parse` and `display` print the quantity's
//!   `human()` text, and makes `default` take a quantity.
//! - `quantity_getter = name` — with `quantity`, add a getter that returns the
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
//! - `case = "..."` — how a variant name becomes its text: `"snake_case"`,
//!   `"kebab-case"`, `"lowercase"` or `"UPPERCASE"`. Without it the text is
//!   the variant name unchanged.
//! - `as_str = name` — the method's name.
//! - `parse` or `parse = name` — add `fn parse(&str) -> Option<Self>`, under
//!   that name, over the unit variants.
//! - `all` — add `const ALL: [Self; N]`, every variant in declaration order.
//!   Every variant must be a unit variant.
//! - `label_value` — implement `prometheus_client`'s `EncodeLabelValue` as the
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

use moxy::{
    ast::{ItemEnum, ItemStruct, ParseError},
    token::TokenStream,
};
use proc_macro::TokenStream as NativeTokenStream;

mod api_names;
mod async_write_delegate;
mod auth_fixtures;
mod bound_start_fixture;
mod cli_main;
mod cli_support;
mod config_table;
mod container_fixtures;
mod cross_storage_fixtures;
mod deterministic_random;
mod dispatch;
mod enum_str;
mod field_defaults;
mod fixtures;
mod gcs_fields;
mod human_units;
mod krabka_env;
mod legacy_fixtures;
mod meta;
mod metrics;
mod network_fixtures;
mod object_ops;
mod object_store_config;
mod object_store_delegate;
mod primitive_cmp;
mod produce_fixtures;
mod projection_identity;
mod raft_fixtures;
mod refined_newtype;
mod remote_metadata_fixtures;
mod runtime_overlay;
mod runtime_policy_fields;
mod s3_fields;
mod sendfile_match;
mod throttle_probes;
mod timer_hooks;
mod timestamped_batch;
mod transport_errors;
mod wire_fixtures;

// Token-to-token fixtures share the same Moxy adapter and error handling.
macro_rules! function_macros {
    ($( $(#[$($doc:tt)*])* $name:ident => $module:ident::$delegate:ident; )*) => {
        $(
            $(#[$($doc)*])*
            #[moxy::function]
            pub fn $name(tokens: TokenStream) -> Result<TokenStream, ParseError> {
                $module::$delegate(tokens)
            }
        )*
    };
}

/// Derives `unregistered` and `register` for a struct of metric handles. The
/// crate documentation lists the field attributes.
#[moxy::derive(RegisterMetrics, attributes(metric))]
pub fn register_metrics(item: ItemStruct) -> Result<TokenStream, ParseError> {
    metrics::expand(item)
}

// Keep implementations in their own modules and document each adapter below.

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

function_macros! {
    /// Generates the broker's dispatch adapters and `register_dispatch_table`
    /// from one table of api names. The crate documentation describes the table.
    dispatch_table => dispatch::expand;

    /// Expands to the throttle-echo audit's `[(API_KEY, Probe); N]` array from one
    /// table of api names. The crate documentation describes the table.
    throttle_probes => throttle_probes::expand;
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
pub fn enum_str(item: ItemEnum) -> Result<TokenStream, ParseError> {
    enum_str::expand(item)
}

/// Derives `Default` from each field's `#[default(EXPR)]`, or
/// `Default::default()` for a field without one. The crate documentation
/// describes the expansion.
#[moxy::derive(FieldDefaults, attributes(default))]
pub fn field_defaults(item: ItemStruct) -> Result<TokenStream, ParseError> {
    field_defaults::expand(item)
}

function_macros! {
    /// Expands to an operator binary's `main`: tracing setup, then
    /// `std::process::exit` with what the named crate's `run_from_args` returns.
    /// The crate documentation describes the expansion.
    cli_main => cli_main::expand;

    /// Generate the bounded exhaustive Stateright runner under the supplied function name.
    bounded_bfs => fixtures::bounded_bfs;

    /// Generate the single-topic delete request fixture with a five-second timeout.
    delete_topic_request => fixtures::delete_topic_request;

    /// Generate the compacted-batch test projection under the supplied struct name.
    compacted_batch => fixtures::compacted_batch;

    /// Generate the benchmark's Kafka record-batch builder under the supplied function name.
    record_batch_fixture => fixtures::record_batch;
}

/// Prepend the common bucket, prefix, redacted credentials and endpoint fields to a GCS config.
/// Select `file` or `runtime` to preserve that configuration's field documentation.
#[moxy::attribute(name = "gcs_fields")]
pub fn gcs_fields(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    gcs_fields::expand(meta, item)
}

function_macros! {
    /// Generate action-to-message/log/timer adaptation inside a simulation impl.
    simulation_actions => fixtures::simulation_actions;
    /// Generate the shared simulation bus routing and replication preconditions.
    simulation_transport => fixtures::simulation_transport;
    /// Generate simulation leader heartbeats with the caller's interval.
    simulation_heartbeat => fixtures::simulation_heartbeat;
    /// Generate a saturating CI benchmark scenario with a caller-supplied name and size.
    benchmark_scenario_fixture => fixtures::benchmark_scenario;

    /// Generate the benchmark report's representative latency percentiles.
    benchmark_latency_fixture => fixtures::benchmark_latency;

    /// Generate the synthetic S3 archive config used by argument-mapping assertions.
    s3_archive_config_fixture => fixtures::s3_archive_config;

}

/// Delegate unchanged metadata-log operations through `self.inner`, before `async_trait`.
#[moxy::attribute(name = "metadata_log_delegate")]
pub fn metadata_log_delegate(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    fixtures::metadata_log_delegate(meta, item)
}

function_macros! {
    /// Generate the finished remote-segment lookup assertions shared by manager fixtures.
    remote_segment_check => fixtures::remote_segment_check;
}

/// Add the shared runtime policy fields before TOML or CLI field derives.
#[moxy::attribute(name = "runtime_policy_fields")]
pub fn runtime_policy_fields(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    runtime_policy_fields::expand(meta, item)
}

function_macros! {
    /// Generate the shared simulation event adapter inside a harness impl.
    simulation_step => fixtures::simulation_step;

    /// Generate the shared two-partition metadata topic fixture.
    snapshot_topic_fixture => fixtures::snapshot_topic;

    /// Generate the timestamped record-batch fixture used by local and remote read tests.
    timestamped_batch => timestamped_batch::expand;

    /// Generate a producer batch builder with the caller's exact record value expression.
    producer_batch_fixture => wire_fixtures::producer_batch;

    /// Generate a one-topic `CreateTopics` request builder with explicit sizes and timeout.
    create_topic_fixture => wire_fixtures::create_topic;

    /// Generate the consumer Fetch request fixture for partition zero with a 1 MiB limit.
    consumer_fetch_fixture => wire_fixtures::consumer_fetch;

    /// Generate the initial single-replica metadata partition record builder.
    single_replica_partition_fixture => wire_fixtures::single_replica_partition;

    /// Generate the version-zero control-marker key and value fixture encoders.
    control_marker_fixture => wire_fixtures::control_marker;

    /// Generate a topic record builder for one partition and one replica.
    topic_record_fixture => wire_fixtures::topic_record;

    /// Generate projection-based Eq and Hash, and optionally Debug, for model state.
    projection_identity => projection_identity::expand;

    /// Generate the shared `MinIO` process/readiness helpers under the supplied module name.
    minio_fixture => container_fixtures::expand;

    /// Start a SCRAM client exchange while preserving its private handshake phase type.
    scram_client_first => auth_fixtures::scram_client_first;

    /// Generate the independent four-state remote-segment transition oracle.
    remote_segment_transition_matrix => fixtures::remote_segment_transition_matrix;

    /// Generate a tracing capture layer with an explicit mutex poison policy.
    capture_layer_fixture => fixtures::capture_layer;
}

/// Add the selected direct `ObjectStore` delegates to an implementation.
#[moxy::attribute(name = "object_store_delegate")]
pub fn object_store_delegate(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    object_store_delegate::expand(meta, item)
}

function_macros! {
    /// Generate remote metadata lifecycle fixtures with an explicit timestamp policy.
    remote_segment_fixtures => remote_metadata_fixtures::segment;

    /// Generate the independent missing-partition metadata assertions.
    remote_metadata_missing => remote_metadata_fixtures::missing;

    /// Generate the direct flush and shutdown delegates for a tuple write wrapper.
    async_write_delegate => async_write_delegate::expand;
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

function_macros! {
    /// Generate timer registration and completion helpers with caller-specific messages.
    timer_hooks => timer_hooks::expand;

    /// Generate the SCRAM proof XOR fixture without imposing extra length checks.
    scram_client_proof_fixture => auth_fixtures::scram_client_proof;

    /// Generate a single-partition Produce fixture with explicit request settings.
    single_partition_produce_fixture => produce_fixtures::single_partition_produce;

    /// Generate a manual Kafka request-frame fixture with explicit header inputs.
    request_frame_fixture => network_fixtures::request_frame;

    /// Generate deterministic patterned bytes for framing tests and benchmarks.
    patterned_bytes_fixture => network_fixtures::patterned_bytes;

    /// Generate the distinctive proposal UUID shared by signing golden fixtures.
    break_glass_golden_id => network_fixtures::break_glass_golden_id;

    /// Literal Kafka 4.3.1 principal bytes: alice or bob, token flag.
    jvm_principal_golden => network_fixtures::jvm_principal_golden;

    /// Generate a response frame prefix independently of production framing.
    frame_prefix_fixture => network_fixtures::frame_prefix;

    /// Generate the independent file-region byte reader used by wire comparisons.
    file_region_fixture => network_fixtures::file_region;
}

/// Expand the common I/O, TLS and SASL error variants at their original position.
#[moxy::attribute(name = "transport_errors")]
pub fn transport_errors(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    transport_errors::expand(meta, item)
}

function_macros! {
    /// Generate a voter-set fixture whose endpoints and directory IDs remain empty.
    empty_endpoint_voters => raft_fixtures::empty_endpoint_voters;

    /// Generate the one-record leader-epoch batch fixture.
    epoch_record_batch_fixture => raft_fixtures::epoch_record_batch;

    /// Generate the epoch-cache `LogView` delegation used by Raft simulations.
    epoch_log_view => raft_fixtures::epoch_log_view;
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

function_macros! {
    /// Generate the canonical compressed legacy record used by policy tests.
    legacy_policy_fixture => legacy_fixtures::policy;

    /// Generate keyed fixture records with explicit count and value size.
    keyed_record_batch_fixture => network_fixtures::keyed_record_batch;
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

function_macros! {
    /// Generate a duration-parser check over each caller's literal cases.
    duration_cases_fixture => cli_support::duration_cases;

    /// Generate a sorted directory-tree fixture with the caller's path projection.
    directory_tree_fixture => cli_support::directory_tree;

    /// Generate a CLI entry point with the caller's documentation.
    parsed_cli_entrypoint => cli_support::parsed_cli_entrypoint;

    /// Generate a duration-parser test from explicit literal cases.
    duration_parser_fixture => cli_support::duration_parser_fixture;

    /// Generate bootstrap parser tests from accepted and refused arguments.
    bootstrap_parser_fixture => cli_support::bootstrap_parser_fixture;

    /// Generate the shared snapshot fixture over an on-disk metadata log.
    snapshot_node_fixture => cli_support::snapshot_node_fixture;

    /// Connect a CLI client with the caller's identity and refusal exit code.
    connect_cli_client => cli_support::connect_cli_client;

    /// Generate the temporary metadata and data directories used by format tests.
    format_directories_fixture => cli_support::format_directories_fixture;

    /// Generate the explicit topic map used by WAL capture fixtures.
    wal_capture_topic_fixture => remote_metadata_fixtures::wal_capture_topic_fixture;

    /// Generate a CLI flag's name, environment and default-value projection.
    flag_metadata_fixture => cli_support::flag_metadata_fixture;

    /// Generate a listener bind retry with an explicit caller timeout.
    bind_retry_fixture => cli_support::bind_retry_fixture;

    /// Generate bound-listener configuration or startup with the caller's diagnostics.
    bound_start_fixture => bound_start_fixture::expand;

    /// Generate directory-assignment requests with explicit broker and partition identities.
    assignment_dirs_fixture => bound_start_fixture::assignment_dirs;

    /// Generate a registry-rendering macro that retains the caller's lock guard.
    metric_registry_fixture => bound_start_fixture::metric_registry;

    /// Generate indexed WORM segment metadata with an explicit segment-id namespace.
    worm_segment_fixture => remote_metadata_fixtures::worm_segment;

    /// Generate the started remote-segment fixture with an explicit metadata crate.
    remote_started_segment => remote_metadata_fixtures::remote_started_segment;

    /// Generate a SCRAM user request preserving optional and ordered user lists.
    scram_users_fixture => bound_start_fixture::scram_users;

    /// Generate the first matching record header decoder with an explicit UTF-8 policy.
    record_header_text => fixtures::record_header_text;

    /// Generate the shared dead-letter metric observations with caller-selected types.
    share_dlq_meters => fixtures::share_dlq_meters;

    /// Generate exported segment paths with an explicit snapshot and lazy epoch projection.
    export_segment_data_fixture => network_fixtures::export_segment_data;
}

/// Keep partition-spawn wrappers on one shared parameter list.
#[moxy::attribute(name = "partition_spawn_parameters")]
pub fn partition_spawn_parameters(
    meta: TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    network_fixtures::partition_spawn_parameters(meta, item)
}

function_macros! {
    /// Generate a checkpoint writer with a caller-selected failure kind.
    epoch_checkpoint_failure => network_fixtures::epoch_checkpoint_failure;

    /// Generate a JVM checkpoint decoder fixture with explicit bytes, name and image.
    jvm_checkpoint_dump_fixture => network_fixtures::jvm_checkpoint_dump;

    /// Compose a compile-time Java string-producer probe with shared imports and setup.
    java_string_producer_source => network_fixtures::java_string_producer;

    /// Define a wall-clock reader preserving caller attributes and overflow behavior.
    epoch_millis_fn => network_fixtures::epoch_millis;
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

function_macros! {
    /// Generate checked Unix-millisecond fixtures with explicit panic contexts.
    unix_millis_fixture => bound_start_fixture::unix_millis;

    /// Generate a Vec request frame with the caller's checked length-prefix type.
    vector_request_fixture => bound_start_fixture::vector_request;
    /// Generate the independent eight-row supported-feature oracle.
    supported_features_fixture => cross_storage_fixtures::supported_features_fixture;

    /// Generate supported-feature rows from explicit wire bounds.
    supported_feature_fixture => cross_storage_fixtures::supported_feature_fixture;
    /// Generate finalized-feature rows from explicit wire levels.
    finalized_feature_fixture => cross_storage_fixtures::finalized_feature_fixture;
    /// Generate the record input used by byte-limit tests.
    record_limit_fixture => cross_storage_fixtures::record_limit_fixture;
    /// Generate keyed compaction batches with explicit producer state and timestamp.
    compaction_record_fixture => cross_storage_fixtures::compaction_record_fixture;
    /// Project registration feature bounds into an ordered map.
    registration_feature_projection => cross_storage_fixtures::registration_feature_projection;
    /// Generate the validated connection dispatch-capacity parser.
    dispatch_capacity_parser => cli_support::dispatch_capacity_parser;
}

/// Share S3 endpoint and credential fields with the original configuration documentation.
#[moxy::attribute]
pub fn s3_fields(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    s3_fields::expand(meta, item)
}

function_macros! {
    /// Define a named `SplitMix64` step that advances the caller's stream state.
    /// Placement shuffles and benchmark byte streams share the exact same arithmetic.
    splitmix64_step => deterministic_random::splitmix64_step;
}

function_macros! {
    /// Generate an independent compression-byte oracle with a named codec level.
    compression_bytes_fixture => cross_storage_fixtures::compression_bytes_fixture;
}

mod archive_rows;

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
