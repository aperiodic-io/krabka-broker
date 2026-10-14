//! `ListTransactions` (`api_key=66`, KIP-664). Admin RPC that returns
//! a summary of every transaction the broker's coordinator is currently
//! tracking. Each summary is a `(transactional_id, producer_id, state)`
//! triple. The request can carry optional state, producer-id, duration, and
//! transactional-id-pattern filters.
//!
//! ## ACL
//!
//! Per-tid `Describe` on `TransactionalId(name)`. The handler silently
//! filters out entries the principal cannot describe, which matches the
//! JVM behavior. Cluster-wide auth is not necessary. The JVM allows
//! un-credentialed listing of "transactions you can describe."
//!
//! ## State strings
//!
//! The wire field is a string. Krabka's [`crate::txn::state::TxnState`]
//! enum already matches the JVM names verbatim (`Empty`, `Ongoing`, ...),
//! so the mapping is direct.

use {
    crate::{
        broker::Broker,
        codes,
        error::BrokerError,
        handlers::{ApiVersion, RequestContext, acl_denied},
        re2j::compile_full_match,
        txn::{state::TxnState, util::now_millis},
    },
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::owned::{
        list_transactions_request::ListTransactionsRequest,
        list_transactions_response::{ListTransactionsResponse, TransactionState},
    },
};

/// Every state name Kafka's `TransactionState.fromName` resolves. The handler
/// echoes any filter string outside this set back in the KIP-664
/// `unknown_state_filters` response field.
///
/// `PrepareEpochFence` is one of Kafka's eight states. krabka never holds it,
/// because its epoch fence is not persisted, so the name is a known filter
/// that matches nothing. `Dead` is a known filter too, and it also matches
/// nothing, because a `Dead` transaction is never listed.
const ALL_TXN_STATE_NAMES: [&str; 8] = [
    "Empty",
    "Ongoing",
    "PrepareCommit",
    "PrepareAbort",
    "PrepareEpochFence",
    "CompleteCommit",
    "CompleteAbort",
    "Dead",
];

/// Kafka's duration filter is a strict lower bound: a transaction whose age
/// equals the filter is excluded. Any negative value disables the filter.
fn matches_duration_filter(start_ms: i64, now_ms: i64, duration_filter: i64) -> bool {
    duration_filter < 0 || now_ms.saturating_sub(start_ms) > duration_filter
}

/// Kafka compiles the KIP-1038 pattern with RE2J and matches the complete
/// transactional id (`Matcher.matches()`).
///
/// # Errors
///
/// Returns the reason a pattern does not compile.
fn compile_transactional_id_pattern(pattern: Option<&str>) -> Result<Option<regex::Regex>, String> {
    pattern
        .filter(|pattern| !pattern.is_empty())
        .map(compile_full_match)
        .transpose()
}

pub(crate) async fn handle(
    broker: &Broker,
    req: ListTransactionsRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<ListTransactionsResponse, BrokerError> {
    // v0/v1 decode this field as `None`; v2 null and empty values both disable
    // the filter. Kafka returns a top-level INVALID_REGULAR_EXPRESSION rather
    // than treating malformed syntax as a pattern that matches nothing.
    let transactional_id_pattern =
        match compile_transactional_id_pattern(req.transactional_id_pattern.as_deref()) {
            Ok(pattern) => pattern,
            Err(error) => {
                tracing::debug!(
                    pattern_bytes = req
                        .transactional_id_pattern
                        .as_deref()
                        .map_or(0, str::len),
                    %error,
                    "invalid ListTransactions transactional-id pattern"
                );
                return Ok(ListTransactionsResponse {
                    throttle_time_ms: 0,
                    error_code: codes::INVALID_REGULAR_EXPRESSION,
                    ..Default::default()
                });
            }
        };

    // Kafka answers COORDINATOR_LOAD_IN_PROGRESS while any owned transaction
    // state partition loads, because the list would be short of whatever those
    // partitions hold.
    if broker.txn_coordinator.any_partition_loading().await {
        return Ok(ListTransactionsResponse {
            throttle_time_ms: 0,
            error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
            ..Default::default()
        });
    }

    let image = broker.controller.current_image();

    // Snapshot every coordinator-local txn entry.
    let entries = broker.txn_coordinator.snapshot().await;

    let state_filter: std::collections::HashSet<String> =
        req.state_filters.iter().cloned().collect();
    let pid_filter: std::collections::HashSet<i64> =
        req.producer_id_filters.iter().copied().collect();
    let now_ms = now_millis();

    // KIP-664: if filtered states include a string the broker doesn't
    // recognize, surface it in `unknown_state_filters` so the client
    // knows its filter is overly conservative.
    // Kafka turns the filter list into a Set in `KafkaApis`, so each unknown
    // name is reported once. `ALL_TXN_STATE_NAMES` is eight entries, so a
    // linear scan beats hashing it into a set on every request.
    let mut seen_unknown = std::collections::HashSet::new();
    let unknown_state_filters: Vec<String> = req
        .state_filters
        .iter()
        .filter(|name| !ALL_TXN_STATE_NAMES.contains(&name.as_str()))
        .filter(|name| seen_unknown.insert(name.as_str()))
        .cloned()
        .collect();

    let mut out: Vec<TransactionState> = Vec::with_capacity(entries.len());
    for entry in entries {
        // Kafka filters the `Dead` state out: it means the transactional id
        // and its metadata are being expired.
        if entry.state == TxnState::Dead {
            continue;
        }
        let state = entry.state.as_str();

        // State filter: empty = no filter; otherwise the entry's state
        // must be one of the requested ones.
        if !state_filter.is_empty() && !state_filter.contains(state) {
            continue;
        }
        // Producer-id filter: same semantics — empty means no filter. The
        // wire filter set is raw `i64`; unwrap the entry's `ProducerId` to match.
        if !pid_filter.is_empty() && !pid_filter.contains(&entry.producer_id.get()) {
            continue;
        }
        // DurationFilter is present from v1. Decoding older versions supplies
        // the protocol default (-1), which disables this strict lower bound.
        if !matches_duration_filter(entry.start_ms, now_ms, req.duration_filter) {
            continue;
        }
        // TransactionalIdPattern is present from v2. Null/empty values compile
        // to `None`, while a non-empty pattern is a full-string match.
        if transactional_id_pattern
            .as_ref()
            .is_some_and(|pattern| !pattern.is_match(&entry.transactional_id))
        {
            continue;
        }
        // ACL: per-tid `Describe` on `TransactionalId`. Silent filter on
        // Deny.
        if acl_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            ResourceType::TransactionalId,
            entry.transactional_id.as_str(),
            AclOperation::Describe,
        ) {
            continue;
        }

        out.push(TransactionState {
            transactional_id: entry.transactional_id.clone(),
            // Unwrap into the raw-`i64` wire field.
            producer_id: entry.producer_id.get(),
            transaction_state: state.to_string(),
            ..Default::default()
        });
    }

    Ok(ListTransactionsResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        unknown_state_filters,
        transaction_states: out,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::{
            broker::Broker,
            handlers::{RequestContext, test_support::transaction_admin_tests::*},
            test_support::dispatch_wire,
        },
        assert2::assert,
    };

    #[yare::parameterized(
        empty = { TxnState::Empty, "Empty" },
        ongoing = { TxnState::Ongoing, "Ongoing" },
        prepare_commit = { TxnState::PrepareCommit, "PrepareCommit" },
        prepare_abort = { TxnState::PrepareAbort, "PrepareAbort" },
        complete_commit = { TxnState::CompleteCommit, "CompleteCommit" },
        complete_abort = { TxnState::CompleteAbort, "CompleteAbort" },
        dead = { TxnState::Dead, "Dead" },
    )]
    fn txn_state_as_str_matches_jvm_names(state: TxnState, want: &str) {
        assert2::assert!(state.as_str() == want, "{state:?}");
    }

    #[yare::parameterized(
        negative_disables = { 20_000, -2, true },
        older_than_cutoff = { 8_999, 1_000, true },
        exactly_on_cutoff = { 9_000, 1_000, false },
        newer_than_cutoff = { 9_001, 1_000, false },
        future_start_with_zero_filter = { 11_000, 0, false },
    )]
    fn duration_filter_is_a_strict_lower_bound(start_ms: i64, filter: i64, expected: bool) {
        let now_ms = 10_000;
        assert2::assert!(
            matches_duration_filter(start_ms, now_ms, filter) == expected,
            "start={start_ms} filter={filter}"
        );
    }

    #[yare::parameterized(
        absent_pattern_accepts_anything = { None, "anything", true },
        empty_pattern_accepts_anything = { Some(""), "anything", true },
        matching_pattern = { Some("txn-.*"), "txn-alpha", true },
        match_must_cover_whole_id = { Some("txn-.*"), "prefix-txn-alpha", false },
        alternation_matches_long_id = { Some("txn|txn-long"), "txn-long", true },
    )]
    fn transactional_id_pattern_is_optional_and_matches_the_whole_id(
        pattern: Option<&str>,
        transactional_id: &str,
        expected: bool,
    ) {
        let compiled = compile_transactional_id_pattern(pattern).expect("valid pattern");
        let matches = compiled
            .as_ref()
            .is_none_or(|pattern| pattern.is_match(transactional_id));
        assert2::assert!(
            matches == expected,
            "{pattern:?} against {transactional_id}"
        );
    }

    /// Kafka compiles the pattern with RE2J, which has no lookahead,
    /// backreferences, or possessive quantifiers.
    #[yare::parameterized(
        lookahead = { "(?=txn-).*" },
        backreference = { r"(txn)-\1" },
        possessive_quantifier = { "a*+" },
        unclosed_group = { "(unclosed" },
    )]
    fn transactional_id_pattern_rejects_unsupported_regex(refused: &str) {
        assert2::assert!(
            compile_transactional_id_pattern(Some(refused)).is_err(),
            "{refused}"
        );
    }

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    /// Serve `req` over the wire at `version`, so that the fields an older
    /// version drops never reach the handler.
    async fn over_the_wire(
        broker: &Broker,
        version: i16,
        req: &ListTransactionsRequest,
        ctx: &RequestContext<'_>,
    ) -> ListTransactionsResponse {
        dispatch_wire(
            broker,
            krabka_protocol::owned::list_transactions_request::API_KEY,
            version,
            req,
            ctx,
        )
        .await
    }

    /// A request version and filters, and the producer ids it keeps.
    struct FilterCase<'a> {
        version: i16,
        duration_filter: i64,
        pattern: Option<&'a str>,
        expected_pids: &'a [i64],
    }

    /// Seed one transaction and exercise the filters through their actual wire
    /// versions. This also protects the producer-id filter from being inverted.
    #[tokio::test]
    async fn filters_follow_wire_versions_and_keep_matching_entries() {
        use {crate::txn::state::TxnEntry, krabka_log::ProducerId};

        let max_ver = krabka_protocol::owned::list_transactions_response::MAX_VERSION;
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [start_broker] allow_all;
        };

        // Lead the __transaction_state partition this tid hashes to so the
        // coordinator can persist the seeded entry.
        let tid = "txn-list-pid-filter";
        let coord = &broker.txn_coordinator;
        lead_transaction_state_partitions(&broker_handle, &[coord.partition_for(tid)]).await;

        let entry = TxnEntry::new_empty(tid.to_string(), ProducerId(100), 0, 60_000, 0);
        coord
            .put(entry, TxnVersion::Classic)
            .await
            .expect("seed txn entry");

        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["admin"];
        };
        // Filter on the seeded pid → the matching entry must be kept.
        let req = ListTransactionsRequest {
            producer_id_filters: vec![100],
            duration_filter: -1,
            ..Default::default()
        };
        let resp = handle(&broker, req, max_ver, &ctx).await.expect("handle");

        let pids: Vec<i64> = resp
            .transaction_states
            .iter()
            .map(|s| s.producer_id)
            .collect();
        assert!(
            pids == vec![100],
            "pid filter must keep the matching entry, got {pids:?}"
        );

        let cases: [FilterCase<'_>; 4] = [
            // v0 omits both newer fields, even when the in-memory request sets
            // DurationFilter.
            FilterCase {
                version: 0,
                duration_filter: i64::MAX,
                pattern: None,
                expected_pids: &[100],
            },
            // v1 carries DurationFilter and this transaction is not old enough
            // to exceed the maximum threshold.
            FilterCase {
                version: 1,
                duration_filter: i64::MAX,
                pattern: None,
                expected_pids: &[],
            },
            // v2 carries TransactionalIdPattern and uses full-string matching.
            FilterCase {
                version: 2,
                duration_filter: -1,
                pattern: Some("txn-list-.*"),
                expected_pids: &[100],
            },
            FilterCase {
                version: 2,
                duration_filter: -1,
                pattern: Some("list"),
                expected_pids: &[],
            },
        ];
        for FilterCase {
            version,
            duration_filter,
            pattern,
            expected_pids,
        } in cases
        {
            let req = ListTransactionsRequest {
                producer_id_filters: vec![100],
                duration_filter,
                transactional_id_pattern: pattern.map(str::to_owned),
                ..Default::default()
            };
            let resp = over_the_wire(&broker, version, &req, &ctx).await;
            let pids: Vec<i64> = resp
                .transaction_states
                .iter()
                .map(|state| state.producer_id)
                .collect();
            assert!(
                pids == expected_pids,
                "version {version}, pattern {pattern:?}"
            );
        }

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handler_reports_unknown_state_filters_and_top_level_fields() {
        let version = krabka_protocol::owned::list_transactions_response::MAX_VERSION;
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker] allow_all;
            user ["admin"];
        };
        let req = ListTransactionsRequest {
            state_filters: vec!["Ongoing".into(), "MysteryState".into()],
            producer_id_filters: vec![42],
            duration_filter: -1,
            transactional_id_pattern: Some("txn-*".into()),
            ..Default::default()
        };

        let resp = handle(&broker, req, version, &ctx).await.expect("handle");

        let expected = krabka_macros::unthrottled_wire!(ListTransactionsResponse {
            error_code: codes::NONE,
            unknown_state_filters: vec!["MysteryState".to_string()],
            transaction_states: vec![],
        });
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka lists no `Dead` transaction, resolves all eight state names, and
    /// reports each unknown name once (`TransactionState.fromName` and the
    /// `Set` in `KafkaApis`).
    #[tokio::test]
    async fn dead_transactions_are_never_listed_and_unknown_filters_are_unique() {
        use {crate::txn::state::TxnEntry, krabka_log::ProducerId};

        let version = krabka_protocol::owned::list_transactions_response::MAX_VERSION;
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [start_broker] allow_all;
        };
        let coordinator = &broker.txn_coordinator;
        // Two transactions in one state partition: one ongoing, one dying.
        let (ongoing_id, dead_id) = ("txn-list-ongoing", "txn-list-dead");
        lead_transaction_state_partitions(&broker_handle, &[
            coordinator.partition_for(ongoing_id),
            coordinator.partition_for(dead_id),
        ])
        .await;
        for tid in [ongoing_id, dead_id] {
            let dead = tid == dead_id;
            let producer_id = if dead { 200 } else { 100 };
            let mut entry =
                TxnEntry::new_empty(tid.to_string(), ProducerId(producer_id), 0, 60_000, 0);
            entry.state = if dead {
                TxnState::Dead
            } else {
                TxnState::Ongoing
            };
            coordinator
                .put(entry, TxnVersion::Classic)
                .await
                .expect("seed the transaction");
        }

        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["admin"];
        };
        // (state filters, listed ids, unknown filters)
        let cases = [
            (Vec::new(), vec![ongoing_id.to_string()], Vec::new()),
            (vec!["Dead".to_string()], Vec::new(), Vec::new()),
            (
                vec!["PrepareEpochFence".to_string()],
                Vec::new(),
                Vec::new(),
            ),
            (
                vec!["Nope".to_string(), "Nope".to_string()],
                Vec::new(),
                vec!["Nope".to_string()],
            ),
        ];
        let mut expected = Vec::new();
        let mut actual = Vec::new();
        for (state_filters, listed, unknown) in cases {
            let request = ListTransactionsRequest {
                state_filters: state_filters.clone(),
                duration_filter: -1,
                ..Default::default()
            };
            let response = handle(&broker, request, version, &ctx)
                .await
                .expect("handle");
            expected.push((state_filters.clone(), listed, unknown));
            actual.push((
                state_filters,
                response
                    .transaction_states
                    .iter()
                    .map(|row| row.transactional_id.clone())
                    .collect(),
                response.unknown_state_filters,
            ));
        }
        assert!(actual == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_pattern_is_rejected_only_when_present_on_the_wire() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker] allow_all;
            user ["admin"];
        };

        for (version, expected_error) in [(1, codes::NONE), (2, codes::INVALID_REGULAR_EXPRESSION)]
        {
            let req = ListTransactionsRequest {
                transactional_id_pattern: Some("(unclosed".into()),
                ..Default::default()
            };
            let response = over_the_wire(&broker, version, &req, &ctx).await;
            assert!(response.error_code == expected_error, "version {version}");
            assert!(response.transaction_states.is_empty(), "version {version}");
        }

        broker_handle.shutdown().await;
    }
}
