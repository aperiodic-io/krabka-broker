//! Item generators for fixtures shared across crate boundaries.

use moxy::{
    ast::ParseError,
    token::{Ident, Span, TokenStream, TokenTree},
};

/// Top-level comma splitting that retains the input's diagnostic span.
pub(crate) struct CommaArguments {
    pub(crate) span: Span,
    parts: Vec<Vec<TokenTree>>,
}

impl CommaArguments {
    pub(crate) fn new(input: TokenStream) -> Self {
        let span = input.span();
        let tokens = Vec::from(input);
        Self {
            span,
            parts: tokens
                .split(TokenTree::is_punct_comma)
                .map(<[TokenTree]>::to_vec)
                .collect(),
        }
    }

    pub(crate) fn parts(&self) -> Vec<&[TokenTree]> {
        self.parts.iter().map(Vec::as_slice).collect()
    }
}

pub(crate) fn name(tokens: TokenStream) -> Result<Ident, ParseError> {
    let tokens: Vec<_> = tokens.into_iter().collect();
    match tokens.as_slice() {
        [TokenTree::Ident(name)] => Ok(name.clone()),
        _ => Err(ParseError::new(
            Span::call_site(),
            "fixture generator needs one item name",
        )),
    }
}

/// Build named fixture items after validating the generator's item name.
pub(crate) fn named_items(
    input: TokenStream,
    build: impl FnOnce(Ident) -> TokenStream,
) -> Result<TokenStream, ParseError> {
    Ok(build(name(input)?))
}

/// Parse an item name followed by a fixed number of generator arguments.
pub(crate) fn named_arguments(
    input: TokenStream,
    count: usize,
) -> Result<(Ident, std::vec::IntoIter<TokenStream>), ParseError> {
    let mut arguments = crate::meta::arguments(input, count)?.into_iter();
    let name = self::name(arguments.next().expect("item name argument"))?;
    Ok((name, arguments))
}

/// Parse the common item-name and crate-path pair of fixture generators.
pub(crate) fn named_root(input: TokenStream) -> Result<(Ident, TokenStream), ParseError> {
    let [name, root]: [TokenStream; 2] = crate::meta::arguments(input, 2)?
        .try_into()
        .expect("two arguments");
    Ok((self::name(name)?, root))
}

/// Assemble a fixture function while leaving its signature and body explicit.
pub(crate) fn function(
    input: TokenStream,
    visibility: &TokenStream,
    parameters: &TokenStream,
    returns: &TokenStream,
    body: &TokenStream,
) -> Result<TokenStream, ParseError> {
    let name = self::name(input)?;
    Ok(moxy::template! {
        {{ visibility }} fn {{ name }}({{ parameters }}) -> {{ returns }} {
            {{ body }}
        }
    })
}

pub(crate) fn capture_layer(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let tokens: Vec<_> = tokens.into_iter().collect();
    let [
        TokenTree::Ident(name),
        comma,
        TokenTree::Ident(captured),
        comma2,
        TokenTree::Ident(policy),
    ] = tokens.as_slice()
    else {
        return Err(ParseError::new(
            Span::call_site(),
            "expected layer name, captured type name, lock policy",
        ));
    };
    if !comma.is_punct_comma() || !comma2.is_punct_comma() {
        return Err(ParseError::new(
            Span::call_site(),
            "expected comma-separated capture layer arguments",
        ));
    }
    let lock = if policy == "unwrap" {
        moxy::template! { .unwrap() }
    } else if policy == "recover_poison" {
        moxy::template! { .unwrap_or_else(::std::sync::PoisonError::into_inner) }
    } else {
        return Err(ParseError::new(
            policy.span(),
            "expected unwrap or recover_poison lock policy",
        ));
    };
    Ok(moxy::template! {
        type {{ captured }} = ::std::sync::Arc<::std::sync::Mutex<Vec<String>>>;
        struct {{ name }}({{ captured }});
        impl<S: ::tracing::Subscriber> ::tracing_subscriber::Layer<S> for {{ name }} {
            fn on_event(&self, event: &::tracing::Event<'_>, _cx: ::tracing_subscriber::layer::Context<'_, S>) {
                let meta = event.metadata();
                self.0.lock() {{ lock }} .push(format!("{}:{}", meta.target(), meta.level()));
            }
        }
    })
}

pub(crate) fn remote_segment_transition_matrix(
    tokens: TokenStream,
) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        fn {{ name }}() -> [[bool; 4]; 4] {
            [
                [true, true, true, false],
                [false, true, true, false],
                [false, false, true, true],
                [false, false, false, true],
            ]
        }
    })
}

pub(crate) fn model_transition(
    tokens: proc_macro::TokenStream,
) -> Result<proc_macro::TokenStream, ParseError> {
    let tokens: Vec<_> = tokens.into_iter().collect();
    let [
        proc_macro::TokenTree::Ident(last),
        comma,
        proc_macro::TokenTree::Ident(action),
        comma2,
        proc_macro::TokenTree::Ident(state),
        semi,
        proc_macro::TokenTree::Group(body),
    ] = tokens.as_slice()
    else {
        return Err(ParseError::new(
            Span::call_site(),
            "expected last, action, state; { transition body }",
        ));
    };
    if !matches!(comma, proc_macro::TokenTree::Punct(punct) if punct.as_char() == ',')
        || !matches!(comma2, proc_macro::TokenTree::Punct(punct) if punct.as_char() == ',')
        || !matches!(semi, proc_macro::TokenTree::Punct(punct) if punct.as_char() == ';')
        || body.delimiter() != proc_macro::Delimiter::Brace
    {
        return Err(ParseError::new(
            Span::call_site(),
            "expected three model bindings and a brace-delimited transition body",
        ));
    }
    let last: Ident = last.clone().into();
    let action: Ident = action.clone().into();
    let state: Ident = state.clone().into();
    let mut method: proc_macro::TokenStream = moxy::template! {
        fn next_state(&self, {{ last }}: &Self::State, {{ action }}: Self::Action) -> Option<Self::State>
    }.into();
    let mut kernel: proc_macro::TokenStream = moxy::template! {
        let mut {{ state }} = {{ last }}.clone();
    }
    .into();
    // Keep the caller's compiler tokens intact. Moxy's Group conversion joins
    // its opening/closing spans, which makes multiline else blocks appear
    // separated from `else` to Clippy's source-based formatting checks.
    kernel.extend(body.stream());
    method.extend([proc_macro::TokenTree::Group(proc_macro::Group::new(
        proc_macro::Delimiter::Brace,
        kernel,
    ))]);
    Ok(method)
}

pub(crate) fn bounded_bfs(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        /// Search breadth-first to completion and reject either truncation cap.
        pub(crate) fn {{ name }}<M>(model: M, label: &str, max_depth: usize, max_states: usize) -> impl ::stateright::Checker<M>
        where M: ::stateright::Model + Send + Sync + 'static,
            M::State: ::std::hash::Hash + Send + Sync + Clone + PartialEq + 'static,
            M::Action: Clone + PartialEq,
        {
            use ::stateright::{Checker as _, Model as _};
            let checker = model.checker().target_max_depth(max_depth).target_state_count(max_states).spawn_bfs().join();
            eprintln!("[{label}] unique_states={} generated={} max_depth={}", checker.unique_state_count(), checker.state_count(), checker.max_depth());
            ::assert2::assert!(checker.max_depth() < max_depth, "[{label}] hit depth cap {max_depth}: depth-truncated, not exhaustive");
            ::assert2::assert!(checker.state_count() < max_states, "[{label}] hit state cap {max_states}: truncated, not exhaustive");
            checker
        }
    })
}

pub(crate) fn compacted_batch(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let (name, full) = if tokens
        .clone()
        .into_iter()
        .any(|token| token.is_punct_comma())
    {
        let arguments = crate::meta::arguments(tokens, 2)?;
        (
            name(arguments[0].clone())?,
            crate::meta::mode(arguments[1].clone(), ["plain", "full"])? == "full",
        )
    } else {
        (name(tokens)?, true)
    };
    Ok(moxy::template! {
        /// Producer state and records retained in one compacted batch.
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct {{ name }} {
            base_offset: i64,
            last_offset: i64,
            producer_id: i64,
            producer_epoch: i16,
            base_sequence: i32,
            @if full { control: bool, }
            records: Vec<(Option<::bytes::Bytes>, Option<::bytes::Bytes>)>,
        }
        impl {{ name }} {
            fn of(batch: &::krabka_protocol::records::RecordBatch) -> Self {
                Self {
                    base_offset: batch.base_offset,
                    last_offset: batch.base_offset + i64::from(batch.last_offset_delta),
                    producer_id: batch.producer_id,
                    producer_epoch: batch.producer_epoch,
                    base_sequence: batch.base_sequence,
                    @if full { control: batch.attributes.is_control_batch(), }
                    records: batch.records.iter().map(|record| (record.key.clone(), record.value.clone())).collect(),
                }
            }
            @if full {
            /// The original one-record batch, relocated to `offset`.
            fn whole(offset: i64, batch: &::krabka_protocol::records::RecordBatch) -> Self {
                Self { base_offset: offset, last_offset: offset, ..Self::of(batch) }
            }
            /// The bare header Kafka's `RETAIN_EMPTY` preserves.
            fn header(offset: i64, batch: &::krabka_protocol::records::RecordBatch) -> Self {
                Self { records: Vec::new(), ..Self::whole(offset, batch) }
            }
            }
        }
    })
}

/// Generate the first matching header lookup with the caller's UTF-8 policy.
pub(crate) fn record_header_text(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let arguments = crate::meta::arguments(tokens, 2)?;
    let name = name(arguments[0].clone())?;
    let strict = crate::meta::mode(arguments[1].clone(), ["strict", "lossy"])? == "strict";
    Ok(moxy::template! {
        fn {{ name }}(record: &::krabka_protocol::records::Record, key: &str) -> Option<String> {
            record.headers.iter()
                .find(|header| header.key == key)
                .and_then(|header| header.value.as_ref())
                .map(|value| {
                    @if strict { String::from_utf8(value.to_vec()).unwrap() }
                    @else { String::from_utf8_lossy(value).into_owned() }
                })
        }
    })
}

/// Generate the three DLQ counter reads with explicit caller types and docs.
pub(crate) fn share_dlq_meters(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let arguments = crate::meta::arguments(tokens, 3)?;
    let mut name_tokens = Vec::from(arguments[0].clone());
    let name = name(name_tokens.pop().into_iter().collect())?;
    let attributes = TokenStream::from(name_tokens);
    let metrics = &arguments[1];
    let label = &arguments[2];
    Ok(moxy::template! {
        {{ attributes }}
        fn {{ name }}(metrics: &{{ metrics }}, group: &str) -> (u64, u64, u64) {
            let label = {{ label }} { group_id: group.to_owned() };
            (
                metrics.share_group_dlq_records.get_or_create(&label).get(),
                metrics.share_group_dlq_produce_requests.get_or_create(&label).get(),
                metrics.share_group_dlq_failed_produce_requests.get_or_create(&label).get(),
            )
        }
    })
}

pub(crate) fn record_batch(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let mut args = crate::meta::arguments(tokens.clone(), 1)
        .or_else(|_| crate::meta::arguments(tokens, 2))?
        .into_iter();
    let name = name(args.next().expect("fixture name"))?;
    let (epoch_field, last_offset) = if let Some(epoch) = args.next() {
        (
            moxy::template! { partition_leader_epoch: {{ epoch }}, },
            moxy::template! { n - 1 },
        )
    } else {
        (TokenStream::new(), moxy::template! { (n - 1).max(0) })
    };
    Ok(moxy::template! {
        fn {{ name }}(n: i32, payload_size: usize) -> ::krabka_protocol::records::RecordBatch {
            ::krabka_protocol::records::RecordBatch {
                {{ epoch_field }}
                last_offset_delta: {{ last_offset }},
                records: (0..n).map(|offset_delta| ::krabka_protocol::records::Record {
                    offset_delta,
                    key: Some(::bytes::Bytes::from(format!("k{offset_delta:08}"))),
                    value: Some(::bytes::Bytes::from(vec![0xABu8; payload_size])),
                    ..Default::default()
                }).collect(),
                ..Default::default()
            }
        }
    })
}

/// Generate the shared action adapter inside either simulation harness impl.
pub(crate) fn simulation_actions(root: TokenStream) -> Result<TokenStream, ParseError> {
    let root =
        crate::meta::required_tokens(root, "simulation_actions needs the consensus crate path")?;
    Ok(moxy::template! {
        /// Apply consensus effects, leaving each harness's durability bookkeeping to its hook.
        pub(super) fn apply_action(&mut self, id: {{ root }}::types::NodeId, action: {{ root }}::action::Action) {
            use {{ root }}::{action::{Action, TimerKind}, event::Event};
            if let Some(messages) = {{ root }}::simulation_support::action_messages(id, &action, &self.voter_ids, &self.all_node_ids(), self.nodes[&id].log.log_end()) {
                for (peer, event) in messages { self.send(id, peer, event); }
                return;
            }
            match action {
                Action::SendFetch { leader_id } => {
                    self.replicate_from_leader(id, leader_id);
                    let log = &self.nodes[&id].log;
                    self.send(id, leader_id, Event::ReceiveFetch { from: id, fetch_epoch: log.last_epoch(), fetch_offset: log.end_offset() });
                }
                Action::AppendLeaderChange { epoch } => self.nodes.get_mut(&id).unwrap().log.append_in_epoch(epoch, 1),
                Action::AdvanceHighWatermark(hwm) => self.advance_simulated_watermark(id, hwm),
                Action::TruncateTo(point) => self.nodes.get_mut(&id).unwrap().log.truncate_to(point.offset),
                Action::ResetTimer { kind, deadline } => {
                    let node = self.nodes.get_mut(&id).unwrap();
                    match kind {
                        TimerKind::Election => node.election_deadline = Some(deadline),
                        TimerKind::Fetch => node.fetch_deadline = Some(deadline),
                        TimerKind::CheckQuorum => node.check_quorum_deadline = Some(deadline),
                    }
                }
                Action::TransitionedTo(_) | Action::PersistQuorumState | Action::ReplyDivergingEpoch(_) => {},
                _ => unreachable!("message actions handled above"),
            }
        }
    })
}

/// Delegate a metadata-log wrapper's unchanged operations before `async_trait` expands it.
pub(crate) fn metadata_log_delegate(
    root: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    let (mut tokens, body) =
        crate::meta::item_body(item, "metadata_log_delegate", TokenTree::is_keyword_impl)?;
    let mut root_tokens: Vec<_> = root.into_iter().collect();
    // Optional transport operations require explicit selectors.
    let mut keyed = false;
    let mut range = false;
    loop {
        let selector = root_tokens.last().and_then(TokenTree::as_ident);
        match selector {
            Some(name) if name == "keyed" => keyed = true,
            Some(name) if name == "range" => range = true,
            _ => break,
        }
        root_tokens.pop();
        if root_tokens
            .last()
            .is_none_or(|token| !token.is_punct_comma())
        {
            return Err(ParseError::new(
                Span::call_site(),
                "expected crate path followed by comma-separated selectors",
            ));
        }
        root_tokens.pop();
    }
    let root = crate::meta::required_tokens(
        TokenStream::from(root_tokens),
        "metadata_log_delegate needs the metadata log crate path",
    )?;
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    let candidates = [
        (
            "partition_count",
            moxy::template! { fn partition_count(&self) -> i32 { self.inner.partition_count() } },
        ),
        (
            "publish",
            moxy::template! { async fn publish(&self, partition: i32, event: ::bytes::Bytes) -> Result<i64, {{ root }}::error::MetadataLogError> { self.inner.publish(partition, event).await } },
        ),
        (
            "subscribe",
            moxy::template! { fn subscribe(&self, assignment: Vec<{{ root }}::log::PartitionStart>) -> ({{ root }}::log::MetadataEventStream, ::std::sync::Arc<dyn {{ root }}::log::AssignmentHandle>) { self.inner.subscribe(assignment) } },
        ),
        (
            "high_water_marks",
            moxy::template! { async fn high_water_marks(&self) -> Result<Vec<i64>, {{ root }}::error::MetadataLogError> { self.inner.high_water_marks().await } },
        ),
    ];
    let existing: Vec<_> = group.tokens.clone().into_iter().collect();
    let has_method = |method: &str| {
        existing.windows(2).any(|tokens| {
            tokens[0].is_keyword_fn() && tokens[1].as_ident().is_some_and(|name| name == method)
        })
    };
    let mut methods = TokenStream::new();
    for (method, implementation) in candidates {
        if !has_method(method) {
            methods.extend(implementation);
        }
    }
    if keyed && !has_method("publish_keyed") {
        methods.extend(moxy::template! {
            async fn publish_keyed(&self, partition: i32, key: ::bytes::Bytes, event: Option<::bytes::Bytes>) -> Result<i64, {{ root }}::error::MetadataLogError> {
                self.inner.publish_keyed(partition, key, event).await
            }
        });
    }
    if range && !has_method("visit_range") {
        methods.extend(moxy::template! {
            async fn visit_range(
                &self,
                partition: i32,
                start: i64,
                end: i64,
                visit: &mut {{ root }}::log::RangeVisitor<'_>,
            ) -> Result<(), {{ root }}::error::MetadataLogError> {
                self.inner.visit_range(partition, start, end, visit).await
            }
        });
    }
    group.tokens = methods.into_iter().chain(group.tokens.clone()).collect();
    Ok(tokens.into())
}

pub(crate) fn remote_segment_check(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        fn {{ name }}(manager: &impl ::krabka_remote_storage::RemoteLogMetadataManager, partition: &::krabka_remote_storage::TopicIdPartition) {
            let got = manager.remote_log_segment_metadata(partition, ::krabka_ids::LeaderEpoch(0), 42).unwrap().expect("segment found");
            ::assert2::check!(got.remote_log_segment_id().id == ::uuid::Uuid::from_u128(10));
            ::assert2::check!(got.custom_metadata() == Some(&::krabka_remote_storage::CustomMetadata(vec![7])));
            ::assert2::check!(manager.highest_offset_for_epoch(partition, ::krabka_ids::LeaderEpoch(0)).unwrap() == Some(99));
        }
    })
}

/// The harness event adapter, shared by browser and storage simulations.
pub(crate) fn simulation_step(root: TokenStream) -> Result<TokenStream, ParseError> {
    let root =
        crate::meta::required_tokens(root, "simulation_step needs the consensus crate path")?;
    Ok(moxy::template! {
        /// Feed one event to the consensus machine, then apply its effects.
        pub(super) fn step(&mut self, id: {{ root }}::NodeId, event: {{ root }}::Event) {
            let now = self.now;
            let fetch_from = if let {{ root }}::Event::ReceiveFetch { from, .. } = &event { Some(*from) } else { None };
            // Release the node borrow before translating effects involving peers.
            let actions = {
                let node = self.nodes.get_mut(&id).unwrap();
                node.machine.on_event(event, &node.log, now)
            };
            // A caught-up fetch parks: answer only new data or divergence so the
            // watchdog bounds the steady-state fetch loop deterministically.
            if let Some(follower) = fetch_from {
                let node = &self.nodes[&id];
                if node.machine.role().is_leader()
                    && let Some(response) = {{ root }}::simulation_support::fetch_response(id, node.machine.quorum_state().leader_epoch, &actions, node.log.end_offset(), self.nodes[&follower].log.end_offset()) {
                    self.send(id, follower, response);
                }
            }
            for action in actions { self.apply_action(id, action); }
            self.reconcile_timers_for_role(id);
        }
    })
}

/// Append exactly the topic and partition records used by snapshot compatibility tests.
pub(crate) fn snapshot_topic(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        fn {{ name }}(image: &mut ::krabka_metadata::MetadataImage) {
            use ::krabka_metadata::{LeaderEpoch, MetadataRecord, NodeId, PartitionRecord, TopicRecord};
            image.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: "orders".into(), topic_id: ::uuid::Uuid::new_v4(), partitions: 2, replication_factor: 1,
            }));
            for partition in 0..2 {
                image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                    topic: "orders".into(), partition, leader: NodeId(1), replicas: vec![NodeId(1)], isr: vec![NodeId(1)], leader_epoch: LeaderEpoch(0), adding_replicas: vec![], removing_replicas: vec![], directories: vec![], partition_epoch: 0,
                }));
            }
        }
    })
}

pub(crate) fn s3_archive_config(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let [name_tokens, config_type]: [TokenStream; 2] = crate::meta::arguments(tokens, 2)?
        .try_into()
        .expect("two arguments");
    let name = name(name_tokens)?;
    Ok(moxy::template! {
        fn {{ name }}() -> {{ config_type }} {
            {{ config_type }} {
                bucket: "backups".into(), region: "eu-west-1".into(),
                endpoint: Some("http://minio:9000".into()),
                access_key_id: Some("key".into()), secret_access_key: Some("secret".into()),
                allow_http: true, ..Default::default()
            }
        }
    })
}

pub(crate) fn benchmark_latency(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        fn {{ name }}() -> LatencyPercentiles {
            use krabka_units::{micros, millis};
            LatencyPercentiles {
                p50: micros(1500), p95: micros(3200), p99: micros(4250),
                p999: millis(9), max: millis(42), mean: micros(1800), count: 600_000,
            }
        }
    })
}

pub(crate) fn benchmark_scenario(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        fn {{ name }}(name: &str, msg_size: krabka_units::ByteSize) -> Scenario {
            use krabka_units::prelude::*;
            Scenario {
                name: name.into(), mode_tag: ModeTag::Ci, msg_size,
                key_size: ByteSize::ZERO, partitions: 6, replication_factor: 1,
                producers: 1, consumers: 1, mode: LoadMode::Saturate,
                acks: Acks::Leader, compression: Compression::None,
                linger: millis(5), batch_size: kibibytes(16),
                duration: secs(60), warmup: secs(10), failover: None,
            }
        }
    })
}

pub(crate) fn simulation_transport(root: TokenStream) -> Result<TokenStream, ParseError> {
    let root =
        crate::meta::required_tokens(root, "simulation_transport needs the consensus crate path")?;
    Ok(moxy::template! {
        fn can_replicate(&self, follower: NodeId, leader: NodeId) -> bool {
            follower != leader
                && !self.partitioned.contains(&follower)
                && !self.partitioned.contains(&leader)
                && self.nodes[&leader].machine.role().is_leader()
        }
        pub(super) fn send(&mut self, src: NodeId, dst: NodeId, event: {{ root }}::event::Event) {
            if !self.partitioned.contains(&src) && !self.partitioned.contains(&dst) {
                self.queue.push_back(Message { src, dst, event });
            }
        }
        pub(super) fn reachable_leader(&self, id: NodeId) -> Option<NodeId> {
            {{ root }}::simulation_support::reachable_leader(
                self.nodes[&id].machine.role(), id, &self.partitioned,
                |leader| self.nodes.get(&leader).is_some_and(|node| node.machine.role().is_leader()),
            )
        }
        fn all_node_ids(&self) -> Vec<NodeId> {
            self.nodes.keys().copied().collect()
        }
    })
}

pub(crate) fn simulation_heartbeat(input: TokenStream) -> Result<TokenStream, ParseError> {
    let [root, interval]: [TokenStream; 2] = crate::meta::arguments(input, 2)?
        .try_into()
        .expect("two arguments");
    Ok(moxy::template! {
        /// Re-broadcast the leader epoch and re-arm its heartbeat timer.
        fn fire_leader_heartbeat(&mut self, id: {{ root }}::types::NodeId) {
            if !self.nodes[&id].machine.role().is_leader() { return; }
            let epoch = self.nodes[&id].machine.quorum_state().leader_epoch;
            self.apply_action(id, {{ root }}::action::Action::SendBeginQuorumEpoch { epoch });
            let deadline = self.now.saturating_add_ms({{ interval }});
            self.nodes.get_mut(&id).unwrap().heartbeat_deadline = Some(deadline);
        }
    })
}

pub(crate) fn delete_topic_request(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(tokens)?;
    Ok(moxy::template! {
        /// Delete one named topic using both legacy and flexible request fields.
        fn {{ name }}(topic: &str) -> ::krabka_protocol::owned::delete_topics_request::DeleteTopicsRequest {
            ::krabka_protocol::owned::delete_topics_request::DeleteTopicsRequest {
                topics: ::std::vec![::krabka_protocol::owned::delete_topics_request::DeleteTopicState {
                    name: Some(topic.into()),
                    ..Default::default()
                }],
                topic_names: ::std::vec![topic.into()],
                timeout_ms: 5_000,
                ..Default::default()
            }
        }
    })
}
