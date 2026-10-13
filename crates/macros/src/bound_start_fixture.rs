//! Held-listener broker startup and its exact single-voter configuration.

use {
    crate::fixtures::{CommaArguments, name as fixture_name},
    moxy::{
        ast::ParseError,
        token::{Delim, Span, TokenStream, TokenTree},
    },
};

pub(crate) fn expand(input: TokenStream) -> Result<TokenStream, ParseError> {
    let arguments = CommaArguments::new(input);
    let parts = arguments.parts();
    let Some([TokenTree::Ident(mode)]) = parts.first().copied() else {
        return Err(arguments_error(arguments.span));
    };
    match (mode.to_string().as_str(), parts.as_slice()) {
        ("config", [_, name, namespace]) if !namespace.is_empty() => {
            let name = fixture_name(TokenStream::from(*name))?;
            let namespace = TokenStream::from(*namespace);
            Ok(moxy::template! {
                fn {{ name }}(
                    log_dir: &::std::path::Path,
                    data_addr: ::std::net::SocketAddr,
                    controller_addr: ::std::net::SocketAddr,
                ) -> {{ namespace }}::BrokerConfig {
                    let mut config = {{ namespace }}::BrokerConfig::for_tests(log_dir.to_path_buf());
                    config.listen_addr = data_addr;
                    config.advertised_listener = data_addr.to_string();
                    config.controller_listen_addr = controller_addr;
                    config.controller_quorum_voters = vec![({{ namespace }}::NodeId(1), controller_addr.to_string())];
                    config
                }
            })
        }
        ("start", [_, name, namespace, policy, config]) if !namespace.is_empty() => {
            let name = fixture_name(TokenStream::from(*name))?;
            let config = fixture_name(TokenStream::from(*config))?;
            let namespace = TokenStream::from(*namespace);
            let expect = match *policy {
                [TokenTree::Ident(policy)] if policy == "expect" => true,
                [TokenTree::Ident(policy)] if policy == "unwrap" => false,
                _ => return Err(arguments_error(arguments.span)),
            };
            let finish = if expect {
                moxy::template! { expect }
            } else {
                moxy::template! { unwrap }
            };
            Ok(moxy::template! {
                async fn {{ name }}(
                    customize: impl FnOnce(&mut {{ namespace }}::BrokerConfig),
                ) -> ({{ namespace }}::BrokerHandle, ::std::net::SocketAddr, ::tempfile::TempDir) {
                    let dir = ::tempfile::TempDir::new().{{ finish }}(@if expect { "tempdir" });
                    let data_listener = ::tokio::net::TcpListener::bind("127.0.0.1:0")
                        .await.{{ finish }}(@if expect { "bind data listener" });
                    let controller_listener = ::tokio::net::TcpListener::bind("127.0.0.1:0")
                        .await.{{ finish }}(@if expect { "bind controller listener" });
                    let data_addr = data_listener.local_addr().{{ finish }}(@if expect { "data addr" });
                    let controller_addr = controller_listener.local_addr()
                        .{{ finish }}(@if expect { "controller addr" });
                    let mut config = {{ config }}(dir.path(), data_addr, controller_addr);
                    customize(&mut config);
                    let broker = {{ namespace }}::Broker::start_with_listeners(
                        config, Some(controller_listener), Some(data_listener)
                    ).await.{{ finish }}(@if expect { "broker start" });
                    (broker, controller_addr, dir)
                }
            })
        }
        _ => Err(arguments_error(arguments.span)),
    }
}

fn arguments_error(span: Span) -> ParseError {
    ParseError::new(
        span,
        "expected `config, name, broker_namespace` or `start, name, broker_namespace, expect|unwrap, config_name`",
    )
}

/// Preserve absent, empty and ordered SCRAM user filters as distinct requests.
pub(crate) fn scram_users(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = fixture_name(input)?;
    Ok(moxy::template! {
        fn {{ name }}(users_filter: Option<Vec<String>>) -> ::krabka_protocol::owned::describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest {
            ::krabka_protocol::owned::describe_user_scram_credentials_request::DescribeUserScramCredentialsRequest {
                users: users_filter.map(|users| users.into_iter().map(|name|
                    ::krabka_protocol::owned::describe_user_scram_credentials_request::UserName {
                        name, ..Default::default()
                    }
                ).collect()),
                ..Default::default()
            }
        }
    })
}

/// Construct the request's ordered directories, topics and partition assignments.
pub(crate) fn assignment_dirs(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = fixture_name(input)?;
    Ok(moxy::template! {
        #[derive(Clone, Copy, ::krabka_macros::FieldDefaults)]
        pub(crate) struct DirectoryAssignmentSetup<'a> {
            #[default(1)]
            pub broker_id: i32,
            pub broker_epoch: i64,
            pub dir: ::uuid::Uuid,
            #[default(::uuid::Uuid::from_u128(1))]
            pub topic: ::uuid::Uuid,
            #[default(&[0])]
            pub partitions: &'a [i32],
        }

        pub(crate) fn {{ name }}(setup: DirectoryAssignmentSetup<'_>) -> ::krabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest {
            use ::krabka_protocol::owned::assign_replicas_to_dirs_request::{
                AssignReplicasToDirsRequest, DirectoryData, TopicData, PartitionData,
            };
            let DirectoryAssignmentSetup { broker_id, broker_epoch, dir, topic, partitions } = setup;
            use ::krabka_protocol::primitives::uuid::Uuid;
            AssignReplicasToDirsRequest {
                broker_id,
                broker_epoch,
                directories: vec![DirectoryData {
                    id: Uuid(dir.into_bytes()),
                    topics: vec![TopicData {
                        topic_id: Uuid(topic.into_bytes()),
                        partitions: partitions.iter().map(|&partition_index| PartitionData {
                            partition_index, ..Default::default()
                        }).collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }
        }
    })
}

/// Keep registry guards and finish diagnostics in the caller's original scope.
pub(crate) fn metric_registry(input: TokenStream) -> Result<TokenStream, ParseError> {
    let error = ParseError::new(
        input.span(),
        "expected metrics, output, guard; finish(args)",
    );
    let tokens = Vec::from(input);
    let sections: Vec<_> = tokens.split(TokenTree::is_punct_semi).collect();
    let [bindings, finish] = sections.as_slice() else {
        return Err(error);
    };
    let [
        metrics @ ..,
        comma,
        TokenTree::Ident(output),
        comma2,
        TokenTree::Ident(guard),
    ] = *bindings
    else {
        return Err(error);
    };
    let [TokenTree::Ident(finish), TokenTree::Group(args)] = *finish else {
        return Err(error);
    };
    if metrics.is_empty()
        || !comma.is_punct_comma()
        || !comma2.is_punct_comma()
        || args.delim != Delim::Paren
    {
        return Err(error);
    }
    let metrics = TokenStream::from(metrics);
    Ok(moxy::template! {
        let mut {{ output }} = String::new();
        let {{ guard }} = ({{ metrics }}).registry.lock().await;
        ::prometheus_client::encoding::text::encode(&mut {{ output }}, &{{ guard }})
            .{{ finish }}{{ args }};
    })
}

/// Read epoch milliseconds with each caller's original checked conversions.
pub(crate) fn unix_millis(input: TokenStream) -> Result<TokenStream, ParseError> {
    let arguments = CommaArguments::new(input);
    let parts = arguments.parts();
    let [binding, epoch_context, conversion_context] = parts.as_slice() else {
        return Err(ParseError::new(
            arguments.span,
            "expected name and two panic contexts",
        ));
    };
    let [visibility @ .., TokenTree::Ident(name)] = *binding else {
        return Err(ParseError::new(arguments.span, "expected a function name"));
    };
    if epoch_context.is_empty() || conversion_context.is_empty() {
        return Err(ParseError::new(
            arguments.span,
            "expected two panic contexts",
        ));
    }
    let visibility = TokenStream::from(visibility);
    let visibility = moxy::parse!({ visibility } as moxy::ast::Visibility)?;
    let epoch_context = TokenStream::from(*epoch_context);
    let conversion_context = TokenStream::from(*conversion_context);
    Ok(moxy::template! {
        /// Current wall-clock milliseconds since the Unix epoch.
        ///
        /// # Panics
        /// Panics if the clock precedes the epoch or milliseconds do not fit an i64.
        {{ visibility }} fn {{ name }}() -> i64 {
            i64::try_from(
                ::std::time::SystemTime::now()
                    .duration_since(::std::time::UNIX_EPOCH)
                    .expect({{ epoch_context }})
                    .as_millis(),
            ).expect({{ conversion_context }})
        }
    })
}

/// Preserve the Vec frame allocation and each fixture's signed length limit.
pub(crate) fn vector_request(input: TokenStream) -> Result<TokenStream, ParseError> {
    let tokens = Vec::from(input);
    let [TokenTree::Ident(name), comma, TokenTree::Ident(length)] = tokens.as_slice() else {
        return Err(ParseError::new(Span::call_site(), "expected name, u32|i32"));
    };
    if !comma.is_punct_comma() || !(length == "u32" || length == "i32") {
        return Err(ParseError::new(
            length.span(),
            "expected u32 or i32 frame length",
        ));
    }
    Ok(moxy::template! {
        fn {{ name }}(
            api_key: i16, version: i16, correlation_id: i32, flexible: bool, body: &[u8],
        ) -> Vec<u8> {
            let mut frame = Vec::new();
            frame.extend_from_slice(&api_key.to_be_bytes());
            frame.extend_from_slice(&version.to_be_bytes());
            frame.extend_from_slice(&correlation_id.to_be_bytes());
            frame.extend_from_slice(&1i16.to_be_bytes());
            frame.push(b'c');
            if flexible { frame.push(0); }
            frame.extend_from_slice(body);
            let mut out = {{ length }}::try_from(frame.len()).unwrap().to_be_bytes().to_vec();
            out.extend_from_slice(&frame);
            out
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{expand, metric_registry},
        assert2::assert,
    };

    #[test]
    fn startup_emits_direct_calls_with_the_selected_panic_policy() {
        for policy in ["expect", "unwrap"] {
            let input = format!("start, start_bound, broker, {policy}, bound_config")
                .parse()
                .unwrap();
            let output = expand(input).unwrap();
            let _: moxy::ast::ItemFn = moxy::parse!({ output.clone() }).unwrap();
            let rendered = output.to_string();
            assert!(!rendered.contains("macro_rules"));
            assert!(!rendered.contains("fixture_result"));
            assert!(rendered.matches(policy).count() == 6);
            assert!(rendered.contains("broker start") == (policy == "expect"));
            assert!(rendered.contains("bind controller listener") == (policy == "expect"));
        }
    }

    #[test]
    fn registry_requires_binding_names_and_a_parenthesized_finish() {
        for input in [
            "metrics, body + suffix, guard; unwrap()",
            "metrics, body, guard; unwrap{}",
            "metrics, body, guard; unwrap() extra",
            "render_registry",
        ] {
            assert!(metric_registry(input.parse().unwrap()).is_err());
        }
    }
}
