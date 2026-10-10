//! Shared command-line entry points and independent parser/filesystem fixtures.

use moxy::{
    ast::ParseError,
    token::{Span, Spanner, TokenStream, TokenTree},
};

pub(crate) fn flag_metadata_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(input)?;
    Ok(moxy::template! {
        fn {{ name }}(arg: &::clap::Arg) -> (&str, String, Vec<String>) {
            let long = arg.get_long().unwrap_or_default();
            let env = arg.get_env().map(|env| env.to_string_lossy().into_owned()).unwrap_or_default();
            let defaults = arg.get_default_values().iter().map(|value| value.to_string_lossy().into_owned()).collect();
            (long, env, defaults)
        }
    })
}

pub(crate) fn bind_retry_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let (name, mut arguments) = crate::fixtures::named_arguments(input, 2)?;
    let timeout = arguments.next().unwrap();
    Ok(moxy::template! {
        pub(super) async fn {{ name }}(addr: ::std::net::SocketAddr) -> ::tokio::net::TcpListener {
            let deadline = ::tokio::time::Instant::now() + {{ timeout }};
            loop {
                match ::tokio::net::TcpListener::bind(addr).await {
                    Ok(listener) => return listener,
                    Err(err) if ::tokio::time::Instant::now() < deadline => {
                        let _ = err;
                        ::tokio::time::sleep(::std::time::Duration::from_millis(10)).await;
                    }
                    Err(err) => panic!("listener address {addr} was not released: {err}"),
                }
            }
        }
    })
}

pub(crate) fn dispatch_capacity_parser(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(input)?;
    Ok(moxy::template! {
        fn {{ name }}(value: &str) -> Result<usize, String> {
            let value = value.parse::<usize>().map_err(|error| error.to_string())?;
            ::krabka_client_core::ConnectionDispatchQueueCapacity::new(value)
                .map(::krabka_client_core::ConnectionDispatchQueueCapacity::get)
        }
    })
}

fn no_options(meta: TokenStream) -> Result<(), ParseError> {
    match meta.into_iter().next() {
        Some(token) => Err(ParseError::new(
            token.span(),
            "this attribute takes no arguments",
        )),
        None => Ok(()),
    }
}

pub(crate) fn argv_entrypoint(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    no_options(meta)?;
    let (tokens, body_index) =
        crate::meta::item_body(item, "argv_entrypoint", TokenTree::is_keyword_fn)?;
    let function = tokens.iter().position(TokenTree::is_keyword_fn).unwrap();
    let declaration = &tokens[function + 1..body_index];
    let [
        TokenTree::Ident(name),
        TokenTree::Group(arguments),
        arrow_start,
        arrow_end,
        TokenTree::Ident(output),
    ] = declaration
    else {
        return Err(ParseError::new(
            Span::call_site(),
            "expected an entry point declared as fn name(...) -> i32",
        ));
    };
    if !arguments.delim.is_paren()
        || !arrow_start.is_punct_minus()
        || !arrow_end.is_punct_gt()
        || output != "i32"
    {
        return Err(ParseError::new(
            name.span(),
            "expected an entry point declared as fn name(...) -> i32",
        ));
    }
    let prefix: TokenStream = tokens[..function].to_vec().into();
    let body = &tokens[body_index];
    let extra = &arguments.tokens;
    Ok(moxy::template! {
        {{ prefix }} fn {{ name }}<I, T>(argv: I, {{ extra }}) -> i32
        where
            I: IntoIterator<Item = T>,
            T: Into<::std::ffi::OsString> + Clone,
        {{ body }}
    })
}

pub(crate) fn bootstrap_cli_fields(
    meta: TokenStream,
    item: TokenStream,
) -> Result<TokenStream, ParseError> {
    no_options(meta)?;
    let (mut tokens, body) = crate::meta::named_body(item, "bootstrap_cli_fields")?;
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    if !group.tokens.is_empty() {
        return Err(ParseError::new(
            group.span(),
            "bootstrap_cli_fields needs an empty Cli body",
        ));
    }
    group.tokens = moxy::template! {
        /// One or more `host:port` pairs to bootstrap against.
        #[arg(long, short = 'b', env = "KRABKA_BOOTSTRAP_SERVER", required = true)]
        pub bootstrap_server: String,

        /// What to do.
        #[command(subcommand)]
        pub command: Command,
    };
    Ok(tokens.into())
}

pub(crate) fn duration_cases(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(input)?;
    Ok(moxy::template! {
        fn {{ name }}(cases: &[(&str, Option<i64>)]) {
            for &(raw, expected) in cases {
                ::assert2::check!(parse_time(raw).ok().map(::krabka_units::Time::millis_i64) == expected, "{raw}");
            }
        }
    })
}

pub(crate) fn directory_tree(input: TokenStream) -> Result<TokenStream, ParseError> {
    let (name, mut arguments) = crate::fixtures::named_arguments(input, 4)?;
    let element = arguments.next().unwrap();
    let docs = arguments.next().unwrap().into_iter().collect::<Vec<_>>();
    let [TokenTree::Group(docs)] = docs.as_slice() else {
        return Err(ParseError::new(
            Span::call_site(),
            "expected a braced documentation block",
        ));
    };
    if !docs.delim.is_brace() {
        return Err(ParseError::new(
            docs.span(),
            "expected a braced documentation block",
        ));
    }
    let docs = &docs.tokens;
    let project: Vec<_> = arguments.next().unwrap().into_iter().collect();
    let project: TokenStream = match project.as_slice() {
        [TokenTree::Group(group)] if group.delim.is_paren() => group.tokens.clone(),
        _ => project.into(),
    };
    Ok(moxy::template! {
        {{ docs }}
        fn {{ name }}(dir: &::std::path::Path) -> Vec<{{ element }}> {
            let mut files = Vec::new();
            let mut pending = vec![dir.to_path_buf()];
            while let Some(next) = pending.pop() {
                for entry in ::std::fs::read_dir(&next).expect("list") {
                    let path = entry.expect("entry").path();
                    if path.is_dir() {
                        pending.push(path);
                    } else {
                        files.push(({{ project }})(dir, &path));
                    }
                }
            }
            files.sort();
            files
        }
    })
}

fn documentation(input: TokenStream) -> Result<TokenStream, ParseError> {
    let tokens: Vec<_> = input.into_iter().collect();
    match tokens.as_slice() {
        [TokenTree::Group(group)] if group.delim.is_brace() => Ok(group.tokens.clone()),
        _ => Err(ParseError::new(
            Span::call_site(),
            "expected a braced documentation block",
        )),
    }
}

pub(crate) fn parsed_cli_entrypoint(input: TokenStream) -> Result<TokenStream, ParseError> {
    let docs = documentation(input)?;
    Ok(moxy::template! {
        {{ docs }}
        pub async fn run_from_args<I, T>(argv: I) -> i32
        where
            I: IntoIterator<Item = T>,
            T: Into<::std::ffi::OsString> + Clone,
        {
            run(Cli::parse_from(argv)).await
        }
    })
}

pub(crate) fn duration_parser_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let mut arguments = crate::meta::arguments(input, 2)?.into_iter();
    let docs = documentation(arguments.next().unwrap())?;
    let cases: Vec<_> = arguments.next().unwrap().into_iter().collect();
    let [TokenTree::Group(cases)] = cases.as_slice() else {
        return Err(ParseError::new(
            Span::call_site(),
            "expected duration case list",
        ));
    };
    if !cases.delim.is_bracket() {
        return Err(ParseError::new(cases.span(), "expected duration case list"));
    }
    let cases = cases.tokens.clone();
    Ok(moxy::template! {
        {{ docs }}
        #[test]
        fn a_time_argument_takes_any_unit() {
            for (raw, expected) in [
                ("500ms", Some(500)),
                ("30s", Some(30_000)),
                {{ cases }}
            ] {
                ::assert2::check!(parse_time(raw).ok().map(::krabka_units::Time::millis_i64) == expected, "{raw}");
            }
        }
    })
}

pub(crate) fn bootstrap_parser_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let mut arguments = crate::meta::arguments(input, 2)?.into_iter();
    let refused = arguments.next().unwrap();
    let accepted = arguments.next().unwrap();
    Ok(moxy::template! {
        /// `--bootstrap-server` is the one flag every subcommand needs, so the
        /// parser refuses a command line without it rather than defaulting to a
        /// guess about where the cluster is.
        #[test]
        fn a_command_line_without_a_bootstrap_server_is_refused() {
            ::assert2::assert!(Cli::try_parse_from({{ refused }}).is_err());
            ::assert2::assert!(Cli::try_parse_from({{ accepted }}).is_ok());
        }
    })
}

pub(crate) fn snapshot_node_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(input)?;
    Ok(moxy::template! {
        fn {{ name }}() -> ::tempfile::TempDir {
            let log_dir = ::tempfile::tempdir().expect("log dir");
            let rlmm = log_dir.path().join("remote-log-metadata");
            ::std::fs::create_dir_all(&rlmm).expect("create the rlmm dir");
            ::std::fs::write(rlmm.join("snapshot"), b"rlmm snapshot bytes").expect("write the rlmm snapshot");
            log_dir
        }
    })
}

pub(crate) fn connect_cli_client(input: TokenStream) -> Result<TokenStream, ParseError> {
    let mut arguments = crate::meta::arguments(input, 3)?.into_iter();
    let bootstrap = arguments.next().unwrap();
    let client_id = arguments.next().unwrap();
    let unreachable = arguments.next().unwrap();
    Ok(moxy::template! {
        match ::krabka_client_core::Client::builder()
            .bootstrap({{ bootstrap }})
            .client_id({{ client_id }})
            .build().await {
            Ok(client) => client,
            Err(error) => {
                eprintln!("cannot reach {}: {error}", {{ bootstrap }});
                return {{ unreachable }};
            }
        }
    })
}

pub(crate) fn format_directories_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(input)?;
    Ok(moxy::template! {
        fn {{ name }}() -> (::tempfile::TempDir, ::std::path::PathBuf, ::std::path::PathBuf) {
            let dir = ::tempfile::tempdir().expect("tempdir");
            let metadata = dir.path().join("meta");
            let data = dir.path().join("data");
            (dir, metadata, data)
        }
    })
}
