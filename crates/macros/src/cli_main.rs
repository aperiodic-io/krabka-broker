//! `cli_main!`: see the crate documentation.

use {
    crate::meta::required_tokens,
    moxy::{
        ast::ParseError,
        token::{TokenStream, TokenTree},
    },
};

/// The two halves of the `cli_main!` input.
struct Split<'a> {
    /// The path of the crate whose `run_from_args` the `main` calls.
    path: &'a [TokenTree],
    /// The arguments of `#[tokio::main(...)]`.
    runtime: &'a [TokenTree],
}

/// Splits `tokens` at its first top-level comma into the crate path and the
/// `#[tokio::main(...)]` arguments. A comma inside a group belongs to the
/// group, so only the outermost one splits.
fn split(tokens: &[TokenTree]) -> Split<'_> {
    match tokens.iter().position(TokenTree::is_punct_comma) {
        Some(comma) => Split {
            path: &tokens[..comma],
            runtime: &tokens[comma + 1..],
        },
        None => Split {
            path: tokens,
            runtime: &[],
        },
    }
}

/// Expands `cli_main!(crate_path, tokio arguments...)` into the binary's
/// `main`.
pub(crate) fn expand(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let tokens = Vec::from(tokens);
    let Split { path, runtime } = split(&tokens);
    let path = required_tokens(
        TokenStream::from(path),
        "`cli_main!` needs the path of the crate whose `run_from_args` it calls",
    )?;
    let runtime = TokenStream::from(runtime);
    Ok(moxy::template! {
        @if runtime.is_empty() {
            #[::tokio::main]
        } @else {
            #[::tokio::main({{ runtime }})]
        }
        async fn main() {
            ::tracing_subscriber::fmt()
                .with_env_filter(
                    ::tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| ::tracing_subscriber::EnvFilter::new("info")),
                )
                .init();
            ::std::process::exit({{ path }}::run_from_args(::std::env::args_os()).await);
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{Split, expand, split},
        crate::meta::compact,
        moxy::token::TokenStream,
    };

    /// Both sides of a split, printed without whitespace.
    #[derive(Debug, PartialEq)]
    struct SplitText {
        path: String,
        runtime: String,
    }

    /// `tokens` parsed, split, and each side printed without whitespace.
    fn split_text(tokens: &str) -> SplitText {
        let tokens: TokenStream = tokens.parse().expect("tokens");
        let Split { path, runtime } = split(&tokens);
        SplitText {
            path: compact(&TokenStream::from(path)),
            runtime: compact(&TokenStream::from(runtime)),
        }
    }

    #[yare::parameterized(
        crate_name = { "krabka_format", "krabka_format", "" },
        trailing_comma = { "krabka_format,", "krabka_format", "" },
        runtime_options = {
            "crate::cli, flavor = \"multi_thread\"",
            "crate::cli",
            "flavor=\"multi_thread\"",
        },
        multiple_runtime_options = {
            "app, flavor = \"multi_thread\", worker_threads = 2",
            "app",
            "flavor=\"multi_thread\",worker_threads=2",
        },
    )]
    fn the_first_top_level_comma_ends_the_path(tokens: &str, path: &str, runtime: &str) {
        assert2::assert!(
            split_text(tokens)
                == SplitText {
                    path: path.to_owned(),
                    runtime: runtime.to_owned(),
                },
            "{tokens}"
        );
    }

    /// `cli_main!($tokens)` expanded and printed without whitespace.
    fn expanded(tokens: &str) -> String {
        let tokens: TokenStream = tokens.parse().expect("tokens");
        compact(&expand(tokens).expect("expands"))
    }

    /// The `main` that `cli_main!` writes under the runtime attribute
    /// `attribute`, calling the `run_from_args` of `path`, without whitespace.
    fn main_under(attribute: &str, path: &str) -> String {
        format!(
            "{attribute}asyncfnmain(){{::tracing_subscriber::fmt().with_env_filter(::tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_|::tracing_subscriber::EnvFilter::new(\"info\")),).init();::std::process::exit({path}::run_from_args(::std::env::args_os()).await);}}"
        )
    }

    #[yare::parameterized(
        crate_name = { "krabka_format", "#[::tokio::main]", "krabka_format" },
        trailing_comma = { "krabka_format,", "#[::tokio::main]", "krabka_format" },
        runtime_options = {
            "crate::cli, flavor = \"multi_thread\"",
            "#[::tokio::main(flavor=\"multi_thread\")]",
            "crate::cli",
        },
        multiple_runtime_options = {
            "app, flavor = \"multi_thread\", worker_threads = 2",
            "#[::tokio::main(flavor=\"multi_thread\",worker_threads=2)]",
            "app",
        },
    )]
    fn the_runtime_arguments_pass_through_to_tokio_main(tokens: &str, attribute: &str, path: &str) {
        assert2::assert!(expanded(tokens) == main_under(attribute, path), "{tokens}");
    }

    #[yare::parameterized(
        empty = { "" },
        comma_only = { "," },
        runtime_options_without_path = { ", flavor = \"multi_thread\"" },
    )]
    fn a_call_without_a_crate_path_is_an_error(tokens: &str) {
        let input: TokenStream = tokens.parse().expect("tokens");
        let Err(error) = expand(input) else {
            panic!("{tokens}");
        };
        assert2::assert!(
            error.message()
                == "`cli_main!` needs the path of the crate whose `run_from_args` it calls",
            "{tokens}"
        );
    }
}
