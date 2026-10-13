//! `cli_main!`: see the crate documentation.

use {
    crate::meta::required_tokens,
    moxy::{
        ast::ParseError,
        token::{TokenStream, TokenTree},
    },
};

/// Splits `tokens` at its first top-level comma into the crate path and the
/// `#[tokio::main(...)]` arguments. A comma inside a group belongs to the
/// group, so only the outermost one splits.
fn split(tokens: &[TokenTree]) -> (&[TokenTree], &[TokenTree]) {
    match tokens.iter().position(TokenTree::is_punct_comma) {
        Some(comma) => (&tokens[..comma], &tokens[comma + 1..]),
        None => (tokens, &[]),
    }
}

/// Expands `cli_main!(crate_path, tokio arguments...)` into the binary's
/// `main`.
pub(crate) fn expand(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let tokens = Vec::from(tokens);
    let (path, runtime) = split(&tokens);
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
        super::{expand, split},
        crate::meta::compact,
        assert2::assert,
        moxy::token::TokenStream,
    };

    /// `tokens` parsed, split, and each side printed without whitespace.
    fn split_text(tokens: &str) -> (String, String) {
        let tokens: TokenStream = tokens.parse().expect("tokens");
        let (path, runtime) = split(&tokens);
        (
            compact(&TokenStream::from(path)),
            compact(&TokenStream::from(runtime)),
        )
    }

    #[test]
    fn the_first_top_level_comma_ends_the_path() {
        for (tokens, path, runtime) in [
            ("krabka_format", "krabka_format", ""),
            ("krabka_format,", "krabka_format", ""),
            (
                "crate::cli, flavor = \"multi_thread\"",
                "crate::cli",
                "flavor=\"multi_thread\"",
            ),
            (
                "app, flavor = \"multi_thread\", worker_threads = 2",
                "app",
                "flavor=\"multi_thread\",worker_threads=2",
            ),
        ] {
            assert!(
                split_text(tokens) == (path.to_owned(), runtime.to_owned()),
                "{tokens}"
            );
        }
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

    #[test]
    fn the_runtime_arguments_pass_through_to_tokio_main() {
        for (tokens, attribute, path) in [
            ("krabka_format", "#[::tokio::main]", "krabka_format"),
            ("krabka_format,", "#[::tokio::main]", "krabka_format"),
            (
                "crate::cli, flavor = \"multi_thread\"",
                "#[::tokio::main(flavor=\"multi_thread\")]",
                "crate::cli",
            ),
            (
                "app, flavor = \"multi_thread\", worker_threads = 2",
                "#[::tokio::main(flavor=\"multi_thread\",worker_threads=2)]",
                "app",
            ),
        ] {
            assert!(expanded(tokens) == main_under(attribute, path), "{tokens}");
        }
    }

    #[test]
    fn a_call_without_a_crate_path_is_an_error() {
        for tokens in ["", ",", ", flavor = \"multi_thread\""] {
            let input: TokenStream = tokens.parse().expect("tokens");
            assert!(let Err(error) = expand(input), "{tokens}");
            assert!(
                error.message()
                    == "`cli_main!` needs the path of the crate whose `run_from_args` it calls",
                "{tokens}"
            );
        }
    }
}
