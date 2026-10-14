//! Error trait implementations for protocol types declared in another crate.

use moxy::{
    ast::{List, Parse, ParseError, Parser, Path, Token},
    token::{Delim, Ident, TokenStream},
};

pub(crate) fn expand(tokens: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(tokens);
    let mut entries = Vec::new();
    while !parser.is_empty() {
        let section = Ident::parse(&parser)?;
        if !matches!(
            section.text(),
            "response" | "code_response" | "row" | "resource_row"
        ) {
            return Err(ParseError::new(
                section.span(),
                "expected `response`, `code_response`, `row` or `resource_row`",
            ));
        }
        let types = parser.parse_group(Delim::Brace)?;
        let paths = List::<Path, Token![,]>::parse_all(&types)?;
        entries.extend(
            paths
                .iter()
                .map(|path| (section.text().to_owned(), path.clone())),
        );
    }

    Ok(moxy::template! {
        @for (section, ty) in &entries {
            @match (section.as_str()) {
                "response" => {
                    impl ErrorResponse for {{ ty }} {
                        fn error(error_code: i16, error_message: Option<String>) -> Self {
                            Self { error_code, error_message, ..Self::default() }
                        }
                    }
                }
                "code_response" => {
                    impl ErrorCodeResponse for {{ ty }} {
                        fn error(error_code: i16) -> Self {
                            Self { error_code, ..Self::default() }
                        }
                    }
                }
                _ => {
                    impl ErrorRow for {{ ty }} {
                        fn error_code(&self) -> i16 { self.error_code }

                        fn set_error(&mut self, error_code: i16, error_message: Option<String>) {
                            self.error_code = error_code;
                            self.error_message = error_message;
                        }
                    }
                    @if section == "resource_row" {
                        impl ResourceErrorRow for {{ ty }} {
                            fn for_resource(resource_type: i8, resource_name: String) -> Self {
                                Self {
                                    resource_type,
                                    resource_name,
                                    error_message: None,
                                    ..Self::default()
                                }
                            }
                        }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, assert2::assert};

    #[yare::parameterized(
        unknown_section = { "unknown { Response }" },
        ungrouped_section = { "response Response" },
        missing_comma = { "response { Response Other }" },
        doubled_comma = { "response { Response,, }" },
        non_type = { "row { 42 }" },
    )]
    fn rejects_unknown_sections_and_malformed_type_lists(input: &str) {
        assert2::assert!(expand(&input.parse().expect("tokens")).is_err());
    }

    #[test]
    fn accepts_empty_sections_and_trailing_commas() {
        assert!(expand(&"response {} row { SomeRow, }".parse().expect("tokens")).is_ok());
    }
}
