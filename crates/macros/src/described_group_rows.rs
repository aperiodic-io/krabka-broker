//! Trait rows for external group-describe response types.

use moxy::{
    ast::{List, ParseError, Parser, Path, Token},
    token::TokenStream,
};

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let rows: List<Path, Token![,]> = List::parse_all(&parser)?;
    if rows.is_empty() {
        return Err(parser.error("expected at least one group response type"));
    }
    Ok(moxy::template! {
        @for ty in rows {
            impl DescribedGroupRow for {{ ty }} {
                fn error_row(group_id: &str, error_code: i16, error_message: Option<String>) -> Self {
                    Self { group_id: group_id.into(), error_code, error_message, ..Self::default() }
                }

                fn group_id(&self) -> &str { &self.group_id }

                fn error_code(&self) -> i16 { self.error_code }

                fn set_authorized_operations(&mut self, bits: i32) {
                    self.authorized_operations = bits;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, assert2::assert};

    #[test]
    fn requires_a_nonempty_comma_separated_type_table() {
        for input in ["", "Response Other", "Response;", "42", "Response, #"] {
            assert!(expand(&input.parse().unwrap()).is_err(), "{input}");
        }
        assert!(expand(&"consumer::Row, share::Row,".parse().unwrap()).is_ok());
    }
}
