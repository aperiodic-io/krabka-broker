//! Construct the flat broker configuration declaration in one expansion.

use {
    crate::meta::{named_body, named_fields},
    moxy::{
        ast::{ItemStruct, ParseError, Visibility},
        token::{Span, TokenStream, TokenTree},
    },
};

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let (mut header, body) = named_body(input.clone(), "broker_config")?;
    let Some(TokenTree::Group(fields)) = input.last() else {
        return Err(ParseError::new(
            input.span(),
            "expected one named-field struct",
        ));
    };
    if body + 1 != header.len() {
        return Err(ParseError::new(
            input.span(),
            "expected one named-field struct",
        ));
    }
    let TokenTree::Group(placeholder) = &mut header[body] else {
        unreachable!()
    };
    // Rust validates field types; parsing only the header avoids Moxy's qualified-type gaps.
    placeholder.tokens = TokenStream::new();
    let mut declaration: ItemStruct = moxy::parse!({ TokenStream::from(header) })?;
    named_fields(&declaration, "broker_config")?;
    // The generated item owns its boundaries; field tokens keep their caller diagnostics.
    let generated = Span::call_site();
    declaration.ident.set_span(generated);
    match &mut declaration.vis {
        Visibility::Inherited => {}
        Visibility::Public { pub_keyword }
        | Visibility::Crate { pub_keyword, .. }
        | Visibility::SelfValue { pub_keyword, .. }
        | Visibility::Super { pub_keyword, .. }
        | Visibility::Restricted { pub_keyword, .. } => pub_keyword.set_span(generated),
    }
    Ok(moxy::template! {
        {{ declaration.attrs }}
        #[derive(Debug, Clone)]
        {{ declaration.vis }} struct {{ declaration.ident }} {{ declaration.generics }} {
            {{ fields.tokens }}
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, assert2::assert};

    #[test]
    fn requires_exactly_one_named_struct() {
        for invalid in [
            "struct Config;",
            "struct Config(bool);",
            "enum Config { Enabled }",
            "struct Config { enabled: bool } struct Other {}",
            "struct Config {} struct Other { value: Option<std::path::PathBuf> }",
            "struct Config {} fn trailing() {}",
        ] {
            assert!(expand(&invalid.parse().unwrap()).is_err());
        }
    }

    #[test]
    fn retains_original_field_documentation_attributes_and_tokens() {
        let input = moxy::template! {
            pub struct Config {
                /// The field's original documentation.
                #[cfg(any())]
                pub excluded: UnknownType,
                pub name: String,
            }
        };
        let original_fields = input.last().unwrap().to_string();
        let expanded = expand(&input).unwrap();
        assert!(expanded.last().unwrap().to_string() == original_fields);
    }
}
