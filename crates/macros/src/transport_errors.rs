//! The common transport failures, preceding each caller's unchanged protocol-specific errors.

use moxy::{
    ast::ParseError,
    token::{Span, TokenStream, TokenTree},
};

use crate::meta::item_body;

pub(crate) fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    if meta.into_iter().next().is_some() {
        return Err(ParseError::new(
            Span::call_site(),
            "transport_errors takes no arguments",
        ));
    }
    let (mut tokens, body) = item_body(item, "transport_errors", TokenTree::is_keyword_enum)?;
    let TokenTree::Group(group) = &mut tokens[body] else {
        unreachable!()
    };
    if group.tokens.clone().into_iter().any(|token| {
        token
            .as_ident()
            .is_some_and(|name| matches!(name.to_string().as_str(), "Io" | "Tls" | "Sasl"))
    }) {
        return Err(ParseError::new(
            group.span.into(),
            "common transport error variants already declared",
        ));
    }
    let mut variants = moxy::template! {
        #[error("io: {0}")]
        Io(#[from] ::std::io::Error),
        #[error("tls: {0}")]
        Tls(String),
        #[error("sasl: {0}")]
        Sasl(String),
    };
    variants.extend(group.tokens.clone());
    group.tokens = variants;
    Ok(tokens.into())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    #[test]
    fn retains_enum_attributes_identity_and_variant_order() {
        let output = super::expand("".parse().unwrap(), "#[derive(Debug, Error)] pub enum ClientError { /// Caller detail\n #[error(\"codec: {0}\")] Codec(String), }".parse().unwrap()).unwrap().to_string();
        for expected in [
            "derive (Debug , Error)",
            "pub enum ClientError",
            "# [from]",
            "Caller detail",
            "codec: {0}",
        ] {
            assert!(output.contains(expected), "{expected}: {output}");
        }
        let variants = ["Io (", "Tls (", "Sasl (", "Codec ("]
            .map(|name| output.find(name).expect("transport variant present"));
        assert!(variants.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
