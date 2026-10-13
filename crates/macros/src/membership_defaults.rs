//! Shared membership timing defaults on ordinary named config fields.

use {
    crate::meta::no_arguments,
    moxy::{
        ast::{Attribute, Fields, ItemStruct, ParseError},
        token::{ToTokenStream, TokenStream},
    },
};

pub(crate) fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    no_arguments(meta, "membership_defaults")?;
    let mut item: ItemStruct = moxy::parse!({ item })?;
    let span = item.ident.span();
    let Fields::Named(fields) = &mut item.fields else {
        return Err(ParseError::new(
            span,
            "`membership_defaults` needs named timing fields",
        ));
    };
    for (name, seconds) in [
        ("session_timeout", "45"),
        ("heartbeat_interval", "5"),
        ("min_session_timeout", "45"),
        ("max_session_timeout", "60"),
        ("min_heartbeat_interval", "5"),
        ("max_heartbeat_interval", "15"),
    ] {
        let field = fields
            .fields
            .inner
            .iter_mut()
            .find(|field| field.ident.as_ref().is_some_and(|ident| ident == name))
            .ok_or_else(|| ParseError::new(span, format!("missing `{name}` timing field")))?;
        if field
            .attrs
            .iter()
            .any(|attribute| attribute.path.is_ident("default"))
        {
            return Err(ParseError::new(
                span,
                format!("`{name}` already has a default"),
            ));
        }
        let seconds: TokenStream = seconds.parse().expect("known timing default");
        let default = moxy::template! {
            #[default(::std::time::Duration::from_secs({{ seconds }}))]
        };
        field.attrs.push(moxy::parse!({ default } as Attribute)?);
    }
    Ok(item.to_token_stream())
}

#[cfg(test)]
mod tests {
    use {super::expand, assert2::assert, moxy::token::TokenStream};

    #[test]
    fn refuses_arguments_and_incomplete_configs() {
        assert!(
            expand(
                "extra".parse().unwrap(),
                "struct Config {}".parse().unwrap()
            )
            .is_err()
        );
        assert!(expand(TokenStream::new(), "struct Config {}".parse().unwrap()).is_err());
        assert!(expand(TokenStream::new(), "struct Config;".parse().unwrap()).is_err());
        assert!(
            expand(
                TokenStream::new(),
                "struct Config { #[default(1)] session_timeout: u64 }"
                    .parse()
                    .unwrap()
            )
            .unwrap_err()
            .to_string()
            .contains("already has a default")
        );
    }
}
