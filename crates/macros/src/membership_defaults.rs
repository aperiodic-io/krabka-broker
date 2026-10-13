//! Shared membership timing defaults on ordinary named config fields.

use {
    crate::meta::no_arguments,
    moxy::{
        ast::{Attribute, Fields, ItemStruct, ParseError},
        token::{ToTokenStream, TokenStream},
    },
};

pub(crate) fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    struct TimingDefault {
        name: &'static str,
        seconds: &'static str,
    }
    no_arguments(meta, "membership_defaults")?;
    let mut item: ItemStruct = moxy::parse!({ item })?;
    let span = item.ident.span();
    let Fields::Named(fields) = &mut item.fields else {
        return Err(ParseError::new(
            span,
            "`membership_defaults` needs named timing fields",
        ));
    };
    for default in [
        TimingDefault {
            name: "session_timeout",
            seconds: "45",
        },
        TimingDefault {
            name: "heartbeat_interval",
            seconds: "5",
        },
        TimingDefault {
            name: "min_session_timeout",
            seconds: "45",
        },
        TimingDefault {
            name: "max_session_timeout",
            seconds: "60",
        },
        TimingDefault {
            name: "min_heartbeat_interval",
            seconds: "5",
        },
        TimingDefault {
            name: "max_heartbeat_interval",
            seconds: "15",
        },
    ] {
        let name = default.name;
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
        let seconds: TokenStream = default.seconds.parse().expect("known timing default");
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
