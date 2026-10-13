//! Flat named routing rows for the krabka-private context dispatch registry.

use {
    crate::meta::{Tokens, expect_name, grouped_value, plain_path},
    moxy::{
        ast::{FromMeta, Meta, Parse, ParseError, Parser, Path, Token},
        token::{Ident, Spanner, TokenStream},
    },
};

#[derive(moxy::FromMeta)]
struct Routing {
    api: Tokens,
    flexible: Tokens,
    handler: Tokens,
}

struct Entry {
    adapter: Ident,
    api: Path,
    flexible: Path,
    handler: Path,
}

struct Table {
    register: Ident,
    entries: Vec<Entry>,
}

fn declaration(input: &TokenStream) -> Result<Table, ParseError> {
    let parser = Parser::from_tokens(input);
    expect_name(
        &parser,
        "register",
        "expected named private registry function",
    )?;
    let register = Ident::parse(&parser)?;
    let _: Token![;] = Parse::parse(&parser)?;
    let mut entries: Vec<Entry> = Vec::new();
    while !parser.is_empty() {
        let adapter = Ident::parse(&parser)?;
        if entries
            .iter()
            .any(|entry| entry.adapter.text() == adapter.text())
        {
            return Err(ParseError::new(
                adapter.span(),
                "duplicate private adapter name",
            ));
        }
        let group = grouped_value(&parser, "expected named private dispatch routing")?;
        if !group.delim.is_brace() {
            return Err(ParseError::new(
                group.span(),
                "private dispatch routing needs braces",
            ));
        }
        let tokens = &group.tokens;
        let metadata: Meta = moxy::parse!({
            moxy::template! { routing({{ tokens }}) }
        })?;
        let routing = Routing::from_meta(&metadata)?;
        entries.push(Entry {
            adapter,
            api: plain_path(
                &routing.api.tokens,
                group.span(),
                "expected private API constant path",
            )?,
            flexible: plain_path(
                &routing.flexible.tokens,
                group.span(),
                "expected schema flexible-version constant path",
            )?,
            handler: plain_path(
                &routing.handler.tokens,
                group.span(),
                "expected private handler function path",
            )?,
        });
    }
    if entries.is_empty() {
        return Err(parser.error("expected at least one private dispatch row"));
    }
    Ok(Table { register, entries })
}

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let table = declaration(input)?;
    Ok(moxy::template! {
        @for entry in &table.entries {
            fn {{ entry.adapter }}<'a>(
                broker: &'a Broker, version: ApiVersion, body: &'a [u8], ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                Box::pin({{ entry.handler }}(broker, version, ctx.correlation_id, body, ctx))
            }
        }

        /// Registers krabka-private context dispatches by raw wire `api_key`.
        /// The schema's flexible minimum is the framing layer's authoritative value.
        pub(super) fn {{ table.register }}(registry: &mut DispatchRegistry) {
            struct PrivateContextDispatch {
                api_key: ApiKeyCode,
                flexible_min: ApiVersion,
                handler: ContextHandler,
            }
            let entries: &[PrivateContextDispatch] = &[
                @for entry in &table.entries {
                    PrivateContextDispatch { api_key: {{ entry.api }}, flexible_min: {{ entry.flexible }}, handler: {{ entry.adapter }} as ContextHandler },
                }
            ];
            for entry in entries {
                let PrivateContextDispatch { api_key, flexible_min, handler } = *entry;
                assert2::assert!(
                    api_key >= crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR,
                    "api_key {api_key} is below the krabka-private floor"
                );
                assert2::assert!(
                    registry.register(DispatchEntry::context(api_key, flexible_min, handler)),
                    "duplicate dispatch registration for api_key {api_key}"
                );
                // Kafka defines no krabka-private api, so no Kafka client
                // quota applies to one. `WriteBarrierMarkers` is inter-broker
                // traffic, and the operator apis are not what a
                // `request_percentage` quota is written for.
                registry.exempt_from_request_quota(api_key);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{declaration, expand},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn named_rows_preserve_correlation_framing_and_admission_order() {
        let input = "register register_private; first { api = keys::FIRST, flexible = schema::FIRST_FLEXIBLE, handler = handlers::first } second { api = keys::SECOND, flexible = schema::SECOND_FLEXIBLE, handler = handlers::second }".parse().unwrap();
        let table = declaration(&input).unwrap();
        assert!(table.entries.len() == 2);
        let output = compact(&expand(&input).unwrap());
        assert!(
            output
                .contains("Box::pin(handlers::first(broker,version,ctx.correlation_id,body,ctx))")
        );
        assert!(output.contains("api_key:keys::SECOND,flexible_min:schema::SECOND_FLEXIBLE,handler:secondasContextHandler"));
        let floor = output
            .find("api_key>=crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR")
            .unwrap();
        let registration = output.find("registry.register(").unwrap();
        let quota = output.find("registry.exempt_from_request_quota(").unwrap();
        assert!(floor < registration && registration < quota);
        assert!(
            output.find("api_key:keys::FIRST").unwrap()
                < output.find("api_key:keys::SECOND").unwrap()
        );
    }

    #[test]
    fn rejects_missing_duplicate_and_nonpath_routing() {
        let row = "first { api = keys::FIRST, flexible = schema::FLEXIBLE, handler = handle }";
        for invalid in [
            String::new(),
            "register table;".into(),
            format!("other table; {row}"),
            format!("register table; {row} {row}"),
            format!(
                "register table; {}",
                row.replace('{', "(").replace('}', ")")
            ),
            format!("register table; {}", row.replace(", handler = handle", "")),
            format!(
                "register table; {}",
                row.replace("keys::FIRST", "selected::<1>()")
            ),
            format!("register table; {}", row.replace("schema::FLEXIBLE", "0")),
            format!(
                "register table; {}",
                row.replace("handle }", "handle, extra = true }")
            ),
        ] {
            assert!(declaration(&invalid.parse().unwrap()).is_err(), "{invalid}");
        }
    }
}
