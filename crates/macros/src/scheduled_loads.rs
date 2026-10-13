//! A visible load-task newtype with caller-target join-failure warnings.

use {
    crate::meta::{impl_block, inner_type},
    moxy::{
        ast::{Attributed, ItemStruct, ParseError},
        token::{Ident, LitStr, Span, TokenStream},
    },
};

#[derive(moxy::FromMeta)]
struct LoadTasks {
    failure_message: String,
}

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    inner_type(&item, "ScheduledLoads")?;
    let policy = item
        .parse_meta::<LoadTasks>("scheduled_loads")?
        .ok_or_else(|| {
            ParseError::new(
                item.ident.span(),
                "`ScheduledLoads` needs `#[scheduled_loads(failure_message = \"message\")]`",
            )
        })?;
    if policy.failure_message.is_empty() {
        return Err(ParseError::new(
            item.ident.span(),
            "load failure message must be nonempty",
        ));
    }
    let message = LitStr::new(&policy.failure_message, item.ident.span());
    let visibility = item.vis.clone();
    let error = Ident::new("error").with_span(Span::mixed_site());
    Ok(impl_block(item, &TokenStream::new(), &moxy::template! {
        /// Waits until every load task of this refresh has ended.
        {{ visibility }} async fn finished(self) {
            crate::task_util::finish_tasks(self.0, |{{ error }}| {
                ::tracing::warn!(target: module_path!(), %{{ error }}, {{ message }});
            }).await;
        }
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::expand,
        crate::meta::{assert_rejected_structs, compact},
        assert2::assert,
        moxy::{
            ast::{ItemImpl, ItemStruct},
            token::ToTokenStream,
        },
    };

    #[test]
    fn finishing_consumes_the_visible_newtype_and_preserves_its_method_visibility() {
        let input: ItemStruct = moxy::parse!({ "#[scheduled_loads(failure_message = \"partition load failed\")] pub(crate) struct Loads(Vec<JoinHandle<()>>);" }).unwrap();
        let implementation: ItemImpl = moxy::parse!({ expand(input).unwrap() }).unwrap();
        assert!(implementation.items.len() == 1);
        let method = implementation.items.first().unwrap().as_fn().unwrap();
        assert!(method.sig.ident == "finished");
        assert!(method.sig.asyncness.is_some());
        assert!(compact(&method.vis.to_token_stream()) == "pub(crate)");
        assert!(method.sig.params.inner.inputs.len() == 1);
        let tokens = compact(&implementation.to_token_stream());
        assert!(tokens.contains("crate::task_util::finish_tasks(self.0,|error|{"));
        assert!(
            tokens
                .contains("::tracing::warn!(target:module_path!(),%error,\"partitionloadfailed\")")
        );
        assert!(tokens.contains("Waitsuntileveryloadtaskofthisrefreshhasended."));
    }

    #[test]
    fn rejects_nonnewtype_layouts_missing_and_invalid_failure_messages() {
        assert_rejected_structs(
            &[
                (
                    "missing warning metadata",
                    "struct Loads(Vec<JoinHandle<()>>);",
                ),
                (
                    "unit layout",
                    "#[scheduled_loads(failure_message = \"failure\")] struct Loads;",
                ),
                (
                    "named layout",
                    "#[scheduled_loads(failure_message = \"failure\")] struct Loads { handles: Vec<JoinHandle<()>> }",
                ),
                (
                    "two fields",
                    "#[scheduled_loads(failure_message = \"failure\")] struct Loads(Vec<JoinHandle<()>>, bool);",
                ),
                (
                    "nonstring warning",
                    "#[scheduled_loads(failure_message = 1)] struct Loads(Vec<JoinHandle<()>>);",
                ),
                (
                    "empty warning",
                    "#[scheduled_loads(failure_message = \"\")] struct Loads(Vec<JoinHandle<()>>);",
                ),
            ],
            expand,
        );
    }
}
