//! A visible named task collection with caller-target join-failure warnings.

use {
    crate::meta::{impl_block, named_fields},
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
    let fields = named_fields(&item, "ScheduledLoads")?;
    if fields.len() != 1
        || fields
            .first()
            .and_then(|field| field.ident.as_ref())
            .is_none_or(|name| name != "tasks")
    {
        return Err(ParseError::new(
            item.ident.span(),
            "`ScheduledLoads` needs exactly one named `tasks` field",
        ));
    }
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
            crate::task_util::finish_tasks(self.tasks, |{{ error }}| {
                ::tracing::warn!(target: module_path!(), %{{ error }}, {{ message }});
            }).await;
        }
    }))
}

#[cfg(test)]
mod tests {
    use {
        super::expand,
        crate::meta::compact,
        assert2::assert,
        moxy::{
            ast::{ItemImpl, ItemStruct},
            token::ToTokenStream,
        },
    };

    #[test]
    fn finishing_consumes_the_visible_tasks_and_preserves_its_method_visibility() {
        let input: ItemStruct = moxy::parse!({ "#[scheduled_loads(failure_message = \"partition load failed\")] pub(crate) struct Loads { tasks: Vec<JoinHandle<()>> }" }).unwrap();
        let implementation: ItemImpl = moxy::parse!({ expand(input).unwrap() }).unwrap();
        assert!(implementation.items.len() == 1);
        let method = implementation.items.first().unwrap().as_fn().unwrap();
        assert!(method.sig.ident == "finished");
        assert!(method.sig.asyncness.is_some());
        assert!(compact(&method.vis.to_token_stream()) == "pub(crate)");
        assert!(method.sig.params.inner.inputs.len() == 1);
        let tokens = compact(&implementation.to_token_stream());
        assert!(tokens.contains("crate::task_util::finish_tasks(self.tasks,|error|{"));
        assert!(
            tokens
                .contains("::tracing::warn!(target:module_path!(),%error,\"partitionloadfailed\")")
        );
        assert!(tokens.contains("Waitsuntileveryloadtaskofthisrefreshhasended."));
    }

    #[test]
    fn rejects_missing_task_fields_and_invalid_failure_messages() {
        for source in [
            "struct Loads { tasks: Vec<JoinHandle<()>> }",
            "#[scheduled_loads(failure_message = \"failure\")] struct Loads;",
            "#[scheduled_loads(failure_message = \"failure\")] struct Loads { handles: Vec<JoinHandle<()>> }",
            "#[scheduled_loads(failure_message = \"failure\")] struct Loads { tasks: Vec<JoinHandle<()>>, extra: bool }",
            "#[scheduled_loads(failure_message = 1)] struct Loads { tasks: Vec<JoinHandle<()>> }",
            "#[scheduled_loads(failure_message = \"\")] struct Loads { tasks: Vec<JoinHandle<()>> }",
        ] {
            let item: ItemStruct = moxy::parse!({ source }).unwrap();
            assert!(expand(item).is_err(), "{source}");
        }
    }
}
