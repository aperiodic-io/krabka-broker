//! The controller adapter's fixed background-task implementations.

use {
    crate::meta::{first_missing_named_field, impl_block, named_fields},
    moxy::{
        ast::{ItemStruct, ParseError},
        token::{Spanner, TokenStream},
    },
};

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let fields = named_fields(&item, "BackgroundControllers")?;
    if let Some(required) = first_missing_named_field(fields, &["handle", "node_id"]) {
        return Err(ParseError::new(
            item.span(),
            format!("BackgroundControllers needs a `{required}` field"),
        ));
    }
    let common = moxy::template! {
        fn current_image(&self) -> ::std::sync::Arc<krabka_metadata::MetadataImage> {
            self.handle.current_image()
        }
        async fn submit_change(&self, records: Vec<krabka_metadata::MetadataRecord>) -> Result<(), String> {
            self.submit(records).await
        }
    };
    let leadership = moxy::template! {
        fn is_leader(&self) -> bool { self.holds_leadership() }
    };
    let traits = [
        (
            moxy::template! { crate::leader_rebalance::ControllerLike },
            leadership.clone(),
        ),
        (
            moxy::template! { crate::reassignment::ReassignmentController },
            moxy::template! {
                {{ leadership }}
                fn watch_image(&self) -> ::tokio::sync::watch::Receiver<::std::sync::Arc<krabka_metadata::MetadataImage>> {
                    self.handle.watch_image()
                }
            },
        ),
        (
            moxy::template! { crate::break_glass::sweep::BreakGlassController },
            TokenStream::new(),
        ),
    ];
    let items = [item.clone(), item.clone(), item];
    let implementations = traits
        .iter()
        .zip(items)
        .map(|((path, extra), item)| {
            let members = moxy::template! { {{ common }} {{ extra }} };
            let trait_prefix = moxy::template! { {{ path }} for };
            let implementation = impl_block(item, &trait_prefix, &members);
            moxy::template! { #[::async_trait::async_trait] {{ implementation }} }
        })
        .collect::<Vec<_>>();
    Ok(moxy::template! { @for implementation in &implementations { {{ implementation }} } })
}

#[cfg(test)]
mod tests {
    use {
        super::expand,
        moxy::{ast::ItemStruct, token::TokenStream},
    };

    #[yare::parameterized(
        unit_struct = { "struct Adapter;" },
        tuple_struct = { "struct Adapter(bool);" },
        missing_node_id = { "struct Adapter { handle: Handle }" },
        missing_handle = { "struct Adapter { node_id: u64 }" },
    )]
    fn rejects_missing_adapter_fields_and_unnamed_structs(source: &str) {
        let item: ItemStruct = moxy::parse!({ source.parse::<TokenStream>().unwrap() }).unwrap();
        assert2::assert!(expand(item).is_err());
    }

    #[yare::parameterized(
        leader_rebalance = { "crate::leader_rebalance::ControllerLike" },
        reassignment = { "crate::reassignment::ReassignmentController" },
        break_glass = { "crate::break_glass::sweep::BreakGlassController" },
    )]
    fn emits_three_trait_implementations_with_original_generics(controller: &str) {
        let item: ItemStruct = moxy::parse!({
            "struct Adapter<T> where T: Handle { handle: T, node_id: u64 }"
                .parse::<TokenStream>()
                .unwrap()
        })
        .unwrap();
        let output = expand(item).unwrap().to_string();
        assert2::assert!(output.matches("async_trait").count() == 6);
        assert2::assert!(output.matches("submit_change").count() == 3);
        assert2::assert!(output.matches("holds_leadership").count() == 2);
        assert2::assert!(output.matches("watch_image").count() == 2);
        let output: String = output
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect();
        assert2::assert!(
            output.contains(&format!("impl<T>{controller}forAdapter<T>whereT:Handle"))
        );
    }
}
