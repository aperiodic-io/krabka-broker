//! The controller adapter's fixed background-task implementations.

use {
    crate::meta::{impl_block, named_fields},
    moxy::{
        ast::{ItemStruct, ParseError},
        token::{Spanner, TokenStream},
    },
};

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let fields = named_fields(&item, "BackgroundControllers")?;
    for required in ["handle", "node_id"] {
        if !fields
            .iter()
            .any(|field| field.ident.as_ref().is_some_and(|name| name == required))
        {
            return Err(ParseError::new(
                item.span(),
                format!("BackgroundControllers needs a `{required}` field"),
            ));
        }
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
        assert2::assert,
        moxy::{ast::ItemStruct, token::TokenStream},
    };

    #[test]
    fn rejects_missing_adapter_fields_and_unnamed_structs() {
        for item in [
            "struct Adapter;",
            "struct Adapter(bool);",
            "struct Adapter { handle: Handle }",
            "struct Adapter { node_id: u64 }",
        ] {
            let item: ItemStruct = moxy::parse!({ item.parse::<TokenStream>().unwrap() }).unwrap();
            assert!(expand(item).is_err());
        }
    }

    #[test]
    fn emits_three_trait_implementations_with_original_generics() {
        let item: ItemStruct = moxy::parse!({
            "struct Adapter<T> where T: Handle { handle: T, node_id: u64 }"
                .parse::<TokenStream>()
                .unwrap()
        })
        .unwrap();
        let output = expand(item).unwrap().to_string();
        assert!(output.matches("async_trait").count() == 6);
        assert!(output.matches("submit_change").count() == 3);
        assert!(output.matches("holds_leadership").count() == 2);
        assert!(output.matches("watch_image").count() == 2);
        let output: String = output
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect();
        for controller in [
            "crate::leader_rebalance::ControllerLike",
            "crate::reassignment::ReassignmentController",
            "crate::break_glass::sweep::BreakGlassController",
        ] {
            assert!(output.contains(&format!("impl<T>{controller}forAdapter<T>whereT:Handle")));
        }
    }
}
