//! `#[derive(PrimitiveCmp)]`: see the crate documentation.

use moxy::{
    ast::{ItemStruct, ParseError},
    token::TokenStream,
};

use crate::meta::inner_type;

/// Expands `#[derive(PrimitiveCmp)]` on `item`.
pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let inner = inner_type(&item, "PrimitiveCmp")?;
    let ident = item.ident;
    let (impl_generics, type_generics, where_clause) = item.generics.split();
    Ok(moxy::template! {
        impl {{ impl_generics }} ::core::cmp::PartialEq<{{ inner }}> for {{ ident }} {{ type_generics }} {{ where_clause }} {
            #[inline]
            fn eq(&self, other: &{{ inner }}) -> bool {
                self.0 == *other
            }
        }

        impl {{ impl_generics }} ::core::cmp::PartialEq<{{ ident }} {{ type_generics }}> for {{ inner }} {{ where_clause }} {
            #[inline]
            fn eq(&self, other: &{{ ident }} {{ type_generics }}) -> bool {
                *self == other.0
            }
        }

        impl {{ impl_generics }} ::core::cmp::PartialOrd<{{ inner }}> for {{ ident }} {{ type_generics }} {{ where_clause }} {
            #[inline]
            fn partial_cmp(&self, other: &{{ inner }}) -> ::core::option::Option<::core::cmp::Ordering> {
                ::core::cmp::PartialOrd::partial_cmp(&self.0, other)
            }
        }

        impl {{ impl_generics }} ::core::cmp::PartialOrd<{{ ident }} {{ type_generics }}> for {{ inner }} {{ where_clause }} {
            #[inline]
            fn partial_cmp(&self, other: &{{ ident }} {{ type_generics }}) -> ::core::option::Option<::core::cmp::Ordering> {
                ::core::cmp::PartialOrd::partial_cmp(self, &other.0)
            }
        }
    })
}
