//! `#[derive(FieldDefaults)]`: see the crate documentation.

use {
    crate::meta::{field_ident, impl_block, named_fields},
    moxy::{
        ast::{Attributed, Field, ItemStruct, ParseError},
        token::{Spanner, TokenStream},
    },
};

/// The expression inside a field's `#[default(...)]`.
struct DefaultExpr(TokenStream);

impl moxy::ast::FromMeta for DefaultExpr {
    fn from_meta(meta: &moxy::ast::Meta) -> Result<Self, ParseError> {
        match &meta.content {
            moxy::ast::MetaContent::List(group) if !group.tokens.is_empty() => {
                Ok(Self(group.tokens.clone()))
            }
            _ => Err(ParseError::new(
                meta.span(),
                "expected `#[default(<expression>)]`",
            )),
        }
    }
}

/// `name: <value>` for `field`: its `#[default(...)]` expression, or
/// `Default::default()`.
fn initializer(field: &Field) -> Result<TokenStream, ParseError> {
    let ident = field_ident(field, "FieldDefaults")?;
    let value = field.parse_meta::<DefaultExpr>("default")?.map_or_else(
        || moxy::template! { ::core::default::Default::default() },
        |DefaultExpr(value)| value,
    );
    Ok(moxy::template! { {{ ident }}: {{ value }}, })
}

/// Expands `#[derive(FieldDefaults)]` on `item`.
pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let initializers = named_fields(&item, "FieldDefaults")?
        .iter()
        .map(initializer)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(impl_block(
        item,
        &moxy::template! { ::core::default::Default for },
        &moxy::template! {
            fn default() -> Self {
                Self {
                    @for initializer in &initializers { {{ initializer }} }
                }
            }
        },
    ))
}
