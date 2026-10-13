//! `#[derive(RuntimeOverlay)]`: see the crate documentation.

use {
    crate::meta::{Tokens, field_ident, impl_block, named_fields},
    moxy::{
        ast::{Attributed, Field, ItemStruct, ParseError},
        token::{Ident, TokenStream},
    },
};

/// The arguments of the `#[overlay(...)]` attribute on the struct.
#[derive(moxy::FromMeta)]
struct OverlayTarget {
    /// The type the fields are copied onto.
    target: Tokens,
}

/// The arguments of one `#[overlay(...)]` field attribute.
#[derive(Default, moxy::FromMeta)]
struct OverlayField {
    /// Leave the field out of the copy.
    #[meta(default)]
    skip: bool,
    /// The field is an `Option` of a refined type: copy its `into_value()`.
    #[meta(default)]
    refined: bool,
    /// The field is not `Copy`: copy it with `clone_from`.
    #[meta(default)]
    clone: bool,
}

/// How one field reaches the target.
enum Assignment {
    Plain(Ident),
    Refined(Ident),
    Clone(Ident),
}

fn copy(field: &Field) -> Result<Option<Assignment>, ParseError> {
    let ident = field_ident(field, "RuntimeOverlay")?.clone();
    let args = field
        .parse_meta::<OverlayField>("overlay")?
        .unwrap_or_default();
    Ok(match (args.skip, args.refined, args.clone) {
        (true, false, false) => None,
        (false, false, false) => Some(Assignment::Plain(ident)),
        (false, true, false) => Some(Assignment::Refined(ident)),
        (false, false, true) => Some(Assignment::Clone(ident)),
        _ => {
            return Err(ParseError::new(
                ident.span(),
                "give at most one of `skip`, `refined` and `clone`",
            ));
        }
    })
}

/// Expands `#[derive(RuntimeOverlay)]` on `item`.
pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let Some(target) = item.parse_meta::<OverlayTarget>("overlay")? else {
        return Err(ParseError::new(
            item.ident.span(),
            "`RuntimeOverlay` needs `#[overlay(target = Type)]` on the struct",
        ));
    };
    let target = target.target.tokens;
    let copies = named_fields(&item, "RuntimeOverlay")?
        .iter()
        .map(copy)
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(impl_block(item, &TokenStream::new(), &moxy::template! {
        pub(crate) fn copy_into(&self, target: &mut {{ target }}) {
            @for copy in &copies {
                @if let Assignment::Plain(field) = copy {
                    target.{{ field }} = self.{{ field }};
                }
                @if let Assignment::Refined(field) = copy {
                    target.{{ field }} = self.{{ field }}.map(|value| value.into_value());
                }
                @if let Assignment::Clone(field) = copy {
                    target.{{ field }}.clone_from(&self.{{ field }});
                }
            }
        }
    }))
}
