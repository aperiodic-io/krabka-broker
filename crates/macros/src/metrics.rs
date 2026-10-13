//! `#[derive(RegisterMetrics)]`: see the crate documentation.

use {
    crate::meta::{Tokens, field_ident, impl_block, named_fields},
    moxy::{
        ast::{Attributed, Field, ItemStruct, ParseError},
        token::{Ident, LitStr, TokenStream},
    },
};

/// The arguments of one `#[metric(...)]` field attribute.
#[derive(Default, moxy::FromMeta)]
struct MetricArgs {
    /// Leave the field out of the registration. It is still constructed.
    #[meta(default)]
    skip: bool,
    /// The registered name, when it is not the field name without `_total`.
    #[meta(default)]
    name: Option<String>,
    /// The help text, kept as the tokens of the literal so that the text
    /// reaches the registry exactly as rustc reads it.
    #[meta(default)]
    help: Option<Tokens>,
    /// The bucket boundaries of a histogram, or of each histogram of a family.
    #[meta(default)]
    buckets: Option<Tokens>,
    /// An expression that constructs the field, in place of `Default`.
    #[meta(default)]
    new: Option<Tokens>,
}

/// One field of the derived struct, with what the derive emits for it.
struct Metric {
    ident: Ident,
    constructor: TokenStream,
    registration: Option<Registration>,
}

/// The name and help text a metric registers under.
struct Registration {
    name: LitStr,
    help: TokenStream,
}

/// The name a metric field registers under when it names none itself.
///
/// `prometheus-client` appends `_total` to the name of every counter, so a
/// field that already ends in `_total` drops it. Any other field registers
/// under its own name.
fn default_name(field: &str) -> &str { field.strip_suffix("_total").unwrap_or(field) }

/// Whether `field` is a `Family<..>` rather than a single metric.
fn is_family(field: &Field) -> bool {
    field
        .ty
        .as_path()
        .and_then(|ty| ty.path.last())
        .is_some_and(|segment| segment.ident == "Family")
}

fn metric(field: &Field) -> Result<Metric, ParseError> {
    let ident = field_ident(field, "RegisterMetrics")?;
    let args = field
        .parse_meta::<MetricArgs>("metric")?
        .unwrap_or_default();

    let constructor = match (args.new, args.buckets) {
        (Some(_), Some(buckets)) => {
            return Err(ParseError::new(
                buckets.tokens.span(),
                "give `new` or `buckets`, not both",
            ));
        }
        (Some(new), None) => new.tokens,
        (None, Some(buckets)) if is_family(field) => moxy::template! {
            ::prometheus_client::metrics::family::Family::new_with_constructor(|| {
                ::prometheus_client::metrics::histogram::Histogram::new({{ buckets.tokens }})
            })
        },
        (None, Some(buckets)) => moxy::template! {
            ::prometheus_client::metrics::histogram::Histogram::new({{ buckets.tokens }})
        },
        (None, None) => moxy::template! { ::core::default::Default::default() },
    };

    let registration = if args.skip {
        None
    } else {
        let Some(help) = args.help else {
            return Err(ParseError::new(
                ident.span(),
                "a registered metric needs `#[metric(help = \"...\")]`, or `skip`",
            ));
        };
        let name = args
            .name
            .unwrap_or_else(|| default_name(ident.text()).to_owned());
        Some(Registration {
            name: LitStr::new(&name, ident.span()),
            help: help.tokens,
        })
    };

    Ok(Metric {
        ident: ident.clone(),
        constructor,
        registration,
    })
}

/// Expands `#[derive(RegisterMetrics)]` on `item`.
pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let metrics = named_fields(&item, "RegisterMetrics")?
        .iter()
        .map(metric)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(impl_block(item, &TokenStream::new(), &moxy::template! {
        fn unregistered() -> Self {
            Self {
                @for metric in &metrics {
                    {{ metric.ident }}: {{ metric.constructor }},
                }
            }
        }

        fn register(&self, registry: &mut ::prometheus_client::registry::Registry) {
            @for metric in &metrics {
                @if let Some(registration) = &metric.registration {
                    registry.register(
                        {{ registration.name }},
                        {{ registration.help }},
                        ::core::clone::Clone::clone(&self.{{ metric.ident }}),
                    );
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use {super::default_name, assert2::assert};

    #[test]
    fn default_name_drops_only_a_trailing_total() {
        for (field, name) in [
            ("isr_shrinks_total", "isr_shrinks"),
            ("partitions_total_led", "partitions_total_led"),
            ("topic_bytes_in", "topic_bytes_in"),
            ("total", "total"),
        ] {
            assert!(default_name(field) == name);
        }
    }
}
