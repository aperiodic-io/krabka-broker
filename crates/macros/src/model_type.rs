//! Conditional proof/runtime derives attached to the original model declaration.

use {
    crate::meta::parenthesized_list,
    moxy::{
        ast::{FromMeta, Item, List, Meta, ParseError, Parser, Path, PathArguments, Token},
        token::{Spanner, TokenStream},
    },
    proc_macro::TokenStream as NativeTokenStream,
};

struct DerivePaths {
    paths: List<Path, Token![,]>,
}

impl DerivePaths {
    fn proof_default() -> Self {
        Self {
            paths: List::parse_all(&Parser::from_tokens(&moxy::template! {
                std::clone::Clone, Copy, DeepModel
            }))
            .expect("fixed proof derives"),
        }
    }

    fn runtime_default() -> Self {
        Self {
            paths: List::parse_all(&Parser::from_tokens(&moxy::template! {
                Clone, Copy, Debug, PartialEq, Eq
            }))
            .expect("fixed runtime derives"),
        }
    }
}

impl FromMeta for DerivePaths {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        let (tokens, list_span) = parenthesized_list(
            meta,
            "expected a parenthesized list of derive paths",
            "expected parenthesized derive paths",
        )?;
        let paths = List::<Path, Token![,]>::parse_all(&Parser::from_tokens(&tokens))?;
        if paths.is_empty()
            || paths.iter().any(|path| {
                path.iter()
                    .any(|segment| !matches!(segment.args, PathArguments::None))
            })
        {
            return Err(ParseError::new(
                list_span,
                "expected at least one derive path without generic arguments",
            ));
        }
        Ok(Self { paths })
    }
}

#[derive(moxy::FromMeta)]
struct ModelPolicy {
    #[meta(default = DerivePaths::proof_default())]
    proof: DerivePaths,
    #[meta(default = DerivePaths::runtime_default())]
    runtime: DerivePaths,
}

fn attributes(meta: &TokenStream, declaration: &TokenStream) -> Result<TokenStream, ParseError> {
    let declaration: Item = moxy::parse!({ declaration.clone() })?;
    if !matches!(declaration, Item::Struct(_) | Item::Enum(_)) {
        return Err(ParseError::new(
            declaration.span(),
            "`model_type` requires one struct or enum",
        ));
    }
    let options: Meta = moxy::parse!({
        moxy::template! { model_type({{ meta }}) }
    })?;
    let ModelPolicy {
        proof: DerivePaths { paths: proof },
        runtime: DerivePaths { paths: runtime },
    } = ModelPolicy::from_meta(&options)?;
    Ok(moxy::template! {
        #[cfg_attr(creusot, derive({{ proof }}))]
        #[cfg_attr(not(creusot), derive({{ runtime }}))]
    })
}

pub(crate) fn expand(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let mut output: NativeTokenStream =
        attributes(&meta.into(), &TokenStream::from(item.clone()))?.into();
    // Field/variant order, attributes and caller proof scopes stay in their original native tokens.
    output.extend(item);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {
        super::{ModelPolicy, attributes},
        assert2::assert,
        moxy::{
            ast::{Attributed, FromMeta, Item, List, Meta, MetaContent, Parser, Path, Token},
            token::{ToTokenStream, TokenStream},
        },
    };

    fn policy(arguments: &str) -> ModelPolicy {
        let input = moxy::template! { model_type({{ arguments.parse::<TokenStream>().unwrap() }}) };
        let options: Meta = moxy::parse!({ input }).unwrap();
        ModelPolicy::from_meta(&options).unwrap()
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ConditionalDerive {
        condition: String,
        paths: Vec<String>,
    }

    impl ConditionalDerive {
        fn from_names(condition: &str, paths: &[&str]) -> Self {
            Self {
                condition: condition.to_owned(),
                paths: paths.iter().map(|path| (*path).to_owned()).collect(),
            }
        }
    }

    fn conditional_derives(item: &Item) -> Vec<ConditionalDerive> {
        item.attrs()
            .iter()
            .map(|attribute| {
                assert!(attribute.path.is_ident("cfg_attr"));
                let MetaContent::List(group) = &attribute.content else {
                    panic!("cfg_attr list")
                };
                let entries =
                    List::<Meta, Token![,]>::parse_all(&Parser::from_tokens(&group.tokens))
                        .unwrap();
                let MetaContent::List(derives) = &entries.last().unwrap().content else {
                    panic!("derive list")
                };
                let paths =
                    List::<Path, Token![,]>::parse_all(&Parser::from_tokens(&derives.tokens))
                        .unwrap()
                        .iter()
                        .map(|path| path.to_token_stream().to_string().replace(' ', ""))
                        .collect();
                ConditionalDerive {
                    condition: entries
                        .first()
                        .unwrap()
                        .to_token_stream()
                        .to_string()
                        .replace(' ', ""),
                    paths,
                }
            })
            .collect()
    }

    #[yare::parameterized(
        named_struct = { "struct Named { later: u64, earlier: i32 }" },
        tuple_struct = { "struct Tuple(u64);" },
        enum_layout = { "enum Choice { Later { value: i64 }, Earlier, Tuple(u32) }" },
    )]
    fn conditional_attributes_preserve_the_exact_default_policy_on_each_layout(source: &str) {
        let original: TokenStream = source.parse().unwrap();
        let prefix = attributes(&TokenStream::new(), &original).unwrap();
        let declaration: Item = moxy::parse!({
            moxy::template! { {{ prefix }} {{ original }} }
        })
        .unwrap();
        assert2::assert!(
            conditional_derives(&declaration)
                == [
                    ConditionalDerive::from_names("creusot", &[
                        "std::clone::Clone",
                        "Copy",
                        "DeepModel"
                    ]),
                    ConditionalDerive::from_names("not(creusot)", &[
                        "Clone",
                        "Copy",
                        "Debug",
                        "PartialEq",
                        "Eq"
                    ]),
                ]
        );
    }

    #[yare::parameterized(
        proof_only = {
            "proof(std::clone::Clone, Copy)",
            vec!["std::clone::Clone", "Copy"],
            vec!["Clone", "Copy", "Debug", "PartialEq", "Eq"]
        },
        runtime_only = {
            "runtime(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)",
            vec!["std::clone::Clone", "Copy", "DeepModel"],
            vec!["Clone", "Copy", "Debug", "Default", "PartialEq", "Eq", "Hash"]
        },
        both_lists = {
            "proof(DeepModel), runtime(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)",
            vec!["DeepModel"],
            vec![
                "Debug",
                "Clone",
                "Copy",
                "PartialEq",
                "Eq",
                "PartialOrd",
                "Ord",
                "Hash",
                "Display",
                "From",
                "Into",
            ],
        },
    )]
    fn overrides_replace_only_the_selected_list_in_original_path_order(
        arguments: &str,
        proof: Vec<&str>,
        runtime: Vec<&str>,
    ) {
        let options = policy(arguments);
        let names = |paths: &List<Path, Token![,]>| {
            paths
                .iter()
                .map(|path| path.to_token_stream().to_string().replace(' ', ""))
                .collect::<Vec<_>>()
        };
        assert2::assert!(names(&options.proof.paths) == proof);
        assert2::assert!(names(&options.runtime.paths) == runtime);
    }

    #[yare::parameterized(
        unknown = { "unknown(Clone)" },
        duplicate = { "proof(Clone), proof(Copy)" },
        assignment = { "runtime = Clone" },
        empty = { "proof()" },
        missing_comma = { "runtime(Debug Eq)" },
        generic = { "proof(Clone<u32>)" },
        literal = { "proof(1)" },
        bracketed = { "proof[Clone]" },
    )]
    fn refuses_invalid_overrides(arguments: &str) {
        let item: TokenStream = "struct Model;".parse().unwrap();
        assert2::assert!(
            attributes(&arguments.parse().unwrap(), &item).is_err(),
            "{arguments}"
        );
    }

    #[yare::parameterized(
        function = { "fn kernel() {}" },
        alias = { "type Alias = u64;" },
        union_item = { "union Bits { value: u64 }" },
        multiple_items = { "struct First; struct Second;" },
    )]
    fn refuses_nontype_items(source: &str) {
        assert2::assert!(attributes(&TokenStream::new(), &source.parse().unwrap()).is_err());
    }
}
