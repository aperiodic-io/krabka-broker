//! Conditional proof/runtime derives attached to the original model declaration.

use {
    moxy::{
        ast::{
            FromMeta, Item, List, Meta, MetaContent, ParseError, Parser, Path, PathArguments, Token,
        },
        token::{Spanner, TokenStream},
    },
    proc_macro::TokenStream as NativeTokenStream,
};

struct DerivePaths(List<Path, Token![,]>);

impl DerivePaths {
    fn proof_default() -> Self {
        Self(
            List::parse_all(&Parser::from_tokens(&moxy::template! {
                std::clone::Clone, Copy, DeepModel
            }))
            .expect("fixed proof derives"),
        )
    }

    fn runtime_default() -> Self {
        Self(
            List::parse_all(&Parser::from_tokens(&moxy::template! {
                Clone, Copy, Debug, PartialEq, Eq
            }))
            .expect("fixed runtime derives"),
        )
    }
}

impl FromMeta for DerivePaths {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        let MetaContent::List(group) = &meta.content else {
            return Err(ParseError::new(
                meta.span(),
                "expected a parenthesized list of derive paths",
            ));
        };
        if !group.delim.is_paren() {
            return Err(ParseError::new(
                group.span(),
                "expected parenthesized derive paths",
            ));
        }
        let paths = List::<Path, Token![,]>::parse_all(&Parser::from_tokens(&group.tokens))?;
        if paths.is_empty()
            || paths.iter().any(|path| {
                path.iter()
                    .any(|segment| !matches!(segment.args, PathArguments::None))
            })
        {
            return Err(ParseError::new(
                group.span(),
                "expected at least one derive path without generic arguments",
            ));
        }
        Ok(Self(paths))
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
        proof: DerivePaths(proof),
        runtime: DerivePaths(runtime),
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

    fn conditional_derives(item: &Item) -> Vec<(String, Vec<String>)> {
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
                (
                    entries
                        .first()
                        .unwrap()
                        .to_token_stream()
                        .to_string()
                        .replace(' ', ""),
                    paths,
                )
            })
            .collect()
    }

    #[test]
    fn conditional_attributes_preserve_the_exact_default_policy_on_each_layout() {
        for source in [
            "struct Named { later: u64, earlier: i32 }",
            "struct Tuple(u64);",
            "enum Choice { Later { value: i64 }, Earlier, Tuple(u32) }",
        ] {
            let original: TokenStream = source.parse().unwrap();
            let prefix = attributes(&TokenStream::new(), &original).unwrap();
            let declaration: Item = moxy::parse!({
                moxy::template! { {{ prefix }} {{ original }} }
            })
            .unwrap();
            assert!(
                conditional_derives(&declaration)
                    == [
                        ("creusot", vec!["std::clone::Clone", "Copy", "DeepModel"]),
                        ("not(creusot)", vec![
                            "Clone",
                            "Copy",
                            "Debug",
                            "PartialEq",
                            "Eq"
                        ]),
                    ]
                    .map(|(condition, paths)| (
                        condition.to_owned(),
                        paths.into_iter().map(str::to_owned).collect::<Vec<_>>(),
                    ))
            );
        }
    }

    #[test]
    fn overrides_replace_only_the_selected_list_in_original_path_order() {
        for (arguments, proof, runtime) in [
            (
                "proof(std::clone::Clone, Copy)",
                vec!["std::clone::Clone", "Copy"],
                vec!["Clone", "Copy", "Debug", "PartialEq", "Eq"],
            ),
            (
                "runtime(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)",
                vec!["std::clone::Clone", "Copy", "DeepModel"],
                vec![
                    "Clone",
                    "Copy",
                    "Debug",
                    "Default",
                    "PartialEq",
                    "Eq",
                    "Hash",
                ],
            ),
            (
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
            ),
        ] {
            let options = policy(arguments);
            let names = |paths: &List<Path, Token![,]>| {
                paths
                    .iter()
                    .map(|path| path.to_token_stream().to_string().replace(' ', ""))
                    .collect::<Vec<_>>()
            };
            assert!(names(&options.proof.0) == proof);
            assert!(names(&options.runtime.0) == runtime);
        }
    }

    #[test]
    fn refuses_invalid_overrides_and_nontype_items() {
        let item: TokenStream = "struct Model;".parse().unwrap();
        for arguments in [
            "unknown(Clone)",
            "proof(Clone), proof(Copy)",
            "runtime = Clone",
            "proof()",
            "runtime(Debug Eq)",
            "proof(Clone<u32>)",
            "proof(1)",
            "proof[Clone]",
        ] {
            assert!(attributes(&arguments.parse().unwrap(), &item).is_err());
        }
        for source in [
            "fn kernel() {}",
            "type Alias = u64;",
            "union Bits { value: u64 }",
            "struct First; struct Second;",
        ] {
            assert!(attributes(&TokenStream::new(), &source.parse().unwrap()).is_err());
        }
    }
}
