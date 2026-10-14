//! Fixed group and cluster gates without recursive resource-name expansion.

use moxy::{
    ast::{Attributes, Parse, ParseError, Parser, Token},
    token::{Group, Ident, Spanner, ToTokens, TokenStream},
};

struct Gate {
    attrs: Attributes,
    name: Ident,
    resource: Ident,
    operation: Ident,
    resource_name: Option<Ident>,
}

impl Gate {
    fn parse(parser: &Parser) -> Result<Self, ParseError> {
        let attrs = Attributes::parse(parser)?;
        let name = Ident::parse(parser)?;
        <Token![:]>::parse(parser)?;
        let resource = Ident::parse(parser)?;
        let arguments = Group::parse(parser)?;
        if !arguments.delim.is_paren() {
            return Err(ParseError::new(
                arguments.span(),
                "gate arguments need parentheses",
            ));
        }
        let args = Parser::from_tokens(&arguments.tokens);
        let operation = Ident::parse(&args)?;
        let resource_name = if Option::<Token![,]>::parse(&args)?.is_some() {
            Some(Ident::parse(&args)?)
        } else {
            None
        };
        if !args.is_empty() {
            return Err(args.error("unexpected gate arguments"));
        }
        match (resource.text(), resource_name.is_some()) {
            ("Group", true) | ("Cluster", false) => {}
            _ => {
                return Err(ParseError::new(
                    resource.span(),
                    "expected Group(operation, name) or Cluster(operation)",
                ));
            }
        }
        Ok(Self {
            attrs,
            name,
            resource,
            operation,
            resource_name,
        })
    }
}

impl ToTokens for Gate {
    fn to_tokens(&self, output: &mut TokenStream) {
        let Self {
            attrs,
            name,
            resource,
            operation,
            resource_name,
        } = self;
        let argument = if let Some(name) = resource_name {
            moxy::template! { {{ name }} }
        } else {
            moxy::template! { acl_wire::CLUSTER_RESOURCE_NAME }
        };
        output.extend(moxy::template! {
            {{ attrs }}
            pub(crate) fn {{ name }}(
                authorizer: &dyn Authorizer,
                image: &MetadataImage,
                ctx: &RequestContext<'_>,
                @if let Some(name) = resource_name { {{ name }}: &str, }
            ) -> bool {
                acl_denied(authorizer, image, ctx, ResourceType::{{ resource }},
                    {{ argument }}, AclOperation::{{ operation }})
            }
        });
    }
}

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    if parser.is_empty() {
        return Err(parser.error("expected at least one gate declaration"));
    }
    let mut gates = Vec::new();
    loop {
        gates.push(Gate::parse(&parser)?);
        if parser.is_empty() {
            break;
        }
        <Token![,]>::parse(&parser)?;
        if parser.is_empty() {
            break;
        }
    }
    Ok(moxy::template! { @for gate in &gates { {{ gate }} } })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact, assert2::assert};

    #[test]
    fn emits_group_parameter_cluster_constant_and_native_documentation() {
        let input = "#[doc = \"group\"] read: Group(Read, group_id), alter: Cluster(Alter),"
            .parse()
            .unwrap();
        let output = compact(&expand(&input).unwrap());
        assert!(output.contains("#[doc=\"group\"]pub(crate)fnread("));
        assert!(output.contains("group_id:&str,"));
        assert!(output.contains("ResourceType::Group,group_id,AclOperation::Read"));
        assert!(
            output.contains(
                "ResourceType::Cluster,acl_wire::CLUSTER_RESOURCE_NAME,AclOperation::Alter"
            )
        );
        assert!(!output.contains("named_acl_gates!"));
    }

    #[yare::parameterized(
        empty = { "" },
        missing_group_name = { "x: Group(Read)" },
        cluster_name = { "x: Cluster(Read, name)" },
        topic_name = { "x: Topic(Read, name)" },
        extra_argument = { "x: Group(Read, name, extra)" },
        bracket_group = { "x: Group[Read, name]" },
        trailing = { "x: Cluster(Read) extra" },
    )]
    fn rejects_empty_declarations_invalid_resources_and_extra_arguments(input: &str) {
        assert2::assert!(expand(&input.parse().unwrap()).is_err(), "{input}");
    }
}
