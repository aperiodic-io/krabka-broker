//! Shared field groups inserted without changing projection layout or hash order.

use {
    crate::meta::{mode, named_fields},
    moxy::{
        ast::{GenericArgument, ItemStruct, ParseError, PathArguments},
        token::TokenStream,
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream, TokenTree},
};

struct FieldAdditions {
    prefix: TokenStream,
    suffix: TokenStream,
}

fn additions(item: &ItemStruct, role: &str) -> Result<FieldAdditions, ParseError> {
    let visibility = &item.vis;
    let fields = named_fields(item, "reconciliation_fields")?;
    if role == "member" {
        return Ok(FieldAdditions {
            prefix: moxy::template! {
                {{ visibility }} id: String,
                {{ visibility }} member_epoch: i32,
                {{ visibility }} assignment_state: MemberAssignmentState,
                /// The coordinator's current assignment, sorted.
                {{ visibility }} assigned: Vec<i32>,
                {{ visibility }} pending_revocation: Vec<i32>,
            },
            suffix: TokenStream::new(),
        });
    }
    if !fields.first().is_some_and(|field| {
        field
            .ident
            .as_ref()
            .is_some_and(|name| name.text() == "members")
    }) {
        return Err(ParseError::new(
            item.ident.span(),
            "group projection must declare members before its extra fields",
        ));
    }
    let member_type = &fields.first().expect("validated members field").ty;
    let simple_member_list = member_type.as_path().and_then(|ty| ty.path.last()).is_some_and(|segment| {
        segment.ident.text() == "Vec" && matches!(&segment.args,
            PathArguments::AngleBracketed(arguments) if arguments.args.len() == 1
                && matches!(arguments.args.first(), Some(GenericArgument::Type(ty))
                    if ty.as_path().is_some_and(|ty| ty.path.iter().all(|segment| matches!(segment.args, PathArguments::None)))))
    });
    if !simple_member_list {
        return Err(ParseError::new(
            item.ident.span(),
            "members must be Vec<ConcreteMember>",
        ));
    }
    Ok(FieldAdditions {
        prefix: moxy::template! {
            {{ visibility }} group_epoch: i32,
            /// The metadata hash that the group recorded last.
            {{ visibility }} metadata_hash: i64,
            {{ visibility }} target_epoch: i32,
        },
        suffix: moxy::template! {
            /// Ground-truth ownership, sorted by member id and partition.
            {{ visibility }} client_owned: Vec<MemberPartitions>,
            /// Last advertised assignment, which the faithful client follows, sorted by id and partition.
            {{ visibility }} advertised: Vec<MemberPartitions>,
        },
    })
}

pub(crate) fn expand(
    metadata: &TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let role = mode(metadata.clone(), ["member", "group"])?;
    let parsed_tokens: TokenStream = item.clone().into();
    let parsed: ItemStruct = moxy::parse!({ parsed_tokens })?;
    let FieldAdditions { prefix, suffix } = additions(&parsed, role)?;
    let mut tokens: Vec<_> = item.into_iter().collect();
    let Some(TokenTree::Group(body)) = tokens.pop() else {
        return Err(ParseError::new(
            parsed.ident.span(),
            "projection needs a named field body",
        ));
    };
    if body.delimiter() != Delimiter::Brace {
        return Err(ParseError::new(
            parsed.ident.span(),
            "projection needs braces",
        ));
    }
    let mut fields: NativeTokenStream = prefix.into();
    let original: Vec<_> = body.stream().into_iter().collect();
    if role == "group" {
        // The typed declaration requires the first field to be members: Vec<Member>.
        // Its comma is top-level; tuple/documentation commas remain in native groups.
        let separator = original
            .iter()
            .position(|token| matches!(token, TokenTree::Punct(comma) if comma.as_char() == ','))
            .ok_or_else(|| {
                ParseError::new(parsed.ident.span(), "members field needs a trailing comma")
            })?;
        fields.extend(original[..=separator].iter().cloned());
        fields.extend(NativeTokenStream::from(suffix));
        fields.extend(original[separator + 1..].iter().cloned());
    } else {
        fields.extend(original);
    }
    let mut replacement = Group::new(Delimiter::Brace, fields);
    replacement.set_span(body.span());
    tokens.push(TokenTree::Group(replacement));
    Ok(tokens.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use {
        super::{FieldAdditions, additions},
        assert2::assert,
    };

    #[test]
    fn member_prefix_and_group_surroundings_keep_original_order() {
        let item =
            moxy::parse!("pub struct Projection { members: Vec<Member>, extra: usize }").unwrap();
        let FieldAdditions { prefix, suffix } = additions(&item, "group").unwrap();
        let prefix = prefix.to_string();
        assert!(prefix.find("group_epoch").unwrap() < prefix.find("metadata_hash").unwrap());
        assert!(prefix.find("metadata_hash").unwrap() < prefix.find("target_epoch").unwrap());
        let suffix = suffix.to_string();
        assert!(suffix.find("client_owned").unwrap() < suffix.find("advertised").unwrap());
        let FieldAdditions { prefix, suffix } = additions(&item, "member").unwrap();
        assert!(suffix.is_empty());
        assert!(
            prefix.to_string().find("assigned").unwrap()
                < prefix.to_string().find("pending_revocation").unwrap()
        );
    }
    #[test]
    fn field_groups_require_named_fields_and_a_concrete_first_member_list() {
        for declaration in [
            "struct Projection;",
            "struct Projection { extra: usize, members: Vec<Member> }",
            "struct Projection { members: Option<Member> }",
            "struct Projection { members: Vec<Member<T>> }",
            "struct Projection { members: Vec<Member, Other> }",
        ] {
            let item = moxy::parse!({ declaration }).unwrap();
            assert!(additions(&item, "group").is_err(), "{declaration}");
        }
        let unnamed = moxy::parse!("struct Projection(Member);").unwrap();
        assert!(additions(&unnamed, "member").is_err());
        let visible = moxy::parse!("pub(super) struct Projection { target: Vec<i32> }").unwrap();
        let fields = additions(&visible, "member").unwrap();
        assert!(fields.prefix.to_string().contains("pub"));
        assert!(fields.prefix.to_string().contains("MemberAssignmentState"));
    }
}
