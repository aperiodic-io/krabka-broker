//! Methods for explicitly declared reconciliation projections and action enums.

use {
    crate::meta::{field_ident, impl_block, named_fields},
    moxy::{
        ast::{
            Attributed, Field, GenericArgument, ItemEnum, ItemStruct, ParseError, PathArguments,
            Type,
        },
        token::{Ident, TokenStream},
    },
};

#[derive(Default, moxy::FromMeta)]
struct FieldRole {
    #[meta(default)]
    extra: bool,
}

struct ExtraArgument {
    name: Ident,
    ty: Type,
}

fn extra_arguments(extra: &[Field]) -> Vec<ExtraArgument> {
    extra
        .iter()
        .map(|field| ExtraArgument {
            name: field.ident.clone().expect("named field"),
            ty: field.ty.clone(),
        })
        .collect()
}

fn extra_fields(item: &ItemStruct, common: &[&str]) -> Result<Vec<Field>, ParseError> {
    if !item.generics.params.is_empty() {
        return Err(ParseError::new(
            item.ident.span(),
            "reconciliation projections must be concrete structs",
        ));
    }
    let fields = named_fields(item, "reconciliation projection")?;
    for name in common {
        if !fields.iter().any(|field| {
            field
                .ident
                .as_ref()
                .is_some_and(|ident| ident.text() == *name)
        }) {
            return Err(ParseError::new(
                item.ident.span(),
                format!("missing reconciliation field `{name}`"),
            ));
        }
    }
    let mut extra = Vec::new();
    for field in fields {
        let name = field_ident(field, "reconciliation projection")?;
        let role = field
            .parse_meta::<FieldRole>("reconciliation")?
            .unwrap_or_default();
        if role.extra == common.contains(&name.text()) {
            return Err(ParseError::new(
                name.span(),
                "tag only additional projection fields with `#[reconciliation(extra)]`",
            ));
        }
        if role.extra {
            extra.push(field.clone());
        }
    }
    Ok(extra)
}

pub(crate) fn member(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let extra = extra_fields(&item, &[
        "id",
        "member_epoch",
        "assignment_state",
        "assigned",
        "pending_revocation",
        "target",
    ])?;
    let arguments = extra_arguments(&extra);
    let visibility = &item.vis;
    let members = moxy::template! {
        {{ visibility }} fn from_member(
            member: &MemberState,
            target: Option<&::std::collections::HashMap<::krabka_protocol::primitives::uuid::Uuid, Vec<i32>>>,
            @for argument in &arguments { {{ argument.name }}: {{ argument.ty }}, }
        ) -> Self {
            Self {
                id: member.member_id.clone(),
                member_epoch: member.member_epoch,
                assignment_state: member.assignment_state,
                assigned: parts_of(Some(&member.assigned_partitions)),
                pending_revocation: parts_of(Some(&member.partitions_pending_revocation)),
                @for argument in &arguments { {{ argument.name }}, }
                target: parts_of(target),
            }
        }
    };
    Ok(impl_block(item, &TokenStream::new(), &members))
}

pub(crate) fn group(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let extra = extra_fields(&item, &[
        "group_epoch",
        "metadata_hash",
        "target_epoch",
        "members",
        "client_owned",
        "advertised",
    ])?;
    let arguments = extra_arguments(&extra);
    let member_list = named_fields(&item, "ReconciliationGroup")?
        .iter()
        .find(|field| {
            field
                .ident
                .as_ref()
                .is_some_and(|ident| ident.text() == "members")
        })
        .expect("validated member field")
        .ty
        .clone();
    let visibility = &item.vis;
    let members = moxy::template! {
        {{ visibility }} fn empty(@for argument in &arguments { {{ argument.name }}: {{ argument.ty }}, }) -> Self {
            Self {
                group_epoch: INITIAL_GROUP_EPOCH,
                metadata_hash: 0,
                target_epoch: INITIAL_GROUP_EPOCH,
                members: vec![],
                client_owned: vec![],
                advertised: vec![],
                @for argument in &arguments { {{ argument.name }}, }
            }
        }
        {{ visibility }} fn from_group(
            group: &GroupState,
            members: {{ member_list }},
            owned: &::std::collections::BTreeMap<String, ::std::collections::BTreeSet<i32>>,
            advertised: &::std::collections::BTreeMap<String, Vec<i32>>,
            @for argument in &arguments { {{ argument.name }}: {{ argument.ty }}, }
        ) -> Self {
            Self {
                group_epoch: group.group_epoch,
                metadata_hash: group.metadata_hash(),
                target_epoch: group.target.epoch,
                members,
                client_owned: owned_to_vec(owned),
                // std map iteration is the boundary; projected rows have named fields.
                advertised: advertised.iter().map(|(id, parts)| MemberPartitions { member_id: id.clone(), partitions: parts.clone() }).collect(),
                @for argument in &arguments { {{ argument.name }}, }
            }
        }
        {{ visibility }} fn assert_epochs_against(&self, post: &GroupState) {
            use crate::coordinator::unified::actor::reconciliation_model_support::{MemberEpoch, assert_member_epochs};
            assert_member_epochs(self.members.iter().map(|member| MemberEpoch { member_id: member.id.as_str(), epoch: member.member_epoch }), post);
        }
        {{ visibility }} fn handoff_witness(&self) -> bool {
            use crate::coordinator::unified::actor::reconciliation_model_support::{AssignmentView, overlaps_others};
            overlaps_others(self.members.iter().map(|member| AssignmentView { member_id: member.id.as_str(), partitions: &member.target }), &self.client_owned)
        }
    };
    Ok(impl_block(item, &TokenStream::new(), &members))
}

pub(crate) fn action(item: ItemEnum) -> Result<TokenStream, ParseError> {
    if !item.generics.params.is_empty() {
        return Err(ParseError::new(
            item.ident.span(),
            "reconciliation actions must be concrete enums",
        ));
    }
    for name in [
        "Join",
        "Leave",
        "Heartbeat",
        "Keepalive",
        "ClientAdd",
        "ClientRevoke",
    ] {
        if !item
            .variants
            .iter()
            .any(|variant| variant.ident.text() == name)
        {
            return Err(ParseError::new(
                item.ident.span(),
                format!("missing reconciliation action `{name}`"),
            ));
        }
    }
    let ident = item.ident;
    let visibility = item.vis;
    Ok(moxy::template! {
        impl {{ ident }} {
            {{ visibility }} fn member_heartbeat(self, epoch: impl Fn(&str) -> Option<i32>) -> Option<MemberHeartbeat> {
                Some(match self {
                    Self::Join(id) => {
                        if epoch(&id).is_some() { return None; }
                        MemberHeartbeat::Join(id)
                    }
                    Self::Leave(id) => { epoch(&id)?; MemberHeartbeat::Leave(id) }
                    Self::Heartbeat(id) => { let value = epoch(&id)?; MemberHeartbeat::Heartbeat(id, value) }
                    Self::Keepalive(id) => { let value = epoch(&id)?; MemberHeartbeat::Keepalive(id, value) }
                    _ => return None,
                })
            }
        }
    })
}

pub(crate) fn epoch(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let fields = named_fields(&item, "ModelEpochLedger")?;
    let epoch = fields.iter().find(|field| {
        field
            .ident
            .as_ref()
            .is_some_and(|name| name.text() == "epoch")
    });
    let violations = fields.iter().find(|field| {
        field
            .ident
            .as_ref()
            .is_some_and(|name| name.text() == "violations")
    });
    if !item.generics.params.is_empty()
        || !epoch.is_some_and(|field| field.ty.as_path().is_some_and(|ty| ty.path.is_ident("i16")))
    {
        return Err(ParseError::new(
            item.ident.span(),
            "`ModelEpochLedger` needs a concrete projection with epoch: i16",
        ));
    }
    let violation = violations
        .and_then(|field| field.ty.as_path())
        .and_then(|ty| ty.path.last())
        .filter(|segment| segment.ident.text() == "BTreeSet")
        .and_then(|segment| match &segment.args {
            PathArguments::AngleBracketed(arguments) if arguments.args.len() == 1 => {
                match arguments.args.first() {
                    Some(GenericArgument::Type(ty)) => ty.as_path().map(|ty| ty.path.clone()),
                    _ => None,
                }
            }
            _ => None,
        })
        .ok_or_else(|| {
            ParseError::new(
                item.ident.span(),
                "`ModelEpochLedger` needs violations: BTreeSet<ViolationType>",
            )
        })?;
    let visibility = &item.vis;
    let members = moxy::template! {
        /// Observe the previous epoch without discarding other ghost violations.
        {{ visibility }} fn observe_prior_epoch(&mut self, previous: i16) {
            record_epoch_regression(previous, self.epoch, &mut self.violations, {{ violation }}::EpochRegressed);
        }
    };
    Ok(impl_block(item, &TokenStream::new(), &members))
}

#[cfg(test)]
mod tests {
    use {
        super::{action, epoch, group, member},
        crate::meta::compact,
        assert2::assert,
    };

    #[test]
    fn projections_require_common_fields_and_mark_only_extra_fields() {
        for input in [
            "struct Member;",
            "struct Member { id: String }",
            "struct Member<T> { id: String, member_epoch: i32, assignment_state: State, assigned: Vec<i32>, pending_revocation: Vec<i32>, target: Vec<i32> }",
            "struct Member { #[reconciliation(extra)] id: String, member_epoch: i32, assignment_state: State, assigned: Vec<i32>, pending_revocation: Vec<i32>, target: Vec<i32> }",
            "struct Member { id: String, member_epoch: i32, assignment_state: State, assigned: Vec<i32>, pending_revocation: Vec<i32>, target: Vec<i32>, epochs: Vec<i32> }",
        ] {
            assert!(member(moxy::parse!({ input }).unwrap()).is_err(), "{input}");
        }
        assert!(group(moxy::parse!("struct Group { members: Vec<Member> }").unwrap()).is_err());
    }

    #[test]
    fn extra_parameters_follow_the_visible_field_order() {
        let item = moxy::parse!("pub struct Member { id: String, member_epoch: i32, assignment_state: State, assigned: Vec<i32>, pending_revocation: Vec<i32>, #[reconciliation(extra)] first: u32, #[reconciliation(extra)] second: bool, target: Vec<i32> }").unwrap();
        let output = compact(&member(item).unwrap());
        assert!(output.contains("first:u32,second:bool,"));
        assert!(output.contains("first,second,target:"));
    }

    #[test]
    fn action_derivation_rejects_missing_standard_variants() {
        let item = moxy::parse!("enum Action { Join(String) }").unwrap();
        assert!(action(item).is_err());
    }

    #[test]
    fn epoch_ledger_uses_the_declared_violation_type_and_rejects_wrong_fields() {
        let item =
            moxy::parse!("struct Projection { epoch: i16, violations: BTreeSet<Violation> }")
                .unwrap();
        let output = compact(&epoch(item).unwrap());
        assert!(output.contains("fnobserve_prior_epoch(&mutself,previous:i16)"));
        assert!(output.contains("Violation::EpochRegressed"));
        for input in [
            "struct Projection { epoch: i32, violations: BTreeSet<Violation> }",
            "struct Projection { epoch: i16, violations: Vec<Violation> }",
            "struct Projection { epoch: i16 }",
        ] {
            assert!(epoch(moxy::parse!({ input }).unwrap()).is_err(), "{input}");
        }
    }
    #[test]
    fn group_constructors_keep_named_ledgers_and_visible_extra_arguments() {
        let item = moxy::parse!("pub struct Projection { group_epoch: i32, metadata_hash: i64, target_epoch: i32, members: Vec<Member>, client_owned: Vec<MemberPartitions>, advertised: Vec<MemberPartitions>, #[reconciliation(extra)] ledger: Vec<OffsetRow> }").unwrap();
        let output = compact(&group(item).unwrap());
        assert!(output.contains("fnempty(ledger:Vec<OffsetRow>,)"));
        assert!(output.contains("client_owned:owned_to_vec(owned)"));
        assert!(output.contains("MemberPartitions{member_id:id.clone(),partitions:parts.clone()}"));
        assert!(
            output.contains("MemberEpoch{member_id:member.id.as_str(),epoch:member.member_epoch}")
        );
    }

    #[test]
    fn declared_action_variants_emit_epoch_gates_and_refuse_generic_models() {
        let item = moxy::parse!("enum Action { Join(String), Leave(String), Heartbeat(String), Keepalive(String), ClientAdd(String, i32), ClientRevoke(String, i32) }").unwrap();
        let output = compact(&action(item).unwrap());
        assert!(output.contains("ifepoch(&id).is_some(){returnNone;}"));
        assert!(output.contains("Self::Leave(id)=>{epoch(&id)?;MemberHeartbeat::Leave(id)}"));
        assert!(output.contains("Self::Heartbeat(id)=>"));
        assert!(output.contains("Self::Keepalive(id)=>"));
        let generic = moxy::parse!("enum Action<T> { Join(T) }").unwrap();
        assert!(action(generic).is_err());
    }

    #[test]
    fn field_roles_and_violation_generic_arguments_reject_incompatible_schemas() {
        let wrong_role = moxy::parse!("struct Member { id: String, member_epoch: i32, assignment_state: State, assigned: Vec<i32>, pending_revocation: Vec<i32>, target: Vec<i32>, #[reconciliation(unknown)] extra: i32 }").unwrap();
        assert!(member(wrong_role).is_err());
        for declaration in [
            "struct Projection { epoch: i16, violations: BTreeSet<Violation, Other> }",
            "struct Projection { epoch: i16, violations: BTreeSet<&Violation> }",
            "struct Projection<T> { epoch: i16, violations: BTreeSet<T> }",
        ] {
            assert!(
                epoch(moxy::parse!({ declaration }).unwrap()).is_err(),
                "{declaration}"
            );
        }
    }
}
