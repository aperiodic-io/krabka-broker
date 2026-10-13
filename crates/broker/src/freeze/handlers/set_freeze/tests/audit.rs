//! Tests for the resources that the ordinary administrative event names.
//!
//! A thaw names the proposal it spent beside the scope it thawed, so a rule
//! that reads those events joins the approval to the transition on that id.

use {
    super::{super::response::admin_resources, PROPOSAL},
    assert2::check,
    krabka_audit::AuditResource,
    uuid::Uuid,
};

#[test]
fn a_thaw_names_the_break_glass_proposal_and_the_scope_in_its_audit_resources() {
    let expected = vec![
        AuditResource {
            resource_type: "TopicFreeze".to_owned(),
            name: "literal:orders".to_owned(),
        },
        AuditResource {
            resource_type: "break-glass-proposal".to_owned(),
            name: PROPOSAL.to_string(),
        },
    ];
    check!(admin_resources("literal:orders", false, PROPOSAL) == expected);
}

#[test]
fn a_freeze_names_only_the_scope_in_its_audit_resources() {
    let expected = vec![AuditResource {
        resource_type: "TopicFreeze".to_owned(),
        name: "prefixed:tenant-a.".to_owned(),
    }];
    check!(admin_resources("prefixed:tenant-a.", true, Uuid::nil()) == expected);
}
