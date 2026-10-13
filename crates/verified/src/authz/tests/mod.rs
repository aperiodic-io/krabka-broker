use {super::*, assert2::check};

fn facts() -> AclFacts {
    AclFacts {
        super_user: false,
        saw_allow: false,
        saw_deny: false,
        default_decision: AclDefault::Deny,
    }
}

mod acl_precedence_matches_kafka_scenarios;
