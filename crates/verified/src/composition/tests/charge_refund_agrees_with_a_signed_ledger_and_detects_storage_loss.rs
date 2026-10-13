use {super::*, assert2::assert};

proptest! {
    #[test]
    fn charge_refund_agrees_with_a_signed_ledger_and_detects_storage_loss(
        (initial, burst, refill, requested) in quota_charge_cases(), probe in any::<u64>(),
    ) {
        let (available, debt) = initial;
        let balance = (i128::from(available) - i128::from(debt) + i128::from(refill)).min(i128::from(burst));
        let after_charge = balance - i128::from(requested);
        let expected = if after_charge < -i128::from(u64::MAX) {
            None
        } else {
            let restored_available = u64::try_from(balance.max(0)).unwrap();
            let restored_debt = u64::try_from((-balance).max(0)).unwrap();
            Some((restored_available, restored_debt, probe.min(restored_available)))
        };
        assert!(quota_charge_refund_restores_consume_budget(
            available, debt, refill, burst, requested, probe) == expected);
    }

    #[test]
    fn bounded_charge_and_repayment_agree_with_a_signed_ledger(
        (initial, burst, refill, requested) in quota_charge_cases(), cap in any::<u64>(),
        extra_repayment in any::<u64>(), probe in any::<u64>(),
    ) {
        let repayment = cap.saturating_add(extra_repayment);
        let refilled = (i128::from(initial.0) - i128::from(initial.1) + i128::from(refill)).min(i128::from(burst));
        let charged = (refilled - i128::from(requested)).max(-i128::from(cap));
        let balance = (charged + i128::from(repayment)).min(i128::from(burst));
        let available = u64::try_from(balance).unwrap();
        assert!(bounded_quota_debt_cannot_outlast_repayment(
            initial, refill, burst, requested, cap, repayment, probe) == (available, 0, probe.min(available)));
    }
}

#[test]
fn quota_round_trip_requires_representable_debt_and_bounded_debt_releases_consumers() {
    for (available, debt, refill, burst, charge, expected) in [
        (5, 0, 0, 20, 12, Some((5, 0, 5))),
        (0, 7, 3, 20, 12, Some((0, 4, 0))),
        (0, u64::MAX, 0, 20, 1, None),
        (0, u64::MAX, 1, 20, 1, Some((0, u64::MAX - 1, 0))),
        (0, u64::MAX, u64::MAX, 20, u64::MAX, Some((0, 0, 0))),
        (
            u64::MAX,
            0,
            u64::MAX,
            u64::MAX,
            u64::MAX,
            Some((u64::MAX, 0, u64::MAX)),
        ),
        (0, 0, 0, 0, u64::MAX, Some((0, 0, 0))),
    ] {
        assert!(
            quota_charge_refund_restores_consume_budget(
                available,
                debt,
                refill,
                burst,
                charge,
                u64::MAX
            ) == expected
        );
    }
    assert!(
        bounded_quota_debt_cannot_outlast_repayment((0, u64::MAX), 0, 20, u64::MAX, 7, 12, 20)
            == (5, 0, 5)
    );
    assert!(
        bounded_quota_debt_cannot_outlast_repayment((0, u64::MAX), 0, 20, u64::MAX, 0, 0, 20)
            == (0, 0, 0)
    );
    // Forgetting debt makes a full refund an over-credit, not a round trip.
    let capped = quota_charge(5, 0, 12, 20, 2);
    assert!(quota_credit(capped.0, capped.1, 12, 20) == (10, 0));
    let saturated = quota_charge(0, u64::MAX, 1, 20, u64::MAX);
    assert!(quota_credit(saturated.0, saturated.1, 1, 20) == (0, u64::MAX - 1));
}

proptest! {
    #[test]
    fn token_description_agrees_with_independent_acl_sets_and_session_gate(
        entries in proptest::collection::vec((0usize..14, any::<bool>(), 0u16..1024), 0..20),
        identity in (any::<bool>(), any::<bool>()),
        relationships in (any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>()),
        policy in (any::<bool>(), any::<bool>(), any::<bool>()),
    ) {
        let operations = [AclOperationKind::All, AclOperationKind::Read, AclOperationKind::Write,
            AclOperationKind::Create, AclOperationKind::Delete, AclOperationKind::Alter,
            AclOperationKind::Describe, AclOperationKind::ClusterAction, AclOperationKind::DescribeConfigs,
            AclOperationKind::AlterConfigs, AclOperationKind::IdempotentWrite, AclOperationKind::TwoPhaseCommit,
            AclOperationKind::CreateTokens, AclOperationKind::DescribeTokens];
        let rows: Vec<_> = entries.iter().map(|&(op, allow, flags)| token_acl(operations[op], allow, flags)).collect();
        let resources: Vec<_> = rows.iter().filter(|row|
            row.resource.resource_type == AclResourceTypeMatch::Same && match row.pattern {
                AclPatternKind::Literal => row.resource.exact_name || row.resource.wildcard_name,
                AclPatternKind::Prefixed => row.resource.name_has_prefix,
            }).collect();
        let matched: Vec<_> = resources.iter().filter(|row|
            (row.principal.0 || row.principal.1) && (row.host.0 || row.host.1 || row.host.2)
            && matches!(row.operation, AclOperationKind::All | AclOperationKind::DescribeTokens)).collect();
        let user_acl = policy.0 || (!matched.iter().any(|row| !row.allow)
            && (matched.iter().any(|row| row.allow) || (policy.1 && resources.is_empty())));
        let expected = identity.0 && !identity.1 && relationships.0
            && (relationships.1 || relationships.2 || relationships.3 || policy.2 || user_acl);
        assert!(token_description_preserves_authentication_and_acl_isolation(
            identity, relationships, &rows, policy) == expected);
    }
}

#[test]
fn token_description_separates_create_permissions_defaults_and_privileged_token_sessions() {
    let create = token_acl(AclOperationKind::CreateTokens, true, 0b11_1111_1111);
    let describe = token_acl(AclOperationKind::DescribeTokens, true, 0b11_1111_1111);
    let deny = TokenDescriptionAcl {
        allow: false,
        ..describe
    };
    let unrelated = TokenDescriptionAcl {
        principal: (false, false),
        ..describe
    };
    let caller = (true, false);
    let relationships = (true, false, false, false);
    let closed = (false, false, false);
    assert!(
        !token_description_preserves_authentication_and_acl_isolation(
            caller,
            relationships,
            &[create],
            closed
        )
    );
    assert!(
        token_description_preserves_authentication_and_acl_isolation(
            caller,
            relationships,
            &[describe],
            closed
        )
    );
    assert!(
        !token_description_preserves_authentication_and_acl_isolation(
            caller,
            relationships,
            &[describe, deny],
            closed
        )
    );
    assert!(
        token_description_preserves_authentication_and_acl_isolation(
            caller,
            relationships,
            &[],
            (false, true, false)
        )
    );
    assert!(
        !token_description_preserves_authentication_and_acl_isolation(
            caller,
            relationships,
            &[unrelated],
            (false, true, false)
        )
    );
    assert!(
        !token_description_preserves_authentication_and_acl_isolation(
            (true, true),
            (true, true, true, true),
            &[describe],
            (true, true, true)
        )
    );
    assert!(
        !token_description_preserves_authentication_and_acl_isolation(
            caller,
            (false, true, true, true),
            &[describe],
            (true, true, true)
        )
    );
}

proptest! {
    #[test]
    fn replayed_window_matches_first_sequence_match_and_physical_coordinates(
        batches in proptest::collection::vec((0i64..=20, 0i32..=6, 0i32..=i32::MAX), 1..=5),
        epoch in 0i16..=14, sequence in any::<i32>(), delta in any::<i32>(),
        retry in any::<bool>(), probe in 0usize..5,
    ) {
        let mut end = 0;
        let rows: Vec<_> = batches.iter().map(|&(gap, delta, last_sequence)| {
            let row = recovered_window_row(end + gap, delta, last_sequence);
            end = row.last_offset + 1;
            row
        }).collect();
        let modulus = 1i64 << 31;
        let (epoch, sequence, delta) = if retry {
            let row = rows[probe % rows.len()];
            (7, i32::try_from((i64::from(row.last_sequence) - i64::from(row.offset_delta))
                .rem_euclid(modulus)).unwrap(), row.offset_delta)
        } else { (epoch, sequence, delta) };
        let expected = if epoch == 7 {
            rows.iter().position(|row| i64::from(sequence) ==
                (i64::from(row.last_sequence) - i64::from(row.offset_delta)).rem_euclid(modulus)
                && i64::from(row.last_sequence) ==
                    (i64::from(sequence) + i64::from(delta)).rem_euclid(modulus))
        } else { None };
        let (decision, coordinates) = replayed_window_preserves_first_retry_coordinates(
            end, &rows, (epoch, sequence, delta));
        if let Some(index) = expected {
            let row = rows[index];
            let slot = if index + 1 == rows.len() { 4 } else { index };
            assert!(decision == ProducerDecision::Duplicate { retained: slot });
            assert!(coordinates == Some((index, row.last_offset - i64::from(row.offset_delta),
                row.last_offset + 1)));
        } else {
            let successor = (i64::from(rows.last().unwrap().last_sequence) + 1).rem_euclid(modulus);
            let expected = if epoch < 7 { ProducerDecision::Fenced }
                else if (epoch > 7 && sequence == 0)
                    || (epoch == 7 && i64::from(sequence) == successor) { ProducerDecision::Append }
                else { ProducerDecision::OutOfOrder };
            assert!(decision == expected && coordinates == None);
        }
    }
}
