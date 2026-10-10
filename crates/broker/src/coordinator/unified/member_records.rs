//! The per-member records of one group transition, which the consumer, share
//! and streams recorders share.
//!
//! Each group type keeps its own record values, but Kafka's
//! `GroupMetadataManager` decides which member records a transition writes the
//! same way for all three. [`MemberValues`] holds the values that a transition
//! may change, taken before it, and [`MemberValues::record_changes`] compares
//! them with the values after it.

/// A pending batch's three per-member record families. `Some(value)` writes
/// the record, and `None` writes a tombstone.
pub(crate) trait MemberRecordFamilies {
    /// The member-metadata (subscription) record value.
    type Metadata: PartialEq;
    /// The target-assignment record value of one member.
    type Target;
    /// The current-assignment record value.
    type Current: PartialEq;

    /// The member-metadata, target-assignment and current-assignment records,
    /// in that order.
    fn member_record_families(&mut self) -> MemberRecordLists<'_, Self>;
}

/// The mutable record lists that [`MemberRecordFamilies`] lends out.
pub(crate) type MemberRecordLists<'a, P> = (
    &'a mut Vec<(String, Option<<P as MemberRecordFamilies>::Metadata>)>,
    &'a mut Vec<(String, Option<<P as MemberRecordFamilies>::Target>)>,
    &'a mut Vec<(String, Option<<P as MemberRecordFamilies>::Current>)>,
);

/// Member ids whose metadata is being removed, in record order.
pub(crate) fn removed_member_ids<M>(metadata: &[(String, Option<M>)]) -> Vec<String> {
    metadata
        .iter()
        .filter(|(_, value)| value.is_none())
        .map(|(member_id, _)| member_id.clone())
        .collect()
}

/// The tombstone families, in Kafka's member-removal order.
pub(crate) enum MemberTombstone {
    Current,
    Target,
    Metadata,
}

/// Append removed members before the surviving members' updates.
pub(crate) fn append_removed_members<M, T, C>(
    batch: &mut super::OffsetRecordBatchBuilder,
    metadata: &[(String, Option<M>)],
    targets: &[(String, Option<T>)],
    current: &[(String, Option<C>)],
    encode_key: impl Fn(MemberTombstone, &str) -> Result<bytes::Bytes, crate::error::BrokerError>,
) -> Result<Vec<String>, crate::error::BrokerError> {
    let removed = removed_member_ids(metadata);
    for member_id in &removed {
        if has_member_tombstone(current, member_id) {
            batch.push(encode_key(MemberTombstone::Current, member_id)?, None);
        }
        if has_member_tombstone(targets, member_id) {
            batch.push(encode_key(MemberTombstone::Target, member_id)?, None);
        }
        batch.push(encode_key(MemberTombstone::Metadata, member_id)?, None);
    }
    Ok(removed)
}

/// The member-metadata and current-assignment values of the members that a
/// transition may change, taken before it. `None` stands for a member that the
/// group does not hold.
pub(crate) struct MemberValues<M, C>(Vec<(String, Option<(M, C)>)>);

impl<M: PartialEq, C: PartialEq> MemberValues<M, C> {
    /// Takes the values of `member_ids` that `values` projects from the group.
    pub(crate) fn take<'a>(
        member_ids: impl IntoIterator<Item = &'a str>,
        values: impl Fn(&str) -> Option<(M, C)>,
    ) -> Self {
        Self(
            member_ids
                .into_iter()
                .map(|member_id| (member_id.to_owned(), values(member_id)))
                .collect(),
        )
    }

    /// Queues the member records of the transition into `pending`, in the
    /// order the members were taken. `values` projects the group after it.
    ///
    /// - A member that went gets the three tombstones of Kafka's
    ///   `removeMember`.
    /// - A member whose subscription changed, a new member included, gets a
    ///   member-metadata record.
    /// - A member whose current assignment changed gets a current-assignment
    ///   record (`maybeReconcile`).
    pub(crate) fn record_changes<P>(self, pending: &mut P, values: impl Fn(&str) -> Option<(M, C)>)
    where
        P: MemberRecordFamilies<Metadata = M, Current = C>,
    {
        let (member_metadata, target_per_member, current_per_member) =
            pending.member_record_families();
        for (member_id, before) in self.0 {
            match (before, values(&member_id)) {
                (Some(_), None) => {
                    member_metadata.push((member_id.clone(), None));
                    target_per_member.push((member_id.clone(), None));
                    current_per_member.push((member_id, None));
                }
                (before, Some((metadata, current))) => {
                    if before.as_ref().map(|(metadata, _)| metadata) != Some(&metadata) {
                        member_metadata.push((member_id.clone(), Some(metadata)));
                    }
                    if before.as_ref().map(|(_, current)| current) != Some(&current) {
                        current_per_member.push((member_id, Some(current)));
                    }
                }
                (None, None) => {}
            }
        }
    }
}

/// Queue changed target assignments in the caller's member order.
pub(crate) fn append_target_records<T: Default, V>(
    records: &mut Vec<(String, Option<V>)>,
    changed: &[String],
    targets: &std::collections::HashMap<String, T>,
    encode: impl Fn(&T) -> V,
) {
    for member_id in changed {
        let missing = T::default();
        let target = targets.get(member_id).unwrap_or(&missing);
        records.push((member_id.clone(), Some(encode(target))));
    }
}

/// Whether this transition includes a tombstone for the named member.
pub(crate) fn has_member_tombstone<V>(records: &[(String, Option<V>)], member_id: &str) -> bool {
    records
        .iter()
        .any(|(id, value)| id == member_id && value.is_none())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// A batch whose member-metadata and current values are plain numbers.
    #[derive(Debug, Default, PartialEq)]
    struct Batch {
        member_metadata: Vec<(String, Option<u8>)>,
        target_per_member: Vec<(String, Option<u8>)>,
        current_per_member: Vec<(String, Option<u8>)>,
    }

    impl MemberRecordFamilies for Batch {
        type Metadata = u8;
        type Target = u8;
        type Current = u8;

        fn member_record_families(&mut self) -> MemberRecordLists<'_, Self> {
            (
                &mut self.member_metadata,
                &mut self.target_per_member,
                &mut self.current_per_member,
            )
        }
    }

    fn lookup<'a>(group: &'a [(&str, (u8, u8))]) -> impl Fn(&str) -> Option<(u8, u8)> + 'a {
        |member_id| {
            group
                .iter()
                .find(|(id, _)| *id == member_id)
                .map(|(_, values)| *values)
        }
    }

    fn records(list: &[(&str, Option<u8>)]) -> Vec<(String, Option<u8>)> {
        list.iter()
            .map(|(member_id, value)| ((*member_id).to_owned(), *value))
            .collect()
    }

    #[test]
    fn records_only_what_the_transition_changed() {
        let before = [
            ("gone", (1, 1)),
            ("same", (2, 2)),
            ("moved", (3, 3)),
            ("resub", (4, 4)),
        ];
        let after = [
            ("same", (2, 2)),
            ("moved", (3, 9)),
            ("resub", (8, 4)),
            ("new", (5, 5)),
        ];
        let taken = MemberValues::take(
            ["gone", "same", "moved", "resub", "new", "absent"],
            lookup(&before),
        );
        let mut batch = Batch::default();
        taken.record_changes(&mut batch, lookup(&after));
        assert!(
            batch
                == Batch {
                    member_metadata: records(&[
                        ("gone", None),
                        ("resub", Some(8)),
                        ("new", Some(5))
                    ]),
                    target_per_member: records(&[("gone", None)]),
                    current_per_member: records(&[
                        ("gone", None),
                        ("moved", Some(9)),
                        ("new", Some(5))
                    ]),
                }
        );
    }
}
