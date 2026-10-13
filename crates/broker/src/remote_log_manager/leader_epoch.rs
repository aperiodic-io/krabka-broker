//! Serializes a segment's leader-epoch map into the Kafka
//! `leader-epoch-checkpoint` text format that the copy path uploads.

use {bytes::Bytes, std::collections::BTreeMap};

/// Serialize a segment's leader-epoch map into Kafka's
/// `leader-epoch-checkpoint` text format (the bytes carried as
/// `LogSegmentData.leader_epoch_index`).
pub(super) fn leader_epoch_index_bytes(epochs: &BTreeMap<krabka_ids::LeaderEpoch, i64>) -> Bytes {
    use std::fmt::Write as _;
    let mut s = String::from("0\n");
    let _ = writeln!(s, "{}", epochs.len());
    for (epoch, start) in epochs {
        // On-disk `leader-epoch-checkpoint` text format: unwrap to the raw
        // `i32` so the serialized bytes stay byte-identical.
        let _ = writeln!(s, "{} {start}", epoch.0);
    }
    Bytes::from(s.into_bytes())
}

#[cfg(test)]
mod tests {
    use {super::*, krabka_ids::LeaderEpoch};

    #[test]
    fn serializes_leader_epoch_index() {
        let mut map = BTreeMap::new();
        map.insert(LeaderEpoch(1), 100);
        map.insert(LeaderEpoch(2), 200);

        let bytes = leader_epoch_index_bytes(&map);
        assert2::check!(bytes == Bytes::from_static(b"0\n2\n1 100\n2 200\n"));
    }
}
