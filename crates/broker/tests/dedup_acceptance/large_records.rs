//! Large keys and values: the index holds a digest whatever the size of the
//! identity, and a partial-drop rebuild carries large records through intact.

use {
    crate::{
        harness::topic,
        support::{
            self,
            dedup::{Rec, StoredRecord, keyed_batch},
        },
    },
    assert2::check,
    krabka_compression::CompressionType,
};

const KEY_BYTES: usize = 100 * 1024;
const VALUE_BYTES: usize = 512 * 1024;

/// `len` bytes that differ for each `seed` and repeat only every 251 bytes,
/// so a truncated, shifted or swapped payload does not compare equal.
fn pattern(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|at| {
            let low = u8::try_from(at % 251).expect("below 251");
            low.wrapping_mul(31).wrapping_add(seed)
        })
        .collect()
}

fn large(key_seed: u8, value_seed: u8) -> Rec {
    Rec {
        key: Some(pattern(key_seed, KEY_BYTES)),
        value: pattern(value_seed, VALUE_BYTES),
        ..Rec::default()
    }
}

fn read_back(offset: i64, rec: &Rec) -> StoredRecord {
    StoredRecord {
        offset,
        key: rec.key.clone(),
        value: Some(rec.value.clone()),
    }
}

/// A 100 KiB key is an identity like a short one, and the records a partial
/// drop keeps around a dropped large record are read back byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_keys_and_values_deduplicate_and_survive_a_rebuild() {
    let p = support::start().await;
    for codec in [CompressionType::None, CompressionType::Zstd] {
        let topic_name = format!("large-{codec:?}").to_lowercase();
        let t = topic()
            .p(&p)
            .name(&topic_name)
            .extra_configs(&[("max.message.bytes", "8388608")])
            .call()
            .await;
        let first_a = large(1, 10);
        let second_a = large(1, 11);
        let b = large(2, 20);
        let small = Rec::key("small").value("after the large ones");
        let send = |records: Vec<Rec>| {
            t.send_batch(keyed_batch().records(&records).compression(codec).call())
        };

        let first = send(vec![first_a.clone()]).await;
        let partial = send(vec![second_a, b.clone(), small.clone()]).await;
        let contents = t.contents().await;

        check!((first.error_code, first.base_offset) == (0, 0), "{codec:?}");
        check!(
            (partial.error_code, partial.base_offset) == (0, 1),
            "{codec:?}"
        );
        check!(
            contents.records
                == [
                    read_back(0, &first_a),
                    read_back(2, &b),
                    read_back(3, &small)
                ],
            "{codec:?}"
        );
        check!(contents.high_watermark == 4, "{codec:?}");
        check!(
            t.metric("dedup_batches_rebuilt_total").await == 1,
            "{codec:?}"
        );
    }
    p.broker.shutdown().await;
}
