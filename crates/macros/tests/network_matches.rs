//! Match generators retain Rust arm bodies instead of rebuilding them from parsed expressions.

enum RecordsPayload {
    Raw(Vec<u8>),
    Legacy(Vec<u8>),
    V2(Vec<u8>),
    FileRegions(Vec<u8>),
    Unknown,
}

async fn decode(payload: Result<RecordsPayload, &'static str>) -> Result<Vec<u8>, &'static str> {
    krabka_macros::records_payload_match! {
        match payload {
            #[cfg(any())]
            Ok(RecordsPayload::Raw(_)) => excluded_arm_must_stay_excluded(),
            Ok(RecordsPayload::Legacy(values)) => {
                let mut converted = Vec::new();
                for value in values {
                    converted.push(value + 1);
                }
                Ok(converted)
            }
            Ok(RecordsPayload::Raw(values) | RecordsPayload::V2(values))
                if values.iter().all(|value| *value != 0) => Ok(values),
            Ok(RecordsPayload::FileRegions(values)) => async { Ok(values) }.await,
            other => async { portable(other) }.await,
        }
    }
}

fn portable(payload: Result<RecordsPayload, &'static str>) -> Result<Vec<u8>, &'static str> {
    match payload {
        Ok(RecordsPayload::FileRegions(values)) => Ok(values),
        Err(error) => Err(error),
        _ => Err("unsupported records"),
    }
}

#[yare::parameterized(
    legacy = { Ok(RecordsPayload::Legacy(vec![1, 2])), Ok(vec![2, 3]) },
    raw = { Ok(RecordsPayload::Raw(vec![4, 5])), Ok(vec![4, 5]) },
    v2 = { Ok(RecordsPayload::V2(vec![6, 7])), Ok(vec![6, 7]) },
    zero_raw_is_refused = { Ok(RecordsPayload::Raw(vec![0])), Err("unsupported records") },
    file_regions = { Ok(RecordsPayload::FileRegions(vec![8, 9])), Ok(vec![8, 9]) },
    unknown_payload = { Ok(RecordsPayload::Unknown), Err("unsupported records") },
    decode_error = { Err("decode failed"), Err("decode failed") },
)]
#[test_macro(tokio::test)]
async fn preserves_loop_tail_result_attributes_guards_and_awaits(
    payload: Result<RecordsPayload, &'static str>,
    expected: Result<Vec<u8>, &'static str>,
) {
    assert2::assert!(decode(payload).await == expected);
}
