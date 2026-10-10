//! The leaf encoders and decoders that the streams record values share.
//!
//! Every streams value is flexible (`"flexibleVersions": "0+"` in the Apache
//! Kafka schemas at tag `4.3.1`), so an array is a compact array and a string
//! is a compact string, and every nested struct ends with its own tagged-field
//! count. The general leaves live in
//! [`persistence::flex`](crate::coordinator::unified::persistence::flex); what
//! is here is the two shapes that only streams records use.

use std::collections::BTreeMap;

use bytes::{BufMut, BytesMut};

use crate::{
    coordinator::unified::persistence::{
        flex::{
            get_compact_array, get_compact_array_len, get_compact_string, get_i32_array,
            put_compact_array, put_compact_array_len, put_compact_string, put_empty_tagged_fields,
            put_i32_array, put_tagged_fields, read_tagged, skip_tagged_fields,
        },
        get_i16,
    },
    error::BrokerError,
};

/// The tag of `TaskIds.AssignmentEpochs` in
/// `StreamsGroupCurrentMemberAssignmentValue`.
const TAG_ASSIGNMENT_EPOCHS: u32 = 0;

/// Encodes a role's task assignment as Kafka's `[]TaskIds`: a compact count,
/// then per entry the compact `SubtopologyId`, the compact `Partitions`
/// (`[]int32`) and the struct's tagged-field count. [`decode_task_map`] reads
/// the same layout.
pub(super) fn encode_task_map(buf: &mut BytesMut, map: &BTreeMap<String, Vec<i32>>) {
    encode_task_map_with_epochs(buf, map, &BTreeMap::new());
}

/// Encodes a role's task assignment as the `[]TaskIds` of
/// `StreamsGroupCurrentMemberAssignmentValue`, whose `TaskIds` declares the
/// tagged `AssignmentEpochs` (tag 0, a nullable `[]int32`, default null). An
/// entry of `epochs` writes the field for its subtopology, and a subtopology
/// without one leaves it at its default, which Kafka's generated writer
/// omits.
pub(super) fn encode_task_map_with_epochs(
    buf: &mut BytesMut,
    map: &BTreeMap<String, Vec<i32>>,
    epochs: &BTreeMap<String, Vec<i32>>,
) {
    put_compact_array_len(buf, map.len());
    for (subtopology_id, partitions) in map {
        put_compact_string(buf, subtopology_id);
        put_i32_array(buf, partitions);
        match epochs.get(subtopology_id) {
            Some(epochs) => {
                let mut payload = BytesMut::new();
                put_i32_array(&mut payload, epochs);
                put_tagged_fields(buf, vec![(TAG_ASSIGNMENT_EPOCHS, payload.freeze())]);
            }
            None => put_empty_tagged_fields(buf),
        }
    }
}

pub(super) fn decode_task_map(buf: &mut &[u8]) -> Result<BTreeMap<String, Vec<i32>>, BrokerError> {
    decode_task_map_with_epochs(buf).map(|(map, _)| map)
}

/// A role's tasks, or their assignment epochs, by subtopology id.
use crate::coordinator::unified::streams::state::TaskMap;

/// Reads what [`encode_task_map_with_epochs`] writes: the task map, and the
/// `AssignmentEpochs` of each subtopology that carries a non-null one.
pub(super) fn decode_task_map_with_epochs(
    buf: &mut &[u8],
) -> Result<(TaskMap, TaskMap), BrokerError> {
    let n = get_compact_array_len(buf)?;
    let mut map = BTreeMap::new();
    let mut epochs = BTreeMap::new();
    for _ in 0..n {
        let subtopology_id = get_compact_string(buf)?;
        let partitions = get_i32_array(buf)?;
        let mut assignment_epochs = None;
        read_tagged(buf, |tag, payload| {
            if tag != TAG_ASSIGNMENT_EPOCHS {
                return Ok(false);
            }
            assignment_epochs =
                match krabka_protocol::primitives::array::get_nullable_array_len(payload, true)? {
                    None => None,
                    Some(count) => {
                        let mut values = Vec::with_capacity(count.min(payload.len() / 4));
                        for _ in 0..count {
                            values.push(krabka_protocol::primitives::fixed::get_i32(payload)?);
                        }
                        Some(values)
                    }
                };
            Ok(true)
        })?;
        if let Some(assignment_epochs) = assignment_epochs {
            epochs.insert(subtopology_id.clone(), assignment_epochs);
        }
        map.insert(subtopology_id, partitions);
    }
    Ok((map, epochs))
}

/// Encodes a `[]KeyValue`-shaped list: a compact count, then per entry two
/// compact strings and the struct's tagged-field count. Kafka's `ClientTags`
/// and `TopicConfigs` both have this shape.
pub(super) fn encode_key_value_list(buf: &mut BytesMut, items: &[(String, String)]) {
    put_compact_array(buf, items.iter(), |buf, (k, v)| {
        put_compact_string(buf, k);
        put_compact_string(buf, v);
        put_empty_tagged_fields(buf);
    });
}

pub(super) fn decode_key_value_list(buf: &mut &[u8]) -> Result<Vec<(String, String)>, BrokerError> {
    get_compact_array(buf, |buf| {
        let k = get_compact_string(buf)?;
        let v = get_compact_string(buf)?;
        skip_tagged_fields(buf)?;
        Ok((k, v))
    })
}

/// Encodes an `[]int16`, which the copartition groups of the topology record
/// use for their indices into the subtopology's own topic lists.
pub(super) fn encode_i16_list(buf: &mut BytesMut, items: &[i16]) {
    put_compact_array(buf, items.iter(), |buf, &v| buf.put_i16(v));
}

pub(super) fn decode_i16_list(buf: &mut &[u8]) -> Result<Vec<i16>, BrokerError> {
    get_compact_array(buf, get_i16)
}
