//! Metadata fixtures shared by the controller and wire tests.

pub(crate) fn encode<M: krabka_protocol::Encode>(message: &M, version: i16) -> bytes::Bytes {
    let mut body = bytes::BytesMut::new();
    message.encode(&mut body, version).expect("encode");
    body.freeze()
}

krabka_macros::topic_record_fixture!(single_partition_topic);
krabka_macros::single_replica_partition_fixture!(single_replica_partition);

pub(crate) fn feature_image(
    kraft_version: u16,
    levels: &[(&str, i16)],
) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&krabka_metadata::MetadataRecord::V1KRaftVersion(
        krabka_metadata::KRaftVersionRecord { kraft_version },
    ));
    for (name, level) in levels {
        image.apply(&krabka_metadata::MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: (*name).into(),
                level: *level,
            },
        ));
    }
    image
}
