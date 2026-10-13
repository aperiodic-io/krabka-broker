//! Remote operation tracing preserves span levels, identity fields, and error events.

use {
    assert2::assert,
    std::sync::{Arc, Mutex},
    tracing_subscriber::fmt::{
        format::FmtSpan,
        writer::{MakeWriter, MutexGuardWriter},
    },
};

struct TopicPartition {
    topic_id: &'static str,
    partition: i32,
}
struct SegmentId {
    topic_id_partition: TopicPartition,
    id: &'static str,
}
struct Metadata {
    id: SegmentId,
    start_offset: i64,
    end_offset: i64,
}

impl Metadata {
    fn remote_log_segment_id(&self) -> &SegmentId { &self.id }

    fn start_offset(&self) -> i64 { self.start_offset }

    fn end_offset(&self) -> i64 { self.end_offset }
}

#[derive(Debug, Clone, Copy)]
enum IndexType {
    Offset,
}

struct Store {
    fail: bool,
}

impl Store {
    #[krabka_macros::remote_operation]
    fn copy_log_segment_data(
        &self,
        metadata: &Metadata,
        data: &str,
    ) -> Result<Option<String>, &'static str> {
        if self.fail {
            Err("storage refused")
        } else {
            Ok(Some(data.to_owned()))
        }
    }

    #[krabka_macros::remote_operation]
    fn fetch_log_segment(
        &self,
        metadata: &Metadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, &'static str> {
        if self.fail {
            Err("storage refused")
        } else {
            Ok(vec![7])
        }
    }

    #[krabka_macros::remote_operation]
    fn fetch_index(
        &self,
        metadata: &Metadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, &'static str> {
        if self.fail {
            Err("storage refused")
        } else {
            Ok(vec![9])
        }
    }

    #[krabka_macros::remote_operation]
    fn delete_log_segment_data(&self, metadata: &Metadata) -> Result<(), &'static str> {
        if self.fail {
            Err("storage refused")
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for Buffer {
    type Writer = MutexGuardWriter<'a, Vec<u8>>;

    fn make_writer(&'a self) -> Self::Writer { self.0.as_ref().make_writer() }
}

#[test]
fn records_only_the_operation_contract_and_preserves_results() {
    let metadata = Metadata {
        id: SegmentId {
            topic_id_partition: TopicPartition {
                topic_id: "topic-id",
                partition: 4,
            },
            id: "segment-id",
        },
        start_offset: 10,
        end_offset: 19,
    };
    let buffer = Buffer::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .without_time()
        .with_ansi(false)
        .with_span_events(FmtSpan::NEW)
        .with_writer(buffer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let store = Store { fail: false };
        assert!(
            store
                .copy_log_segment_data(&metadata, "private-payload")
                .unwrap()
                == Some("private-payload".to_owned())
        );
        assert!(store.fetch_log_segment(&metadata, 2, Some(6)).unwrap() == vec![7]);
        assert!(store.fetch_index(&metadata, IndexType::Offset).unwrap() == vec![9]);
        assert!(store.delete_log_segment_data(&metadata).is_ok());
        assert!(Store { fail: true }.delete_log_segment_data(&metadata) == Err("storage refused"));
    });
    let output = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
    let lines: Vec<_> = output.lines().collect();
    let target = format!("{}:", module_path!());
    assert!(lines.len() == 6);
    for (line, operation, level) in [
        (lines[0], "copy_log_segment_data", "INFO"),
        (lines[1], "fetch_log_segment", "DEBUG"),
        (lines[2], "fetch_index", "DEBUG"),
        (lines[3], "delete_log_segment_data", "INFO"),
        (lines[4], "delete_log_segment_data", "INFO"),
        (lines[5], "delete_log_segment_data", "ERROR"),
    ] {
        assert!(line.contains(level));
        assert!(line.contains(operation));
        assert!(line.contains("topic_id=topic-id partition=4 segment=segment-id"));
        assert!(line.contains(&target));
    }
    assert!(lines[0].contains("start_offset=10 end_offset=19"));
    assert!(lines[1].contains("start_position=2 end_position=Some(6)"));
    assert!(lines[2].contains("index_type=Offset"));
    assert!(lines[5].contains("error=storage refused"));
    assert!(!output.contains("private-payload"));
    assert!(!output.contains("self="));
    assert!(!output.contains("metadata="));
    assert!(!output.contains("data="));
}
