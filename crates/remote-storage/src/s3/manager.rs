//! The [`RemoteStorageManager`] impl that binds the copy, fetch, and delete
//! modules to the KIP-405 SPI.
//!
//! A trait impl cannot be split across files, so this module holds the whole
//! block and nothing else. Each method carries the tracing span for the
//! operation and hands the work to the inherent method that implements it.

use {
    super::S3RemoteStorage,
    crate::{
        error::RemoteStorageError,
        metadata::{CustomMetadata, RemoteLogSegmentMetadata},
        storage_manager::{IndexType, LogSegmentData, RemoteStorageManager},
    },
};

impl RemoteStorageManager for S3RemoteStorage {
    #[krabka_macros::remote_operation]
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        self.copy_segment_objects(metadata, data)
    }

    #[krabka_macros::remote_operation]
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        self.fetch_segment_range(metadata, start_position, end_position)
    }

    #[krabka_macros::remote_operation]
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        self.fetch_index_bytes(metadata, index_type)
    }

    #[krabka_macros::remote_operation]
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        self.delete_segment_objects(metadata)
    }
}
