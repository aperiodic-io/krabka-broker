//! The [`RemoteStorageManager`] impl that binds the copy, fetch, and delete
//! modules to the KIP-405 SPI.
//!
//! A trait impl cannot be split across files, so this module holds the whole
//! block and nothing else. Each method carries the tracing span for the
//! operation and hands the work to the inherent method that implements it.

use {
    super::S3RemoteStorage,
    crate::storage_manager::{RemoteStorageManager, remote_operation},
};

impl RemoteStorageManager for S3RemoteStorage {
    remote_operation! {
        copy(self, metadata, data) {
            self.copy_segment_objects(metadata, data)
        }
    }

    remote_operation! {
        fetch(self, metadata, start_position, end_position) {
            self.fetch_segment_range(metadata, start_position, end_position)
        }
    }

    remote_operation! {
        index(self, metadata, index_type) {
            self.fetch_index_bytes(metadata, index_type)
        }
    }

    remote_operation! {
        delete(self, metadata) {
            self.delete_segment_objects(metadata)
        }
    }
}
