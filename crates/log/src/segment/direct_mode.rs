//! An active segment that writes through `O_DIRECT`.
//!
//! Under `preallocate`, the active segment writes its batches through a
//! second, `O_DIRECT` handle on its `.log` file and keeps the newest of them in
//! memory, so they reach the disk without passing through the page cache, and
//! a consumer reading right behind the producer still reads from memory. The
//! buffered handle stays: reads, `sendfile`, `fsync` and every truncate go
//! through it, and the kernel keeps the two coherent.
//!
//! A direct write must cover whole blocks, so the file runs up to a block
//! past its last batch while the segment is active: the [`DirectWriter`]
//! zero-pads the partial last block and rewrites it on the next append. The
//! padding goes when the segment stops writing directly -- at the seal, when
//! `preallocate` is turned off, and when the segment is dropped. A crash
//! leaves it, and tail recovery cuts it as it cuts any bytes that do not
//! decode as a batch, which is what Kafka's own `preallocate` leaves behind.

use {
    super::{
        Segment,
        direct::{DioAlign, DirectWriter, TailCache},
        io::{read_full_at, seek_to_log_size},
    },
    crate::{error::LogError, io::DirectFile, name},
    krabka_units::prelude::{ByteSize, ByteSizeExt as _},
};

/// The `O_DIRECT` writer of an active segment and the newest bytes it wrote.
///
/// `cache.end()` is the segment's `log_size` for as long as the segment
/// writes directly: every append pushes what it wrote, and every truncate
/// cuts it back.
pub(super) struct DirectMode {
    pub(super) writer: DirectWriter,
    pub(super) cache: TailCache,
}

impl std::fmt::Debug for DirectMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectMode")
            .field("end", &self.writer.end())
            .field("cached_from", &self.cache.start())
            .finish_non_exhaustive()
    }
}

impl Drop for DirectMode {
    /// Cut the padding past the last batch once the queued writes are done,
    /// so a segment closed cleanly is exactly as long as its batches. A
    /// failure leaves it for recovery.
    fn drop(&mut self) { let _ = self.writer.cut_padding(); }
}

impl Segment {
    pub(crate) fn writes_direct(&self) -> bool { self.direct.is_some() }

    /// Whether this segment has written through `O_DIRECT` since it was
    /// opened, sealed or not. See `wrote_direct` on [`Segment`].
    #[krabka_macros::sendfile_cfg]
    pub(crate) fn wrote_direct(&self) -> bool { self.wrote_direct }

    /// Write this segment's appends through `O_DIRECT` from here on, keeping
    /// up to `tail_cache` of the newest bytes in memory for reads.
    ///
    /// The log calls this before every append under `preallocate`, so it asks
    /// once per segment: a filesystem that cannot is not asked again until the
    /// segment has gone back to buffered writes. A refusal leaves the segment
    /// writing through the page cache, as it would without `preallocate`, so
    /// it is logged and not returned.
    pub(crate) fn write_direct(&mut self, tail_cache: ByteSize) {
        if self.direct.is_some() || self.direct_asked || self.sealed {
            return;
        }
        self.direct_asked = true;
        match self.open_direct_mode(tail_cache) {
            Ok(mode) => {
                self.direct = Some(Box::new(mode));
                self.wrote_direct = true;
            }
            Err(error) => super::lifecycle::log_refused("direct", self.base_offset, &error),
        }
    }

    fn open_direct_mode(&self, tail_cache: ByteSize) -> std::io::Result<DirectMode> {
        let DirectFile {
            file,
            mem_align,
            block,
        } = self
            .io
            .open_direct(&name::log_path(&self.dir, self.base_offset.0))?;
        let tail = self.read_partial_block(block)?;
        let writer = DirectWriter::new(
            file,
            DioAlign {
                mem: mem_align,
                block,
            },
            self.log_size,
            tail,
            std::sync::Arc::clone(&self.io),
        )?;
        Ok(DirectMode {
            writer,
            cache: TailCache::new(tail_cache.bytes_usize(), self.log_size),
        })
    }

    /// Go back to writing through the page cache: cut the padding and put the
    /// buffered handle's cursor at the end, where the next append lands.
    ///
    /// # Errors
    /// Returns the truncate or seek error.
    /// The error of a queued write that failed, so a seal does not hide it.
    pub(crate) fn write_buffered(&mut self) -> Result<(), LogError> {
        self.drain_direct()?;
        self.direct_asked = false;
        if self.direct.take().is_some() {
            self.log_file.set_len(self.log_size)?;
            seek_to_log_size(&self.log_file, self.log_size)?;
        }
        Ok(())
    }

    /// Wait until every queued `O_DIRECT` write is on the disk.
    ///
    /// # Errors
    /// The error of a failed write.
    pub(crate) fn drain_direct(&self) -> std::io::Result<()> {
        self.direct
            .as_ref()
            .map_or(Ok(()), |mode| mode.writer.drain())
    }

    /// Wait until no queued `O_DIRECT` write is running, whatever became of
    /// them. A truncate waits for this, so no write lands past the new end
    /// after it.
    pub(super) fn settle_direct(&self) { let _ = self.drain_direct(); }

    /// Bring the direct writer and the cache back in line with `log_size`
    /// after a truncate: the partial last block is now whatever the file
    /// holds there, and nothing at or past the end is cached.
    pub(super) fn resync_direct(&mut self) -> Result<(), LogError> {
        let Some(block) = self.direct.as_ref().map(|mode| mode.writer.block()) else {
            return Ok(());
        };
        let tail = self.read_partial_block(block)?;
        let log_size = self.log_size;
        if let Some(mode) = self.direct.as_mut() {
            mode.writer.reset(log_size, tail)?;
            mode.cache.truncate(log_size);
        }
        Ok(())
    }

    /// The bytes of the block `log_size` falls in, from its start to
    /// `log_size`, read through the buffered handle.
    fn read_partial_block(&self, block: usize) -> std::io::Result<Vec<u8>> {
        let block = u64::try_from(block)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let in_block = self.log_size.checked_rem(block).unwrap_or(0);
        let mut tail = vec![
            0;
            usize::try_from(in_block).map_err(|_| std::io::Error::from(
                std::io::ErrorKind::InvalidInput
            ))?
        ];
        let read = read_full_at(&self.log_file, self.log_size - in_block, &mut tail)?;
        if read < tail.len() {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        Ok(tail)
    }
}

impl Segment {
    /// The cached bytes from `pos` on, up to about `max`, when the segment
    /// writes directly and holds `pos` in memory. See [`TailCache::bytes_from`].
    pub(super) fn cached_bytes(&self, pos: u64, max: usize) -> Option<bytes::Bytes> {
        self.direct.as_ref()?.cache.bytes_from(pos, max)
    }
}
