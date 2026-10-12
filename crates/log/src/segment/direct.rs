//! The `O_DIRECT` write path of an active segment's `.log` file.
//!
//! An `O_DIRECT` write skips the page cache, so the kernel puts three rules on
//! it: the buffer address is a multiple of the memory alignment, and the file
//! offset and the length are multiples of the block size. [`DioAlign`] holds
//! the two values for one file.
//!
//! Kafka record batches have arbitrary sizes and go to arbitrary byte
//! positions, so an append almost never starts or stops on a block boundary.
//! [`DirectWriter`] therefore keeps the partial last block of the file in
//! memory, and each append writes that block again together with the new
//! bytes. It writes `[tail_start, padded_end)`, where `tail_start` is the
//! logical end rounded down to a block and `padded_end` is the new logical end
//! rounded up to a block. Zeros fill the space after the logical end. The file
//! can thus be up to `block - 1` bytes longer than the segment. That is
//! expected: the segment knows its logical size and removes the padding when
//! it seals the file.
//!
//! The page cache does not hold what an `O_DIRECT` write put on disk, so a
//! consumer that reads right behind the producer would go to the device for
//! every fetch. [`TailCache`] keeps a bounded window of the most recently
//! appended bytes in memory to serve those reads.

use std::{
    collections::VecDeque,
    fs::File,
    io::{Error, ErrorKind},
};

use bytes::Bytes;

/// The `O_DIRECT` alignment rules of one file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DioAlign {
    /// Required alignment of a buffer's address, in bytes. A power of two, >= 1.
    pub(crate) mem: usize,
    /// Required alignment of a file offset and of a write's length, in bytes. A power of two, >= 1.
    pub(crate) block: usize,
}

impl DioAlign {
    fn validate(self) -> std::io::Result<()> {
        if self.mem.is_power_of_two() && self.block.is_power_of_two() {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::InvalidInput,
                format!("O_DIRECT alignment is not a power of two: {self:?}"),
            ))
        }
    }

    /// The unit of progress that keeps the next write legal: a write that
    /// starts this many bytes further into an aligned buffer, at an aligned
    /// offset, is aligned in both address and offset. Both values are powers
    /// of two, so the larger is a multiple of the smaller.
    fn step(self) -> usize {
        self.mem.max(self.block)
    }
}

/// A growable byte buffer whose contents start on an `align`-aligned address.
///
/// The buffer over-allocates a `Vec<u8>` by `align - 1` bytes and starts at
/// the first aligned byte in it, so it needs no `unsafe` allocator call. The
/// `Vec` never changes length after it is made, so its heap address, and with
/// it the alignment, stays fixed until the next growth. A growth makes a new
/// `Vec`, aligns it again, and copies the contents.
pub(crate) struct AlignedBuf {
    align: usize,
    storage: Vec<u8>,
    offset: usize,
    len: usize,
}

impl AlignedBuf {
    pub(crate) fn new(align: usize) -> Self {
        Self {
            align: align.max(1),
            storage: Vec::new(),
            offset: 0,
            len: 0,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.len = 0;
    }

    pub(crate) fn extend_from_slice(&mut self, bytes: &[u8]) {
        let new_len = self.len + bytes.len();
        self.reserve(new_len);
        let at = self.offset + self.len;
        self.storage[at..at + bytes.len()].copy_from_slice(bytes);
        self.len = new_len;
    }

    /// Set the length to `len`. New bytes are zero, also where an earlier
    /// use of the buffer left other data.
    pub(crate) fn resize(&mut self, len: usize) {
        if len > self.len {
            self.reserve(len);
            self.storage[self.offset + self.len..self.offset + len].fill(0);
        }
        self.len = len;
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.storage[self.offset..self.offset + self.len]
    }

    fn capacity(&self) -> usize {
        self.storage.len() - self.offset
    }

    /// Make room for `needed` bytes after the aligned start.
    fn reserve(&mut self, needed: usize) {
        if needed <= self.capacity() {
            return;
        }
        let capacity = needed.max(self.capacity().saturating_mul(2));
        let storage = vec![0; capacity + self.align - 1];
        // The modulo works on the address alone, so it is right for every
        // allocation, where `align_offset` may report that it cannot help.
        let misalignment = storage.as_ptr().addr() % self.align;
        let offset = (self.align - misalignment) % self.align;
        let mut grown = Self {
            align: self.align,
            storage,
            offset,
            len: 0,
        };
        grown.storage[offset..offset + self.len].copy_from_slice(self.as_slice());
        grown.len = self.len;
        *self = grown;
    }
}

/// The `O_DIRECT` side of an active segment: the partial last block and a
/// reusable staging buffer.
pub(crate) struct DirectWriter {
    file: File,
    align: DioAlign,
    end: u64,
    tail: Vec<u8>,
    staging: AlignedBuf,
}

impl DirectWriter {
    /// Take over the `O_DIRECT` handle `file` on a segment's `.log`.
    ///
    /// `end` is the segment's logical size. `tail` holds the file's bytes in
    /// `[end - end % block, end)`; the caller reads them through its buffered
    /// handle.
    ///
    /// # Errors
    ///
    /// `InvalidInput` when an alignment is not a power of two, or when
    /// `tail.len()` is not `end % block`.
    pub(crate) fn new(
        file: File,
        align: DioAlign,
        end: u64,
        tail: Vec<u8>,
    ) -> std::io::Result<Self> {
        align.validate()?;
        check_tail(align, end, &tail)?;
        Ok(Self {
            file,
            align,
            end,
            tail,
            staging: AlignedBuf::new(align.mem),
        })
    }

    pub(crate) fn end(&self) -> u64 {
        self.end
    }

    pub(crate) fn file(&self) -> &File {
        &self.file
    }

    /// The block size that file offsets and write lengths align to.
    pub(crate) fn block(&self) -> usize {
        self.align.block
    }

    /// Append the concatenation of `parts` at [`Self::end`].
    ///
    /// The staging buffer gets the partial last block, then `parts`, then
    /// zeros up to a block multiple. `write` then puts it on disk at the start
    /// of the partial last block. `write(file, buf, offset)` is one positioned
    /// write; this function calls it again after a short write and after
    /// `Interrupted`. A short write that stops off an aligned position goes on
    /// from the last aligned position, because the next write must be aligned
    /// too. It writes some bytes again, with the same contents.
    ///
    /// An empty append writes nothing.
    ///
    /// # Errors
    ///
    /// The first error of `write` other than `Interrupted`, or `WriteZero` when
    /// `write` makes no aligned progress. After an error, [`Self::end`] and the
    /// partial last block are as they were before the call, and the file can
    /// hold some of the new bytes past `end`; the caller truncates them.
    pub(crate) fn append(
        &mut self,
        parts: &[&[u8]],
        mut write: impl FnMut(&File, &[u8], u64) -> std::io::Result<usize>,
    ) -> std::io::Result<()> {
        let total: usize = parts.iter().map(|part| part.len()).sum();
        if total == 0 {
            return Ok(());
        }
        let block = self.align.block;
        let logical = self.tail.len() + total;
        let padded = logical.div_ceil(block) * block;

        self.staging.clear();
        self.staging.extend_from_slice(&self.tail);
        for part in parts {
            self.staging.extend_from_slice(part);
        }
        self.staging.resize(padded);

        let tail_start = self.end - to_u64(self.tail.len());
        let step = self.align.step();
        let staged = self.staging.as_slice();
        let mut done = 0;
        while done < staged.len() {
            match write(&self.file, &staged[done..], tail_start + to_u64(done)) {
                Ok(written) => {
                    let reached = done.saturating_add(written).min(staged.len());
                    // The end of the buffer is block-aligned, and a write that
                    // reaches it ends the loop, so only a short write rounds.
                    let next = if reached == staged.len() {
                        reached
                    } else {
                        reached - reached % step
                    };
                    if next <= done {
                        return Err(Error::new(
                            ErrorKind::WriteZero,
                            format!(
                                "O_DIRECT write at {} made no aligned progress",
                                tail_start + to_u64(done)
                            ),
                        ));
                    }
                    done = next;
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }

        let keep = logical % block;
        self.tail.clear();
        self.tail
            .extend_from_slice(&staged[logical - keep..logical]);
        self.end += to_u64(total);
        Ok(())
    }

    /// Adopt `end` and its partial last block `tail` after the caller
    /// truncated the file to `end`.
    ///
    /// # Errors
    ///
    /// `InvalidInput` when `tail.len()` is not `end % block`. The writer does
    /// not change then.
    pub(crate) fn reset(&mut self, end: u64, tail: Vec<u8>) -> std::io::Result<()> {
        check_tail(self.align, end, &tail)?;
        self.end = end;
        self.tail = tail;
        Ok(())
    }
}

fn check_tail(align: DioAlign, end: u64, tail: &[u8]) -> std::io::Result<()> {
    let expected = end % to_u64(align.block);
    if to_u64(tail.len()) == expected {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "partial last block of a segment that ends at {end} has {} bytes, not {expected}",
                tail.len()
            ),
        ))
    }
}

fn to_u64(n: usize) -> u64 {
    u64::try_from(n).expect("a usize fits in a u64 on every supported target")
}

/// The most recently appended bytes of an active segment, `[start, end)`, at
/// most about `cap` bytes.
///
/// The window holds whole appends as shared [`Bytes`] chunks, so a push copies
/// nothing, and eviction drops the oldest chunk.
pub(crate) struct TailCache {
    cap: usize,
    start: u64,
    len: usize,
    chunks: VecDeque<Bytes>,
}

impl TailCache {
    /// An empty window that begins at `start`, the segment's logical end when
    /// the cache is made.
    pub(crate) fn new(cap: usize, start: u64) -> Self {
        Self {
            cap,
            start,
            len: 0,
            chunks: VecDeque::new(),
        }
    }

    pub(crate) fn start(&self) -> u64 {
        self.start
    }

    pub(crate) fn end(&self) -> u64 {
        self.start + to_u64(self.len)
    }

    /// Record `bytes` appended at [`Self::end`].
    ///
    /// Then drop the oldest chunks while the window is larger than `cap`. The
    /// newest chunk stays even when it alone is larger than `cap`, so a reader
    /// right behind the producer always finds the last append.
    pub(crate) fn push(&mut self, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }
        self.len += bytes.len();
        self.chunks.push_back(bytes);
        while self.len > self.cap && self.chunks.len() > 1 {
            let evicted = self
                .chunks
                .pop_front()
                .expect("more than one chunk remains");
            self.len -= evicted.len();
            self.start += to_u64(evicted.len());
        }
    }

    /// Record the batches of one coalesced write, `group[..ends[0]]`,
    /// `group[ends[0]..ends[1]]` and so on, appended at [`Self::end`].
    ///
    /// A slice of `group` keeps the whole group allocation alive, so a batch
    /// that stays in the window gets storage of its own: otherwise one small
    /// cached batch would hold up to the group's full size and the window
    /// would no longer be bounded by `cap`. A batch that this same push
    /// evicts is pushed as a slice and dropped at once, so only the
    /// survivors are copied.
    pub(crate) fn push_group(&mut self, group: &Bytes, ends: &[usize]) {
        // Walking back from the newest batch, the survivors are those whose
        // bytes still fit in `cap`; the newest survives regardless.
        let mut kept = 0usize;
        let mut first_kept = ends.len();
        for i in (0..ends.len()).rev() {
            let start = if i == 0 { 0 } else { ends[i - 1] };
            let len = ends[i] - start;
            if first_kept != ends.len() && kept + len > self.cap {
                break;
            }
            kept += len;
            first_kept = i;
        }
        let mut start = 0;
        for (i, &end) in ends.iter().enumerate() {
            let batch = if i >= first_kept {
                Bytes::copy_from_slice(&group[start..end])
            } else {
                group.slice(start..end)
            };
            self.push(batch);
            start = end;
        }
    }

    /// Copy `[pos, pos + out.len())` into `out` and return true when the whole
    /// range is in the window. Return false and leave `out` as it is when it
    /// is not.
    pub(crate) fn read(&self, pos: u64, out: &mut [u8]) -> bool {
        let Some(read_end) = pos.checked_add(to_u64(out.len())) else {
            return false;
        };
        if pos < self.start || read_end > self.end() {
            return false;
        }
        let mut skip =
            usize::try_from(pos - self.start).expect("an offset in the window fits in a usize");
        let mut filled = 0;
        for chunk in &self.chunks {
            if filled == out.len() {
                break;
            }
            if skip >= chunk.len() {
                skip -= chunk.len();
                continue;
            }
            let take = (chunk.len() - skip).min(out.len() - filled);
            out[filled..filled + take].copy_from_slice(&chunk[skip..skip + take]);
            filled += take;
            skip = 0;
        }
        true
    }

    /// Forget every byte at or past `end`, as a truncate of the segment does.
    ///
    /// When `end` is at or before the window's start, the window becomes
    /// empty and begins at `end`.
    pub(crate) fn truncate(&mut self, end: u64) {
        if end >= self.end() {
            return;
        }
        if end <= self.start {
            self.chunks.clear();
            self.len = 0;
            self.start = end;
            return;
        }
        let keep =
            usize::try_from(end - self.start).expect("an offset in the window fits in a usize");
        let mut kept = 0;
        let mut chunks = 0;
        for chunk in &mut self.chunks {
            if kept == keep {
                break;
            }
            let take = chunk.len().min(keep - kept);
            chunk.truncate(take);
            kept += take;
            chunks += 1;
        }
        self.chunks.truncate(chunks);
        self.len = keep;
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{cell::Cell, os::unix::fs::FileExt};

    use assert2::{assert, check};

    use super::*;

    const MEM: usize = 512;

    /// The bytes `0..len` of a pattern that never repeats inside one block,
    /// tagged by `seed` so two appends differ.
    fn pattern(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| {
                seed.wrapping_mul(31)
                    .wrapping_add(u8::try_from(i % 251).unwrap())
            })
            .collect()
    }

    /// A `write` that checks the `O_DIRECT` rules on each call and then writes
    /// through `write_at`. It records each call's offset and length.
    fn checked_write(
        align: DioAlign,
        calls: &std::cell::RefCell<Vec<(u64, usize)>>,
    ) -> impl FnMut(&File, &[u8], u64) -> std::io::Result<usize> + '_ {
        move |file, buf, offset| {
            assert!(buf.as_ptr().addr() % align.mem == 0);
            assert!(offset % to_u64(align.block) == 0);
            assert!(buf.len() % align.block == 0);
            calls.borrow_mut().push((offset, buf.len()));
            file.write_at(buf, offset)
        }
    }

    fn file_contents(file: &File) -> Vec<u8> {
        let len = usize::try_from(file.metadata().unwrap().len()).unwrap();
        let mut out = vec![0; len];
        file.read_exact_at(&mut out, 0).unwrap();
        out
    }

    /// What the file holds after appends of `logical`: those bytes, then zeros
    /// up to the next block.
    fn padded(logical: &[u8], block: usize) -> Vec<u8> {
        let mut expected = logical.to_vec();
        expected.resize(logical.len().div_ceil(block) * block, 0);
        expected
    }

    fn writer(block: usize) -> DirectWriter {
        let align = DioAlign { mem: MEM, block };
        DirectWriter::new(tempfile::tempfile().unwrap(), align, 0, Vec::new()).unwrap()
    }

    #[test]
    fn appends_of_assorted_sizes_write_aligned_blocks_from_the_tail_start() {
        // Each case: block size, then the size of each append in turn.
        let cases: &[(usize, &[usize])] = &[
            (512, &[1, 10, 100]),
            (512, &[511, 1, 1]),
            (512, &[512, 512, 1024]),
            (512, &[300, 300, 300, 300]),
            (512, &[1500, 7, 2048, 513]),
            (4096, &[1, 4095, 4097, 100]),
            (4096, &[10_000, 3, 8192]),
        ];
        for &(block, sizes) in cases {
            let align = DioAlign { mem: MEM, block };
            let mut writer = writer(block);
            check!(writer.block() == block);
            let mut logical = Vec::new();
            for (seed, &size) in sizes.iter().enumerate() {
                let bytes = pattern(u8::try_from(seed).unwrap(), size);
                let tail_start = writer.end() - writer.end() % to_u64(block);
                let calls = std::cell::RefCell::new(Vec::new());
                writer
                    .append(&[&bytes], checked_write(align, &calls))
                    .unwrap();
                logical.extend_from_slice(&bytes);

                let new_end = to_u64(logical.len());
                let padded_end = new_end.div_ceil(to_u64(block)) * to_u64(block);
                let expected_calls = vec![(
                    tail_start,
                    usize::try_from(padded_end - tail_start).unwrap(),
                )];
                check!(
                    calls.into_inner() == expected_calls,
                    "block {block}, sizes {sizes:?}"
                );
                check!(writer.end() == new_end);
                check!(writer.tail == logical[logical.len() - logical.len() % block..]);
                check!(
                    file_contents(writer.file()) == padded(&logical, block),
                    "block {block}, sizes {sizes:?}"
                );
            }
        }
    }

    #[test]
    fn several_parts_append_as_their_concatenation() {
        let align = DioAlign {
            mem: MEM,
            block: 512,
        };
        let mut writer = writer(512);
        let (a, b, c) = (pattern(1, 100), pattern(2, 0), pattern(3, 700));
        let calls = std::cell::RefCell::new(Vec::new());
        writer
            .append(&[&a, &b, &c], checked_write(align, &calls))
            .unwrap();
        let logical = [a, c].concat();
        check!(writer.end() == 800);
        check!(file_contents(writer.file()) == padded(&logical, 512));
    }

    #[test]
    fn an_empty_append_writes_nothing() {
        let mut writer = writer(512);
        let calls = Cell::new(0);
        let result = writer.append(&[&[], &[]], |_, _, _| {
            calls.set(calls.get() + 1);
            Ok(0)
        });
        check!(result.is_ok());
        check!(calls.get() == 0);
        check!(writer.end() == 0);
    }

    /// A failed append leaves the writer as it was; once the caller truncates
    /// the file back, the next append writes the right bytes.
    #[test]
    fn a_failed_append_leaves_end_and_tail_unchanged() {
        // Each case: how many bytes each scripted call reports before the
        // error, which is the first call with no entry.
        let cases: &[&[usize]] = &[&[], &[512], &[512, 1024]];
        for &script in cases {
            let block = 512;
            let align = DioAlign { mem: MEM, block };
            let mut writer = writer(block);
            let first = pattern(1, 700);
            writer.append(&[&first], FileExt::write_at).unwrap();
            let tail_before = writer.tail.clone();

            let calls = Cell::new(0);
            let failed = writer.append(&[&pattern(2, 3000)], |file, buf, offset| {
                let call = calls.get();
                calls.set(call + 1);
                match script.get(call) {
                    Some(&len) => file.write_at(&buf[..len], offset),
                    None => Err(Error::other("disk on fire")),
                }
            });
            check!(failed.unwrap_err().to_string() == "disk on fire");
            check!(writer.end() == 700);
            check!(writer.tail == tail_before);

            writer.file().set_len(writer.end()).unwrap();
            let next = pattern(3, 900);
            let checked = std::cell::RefCell::new(Vec::new());
            writer
                .append(&[&next], checked_write(align, &checked))
                .unwrap();
            check!(writer.end() == 1600);
            check!(
                file_contents(writer.file()) == padded(&[first.clone(), next].concat(), block),
                "script {script:?}"
            );
        }
    }

    #[test]
    fn an_interrupted_write_is_retried() {
        let mut writer = writer(512);
        let interrupted = Cell::new(false);
        let bytes = pattern(1, 600);
        writer
            .append(&[&bytes], |file, buf, offset| {
                if interrupted.replace(true) {
                    file.write_at(buf, offset)
                } else {
                    Err(Error::from(ErrorKind::Interrupted))
                }
            })
            .unwrap();
        check!(writer.end() == 600);
        check!(file_contents(writer.file()) == padded(&bytes, 512));
    }

    #[test]
    fn a_write_of_zero_bytes_fails_with_write_zero() {
        let mut writer = writer(512);
        let result = writer.append(&[&pattern(1, 10)], |_, _, _| Ok(0));
        check!(result.unwrap_err().kind() == ErrorKind::WriteZero);
        check!(writer.end() == 0);
        check!(writer.tail.is_empty());
    }

    /// A short write that stops inside a block goes on from the start of that
    /// block, so every call stays aligned.
    #[test]
    fn a_short_unaligned_write_resumes_from_an_aligned_position() {
        let block = 512;
        let align = DioAlign { mem: MEM, block };
        let mut writer = writer(block);
        let bytes = pattern(1, 1500);
        let calls = std::cell::RefCell::new(Vec::new());
        let mut checked = checked_write(align, &calls);
        let first = Cell::new(true);
        writer
            .append(&[&bytes], |file, buf, offset| {
                let n = checked(file, buf, offset)?;
                Ok(if first.replace(false) { n.min(700) } else { n })
            })
            .unwrap();
        check!(*calls.borrow() == vec![(0, 1536), (512, 1024)]);
        check!(file_contents(writer.file()) == padded(&bytes, block));
    }

    #[test]
    fn a_short_write_with_no_aligned_progress_fails_with_write_zero() {
        let mut writer = writer(512);
        let result = writer.append(&[&pattern(1, 1000)], |_, _, _| Ok(100));
        check!(result.unwrap_err().kind() == ErrorKind::WriteZero);
        check!(writer.end() == 0);
    }

    #[test]
    fn reset_after_a_truncate_continues_from_the_new_end() {
        let block = 512;
        let align = DioAlign { mem: MEM, block };
        let mut writer = writer(block);
        let first = pattern(1, 1300);
        writer.append(&[&first], FileExt::write_at).unwrap();

        writer.file().set_len(900).unwrap();
        writer.reset(900, first[512..900].to_vec()).unwrap();
        check!(writer.end() == 900);

        let next = pattern(2, 200);
        let calls = std::cell::RefCell::new(Vec::new());
        writer
            .append(&[&next], checked_write(align, &calls))
            .unwrap();
        check!(calls.into_inner() == vec![(512, 1024)]);
        check!(writer.end() == 1100);
        check!(file_contents(writer.file()) == padded(&[&first[..900], &next[..]].concat(), block));
    }

    #[test]
    fn new_and_reset_reject_a_tail_of_the_wrong_length() {
        let align = DioAlign {
            mem: MEM,
            block: 512,
        };
        // Each case: the end, the tail length, and whether they agree.
        let cases = [
            (0, 0, true),
            (0, 1, false),
            (512, 0, true),
            (512, 512, false),
            (700, 188, true),
            (700, 187, false),
            (700, 700, false),
        ];
        for (end, tail_len, valid) in cases {
            let created =
                DirectWriter::new(tempfile::tempfile().unwrap(), align, end, vec![0; tail_len]);
            check!(created.is_ok() == valid, "new: end {end}, tail {tail_len}");
            if let Err(error) = created {
                check!(error.kind() == ErrorKind::InvalidInput);
            }

            let mut writer = writer(512);
            let reset = writer.reset(end, vec![0; tail_len]);
            check!(reset.is_ok() == valid, "reset: end {end}, tail {tail_len}");
            if let Err(error) = reset {
                check!(error.kind() == ErrorKind::InvalidInput);
                check!(writer.end() == 0);
            }
        }
    }

    #[test]
    fn new_rejects_an_alignment_that_is_not_a_power_of_two() {
        let cases = [
            DioAlign { mem: 0, block: 512 },
            DioAlign { mem: 512, block: 0 },
            DioAlign {
                mem: 512,
                block: 1000,
            },
            DioAlign { mem: 3, block: 512 },
        ];
        for align in cases {
            let created = DirectWriter::new(tempfile::tempfile().unwrap(), align, 0, Vec::new());
            check!(
                created.err().map(|error| error.kind()) == Some(ErrorKind::InvalidInput),
                "{align:?}"
            );
        }
    }

    #[test]
    fn aligned_buf_stays_aligned_and_keeps_its_contents_as_it_grows() {
        for align in [1, 2, 8, 64, 512, 4096] {
            let mut buf = AlignedBuf::new(align);
            check!(buf.as_slice().is_empty());
            let mut expected = Vec::new();
            for (seed, size) in [3, 100, 1000, 5000, 20_000].into_iter().enumerate() {
                let bytes = pattern(u8::try_from(seed).unwrap(), size);
                buf.extend_from_slice(&bytes);
                expected.extend_from_slice(&bytes);
                check!(buf.as_slice().as_ptr().addr() % align == 0, "align {align}");
                check!(buf.as_slice().len() == expected.len());
                check!(buf.as_slice() == &expected[..], "align {align}");
            }
        }
    }

    #[test]
    fn aligned_buf_resize_zero_fills_over_old_contents() {
        let mut buf = AlignedBuf::new(512);
        buf.extend_from_slice(&[7; 100]);
        buf.clear();
        check!(buf.as_slice().is_empty());
        buf.extend_from_slice(&[1; 10]);
        buf.resize(40);
        check!(buf.as_slice() == &[[1; 10].as_slice(), &[0; 30]].concat()[..]);
        buf.resize(5);
        check!(buf.as_slice() == &[1; 5]);
        buf.resize(3000);
        check!(buf.as_slice() == &[[1; 5].as_slice(), &[0; 2995]].concat()[..]);
        check!(buf.as_slice().as_ptr().addr() % 512 == 0);
    }

    /// A window that starts at 1000 and holds chunks of 10, 20 and 30 bytes.
    fn three_chunks(cap: usize) -> (TailCache, Vec<u8>) {
        let mut cache = TailCache::new(cap, 1000);
        let mut all = Vec::new();
        for (seed, size) in [(1, 10), (2, 20), (3, 30)] {
            let bytes = pattern(seed, size);
            all.extend_from_slice(&bytes);
            cache.push(Bytes::from(bytes));
        }
        (cache, all)
    }

    fn read(cache: &TailCache, pos: u64, len: usize) -> Option<Vec<u8>> {
        let mut out = vec![0xAA; len];
        if cache.read(pos, &mut out) {
            Some(out)
        } else {
            check!(
                out == vec![0xAA; len],
                "a failed read leaves the buffer alone"
            );
            None
        }
    }

    #[test]
    fn tail_cache_reads_across_chunk_boundaries() {
        let (cache, all) = three_chunks(1 << 20);
        check!((cache.start(), cache.end()) == (1000, 1060));
        // Each case: the position, the length, and the expected window slice.
        let cases: &[(u64, usize, Option<std::ops::Range<usize>>)] = &[
            (1000, 60, Some(0..60)),
            (1000, 10, Some(0..10)),
            (1005, 10, Some(5..15)),
            (1009, 30, Some(9..39)),
            (1030, 30, Some(30..60)),
            (1059, 1, Some(59..60)),
            (1060, 0, Some(60..60)),
            (1000, 0, Some(0..0)),
            (999, 1, None),
            (999, 0, None),
            (1050, 11, None),
            (1061, 0, None),
            (2000, 5, None),
            (u64::MAX, 1, None),
        ];
        for (pos, len, range) in cases.iter().cloned() {
            check!(
                read(&cache, pos, len) == range.map(|range| all[range].to_vec()),
                "pos {pos}, len {len}"
            );
        }
    }

    #[test]
    fn tail_cache_evicts_the_oldest_chunks_past_the_cap() {
        // Each case: the cap, and the start of the window after the three
        // pushes.
        let cases = [
            (1000, 1000),
            (60, 1000),
            (59, 1010),
            (50, 1010),
            (30, 1030),
            (29, 1030),
            (0, 1030),
        ];
        for (cap, start) in cases {
            let (cache, all) = three_chunks(cap);
            check!((cache.start(), cache.end()) == (start, 1060), "cap {cap}");
            check!(
                read(&cache, start, 1060 - usize::try_from(start).unwrap())
                    == Some(all[usize::try_from(start - 1000).unwrap()..].to_vec())
            );
            check!(read(&cache, start - 1, 1).is_none());
        }
    }

    /// A coalesced write leaves the same window as pushing its batches one
    /// by one, and no batch left in the window shares the group's allocation,
    /// so the window still holds at most `cap` bytes (or the newest batch).
    #[test]
    fn tail_cache_group_push_matches_single_pushes_and_detaches_survivors() {
        for cap in [1000, 60, 59, 50, 30, 29, 0] {
            let (single, all) = three_chunks(cap);
            let group = Bytes::from(all.clone());
            let mut grouped = TailCache::new(cap, 1000);
            grouped.push_group(&group, &[10, 30, 60]);
            check!(
                (grouped.start(), grouped.end()) == (single.start(), single.end()),
                "cap {cap}"
            );
            let start = single.start();
            let len = 1060 - usize::try_from(start).unwrap();
            check!(
                read(&grouped, start, len) == read(&single, start, len),
                "cap {cap}"
            );
            let group_range = group.as_ptr().addr()..group.as_ptr().addr() + group.len();
            for chunk in &grouped.chunks {
                check!(
                    !group_range.contains(&chunk.as_ptr().addr()),
                    "cap {cap}: a retained batch still points into the group"
                );
            }
        }
    }

    #[test]
    fn tail_cache_ignores_an_empty_push() {
        let mut cache = TailCache::new(0, 5);
        cache.push(Bytes::from_static(b"abc"));
        cache.push(Bytes::new());
        check!((cache.start(), cache.end()) == (5, 8));
        check!(read(&cache, 5, 3) == Some(b"abc".to_vec()));
    }

    #[test]
    fn tail_cache_truncate_forgets_bytes_past_the_new_end() {
        // Each case: the truncate point, then the window it leaves.
        let cases = [
            (2000, 1000, 1060),
            (1060, 1000, 1060),
            (1045, 1000, 1045),
            (1030, 1000, 1030),
            (1010, 1000, 1010),
            (1003, 1000, 1003),
            (1000, 1000, 1000),
            (900, 900, 900),
        ];
        for (end, start, new_end) in cases {
            let (mut cache, all) = three_chunks(1 << 20);
            cache.truncate(end);
            check!(
                (cache.start(), cache.end()) == (start, new_end),
                "truncate to {end}"
            );
            let kept = usize::try_from(new_end - start).unwrap();
            let from = usize::try_from(start.saturating_sub(1000)).unwrap();
            check!(read(&cache, start, kept) == Some(all[from..from + kept].to_vec()));
            check!(read(&cache, new_end, 1).is_none());

            // The window grows again from the new end.
            cache.push(Bytes::from_static(b"xyz"));
            check!(cache.end() == new_end + 3);
            check!(read(&cache, new_end, 3) == Some(b"xyz".to_vec()));
        }
    }
}
