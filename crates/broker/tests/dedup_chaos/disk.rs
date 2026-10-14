//! The damage a crash leaves in a partition's newest segment, applied while
//! the broker is down and before its next boot recovers the log.

use std::{
    ops::Range,
    path::{Path, PathBuf},
};

/// What happens to the last batch of the segment.
#[derive(Clone, Copy, Debug)]
pub enum Tail {
    /// Its first half is written again behind it: a header that promises
    /// more bytes than follow it.
    Tear,
    /// It is cut in half, so the replica loses it.
    Cut,
}

/// Damage the newest segment of `partition_dir`. Returns what was done, or
/// why nothing was: a partition whose newest segment holds no batch yet.
pub fn damage(partition_dir: &Path, tail: Tail) -> String {
    let Some(segment) = newest_segment(partition_dir) else {
        return format!("no segment in {}", partition_dir.display());
    };
    let bytes = std::fs::read(&segment)
        .unwrap_or_else(|error| panic!("read {}: {error}", segment.display()));
    let Some(last) = batch_ranges(&bytes).pop() else {
        return format!("no batch in {}", segment.display());
    };
    let half = last.start + (last.end - last.start) / 2;
    match tail {
        Tail::Tear => {
            let torn = [&bytes[..last.end], &bytes[last.start..half]].concat();
            std::fs::write(&segment, torn)
                .unwrap_or_else(|error| panic!("tear {}: {error}", segment.display()));
            format!(
                "tore the batch at byte {} of {}",
                last.start,
                segment.display()
            )
        }
        Tail::Cut => {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&segment)
                .unwrap_or_else(|error| panic!("open {}: {error}", segment.display()));
            file.set_len(u64::try_from(half).expect("a segment length"))
                .unwrap_or_else(|error| panic!("cut {}: {error}", segment.display()));
            format!(
                "cut the batch at byte {} of {}",
                last.start,
                segment.display()
            )
        }
    }
}

/// The `.log` file of `partition_dir` with the highest base offset.
fn newest_segment(partition_dir: &Path) -> Option<PathBuf> {
    std::fs::read_dir(partition_dir)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "log"))
        .max()
}

/// The byte range of each whole batch at the start of a segment, by the
/// length each batch states after its eight-byte base offset. A length that
/// runs past the end of the file ends the walk.
fn batch_ranges(segment: &[u8]) -> Vec<Range<usize>> {
    let mut ranges: Vec<Range<usize>> = Vec::new();
    loop {
        let start = ranges.last().map_or(0, |range| range.end);
        let Some(stated) = segment
            .get(start + 8..start + 12)
            .and_then(|field| <[u8; 4]>::try_from(field).ok())
            .and_then(|field| usize::try_from(u32::from_be_bytes(field)).ok())
        else {
            return ranges;
        };
        let end = start + 12 + stated;
        if end > segment.len() {
            return ranges;
        }
        ranges.push(start..end);
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// A fake batch: a base offset, the stated length, and that many bytes.
    fn batch(fill: u8, body: usize) -> Vec<u8> {
        let mut bytes = vec![0; 8];
        bytes.extend(u32::try_from(body).expect("a body length").to_be_bytes());
        bytes.extend(std::iter::repeat_n(fill, body));
        bytes
    }

    #[test]
    fn the_walk_stops_at_the_first_incomplete_batch() {
        let whole = [batch(1, 20), batch(2, 30)].concat();
        let torn = [&whole[..], &batch(3, 40)[..10]].concat();
        assert!(batch_ranges(&whole) == [0..32, 32..74]);
        assert!(batch_ranges(&torn) == [0..32, 32..74]);
        assert!(batch_ranges(&[]).is_empty());
    }

    #[test]
    fn a_tear_keeps_every_batch_and_a_cut_loses_the_last() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let segment = dir.path().join("00000000000000000000.log");
        let whole = [batch(1, 20), batch(2, 30)].concat();
        // The last batch is bytes 32..74, so its first half is 21 bytes.
        let cases = [
            (Tail::Tear, vec![32, 74], 74 + 21),
            (Tail::Cut, vec![32], 32 + 21),
        ];
        for (tail, ends, len) in cases {
            std::fs::write(&segment, &whole).expect("write the segment");
            damage(dir.path(), tail);
            let damaged = std::fs::read(&segment).expect("read the segment");
            let kept: Vec<usize> = batch_ranges(&damaged)
                .iter()
                .map(|range| range.end)
                .collect();
            assert!(kept == ends, "{tail:?}");
            assert!(damaged.len() == len, "{tail:?}");
        }
    }
}
