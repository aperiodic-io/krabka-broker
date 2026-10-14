//! The soak process's own memory and descriptors, as Linux reports them.
//!
//! The brokers run inside the soak process, so its resident set and its
//! descriptor table are theirs, plus a test harness that stays the same size
//! for the whole run.
//!
//! The descriptors are split three ways, because they move for different
//! reasons. Segment files (`.log`, `.index`, `.timeindex` and the other files
//! named after a segment's base offset) come and go as segments roll and
//! retention deletes them, so their count swings by hundreds within a cycle
//! and is bounded by the retention settings, not by time. Every other
//! descriptor -- sockets, pipes, the metadata and audit sidecars -- should
//! not move at all once the cluster is up. And a file that is open after it
//! was deleted is a handle that outlived its owner: its disk space is not
//! given back until the handle closes.

use std::{fmt, fs, path::Path};

/// The open descriptors of the process, by kind.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Descriptors {
    /// Every open descriptor.
    pub total: u64,
    /// Descriptors of segment files, deleted or not.
    pub segments: u64,
    /// Descriptors of files that were deleted while open.
    pub deleted: u64,
}

impl Descriptors {
    /// Every descriptor that is not a segment file.
    pub fn other(self) -> u64 { self.total - self.segments }

    /// Count the descriptors whose targets `/proc/self/fd` links to.
    fn of<'a>(targets: impl IntoIterator<Item = &'a str>) -> Self {
        let mut descriptors = Self::default();
        for target in targets {
            descriptors.total += 1;
            let (path, deleted) = match target.strip_suffix(" (deleted)") {
                Some(path) => (path, true),
                None => (target, false),
            };
            descriptors.deleted += u64::from(deleted);
            descriptors.segments += u64::from(is_segment_file(path));
        }
        descriptors
    }
}

/// The three counts as comma-separated columns: total, segment files,
/// deleted files.
impl fmt::Display for Descriptors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{},{},{}", self.total, self.segments, self.deleted)
    }
}

/// Whether `path` names a file of a log segment: its name starts with the
/// segment's base offset, twenty digits, and an extension.
fn is_segment_file(path: &str) -> bool {
    let name = Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    name.len() > 21
        && name.as_bytes()[..20].iter().all(u8::is_ascii_digit)
        && name.as_bytes()[20] == b'.'
}

/// One reading of `/proc/self/status` and `/proc/self/fd`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessSample {
    /// `VmRSS`: the resident set, anonymous and file-backed.
    pub rss_kib: u64,
    /// `VmHWM`: the highest resident set since the peak was last reset.
    pub peak_rss_kib: u64,
    /// `RssAnon`: the anonymous part of the resident set, the heap and the
    /// thread stacks, which is where a leak in the index would show.
    pub anon_kib: u64,
    pub fds: Descriptors,
    /// `Threads`: every thread adds its stack and, without
    /// `MALLOC_ARENA_MAX=1`, can get an allocator arena of its own.
    pub threads: u64,
}

impl ProcessSample {
    pub fn read() -> Self {
        let status = fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
        let targets: Vec<String> = fs::read_dir("/proc/self/fd")
            .expect("list /proc/self/fd")
            .filter_map(Result::ok)
            .filter_map(|entry| fs::read_link(entry.path()).ok())
            .map(|target| target.to_string_lossy().into_owned())
            .collect();
        Self::from_status(&status, Descriptors::of(targets.iter().map(String::as_str)))
    }

    fn from_status(status: &str, fds: Descriptors) -> Self {
        Self {
            rss_kib: status_kib(status, "VmRSS"),
            peak_rss_kib: status_kib(status, "VmHWM"),
            anon_kib: status_kib(status, "RssAnon"),
            fds,
            threads: status_value(status, "Threads"),
        }
    }
}

/// The text of one `Name:\tvalue` field of a `/proc/<pid>/status` text.
fn status_field<'a>(status: &'a str, name: &str) -> &'a str {
    status
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(field, _)| *field == name)
        .map_or_else(
            || panic!("no {name} in /proc/self/status"),
            |(_, value)| value.trim(),
        )
}

/// One `Name:   1234 kB` field of a `/proc/<pid>/status` text.
fn status_kib(status: &str, name: &str) -> u64 {
    let value = status_field(status, name);
    value
        .strip_suffix(" kB")
        .and_then(|kib| kib.trim().parse().ok())
        .unwrap_or_else(|| panic!("{name} is not a size in kB: {value}"))
}

/// One `Name:   1234` field of a `/proc/<pid>/status` text.
fn status_value(status: &str, name: &str) -> u64 {
    let value = status_field(status, name);
    value
        .parse()
        .unwrap_or_else(|_| panic!("{name} is not a count: {value}"))
}

/// Reset `VmHWM` to the current resident set, so that the next reading of it
/// is the peak of what follows.
pub fn reset_peak() { fs::write("/proc/self/clear_refs", "5").expect("reset VmHWM"); }

mod tests {
    use {super::*, assert2::assert};

    const STATUS: &str = "Name:\tdedup_soak\nVmPeak:\t 9000000 kB\nVmHWM:\t  812345 kB\nVmRSS:\t  \
                          700001 kB\nRssAnon:\t  650000 kB\nRssFile:\t   50001 kB\nThreads:\t42\n";

    #[test]
    fn the_status_fields_are_read_by_name() {
        let fds = Descriptors {
            total: 17,
            segments: 9,
            deleted: 1,
        };
        assert!(
            ProcessSample::from_status(STATUS, fds)
                == ProcessSample {
                    rss_kib: 700_001,
                    peak_rss_kib: 812_345,
                    anon_kib: 650_000,
                    fds,
                    threads: 42,
                }
        );
    }

    #[test]
    #[should_panic(expected = "no RssAnon in /proc/self/status")]
    fn a_missing_field_is_an_error() { status_kib("VmRSS:\t1 kB\n", "RssAnon"); }

    #[test]
    fn descriptors_are_told_apart_by_their_targets() {
        let targets = [
            "socket:[1234]",
            "pipe:[99]",
            "/tmp/.tmpA/t-0/00000000000000000000.log",
            "/tmp/.tmpA/t-0/00000000000000000100.timeindex",
            "/tmp/.tmpA/t-0/00000000000000000100.index.37.deleted (deleted)",
            "/tmp/.tmpA/audit-spool/audit.spool",
            "/tmp/.tmpA/notes.txt (deleted)",
            "/tmp/.tmpA/t-0/0000.log",
        ];
        let fds = Descriptors::of(targets);
        assert!(
            fds == Descriptors {
                total: 8,
                segments: 3,
                deleted: 2,
            }
        );
        assert!(fds.other() == 5);
        assert!(fds.to_string() == "8,3,2");
    }

    #[test]
    fn this_process_reads_back_a_resident_set_and_descriptors() {
        let sample = ProcessSample::read();
        assert!(sample.rss_kib > 0);
        assert!(sample.peak_rss_kib >= sample.rss_kib);
        assert!(sample.anon_kib <= sample.rss_kib);
        assert!(sample.fds.total > 0);
        assert!(sample.threads > 0);
    }
}
