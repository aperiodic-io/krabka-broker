//! Kafka's `meta.properties`: the file that marks a formatted log directory
//! and records the identity of the node and of the directory.
//!
//! `kafka-storage format` writes the file into every directory it formats,
//! and the broker reads it from every directory on every boot. Kafka's tools
//! read it too: `kafka-metadata-quorum add-controller` takes the directory id
//! of the new controller from the `meta.properties` in its
//! `metadata.log.dir`. krabka writes and reads the same file, so a directory
//! that one side formats is a formatted directory to the other side.
//!
//! The file is a Java properties file, written by `Properties.store` through
//! Kafka's `PropertiesUtils.writePropertiesFile`. A `KRaft` node writes
//! version 1 (`MetaPropertiesVersion.V1`):
//!
//! ```text
//! #
//! #Thu Feb 29 12:34:56 UTC 2024
//! cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
//! directory.id=U-hCdTzaSPyFB3swwc7Qdw
//! node.id=1
//! version=1
//! ```
//!
//! The first line is the empty comment that `writePropertiesFile` passes to
//! `store`. The second line is the time of the write, in the form of
//! `java.util.Date.toString`. The keys follow in their natural order, which
//! is the order `Properties.store` uses on Java 18 and later. [`MetaProperties`]
//! writes the same bytes, with the time in UTC. It reads what
//! `Properties.load` reads, so it reads the file of every Java version.

use {
    crate::ids::{ClusterId, DirectoryId, KafkaUuidError},
    std::{
        collections::HashMap,
        fs,
        io::{self, Write as _},
        path::Path,
        time::{SystemTime, UNIX_EPOCH},
    },
    thiserror::Error,
    time::OffsetDateTime,
};

/// The file name, Kafka's `MetaPropertiesEnsemble.META_PROPERTIES_NAME`.
pub const META_PROPERTIES: &str = "meta.properties";

/// The name of the file while it is written, before the rename that
/// publishes it. `PropertiesUtils.writePropertiesFile` adds `.tmp`.
pub const META_PROPERTIES_TMP: &str = "meta.properties.tmp";

/// The version of the file that a `KRaft` node writes,
/// `MetaPropertiesVersion.V1`.
pub const META_PROPERTIES_VERSION: i32 = 1;

/// The property names, from Kafka's `MetaProperties`.
const VERSION_PROP: &str = "version";
const CLUSTER_ID_PROP: &str = "cluster.id";
const NODE_ID_PROP: &str = "node.id";
const DIRECTORY_ID_PROP: &str = "directory.id";

/// The content of a version 1 `meta.properties`.
///
/// Kafka's `MetaProperties` keeps the cluster id as a string. krabka keeps it
/// as 16 bytes, so a cluster id that is not a Kafka `Uuid` does not read.
/// The node id is Kafka's `int`. The directory id is optional when the file
/// is read, as in Kafka: a broker gives a directory without one an id at its
/// next start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaProperties {
    /// The cluster that the directory belongs to.
    pub cluster_id: ClusterId,
    /// The node that the directory belongs to, Kafka's `node.id`.
    pub node_id: i32,
    /// The directory's own id (KIP-858).
    pub directory_id: Option<DirectoryId>,
}

/// Why a `meta.properties` does not read.
///
/// Each message is the message of the exception that Kafka's
/// `PropertiesUtils.readPropertiesFile` and `MetaProperties.Builder` throw
/// for the same file, except where a variant says otherwise.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MetaPropertiesError {
    /// The file is there, but it cannot be read.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// A `\u` escape is not followed by four hexadecimal digits.
    #[error("Malformed \\uxxxx encoding.")]
    MalformedEscape,

    /// `version` is not a number.
    #[error("Invalid meta.properties version string '{0}'")]
    InvalidVersion(String),

    /// `version` is a number that Kafka does not define.
    #[error("Unknown meta.properties version number {0}")]
    UnknownVersion(i32),

    /// The file is version 0, which only a `ZooKeeper` broker writes. Kafka
    /// reads it. krabka does not: it is a krabka message.
    #[error(
        "Unsupported meta.properties version 0: krabka reads version 1, which a KRaft node writes"
    )]
    VersionZero,

    /// The file has no `node.id`.
    #[error("Failed to find node.id")]
    NoNodeId,

    /// `node.id` is not an `int`.
    #[error("Unable to read node.id as a base-10 number.")]
    NodeIdNotANumber,

    /// The file has no `cluster.id`.
    #[error("cluster.id was not found.")]
    NoClusterId,

    /// `cluster.id` is not a Kafka `Uuid`. Kafka accepts any string there, so
    /// this is a krabka message, in the form of Kafka's message for
    /// `directory.id`.
    #[error("Unable to read cluster.id as a Uuid: {0}")]
    ClusterIdNotAUuid(KafkaUuidError),

    /// `directory.id` is not a Kafka `Uuid`.
    #[error("Unable to read directory.id as a Uuid: {0}")]
    DirectoryIdNotAUuid(KafkaUuidError),
}

impl MetaProperties {
    /// Reads `<dir>/meta.properties`. Returns `None` when there is no such
    /// file, which is how Kafka's `MetaPropertiesEnsemble.Loader` finds an
    /// empty directory.
    ///
    /// # Errors
    ///
    /// Returns [`MetaPropertiesError::Io`] when the file is there but cannot
    /// be read, and another variant when it does not parse.
    pub fn read(dir: &Path) -> Result<Option<Self>, MetaPropertiesError> {
        match fs::read(dir.join(META_PROPERTIES)) {
            Ok(bytes) => Self::parse(&bytes).map(Some),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Parses the bytes of a `meta.properties`, as Kafka's
    /// `new MetaProperties.Builder(props).build()` does after
    /// `Properties.load`.
    ///
    /// The checks run in Kafka's order, so a file with several faults is
    /// reported with Kafka's first message.
    ///
    /// # Errors
    ///
    /// Returns the [`MetaPropertiesError`] for the first fault.
    pub fn parse(bytes: &[u8]) -> Result<Self, MetaPropertiesError> {
        let props = load_properties(bytes)?;
        let version = props.get(VERSION_PROP).map_or("0", String::as_str);
        // `MetaPropertiesVersion.fromNumberString` trims as `String.trim`
        // does, and `Integer.parseInt` does the rest.
        let number: i32 = version
            .trim_matches(|c: char| c <= ' ')
            .parse()
            .map_err(|_| MetaPropertiesError::InvalidVersion(version.to_owned()))?;
        match number {
            META_PROPERTIES_VERSION => {}
            0 => return Err(MetaPropertiesError::VersionZero),
            other => return Err(MetaPropertiesError::UnknownVersion(other)),
        }
        let node_id: i32 = props
            .get(NODE_ID_PROP)
            .ok_or(MetaPropertiesError::NoNodeId)?
            .parse()
            .map_err(|_| MetaPropertiesError::NodeIdNotANumber)?;
        let cluster_id = props.get(CLUSTER_ID_PROP);
        let directory_id: Option<DirectoryId> = props
            .get(DIRECTORY_ID_PROP)
            .map(|id| id.parse().map_err(MetaPropertiesError::DirectoryIdNotAUuid))
            .transpose()?;
        let cluster_id: ClusterId = cluster_id
            .ok_or(MetaPropertiesError::NoClusterId)?
            .parse()
            .map_err(MetaPropertiesError::ClusterIdNotAUuid)?;
        Ok(Self {
            cluster_id,
            node_id,
            directory_id,
        })
    }

    /// The text of the file that Kafka's `writePropertiesFile` writes for
    /// these ids at `now`, with `now` in UTC.
    ///
    /// No value needs an escape: a node id is digits and a sign, and Kafka's
    /// `Uuid` form uses only letters, digits, `-` and `_`, which
    /// `Properties.store` writes as they are.
    #[must_use]
    pub fn to_file_text(&self, now: SystemTime) -> String {
        let directory_id = self
            .directory_id
            .map_or_else(String::new, |id| format!("{DIRECTORY_ID_PROP}={id}\n"));
        format!(
            "#\n#{}\n{CLUSTER_ID_PROP}={}\n{directory_id}{NODE_ID_PROP}={}\n\
             {VERSION_PROP}={META_PROPERTIES_VERSION}\n",
            java_date(now),
            self.cluster_id,
            self.node_id,
        )
    }

    /// Writes `<dir>/meta.properties` as Kafka's `writePropertiesFile` does
    /// with `fsync` set: the text goes to `meta.properties.tmp`, which is
    /// synced and renamed into place, and then, on Unix, the directory is
    /// synced. A write that stops partway therefore never leaves a truncated
    /// file.
    ///
    /// # Errors
    ///
    /// Returns the first I/O error. A rename that fails removes the
    /// temporary file, as Kafka does.
    pub fn write(&self, dir: &Path) -> io::Result<()> { self.write_observed(dir, |_| Ok(())) }

    /// [`Self::write`], calling `after` with the path of each file once it
    /// is written: the temporary file, then `meta.properties`. An error from
    /// `after` stops the write there, which is how `krabka-format` injects a
    /// fault for its tests.
    pub(crate) fn write_observed(
        &self,
        dir: &Path,
        mut after: impl FnMut(&Path) -> io::Result<()>,
    ) -> io::Result<()> {
        let text = self.to_file_text(SystemTime::now());
        let tmp = dir.join(META_PROPERTIES_TMP);
        let mut file = fs::File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        after(&tmp)?;
        let path = dir.join(META_PROPERTIES);
        if let Err(error) = fs::rename(&tmp, &path) {
            // Kafka deletes the temporary file and reports the rename.
            let _ = fs::remove_file(&tmp);
            return Err(error);
        }
        // Syncing the directory is what makes the rename durable, and only
        // Unix supports it through `std`: elsewhere, `wasm32-wasip1`
        // included, opening or syncing a directory fails. The sync is
        // skipped there, as `krabka-log` skips its own directory syncs.
        #[cfg(unix)]
        fs::File::open(dir)?.sync_all()?;
        after(&path)
    }
}

#[bon::builder]
pub(crate) fn node_id_mismatch_message(stored: i32, previous: i32, path: &Path) -> String {
    format!(
        "Stored node id {stored} doesn't match previous node id {previous} in {}. If you \
         moved your data, make sure your configured node id matches. If you intend \
         to create a new node, you should remove all data in your data directories.",
        path.display(),
    )
}

/// Kafka's `MetaPropertiesEnsemble.verify`, over the readable files of the
/// directories of one node, sorted by path.
///
/// The set agrees on one cluster id and one node id, and no two directories
/// share a directory id, and no directory id is reserved. `expected_cluster_id`
/// and `expected_node_id` are the ids that the set has to agree with, when
/// they are known. Returns the cluster id of the set: `expected_cluster_id`
/// when given, else the id of the first file, or `None` for an empty set.
///
/// # Errors
///
/// Returns Kafka's message, word for word, for the first disagreement.
pub fn verify_ensemble<'a>(
    formatted: impl IntoIterator<Item = (&'a Path, &'a MetaProperties)>,
    expected_cluster_id: Option<ClusterId>,
    expected_node_id: Option<i32>,
) -> Result<Option<ClusterId>, String> {
    let mut cluster_id = expected_cluster_id;
    let mut node_id = expected_node_id;
    let mut seen: HashMap<DirectoryId, &Path> = HashMap::new();
    for (dir, meta) in formatted {
        let path = dir.join(META_PROPERTIES);
        match cluster_id {
            None => cluster_id = Some(meta.cluster_id),
            Some(expected) if expected != meta.cluster_id => {
                return Err(format!(
                    "Invalid cluster.id in: {}. Expected {expected}, but read {}",
                    path.display(),
                    meta.cluster_id,
                ));
            }
            Some(_) => {}
        }
        match node_id {
            None => node_id = Some(meta.node_id),
            Some(expected) if expected != meta.node_id => {
                return Err(node_id_mismatch_message()
                    .stored(meta.node_id)
                    .previous(expected)
                    .path(&path)
                    .call());
            }
            Some(_) => {}
        }
        if let Some(directory_id) = meta.directory_id {
            if directory_id.is_reserved() {
                return Err(format!(
                    "Invalid reserved directory ID {directory_id} found in {}",
                    dir.display(),
                ));
            }
            if let Some(previous) = seen.insert(directory_id, dir) {
                // Kafka prints the `Optional` wrapper of the id here.
                return Err(format!(
                    "Duplicate directory ID Optional[{directory_id}] found. It was the ID of {}, \
                     but also of {}",
                    previous.display(),
                    dir.display(),
                ));
            }
        }
    }
    Ok(cluster_id)
}

/// Kafka's `Properties.load(InputStream)`: the key and value of each entry.
/// A key that appears twice keeps its last value.
///
/// `load(InputStream)` reads ISO 8859-1, so each byte is the character of
/// the same value. An escape `\uXXXX` gives a UTF-16 code unit.
fn load_properties(bytes: &[u8]) -> Result<HashMap<String, String>, MetaPropertiesError> {
    let mut props = HashMap::new();
    for line in logical_lines(bytes) {
        let RawEntry { key, value } = split_entry(&line);
        props.insert(unescape(key)?, unescape(value)?);
    }
    Ok(props)
}

/// The white space that `Properties.load` skips.
fn is_white_space(byte: u8) -> bool { matches!(byte, b' ' | b'\t' | b'\x0c') }

fn is_line_end(byte: u8) -> bool { matches!(byte, b'\n' | b'\r') }

/// The logical lines of a properties file, as `Properties.LineReader` reads
/// them.
///
/// A natural line ends at `\n`, `\r`, or `\r\n`. White space at the start of
/// a line is skipped, and a blank line is skipped. A line whose first other
/// character is `#` or `!` is a comment. A line that ends in an odd number of
/// backslashes goes on in the next line: the last backslash is dropped, and
/// so is the white space at the start of the next line.
fn logical_lines(input: &[u8]) -> Vec<Vec<u8>> {
    let mut lines = Vec::new();
    let mut line = Vec::new();
    let mut skip_white_space = true;
    let mut appended_line_begin = false;
    let mut preceding_backslash = false;
    let mut skip_lf = false;
    let mut bytes = input.iter().copied();
    while let Some(byte) = bytes.next() {
        if skip_lf {
            skip_lf = false;
            if byte == b'\n' {
                continue;
            }
        }
        if skip_white_space {
            if is_white_space(byte) || (!appended_line_begin && is_line_end(byte)) {
                continue;
            }
            skip_white_space = false;
            appended_line_begin = false;
        }
        if line.is_empty() && matches!(byte, b'#' | b'!') {
            if !bytes.by_ref().any(is_line_end) {
                break;
            }
            skip_white_space = true;
            continue;
        }
        if !is_line_end(byte) {
            line.push(byte);
            preceding_backslash = byte == b'\\' && !preceding_backslash;
        } else if line.is_empty() {
            skip_white_space = true;
        } else if preceding_backslash {
            line.pop();
            skip_white_space = true;
            appended_line_begin = true;
            preceding_backslash = false;
            skip_lf = byte == b'\r';
        } else {
            lines.push(std::mem::take(&mut line));
            skip_white_space = true;
        }
    }
    if !line.is_empty() {
        if preceding_backslash {
            line.pop();
        }
        lines.push(line);
    }
    lines
}

/// The raw key and value of one logical line, before their escapes resolve.
struct RawEntry<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

/// Splits a logical line into its key and its value, as `Properties.load0`
/// does.
///
/// The key ends at the first `=`, `:`, or white space that no backslash
/// escapes. The value starts after the white space that follows, with at
/// most one `=` or `:` among it.
fn split_entry(line: &[u8]) -> RawEntry<'_> {
    let mut key_len = 0;
    let mut value_start = line.len();
    let mut has_separator = false;
    let mut preceding_backslash = false;
    while let Some(&byte) = line.get(key_len) {
        if !preceding_backslash && matches!(byte, b'=' | b':') {
            value_start = key_len + 1;
            has_separator = true;
            break;
        }
        if !preceding_backslash && is_white_space(byte) {
            value_start = key_len + 1;
            break;
        }
        preceding_backslash = byte == b'\\' && !preceding_backslash;
        key_len += 1;
    }
    while let Some(&byte) = line.get(value_start) {
        if !is_white_space(byte) {
            if !has_separator && matches!(byte, b'=' | b':') {
                has_separator = true;
            } else {
                break;
            }
        }
        value_start += 1;
    }
    RawEntry {
        key: &line[..key_len],
        value: &line[value_start..],
    }
}

/// Resolves the escapes of a key or a value, as `Properties.loadConvert`
/// does: `\t`, `\r`, `\n`, `\f`, and `\uXXXX` are the characters they name,
/// and a backslash before any other character is dropped.
fn unescape(raw: &[u8]) -> Result<String, MetaPropertiesError> {
    let mut units: Vec<u16> = Vec::with_capacity(raw.len());
    let mut bytes = raw.iter().copied();
    while let Some(byte) = bytes.next() {
        if byte != b'\\' {
            units.push(u16::from(byte));
            continue;
        }
        let Some(escaped) = bytes.next() else {
            break;
        };
        let unit = match escaped {
            b'u' => {
                let mut value = 0u16;
                for _ in 0..4 {
                    let digit = bytes
                        .next()
                        .and_then(|digit| char::from(digit).to_digit(16))
                        .and_then(|digit| u16::try_from(digit).ok())
                        .ok_or(MetaPropertiesError::MalformedEscape)?;
                    value = (value << 4) | digit;
                }
                value
            }
            b't' => u16::from(b'\t'),
            b'r' => u16::from(b'\r'),
            b'n' => u16::from(b'\n'),
            b'f' => 0x0c,
            other => u16::from(other),
        };
        units.push(unit);
    }
    Ok(String::from_utf16_lossy(&units))
}

/// `java.util.Date.toString` in UTC: `EEE MMM dd HH:mm:ss zzz yyyy`.
///
/// `Properties.store` writes it as the comment on the second line. A time
/// before 1970 is written as 1970, which no clock that formats a directory
/// reads.
fn java_date(now: SystemTime) -> String {
    let format = time::format_description::parse_borrowed::<2>(
        "[weekday repr:short] [month repr:short] [day] [hour]:[minute]:[second] UTC [year]",
    )
    .expect("the Date.toString description is well formed");
    OffsetDateTime::from(now.max(UNIX_EPOCH))
        .format(&format)
        .expect("a UTC date after 1970 has every component the description names")
}

#[cfg(test)]
mod tests {
    use {super::*, std::time::Duration, uuid::Uuid};

    const CLUSTER: &str = "AQIDBAUGBwgJCgsMDQ4PEA";
    const DIRECTORY: &str = "U-hCdTzaSPyFB3swwc7Qdw";

    /// A `verify_ensemble` case: what it is, the set, the expected cluster
    /// id and node id, and the result.
    struct EnsembleCase {
        what: &'static str,
        set: Vec<DirectoryFile>,
        cluster_id: Option<ClusterId>,
        node_id: Option<i32>,
        want: Result<Option<ClusterId>, String>,
    }

    /// The `meta.properties` read from one directory.
    struct DirectoryFile {
        dir: &'static str,
        meta: MetaProperties,
    }

    fn directory(dir: &'static str, meta: MetaProperties) -> DirectoryFile {
        DirectoryFile { dir, meta }
    }

    fn at(seconds: u64) -> SystemTime { UNIX_EPOCH + Duration::from_secs(seconds) }

    fn meta(node_id: i32, directory_id: Option<&str>) -> MetaProperties {
        MetaProperties {
            cluster_id: CLUSTER.parse().expect("cluster id"),
            node_id,
            directory_id: directory_id.map(|id| id.parse().expect("directory id")),
        }
    }

    /// `Date.toString` in UTC, checked against `date -u` for each instant.
    #[yare::parameterized(
        epoch = { 0, "Thu Jan 01 00:00:00 UTC 1970" },
        leap_day_2000 = { 951_782_400, "Tue Feb 29 00:00:00 UTC 2000" },
        kafka_epoch_example = { 1_000_000_000, "Sun Sep 09 01:46:40 UTC 2001" },
        leap_day_2024 = { 1_709_210_096, "Thu Feb 29 12:34:56 UTC 2024" },
        year_2099_end = { 4_102_444_799, "Thu Dec 31 23:59:59 UTC 2099" },
        zero_padded = { 1_767_596_889, "Mon Jan 05 07:08:09 UTC 2026" },
    )]
    fn the_date_comment_is_java_date_to_string(seconds: u64, want: &str) {
        assert2::check!(java_date(at(seconds)) == want, "{seconds}");
    }

    #[test]
    fn pre_epoch_date_clamps_to_epoch() {
        assert2::check!(
            java_date(UNIX_EPOCH - Duration::from_secs(1)) == "Thu Jan 01 00:00:00 UTC 1970"
        );
    }

    /// The bytes are the bytes Kafka's `PropertiesUtils.writePropertiesFile`
    /// writes for the same ids at the same time on a UTC host:
    /// `Properties.store(writer, "")` puts the empty comment, the date
    /// comment, and the entries in key order, each line ended by `\n`.
    #[yare::parameterized(
        with_directory_id = {
            "a formatted directory",
            meta(1, Some(DIRECTORY)),
            "#\n#Thu Feb 29 12:34:56 UTC 2024\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\nnode.id=1\nversion=1\n"
        },
        without_directory_id = {
            "no directory id",
            meta(2_147_483_647, None),
            "#\n#Thu Feb 29 12:34:56 UTC 2024\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n\
             node.id=2147483647\nversion=1\n"
        },
    )]
    fn the_file_is_the_one_kafka_writes(what: &str, meta: MetaProperties, want: &str) {
        assert2::check!(meta.to_file_text(at(1_709_210_096)) == want, "{what}");
    }

    /// Every file krabka writes reads back as the same ids.
    #[yare::parameterized(
        ordinary = { meta(0, Some(DIRECTORY)) },
        reserved_directory_id = { meta(1, Some("AAAAAAAAAAAAAAAAAAAAZA")) },
        high_bit_directory_id = { meta(2_147_483_647, Some("_____________________w")) },
        no_directory_id = { meta(7, None) },
        random_directory_id = { MetaProperties {
                cluster_id: ClusterId(Uuid::from_u128(0)),
                node_id: 3,
                directory_id: Some(DirectoryId::random()),
            } },
    )]
    fn the_written_file_parses_back_to_the_same_ids(written: MetaProperties) {
        let tmp = tempfile::tempdir().expect("tempdir");
        written.write(tmp.path()).expect("write");
        assert2::check!(MetaProperties::read(tmp.path()).expect("read") == Some(written));
        assert2::check!(!tmp.path().join(META_PROPERTIES_TMP).exists());
    }

    #[test]
    fn an_empty_directory_has_no_meta_properties() {
        let empty = tempfile::tempdir().expect("tempdir");
        assert2::check!(MetaProperties::read(empty.path()).expect("read").is_none());
    }

    /// `Properties.load` reads more than `store` writes: the file of an
    /// older Java, `:` and white space as separators, comments, line
    /// continuations, escapes, and `\r\n`. A key given twice keeps its last
    /// value.
    #[yare::parameterized(
        kafka_hash_order = { "#\n#Thu Feb 29 12:34:56 UTC 2024\nnode.id=1\nversion=1\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n" },
        no_comments = { "version=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=1\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\n" },
        colon_and_whitespace_separators = { "version : 1\n  cluster.id:AQIDBAUGBwgJCgsMDQ4PEA\nnode.id 1\n\
             \tdirectory.id \t= U-hCdTzaSPyFB3swwc7Qdw" },
        comments_and_blank_lines = { "! a comment\n\n   # another\nversion=1\n\n\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n\
             node.id=1\ndirectory.id=U-hCdTzaSPyFB3swwc7Qdw\n" },
        crlf_and_cr_endings = { "#\r\nversion=1\r\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\rnode.id=1\r\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\r\n" },
        continued_line = { "version=1\ncluster.id=AQIDBAUGBwgJ\\\n    CgsMDQ4PEA\nnode.id=1\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\\\r\n" },
        escaped_key_and_value = { "vers\\ion=\\u0031\ncluster\\.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=1\n\
             directory.id=U-hCdTzaSPyF\\B3swwc7Qdw\n" },
        duplicate_key_keeps_last = { "version=1\nnode.id=9\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=1\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\n" },
        unknown_key_is_ignored = { "version=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=1\nbroker.id=4\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\n" },
    )]
    fn the_reader_accepts_what_properties_load_accepts(text: &str) {
        let want = meta(1, Some(DIRECTORY));
        assert2::check!(
            MetaProperties::parse(text.as_bytes()).map_err(|e| e.to_string()) == Ok(want),
            "{text:?}"
        );
    }

    /// A file that does not read gets the message of the exception Kafka
    /// throws for it, and the first fault in Kafka's order wins.
    #[yare::parameterized(
        bad_version = { "version=one\n", "Invalid meta.properties version string 'one'" },
        unknown_version = {
            "version=2\nnode.id=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n",
            "Unknown meta.properties version number 2",
        },
        unsupported_zero = {
            "version=0\nbroker.id=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n",
            "Unsupported meta.properties version 0: krabka reads version 1, which a KRaft node writes",
        },
        missing_version = {
            "node.id=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n",
            "Unsupported meta.properties version 0: krabka reads version 1, which a KRaft node writes",
        },
        old_json = {
            "{\n  \"cluster_id\": \"AQIDBAUGBwgJCgsMDQ4PEA\",\n  \"version\": 3\n}\n",
            "Unsupported meta.properties version 0: krabka reads version 1, which a KRaft node writes",
        },
        no_node_or_cluster = { "version=1\n", "Failed to find node.id" },
        node_trailing_space = {
            "version=1\nnode.id=1 \ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n",
            "Unable to read node.id as a base-10 number.",
        },
        node_overflow = {
            "version=1\nnode.id=2147483648\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n",
            "Unable to read node.id as a base-10 number.",
        },
        bad_directory_precedes_cluster_missing = {
            "version=1\nnode.id=1\ndirectory.id=nope\n",
            "Unable to read directory.id as a Uuid: Input string `nope` decoded as 3 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
        },
        no_cluster = { "version=1\nnode.id=1\n", "cluster.id was not found." },
        invalid_cluster = {
            "version=1\nnode.id=1\ncluster.id=AAAAAA\n",
            "Unable to read cluster.id as a Uuid: Input string `AAAAAA` decoded as 4 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
        },
        malformed_escape = {
            "version=1\nnode.id=1\ncluster.id=\\u00zz\n",
            "Malformed \\uxxxx encoding.",
        },
    )]
    fn a_bad_file_gets_kafkas_message(text: &str, want: &str) {
        assert2::check!(
            MetaProperties::parse(text.as_bytes()).map_err(|e| e.to_string())
                == Err(want.to_owned()),
            "{text:?}"
        );
    }

    /// `verify_ensemble` agrees with Kafka's `MetaPropertiesEnsemble.verify`
    /// for each way a set can disagree.
    #[yare::parameterized(
        nothing_formatted = { "nothing formatted" },
        one_cluster_and_node = { "one cluster, one node" },
        set_supplies_node_id = { "the set supplies the node id" },
        cluster_id_disagrees = { "the given cluster id disagrees" },
        node_id_disagrees = { "the configured node id disagrees" },
        directory_node_ids_disagree = { "two directories disagree on the node id" },
        reserved_directory_id = { "a reserved directory id" },
        shared_directory_id = { "a shared directory id" },
        missing_directory_ids = { "no directory id is no conflict" },
    )]
    fn verify_ensemble_matches_kafka(scenario: &str) {
        let c1 = ClusterId(Uuid::from_u128(0xc1));
        let c2 = ClusterId(Uuid::from_u128(0xc2));
        let file = |cluster_id, node_id, directory: u128| MetaProperties {
            cluster_id,
            node_id,
            directory_id: Some(DirectoryId(Uuid::from_u128(directory))),
        };
        let cases: Vec<EnsembleCase> = vec![
            EnsembleCase {
                what: "nothing formatted",
                set: vec![],
                cluster_id: None,
                node_id: Some(1),
                want: Ok(None),
            },
            EnsembleCase {
                what: "one cluster, one node",
                set: vec![
                    directory("/a", file(c1, 1, 0x1000)),
                    directory("/b", file(c1, 1, 0x2000)),
                ],
                cluster_id: None,
                node_id: Some(1),
                want: Ok(Some(c1)),
            },
            EnsembleCase {
                what: "the set supplies the node id",
                set: vec![
                    directory("/a", file(c1, 4, 0x1000)),
                    directory("/b", file(c1, 4, 0x2000)),
                ],
                cluster_id: Some(c1),
                node_id: None,
                want: Ok(Some(c1)),
            },
            EnsembleCase {
                what: "the given cluster id disagrees",
                set: vec![directory("/a", file(c1, 1, 0x1000))],
                cluster_id: Some(c2),
                node_id: Some(1),
                want: Err(format!(
                    "Invalid cluster.id in: {}. Expected {c2}, but read {c1}",
                    Path::new("/a").join(META_PROPERTIES).display()
                )),
            },
            EnsembleCase {
                what: "the configured node id disagrees",
                set: vec![directory("/a", file(c1, 2, 0x1000))],
                cluster_id: None,
                node_id: Some(1),
                want: Err(node_id_mismatch_message()
                    .stored(2)
                    .previous(1)
                    .path(&Path::new("/a").join(META_PROPERTIES))
                    .call()),
            },
            EnsembleCase {
                what: "two directories disagree on the node id",
                set: vec![
                    directory("/a", file(c1, 3, 0x1000)),
                    directory("/b", file(c1, 4, 0x2000)),
                ],
                cluster_id: None,
                node_id: None,
                want: Err(format!(
                    "Stored node id 4 doesn't match previous node id 3 in {}. If you moved \
                         your data, make sure your configured node id matches. If you intend to \
                         create a new node, you should remove all data in your data directories.",
                    Path::new("/b").join(META_PROPERTIES).display()
                )),
            },
            EnsembleCase {
                what: "a reserved directory id",
                set: vec![directory("/a", file(c1, 1, 2))],
                cluster_id: None,
                node_id: Some(1),
                want: Err(
                    "Invalid reserved directory ID AAAAAAAAAAAAAAAAAAAAAg found in /a".to_owned(),
                ),
            },
            EnsembleCase {
                what: "a shared directory id",
                set: vec![
                    directory("/a", file(c1, 1, 0x1000)),
                    directory("/b", file(c1, 1, 0x1000)),
                ],
                cluster_id: None,
                node_id: Some(1),
                want: Err(format!(
                    "Duplicate directory ID Optional[{}] found. It was the ID of /a, but also of \
                     /b",
                    DirectoryId(Uuid::from_u128(0x1000))
                )),
            },
            EnsembleCase {
                what: "no directory id is no conflict",
                set: vec![
                    directory("/a", MetaProperties {
                        directory_id: None,
                        ..file(c1, 1, 0)
                    }),
                    directory("/b", MetaProperties {
                        directory_id: None,
                        ..file(c1, 1, 0)
                    }),
                ],
                cluster_id: None,
                node_id: Some(1),
                want: Ok(Some(c1)),
            },
        ];
        let EnsembleCase {
            what,
            set,
            cluster_id,
            node_id,
            want,
        } = cases
            .into_iter()
            .find(|case| case.what == scenario)
            .expect("named yare case exists");
        let got = verify_ensemble(
            set.iter().map(|file| (Path::new(file.dir), &file.meta)),
            cluster_id,
            node_id,
        );
        assert2::check!(got == want, "{what}");
    }
}
