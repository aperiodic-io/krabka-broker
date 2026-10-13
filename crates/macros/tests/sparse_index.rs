//! The open attribute preserves local macro scopes and closure patterns.

use {
    assert2::assert,
    std::{
        io,
        path::{Path, PathBuf},
    },
};

struct BorrowedPath;

impl BorrowedPath {
    #[krabka_macros::index_open]
    fn open(path: &Path) -> io::Result<Vec<i32>> {
        let adjustment = i32::try_from(path.components().count()).map_err(io::Error::other)?;
        macro_rules! adjusted {
            ($value:expr) => {
                $value + adjustment
            };
        }
        let values = [2, 4];
        let by_reference: Vec<_> = values.iter().map(|&value| adjusted!(value)).collect();
        let rows: Vec<_> = by_reference
            .iter()
            .zip([10, 20])
            .map(|(&value, extra)| adjusted!(value + extra))
            .collect();
        tracing::Span::current().record("entries", rows.len());
        Ok(rows)
    }
}

struct OwnedPath;

impl OwnedPath {
    #[krabka_macros::index_open]
    fn open(path: PathBuf) -> io::Result<usize> {
        if path.as_os_str().is_empty() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "no index path"));
        }
        Ok(path.components().count())
    }
}

#[test]
fn original_method_tokens_keep_local_macro_and_pattern_bindings() {
    assert!(BorrowedPath::open(Path::new("segment.index")).unwrap() == [14, 26]);
    assert!(OwnedPath::open(PathBuf::from("segment.timeindex")).unwrap() == 1);
    let failure = OwnedPath::open(PathBuf::new()).unwrap_err();
    assert!(failure.kind() == io::ErrorKind::NotFound && failure.to_string() == "no index path");
}
