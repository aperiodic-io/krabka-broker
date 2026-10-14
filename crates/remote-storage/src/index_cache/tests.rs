//! Behavior of the bounded remote index cache: a second lookup of the same
//! index does not re-download, the byte budget evicts by recency, a deleted
//! segment releases its bytes, and the disabled cache stores nothing.

use {
    super::*,
    assert2::check,
    std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[test]
fn simultaneous_fills_never_publish_partial_cached_bytes() {
    let TestCache { dir: _dir, cache } = test_cache(4 * 1024 * 1024);
    let segment = Uuid::new_v4();
    let expected = vec![7; 1024 * 1024];
    let fill_barrier = std::sync::Barrier::new(16);
    std::thread::scope(|scope| {
        for _ in 0..16 {
            scope.spawn(|| {
                let (bytes, _) = cache
                    .get_or_fetch(segment, IndexType::Transaction, || {
                        fill_barrier.wait();
                        Ok(expected.clone())
                    })
                    .expect("fill");
                check!(bytes == expected);
                for _ in 0..32 {
                    let (bytes, outcome) = cache
                        .get_or_fetch(segment, IndexType::Transaction, || {
                            panic!("published entry must stay cached")
                        })
                        .expect("hit");
                    check!(outcome == IndexCacheOutcome::Hit);
                    check!(bytes == expected);
                }
            });
        }
    });
    check!(cache.stats().entries == 1);
    check!(cache.stats().bytes == expected.len() as u64);
}

#[test]
fn stale_temporary_paths_do_not_prevent_cache_replacement() {
    let TestCache {
        dir: directory,
        cache,
    } = test_cache(1024);
    let segment = Uuid::new_v4();
    cache
        .get_or_fetch(segment, IndexType::Offset, || Ok(vec![7; 32]))
        .unwrap();
    let key = CacheKey {
        segment_id: segment,
        index_type: IndexType::Offset,
    };
    let path = entry_path(&directory.path().join(REMOTE_INDEX_CACHE_DIR), key);
    std::fs::create_dir(path.with_extension("tmp")).unwrap();
    cache.store(key, &path, &[9; 64]);
    let (bytes, outcome) = cache
        .get_or_fetch(segment, IndexType::Offset, || {
            panic!("replacement must be cached")
        })
        .unwrap();
    check!(bytes == vec![9; 64]);
    check!(outcome == IndexCacheOutcome::Hit);
    check!(cache.stats().bytes == 64);
}

/// A fetcher that counts its calls and returns `len` bytes.
fn counting_fetch(
    calls: &Arc<AtomicUsize>,
    len: usize,
) -> impl FnOnce() -> Result<Vec<u8>, RemoteStorageError> {
    let calls = Arc::clone(calls);
    move || {
        calls.fetch_add(1, Ordering::Relaxed);
        Ok(vec![7_u8; len])
    }
}

/// A cache and the temporary directory that backs it.
struct TestCache {
    dir: tempfile::TempDir,
    cache: RemoteIndexCache,
}

fn test_cache(budget: u64) -> TestCache {
    let dir = tempfile::tempdir().expect("tempdir");
    let cache = RemoteIndexCache::new(dir.path(), budget).expect("cache");
    TestCache { dir, cache }
}

#[yare::parameterized(
    repeated_index = { IndexType::Offset, 32, IndexCacheOutcome::Hit, 1 },
    different_index_types = { IndexType::Timestamp, 16, IndexCacheOutcome::Miss, 2 },
)]
fn lookups_are_keyed_by_segment_and_index_type(
    second_type: IndexType,
    length: usize,
    second_expected: IndexCacheOutcome,
    entries: u64,
) {
    let TestCache { dir: _dir, cache } = test_cache(1 << 20);
    let segment = Uuid::new_v4();
    let calls = Arc::new(AtomicUsize::new(0));
    let (first, first_outcome) = cache
        .get_or_fetch(segment, IndexType::Offset, counting_fetch(&calls, length))
        .expect("first lookup");
    let (second, second_outcome) = cache
        .get_or_fetch(segment, second_type, counting_fetch(&calls, length))
        .expect("second lookup");

    check!(first == second);
    check!(first_outcome == IndexCacheOutcome::Miss);
    check!(second_outcome == second_expected);
    check!(calls.load(Ordering::Relaxed) == usize::try_from(entries).unwrap());
    check!(
        cache.stats()
            == IndexCacheStats {
                hits: 2 - entries,
                misses: entries,
                evictions: 0,
                entries,
                bytes: 32,
            }
    );
}
#[test]
fn the_byte_budget_evicts_the_least_recently_used_entry() {
    // Room for exactly two 40-byte entries.
    let TestCache { dir: _dir, cache } = test_cache(80);
    let calls = Arc::new(AtomicUsize::new(0));
    let (first, second, third) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());

    cache
        .get_or_fetch(first, IndexType::Offset, counting_fetch(&calls, 40))
        .expect("first");
    cache
        .get_or_fetch(second, IndexType::Offset, counting_fetch(&calls, 40))
        .expect("second");
    // Re-reading `first` makes `second` the least recently used one.
    cache
        .get_or_fetch(first, IndexType::Offset, counting_fetch(&calls, 40))
        .expect("first again");
    cache
        .get_or_fetch(third, IndexType::Offset, counting_fetch(&calls, 40))
        .expect("third");

    check!(cache.stats().entries == 2);
    check!(cache.stats().bytes == 80);
    check!(cache.stats().evictions == 1);
    // `second` was the victim, so it downloads again; `first` and `third` do not.
    let before = calls.load(Ordering::Relaxed);
    cache
        .get_or_fetch(first, IndexType::Offset, counting_fetch(&calls, 40))
        .expect("first still cached");
    check!(calls.load(Ordering::Relaxed) == before);
    cache
        .get_or_fetch(second, IndexType::Offset, counting_fetch(&calls, 40))
        .expect("second evicted");
    check!(calls.load(Ordering::Relaxed) == before + 1);
}

#[test]
fn an_entry_larger_than_the_whole_budget_is_returned_but_not_stored() {
    let TestCache { dir: _dir, cache } = test_cache(16);
    let calls = Arc::new(AtomicUsize::new(0));
    let segment = Uuid::new_v4();

    let (bytes, _) = cache
        .get_or_fetch(segment, IndexType::Offset, counting_fetch(&calls, 64))
        .expect("lookup");

    check!(bytes.len() == 64);
    check!(cache.stats().entries == 0);
    cache
        .get_or_fetch(segment, IndexType::Offset, counting_fetch(&calls, 64))
        .expect("lookup again");
    check!(calls.load(Ordering::Relaxed) == 2);
}

#[test]
fn removing_a_segment_releases_every_index_it_held() {
    let TestCache { dir, cache } = test_cache(1 << 20);
    let segment = Uuid::new_v4();
    let calls = Arc::new(AtomicUsize::new(0));
    for index_type in [
        IndexType::Offset,
        IndexType::Timestamp,
        IndexType::Transaction,
    ] {
        cache
            .get_or_fetch(segment, index_type, counting_fetch(&calls, 24))
            .expect("lookup");
    }
    check!(cache.stats().bytes == 72);

    cache.remove_segment(segment);

    check!(
        cache.stats()
            == IndexCacheStats {
                hits: 0,
                misses: 3,
                evictions: 0,
                entries: 0,
                bytes: 0,
            }
    );
    let files = std::fs::read_dir(dir.path().join(REMOTE_INDEX_CACHE_DIR))
        .expect("cache dir")
        .count();
    check!(files == 0);
}

#[test]
fn a_disabled_cache_fetches_every_time_and_writes_nothing() {
    let cache = RemoteIndexCache::disabled();
    let calls = Arc::new(AtomicUsize::new(0));
    let segment = Uuid::new_v4();

    for _ in 0..3 {
        let (_, outcome) = cache
            .get_or_fetch(segment, IndexType::Offset, counting_fetch(&calls, 8))
            .expect("lookup");
        check!(outcome == IndexCacheOutcome::Disabled);
    }

    check!(!cache.is_enabled());
    check!(calls.load(Ordering::Relaxed) == 3);
    check!(cache.stats() == IndexCacheStats::default());
}

#[test]
fn a_fetch_failure_is_returned_and_leaves_the_cache_empty() {
    let TestCache { dir: _dir, cache } = test_cache(1 << 20);

    let result = cache.get_or_fetch(Uuid::new_v4(), IndexType::Offset, || {
        Err(RemoteStorageError::Io(std::io::Error::other("boom")))
    });

    match result {
        Err(RemoteStorageError::Io(error)) => {
            check!(error.to_string() == "boom");
        }
        other => panic!("expected the fetcher's Io error, got {other:?}"),
    }
    check!(cache.stats().entries == 0);
}

#[test]
fn opening_the_cache_empties_a_directory_an_earlier_process_left() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join(REMOTE_INDEX_CACHE_DIR);
    std::fs::create_dir_all(&root).expect("pre-create");
    std::fs::write(root.join("stale.index"), b"stale").expect("stale entry");

    let cache = RemoteIndexCache::new(dir.path(), 1 << 20).expect("cache");

    check!(cache.stats() == IndexCacheStats::default());
    check!(std::fs::read_dir(&root).expect("cache dir").count() == 0);
}
