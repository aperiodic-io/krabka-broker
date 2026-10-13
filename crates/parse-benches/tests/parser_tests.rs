use {
    assert2::assert,
    krabka_parse_benches::{
        Args, BenchmarkComparison, BenchmarkMetric, BenchmarkSummary, NamedMetric,
        ParseBenchesError, compare_summaries, format_rfc3339_utc, parse_bencher_line,
        parse_benchmark_dir, read_summaries, resolve_commit_sha, run_from_args,
    },
    std::{
        collections::BTreeMap,
        fs::{self, File},
        io::Write,
        path::PathBuf,
        time::{Duration, UNIX_EPOCH},
    },
    tempfile::tempdir,
};

// Representative 29-metric log engine output fixture matching Criterion bencher format (space-padded, no commas)
const SAMPLE_CRITERION_BENCHER_OUTPUT: &str = r"
running 29 tests
test log/append/1rec_64B ... bench:        2225 ns/iter (+/- 8)
test log/append/10rec_64B ... bench:        5845 ns/iter (+/- 151)
test log/append/100rec_256B ... bench:       31200 ns/iter (+/- 412)
test log/append/100rec_1024B ... bench:       97083 ns/iter (+/- 1369686)
test log/append/500rec_256B ... bench:      120400 ns/iter (+/- 2300)
test log/append_large_message/owned_1rec_100KiB ... bench:      310000 ns/iter (+/- 4500)
test log/append_large_message/verbatim_1rec_100KiB ... bench:      180000 ns/iter (+/- 1200)
test log/append_large_message/verbatim_1rec_512KiB ... bench:      890000 ns/iter (+/- 9800)
test log/append_handoff/direct_mutex_verbatim_1rec_100KiB ... bench:      190000 ns/iter (+/- 2100)
test log/append_handoff/spawn_blocking_mutex_verbatim_1rec_100KiB ... bench:      420000 ns/iter (+/- 5000)
test log/append_handoff/block_in_place_mutex_verbatim_1rec_100KiB ... bench:      250000 ns/iter (+/- 3200)
test log/read/from_start_1MiB ... bench:     1500000 ns/iter (+/- 12000)
test log/read/from_start_unbounded ... bench:     3686508 ns/iter (+/- 11500)
test log/read/from_middle_1MiB ... bench:     1510000 ns/iter (+/- 13200)
test log/read/from_end_minus_100_1MiB ... bench:       17726 ns/iter (+/- 140)
test log/read/past_end_returns_empty ... bench:          22 ns/iter (+/- 0)
test log/open/50_appends_validate_on_open ... bench:      532433 ns/iter (+/- 1100)
test log/open/50_appends_no_validate ... bench:       45000 ns/iter (+/- 900)
test log/open/200_appends_validate_on_open ... bench:      320000 ns/iter (+/- 4200)
test log/open/200_appends_no_validate ... bench:      160000 ns/iter (+/- 2100)
test log/open/500_appends_validate_on_open ... bench:      780000 ns/iter (+/- 8900)
test log/open/500_appends_no_validate ... bench:     4880352 ns/iter (+/- 4500)
test log/truncate/truncate_recent_offset ... bench:       25000 ns/iter (+/- 500)
test log/accessors/log_end_offset ... bench:          15 ns/iter (+/- 1)
test log/accessors/log_start_offset ... bench:          14 ns/iter (+/- 1)
test log/accessors/lso ... bench:          16 ns/iter (+/- 2)
test log/file_write_shapes/seek_end_writev_100KiB ... bench:      210000 ns/iter (+/- 3400)
test log/file_write_shapes/writev_at_current_cursor_100KiB ... bench:      205000 ns/iter (+/- 3100)
test log/file_write_shapes/write_all_at_twice_100KiB ... bench:      230000 ns/iter (+/- 3900)

test result: ok. 29 passed; 0 failed; 0 ignored; 29 measured; 0 filtered out
";

const COMMA_GROUPED_BENCHER_OUTPUT: &str =
    "test log/append/100rec_1024B ... bench: 95,979 ns/iter (+/- 1,377,729)";

#[test]
fn parses_real_criterion_output_correctly() {
    let line = "test log/append/1rec_64B ... bench:        2225 ns/iter (+/- 8)";
    let result = parse_bencher_line(line).unwrap();

    assert!(result.is_some());
    let NamedMetric { name, metric } = result.unwrap();
    assert!(name == "log/append/1rec_64B");
    assert!((metric.ns_per_iter - 2225.0).abs() < f64::EPSILON);
    assert!((metric.variance_ns - 8.0).abs() < f64::EPSILON);
}

#[test]
fn parses_comma_grouped_bencher_line_correctly() {
    let result = parse_bencher_line(COMMA_GROUPED_BENCHER_OUTPUT).unwrap();

    assert!(result.is_some());
    let NamedMetric { name, metric } = result.unwrap();
    assert!(name == "log/append/100rec_1024B");
    assert!((metric.ns_per_iter - 95_979.0).abs() < f64::EPSILON);
    assert!((metric.variance_ns - 1_377_729.0).abs() < f64::EPSILON);
}

#[test]
fn ignores_non_matching_lines() {
    let result = parse_bencher_line("running 29 tests").unwrap();
    assert!(result.is_none());

    let result = parse_bencher_line("test result: ok. 29 passed").unwrap();
    assert!(result.is_none());
}

#[test]
fn parses_full_29_sample_benchmark_output() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("log-engine.txt");
    let mut file = File::create(&file_path).unwrap();
    file.write_all(SAMPLE_CRITERION_BENCHER_OUTPUT.as_bytes())
        .unwrap();

    let benchmarks = parse_benchmark_dir(dir.path()).unwrap();
    assert!(benchmarks.len() == 29);

    let append_metric = &benchmarks["log/append/100rec_1024B"];
    assert!((append_metric.ns_per_iter - 97_083.0).abs() < f64::EPSILON);
    assert!((append_metric.variance_ns - 1_369_686.0).abs() < f64::EPSILON);

    let zero_variance_metric = &benchmarks["log/read/past_end_returns_empty"];
    assert!((zero_variance_metric.ns_per_iter - 22.0).abs() < f64::EPSILON);
    assert!((zero_variance_metric.variance_ns - 0.0).abs() < f64::EPSILON);

    let accessor_metric = &benchmarks["log/accessors/log_end_offset"];
    assert!((accessor_metric.ns_per_iter - 15.0).abs() < f64::EPSILON);
    assert!((accessor_metric.variance_ns - 1.0).abs() < f64::EPSILON);
}

#[test]
fn parses_multiple_txt_files() {
    let dir = tempdir().unwrap();

    let f1_path = dir.path().join("file1.txt");
    fs::write(
        &f1_path,
        "test bench_a ... bench: 100 ns/iter (+/- 5)\ntest bench_b ... bench: 200 ns/iter (+/- \
         10)\n",
    )
    .unwrap();

    let f2_path = dir.path().join("file2.txt");
    fs::write(
        &f2_path,
        "test bench_c ... bench: 300 ns/iter (+/- 15)\ntest bench_d ... bench: 400 ns/iter (+/- \
         20)\n",
    )
    .unwrap();

    let benchmarks = parse_benchmark_dir(dir.path()).unwrap();
    assert!(benchmarks.len() == 4);
    assert!(benchmarks.contains_key("bench_a"));
    assert!(benchmarks.contains_key("bench_b"));
    assert!(benchmarks.contains_key("bench_c"));
    assert!(benchmarks.contains_key("bench_d"));
}

#[test]
fn fails_when_results_directory_does_not_exist() {
    let missing_path = PathBuf::from("nonexistent-dir-for-tests-12345");
    let err = parse_benchmark_dir(&missing_path).unwrap_err();

    match err {
        ParseBenchesError::DirectoryNotFound(p) => assert!(p == missing_path),
        other => panic!("expected DirectoryNotFound, got {other:?}"),
    }
}

#[test]
fn fails_when_no_txt_files_in_directory() {
    let dir = tempdir().unwrap();
    let err = parse_benchmark_dir(dir.path()).unwrap_err();

    match err {
        ParseBenchesError::NoTxtFiles(p) => assert!(p == dir.path()),
        other => panic!("expected NoTxtFiles, got {other:?}"),
    }
}

#[test]
fn fails_when_txt_files_contain_no_benchmark_lines() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("empty.txt");
    fs::write(
        &file_path,
        "Compiling krabka-log v0.4.0\nFinished bench profile\n",
    )
    .unwrap();

    let err = parse_benchmark_dir(dir.path()).unwrap_err();
    match err {
        ParseBenchesError::NoMetricsParsed(p) => assert!(p == dir.path()),
        other => panic!("expected NoMetricsParsed, got {other:?}"),
    }
}

#[test]
fn fails_on_duplicate_benchmark_in_same_file() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("dups.txt");
    fs::write(
        &file_path,
        "test log/append/1rec_64B ... bench: 100 ns/iter (+/- 5)\ntest log/append/1rec_64B ... \
         bench: 120 ns/iter (+/- 6)\n",
    )
    .unwrap();

    let err = parse_benchmark_dir(dir.path()).unwrap_err();
    match err {
        ParseBenchesError::DuplicateBenchmark { name, file } => {
            assert!(name == "log/append/1rec_64B");
            assert!(file == file_path);
        }
        other => panic!("expected DuplicateBenchmark, got {other:?}"),
    }
}

#[test]
fn fails_on_duplicate_benchmark_across_files() {
    let dir = tempdir().unwrap();
    fs::write(
        dir.path().join("part1.txt"),
        "test shared_bench ... bench: 100 ns/iter (+/- 5)\n",
    )
    .unwrap();
    let part2_path = dir.path().join("part2.txt");
    fs::write(
        &part2_path,
        "test shared_bench ... bench: 150 ns/iter (+/- 8)\n",
    )
    .unwrap();

    let err = parse_benchmark_dir(dir.path()).unwrap_err();
    match err {
        ParseBenchesError::DuplicateBenchmark { name, file } => {
            assert!(name == "shared_bench");
            assert!(file == part2_path);
        }
        other => panic!("expected DuplicateBenchmark, got {other:?}"),
    }
}

#[test]
fn formats_rfc3339_utc_timestamp_accurately() {
    // 2026-08-27T10:25:37Z in unix epoch seconds: 1787826337
    let sample_time = UNIX_EPOCH + Duration::from_secs(1_787_826_337);
    let formatted = format_rfc3339_utc(sample_time);
    assert!(formatted == "2026-08-27T10:25:37Z");

    // Non-zero subsecond input is truncated to whole seconds per YYYY-MM-DDTHH:MM:SSZ specification
    let subsecond_time = UNIX_EPOCH + Duration::new(1_787_826_337, 188_979_529);
    assert!(format_rfc3339_utc(subsecond_time) == "2026-08-27T10:25:37Z");

    // 1970-01-01T00:00:00Z
    assert!(format_rfc3339_utc(UNIX_EPOCH) == "1970-01-01T00:00:00Z");
}

#[test]
fn resolves_commit_sha_appropriately() {
    // Explicit CLI argument takes precedence over environment
    assert!(resolve_commit_sha(Some("abcdef123456"), None) == "abcdef123456");
    assert!(resolve_commit_sha(Some("abcdef123456"), Some("fedcba987654")) == "abcdef123456");
    assert!(resolve_commit_sha(Some("short"), None) == "short");

    // Fallback to environment SHA when CLI argument is absent or blank
    assert!(resolve_commit_sha(None, Some("fedcba987654")) == "fedcba987654");
    assert!(resolve_commit_sha(Some(""), Some("fedcba987654")) == "fedcba987654");
    assert!(resolve_commit_sha(Some("   "), Some("fedcba987654")) == "fedcba987654");

    // Default to "unknown" when neither provides a non-blank value
    assert!(resolve_commit_sha(None, None) == "unknown");
    assert!(resolve_commit_sha(Some(""), None) == "unknown");
    assert!(resolve_commit_sha(None, Some("")) == "unknown");
    assert!(resolve_commit_sha(Some("   "), Some("   ")) == "unknown");

    assert!(resolve_commit_sha(Some("🦀krabka123"), None) == "🦀krabka123");
}

#[test]
fn runs_end_to_end_writing_valid_json() {
    let dir = tempdir().unwrap();
    let results_dir = dir.path().join("bench-results");
    fs::create_dir_all(&results_dir).unwrap();

    let bench_file = results_dir.join("log-engine.txt");
    fs::write(&bench_file, SAMPLE_CRITERION_BENCHER_OUTPUT).unwrap();

    let out_json = results_dir.join("broker-benchmarks.json");

    let args = Args {
        results_dir: results_dir.clone(),
        output: Some(out_json.clone()),
        suite: "krabka-broker".to_string(),
        commit: Some("deadbeef999".to_string()),
        reference_dir: None,
        candidate_dir: None,
        minimum_tolerance_percent: 3.0,
    };

    let summary = run_from_args(&args).unwrap();
    assert!(summary.suite == "krabka-broker");
    assert!(summary.commit == "deadbeef999");
    assert!(summary.benchmarks.len() == 29);
    assert!(out_json.exists());

    let json_content = fs::read_to_string(&out_json).unwrap();
    let parsed_back: BenchmarkSummary = serde_json::from_str(&json_content).unwrap();
    assert!(parsed_back == summary);
}

fn repeated_summaries(values: &[f64], commit: &str) -> Vec<BenchmarkSummary> {
    values
        .iter()
        .map(|value| BenchmarkSummary {
            suite: "test".to_string(),
            commit: commit.to_string(),
            timestamp: "2026-09-09T00:00:00Z".to_string(),
            benchmarks: BTreeMap::from([("hot/path".to_string(), BenchmarkMetric {
                ns_per_iter: *value,
                variance_ns: 1.0,
            })]),
        })
        .collect()
}

#[test]
fn unchanged_controls_pass_the_variance_calibrated_verdict() {
    let reference = repeated_summaries(&[100.0, 102.0, 99.0], "reference");
    let candidate = repeated_summaries(&[101.0, 100.0, 103.0], "candidate");

    let verdict = compare_summaries(&reference, &candidate, 0.03).unwrap();

    assert!(verdict.passed);
    assert!(verdict.benchmarks["hot/path"].passed);
}

#[test]
fn a_deliberately_slowed_benchmark_fails_the_verdict() {
    let reference = repeated_summaries(&[100.0, 102.0, 99.0], "reference");
    let candidate = repeated_summaries(&[120.0, 121.0, 119.0], "candidate");

    let verdict = compare_summaries(&reference, &candidate, 0.03).unwrap();

    assert!(!verdict.passed);
    assert!(!verdict.benchmarks["hot/path"].passed);
}

#[test]
fn all_zero_samples_are_tied_below_output_resolution() {
    let reference = repeated_summaries(&[0.0, 0.0, 0.0], "reference");
    let candidate = repeated_summaries(&[0.0, 0.0, 0.0], "candidate");

    let verdict = compare_summaries(&reference, &candidate, 0.03).unwrap();

    assert!(verdict.passed);
    assert!(
        verdict.benchmarks["hot/path"]
            == BenchmarkComparison {
                reference_median_ns: 0.0,
                candidate_median_ns: 0.0,
                ratio: 1.0,
                tolerance: 0.03,
                passed: true,
            }
    );
}

#[test]
fn missing_repeated_samples_cannot_pass() {
    let reference = repeated_summaries(&[100.0, 101.0], "reference");
    let candidate = repeated_summaries(&[100.0, 101.0, 99.0], "candidate");

    let error = compare_summaries(&reference, &candidate, 0.03).unwrap_err();

    assert!(matches!(error, ParseBenchesError::TooFewSamples {
        side: "reference",
        count: 2
    }));
}

#[test]
fn comparison_rejects_invalid_inputs() {
    let good = repeated_summaries(&[100.0, 101.0, 99.0], "good");
    for tolerance in [-1.0, f64::NAN, f64::INFINITY] {
        assert!(matches!(
            compare_summaries(&good, &good, tolerance),
            Err(ParseBenchesError::InvalidTolerance)
        ));
    }

    let empty = vec![
        BenchmarkSummary {
            suite: "test".into(),
            commit: "empty".into(),
            timestamp: "2026-09-09T00:00:00Z".into(),
            benchmarks: BTreeMap::new(),
        };
        3
    ];
    assert!(matches!(
        compare_summaries(&empty, &empty, 0.03),
        Err(ParseBenchesError::EmptyBenchmarkSet)
    ));

    let other = repeated_summaries(&[100.0, 101.0, 99.0], "other")
        .into_iter()
        .map(|mut summary| {
            summary.benchmarks = BTreeMap::from([("other/path".into(), BenchmarkMetric {
                ns_per_iter: 1.0,
                variance_ns: 0.0,
            })]);
            summary
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        compare_summaries(&other, &good, 0.03),
        Err(ParseBenchesError::BenchmarkSetMismatch)
    ));
    assert!(matches!(
        compare_summaries(&good, &other, 0.03),
        Err(ParseBenchesError::BenchmarkSetMismatch)
    ));

    let invalid = repeated_summaries(&[100.0, 0.0, 99.0], "invalid");
    assert!(matches!(
        compare_summaries(&good, &invalid, 0.03),
        Err(ParseBenchesError::InvalidSample(name)) if name == "hot/path"
    ));
}

#[test]
fn comparison_handles_even_sample_counts_and_short_candidate() {
    let reference = repeated_summaries(&[98.0, 100.0, 102.0, 104.0], "reference");
    let candidate = repeated_summaries(&[99.0, 101.0, 103.0, 105.0], "candidate");
    let verdict = compare_summaries(&reference, &candidate, 0.03).unwrap();
    assert!((verdict.benchmarks["hot/path"].reference_median_ns - 101.0).abs() < f64::EPSILON);
    assert!((verdict.benchmarks["hot/path"].candidate_median_ns - 102.0).abs() < f64::EPSILON);

    assert!(matches!(
        compare_summaries(&reference, &candidate[..2], 0.03),
        Err(ParseBenchesError::TooFewSamples {
            side: "candidate",
            count: 2
        })
    ));
}

#[test]
fn reads_repeated_summaries_in_filename_order() {
    let dir = tempdir().unwrap();
    for (name, commit) in [("c.json", "c"), ("a.json", "a"), ("b.json", "b")] {
        let summary = &repeated_summaries(&[100.0], commit)[0];
        serde_json::to_writer(File::create(dir.path().join(name)).unwrap(), summary).unwrap();
    }
    fs::write(dir.path().join("ignored.txt"), "not json").unwrap();

    let summaries = read_summaries(dir.path(), "reference").unwrap();
    assert!(
        summaries
            .iter()
            .map(|summary| summary.commit.as_str())
            .collect::<Vec<_>>()
            == ["a", "b", "c"]
    );
}

#[test]
fn reading_summaries_rejects_missing_short_and_invalid_sets() {
    let missing = PathBuf::from("missing-summary-dir-for-tests-12345");
    assert!(matches!(
        read_summaries(&missing, "reference"),
        Err(ParseBenchesError::DirectoryNotFound(path)) if path == missing
    ));

    let short = tempdir().unwrap();
    fs::write(short.path().join("one.json"), "{}").unwrap();
    assert!(matches!(
        read_summaries(short.path(), "candidate"),
        Err(ParseBenchesError::TooFewSamples {
            side: "candidate",
            count: 1
        })
    ));

    let invalid = tempdir().unwrap();
    for name in ["a.json", "b.json", "c.json"] {
        fs::write(invalid.path().join(name), "not json").unwrap();
    }
    assert!(matches!(
        read_summaries(invalid.path(), "reference"),
        Err(ParseBenchesError::Json(_))
    ));
}
