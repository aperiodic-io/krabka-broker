//! `krabka-parse-benches` — parse Criterion benchmark outputs into a JSON summary.

use {
    clap::Parser,
    krabka_parse_benches::{Args, compare_summaries, read_summaries, run_from_args},
    std::process::ExitCode,
};

fn main() -> ExitCode {
    let args = Args::parse();
    let out_path = args
        .output
        .clone()
        .unwrap_or_else(|| args.results_dir.join("broker-benchmarks.json"));

    if let (Some(reference_dir), Some(candidate_dir)) = (&args.reference_dir, &args.candidate_dir) {
        let verdict = read_summaries(reference_dir, "reference").and_then(|reference| {
            let candidate = read_summaries(candidate_dir, "candidate")?;
            compare_summaries(
                &reference,
                &candidate,
                args.minimum_tolerance_percent / 100.0,
            )
        });
        return match verdict {
            Ok(verdict) => {
                if let Some(parent) = out_path.parent()
                    && let Err(error) = std::fs::create_dir_all(parent)
                {
                    eprintln!("Error: {error}");
                    return ExitCode::FAILURE;
                }
                let write = std::fs::File::create(&out_path).and_then(|file| {
                    serde_json::to_writer_pretty(file, &verdict).map_err(std::io::Error::other)
                });
                if let Err(error) = write {
                    eprintln!("Error: {error}");
                    ExitCode::FAILURE
                } else if verdict.passed {
                    println!("Benchmark verdict passed: {}", out_path.display());
                    ExitCode::SUCCESS
                } else {
                    eprintln!("Benchmark regression: {}", out_path.display());
                    ExitCode::FAILURE
                }
            }
            Err(error) => {
                eprintln!("Error: {error}");
                ExitCode::FAILURE
            }
        };
    }

    match run_from_args(&args) {
        Ok(summary) => {
            println!(
                "Successfully parsed {} benchmark metrics to {}",
                summary.benchmarks.len(),
                out_path.display()
            );
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("Error: {err}");
            ExitCode::FAILURE
        }
    }
}
