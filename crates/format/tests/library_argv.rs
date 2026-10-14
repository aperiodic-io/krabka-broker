//! Library argument parsing must return control to its embedding process.

use std::process::Command;

#[yare::parameterized(
    unknown_flag = { "--unknown-format-flag", 2 },
    help = { "--help", 0 },
    version = { "--version", 0 },
)]
fn library_argv_returns_instead_of_exiting_the_embedding_process(flag: &str, expected: i32) {
    const CHILD: &str = "KRABKA_FORMAT_ARGV_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            assert2::assert!(krabka_format::run_from_args(["format", flag]).await == expected);
            assert2::assert!(
                krabka_format::run_from_args_with_records(["format", flag], Vec::new()).await
                    == expected
            );
        });
        println!("formatter returned to caller");
        return;
    }
    let current = std::thread::current();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            current.name().expect("test thread name"),
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert2::assert!(output.status.success(), "{output:?}");
    assert2::assert!(
        String::from_utf8_lossy(&output.stdout).contains("formatter returned to caller")
    );
}
