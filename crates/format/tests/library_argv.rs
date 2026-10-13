//! Library argument parsing must return control to its embedding process.

use {assert2::assert, std::process::Command};

#[test]
fn library_argv_returns_instead_of_exiting_the_embedding_process() {
    const CHILD: &str = "KRABKA_FORMAT_ARGV_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            for (flag, expected) in [
                ("--unknown-format-flag", 2),
                ("--help", 0),
                ("--version", 0),
            ] {
                assert!(krabka_format::run_from_args(["format", flag]).await == expected);
                assert!(
                    krabka_format::run_from_args_with_records(["format", flag], Vec::new()).await
                        == expected
                );
            }
        });
        println!("formatter returned to caller");
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "library_argv_returns_instead_of_exiting_the_embedding_process",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("formatter returned to caller"));
}
