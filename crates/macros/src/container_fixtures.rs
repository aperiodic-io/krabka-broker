//! External `MinIO` process primitives shared by acceptance and restore fixtures.

use crate::{fixtures::name, prelude::*};

pub(crate) fn expand(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    Ok(moxy::template! {
        mod {{ name }} {
            /// Start the server with the published port and fixture credentials.
            pub(super) fn start(name: &str, port: u16, image: &str, access: &str, secret: &str) {
                let status = ::std::process::Command::new("docker").args([
                    "run", "-d", "--rm", "--name", name,
                    "-p", &format!("{port}:9000"),
                    "-e", &format!("MINIO_ROOT_USER={access}"),
                    "-e", &format!("MINIO_ROOT_PASSWORD={secret}"),
                    image, "server", "/data",
                ]).stdout(::std::process::Stdio::null())
                  .stderr(::std::process::Stdio::inherit())
                  .status().expect("spawn docker run minio");
                ::assert2::assert!(status.success(), "docker run minio failed");
                wait(port);
            }

            /// Remove the container without disturbing a failure already in flight.
            pub(super) fn remove(name: &str) {
                let _ = ::std::process::Command::new("docker").args(["rm", "-f", name])
                    .stdout(::std::process::Stdio::null())
                    .stderr(::std::process::Stdio::null()).status();
            }

            /// Bound external readiness and allow the bucket API to initialize.
            pub(super) fn wait(port: u16) {
                let addr: ::std::net::SocketAddr = format!("127.0.0.1:{port}")
                    .parse().expect("static addr");
                // A TCP accept does not imply a fully initialized S3 bucket API.
                // This bounded external-process poll has no corresponding broker metric.
                let gap = ::std::time::Duration::from_millis(500);
                for _ in 0..60 {
                    let accepted = ::std::net::TcpStream::connect_timeout(&addr, gap).is_ok();
                    ::std::thread::sleep(gap);
                    if accepted { return; }
                }
                panic!("MinIO never accepted TCP on 127.0.0.1:{port}");
            }

            /// Create the bucket from the caller's script and retain the original failure output.
            pub(super) fn make_bucket(image: &str, script: &str) {
                let out = mc(image, script, "spawn mc mb");
                ::assert2::assert!(
                    out.status.success(),
                    "mc mb failed: stdout={}, stderr={}",
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr),
                );
            }

            /// Run the caller's exact shell script; the caller checks its outcome.
            pub(super) fn mc(image: &str, script: &str, context: &str) -> ::std::process::Output {
                ::std::process::Command::new("docker").args([
                    "run", "--rm", "--add-host=host.docker.internal:host-gateway",
                    "--entrypoint", "/bin/sh", image, "-c", script,
                ]).output().expect(context)
            }
        }
    })
}
