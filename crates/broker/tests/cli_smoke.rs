use std::process::Command;

use assert2::assert;

fn broker_bin() -> std::path::PathBuf {
    let exe = std::env::var_os("CARGO_BIN_EXE_krabka-broker")
        .expect("cargo provides CARGO_BIN_EXE_<bin> in test env");
    std::path::PathBuf::from(exe)
}

/// Formats a fresh standalone log directory.
///
/// KIP-853 needs every node formatted before `krabka-broker` boots: the step
/// seeds `meta.properties` and the singleton `VotersRecord`, and the broker
/// treats an unformatted dir as operator error and aborts startup.
///
/// Called in process rather than spawned. The formatting is setup for the boot
/// test below, not the thing under test -- `krabka-format`'s own `format_smoke`
/// suite runs the real binary -- and a subprocess would need a Cargo working
/// tree to build from, which a Bazel test sandbox does not have. This test is
/// synchronous, so it drives the async formatter on a current-thread runtime.
fn run_krabka_format(log_dir: &std::path::Path, node_id: u32, controller_listener: &str) {
    format_log_dir(
        log_dir,
        node_id,
        &["--standalone", "--controller-listener", controller_listener],
    );
}

/// Formats a fresh log directory for `node_id` with the `quorum` flags of
/// `krabka-format`, as [`run_krabka_format`] does.
fn format_log_dir(log_dir: &std::path::Path, node_id: u32, quorum: &[&str]) {
    let node_id = node_id.to_string();
    let mut argv = vec![
        "krabka-format",
        "--log-dir",
        log_dir.to_str().unwrap(),
        "--node-id",
        &node_id,
    ];
    argv.extend_from_slice(quorum);
    let code = current_thread_runtime().block_on(krabka_format::run_from_args(argv));
    assert!(code == 0, "krabka-format exited {code}");
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

#[test]
fn help_mentions_cluster_id_and_advertised_listener() {
    let out = Command::new(broker_bin()).arg("--help").output().unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let help = String::from_utf8(out.stdout).unwrap();
    assert!(
        help.contains("--cluster-id"),
        "help missing --cluster-id:\n{help}"
    );
    assert!(
        help.contains("--advertised-listener"),
        "help missing --advertised-listener:\n{help}"
    );
}

#[test]
fn version_returns_zero() {
    let out = Command::new(broker_bin())
        .arg("--version")
        .output()
        .unwrap();
    assert!(out.status.success());
}

/// Boot `krabka-broker` with `--config-file` set to a minimal TOML, and
/// assert that the process binds the listener declared in the file. The port
/// comes from the file, not from a CLI flag.
#[test]
fn boots_with_config_file_listener() {
    use std::io::Write as _;

    let tmp = tempfile::tempdir().expect("tempdir");
    let log_dir = tmp.path().join("data");

    // KIP-853: the broker refuses to boot an unformatted log dir, so seed
    // it first. `krabka format` creates the directory itself (it must be
    // empty or non-existent), so don't pre-create it.
    run_krabka_format(&log_dir, 1, "127.0.0.1:9093");

    // Pick an ephemeral port by binding briefly, then release it.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let cfg_path = tmp.path().join("broker.toml");
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    writeln!(
        f,
        r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "127.0.0.1:{port}"
advertised = "127.0.0.1:{port}"
protocol = "Plaintext"
"#
    )
    .unwrap();

    let mut child = Command::new(broker_bin())
        .arg(format!("--config-file={}", cfg_path.display()))
        .arg(format!("--log-dir={}", log_dir.display()))
        .arg("--broker-id=1")
        .arg("--metrics-listen-addr=none")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn krabka-broker");

    // Poll for the port to accept connections within 10 seconds.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut connected = false;
    while std::time::Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            connected = true;
            break;
        }
        // intentional: waiting on a spawned krabka-broker subprocess to bind its
        // TCP listener; no in-process BrokerHandle, image, or metric to await here.
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Tear down before assertions so a hang doesn't leave a stray process.
    let _ = child.kill();
    let _ = child.wait();

    assert!(connected, "broker never opened TCP listener on port {port}");
}

#[test]
fn errors_when_config_file_and_listen_addr_both_set() {
    let out = Command::new(broker_bin())
        .arg("--config-file=/tmp/nonexistent.toml")
        .arg("--listen-addr=127.0.0.1:9092")
        .output()
        .expect("spawn krabka-broker");

    assert!(!out.status.success(), "expected non-zero exit");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("config-file") && stderr.contains("listen-addr"),
        "expected clap mutual-exclusion error, got stderr:\n{stderr}"
    );
}

/// A log directory formatted for node 1 does not start node 2. The broker
/// exits 1 with the message of Kafka's `MetaPropertiesEnsemble.verify`, as
/// `KafkaRaftServer.initializeLogDirs` refuses the same directory.
#[test]
fn refuses_a_log_dir_formatted_for_another_node() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_dir = tmp.path().join("data");
    run_krabka_format(&log_dir, 1, "127.0.0.1:9093");

    let mut cmd = Command::new(broker_bin());
    cmd.args([
        format!("--log-dir={}", log_dir.display()),
        "--broker-id=2".into(),
        "--listen-addr=127.0.0.1:0".into(),
        "--controller-listen-addr=127.0.0.1:0".into(),
        "--metrics-listen-addr=none".into(),
    ]);
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().expect("spawn krabka-broker");
    let timeout = std::time::Duration::from_secs(30);
    let deadline = std::time::Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll krabka-broker") {
            break Some(status);
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            break None;
        }
        // intentional: waiting on a spawned krabka-broker subprocess to exit;
        // it has no in-process handle to await.
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    let output = child.wait_with_output().expect("collect stderr");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        status.and_then(|status| status.code()) == Some(1),
        "{stderr}"
    );
    let want = format!(
        "startup failed: Stored node id 1 doesn't match previous node id 2 in {}. If you moved \
         your data, make sure your configured node id matches. If you intend to create a new \
         node, you should remove all data in your data directories.",
        log_dir.join("meta.properties").display()
    );
    assert!(stderr.contains(&want), "{stderr}");
}

/// The cluster id of the nodes that [`each_role_opens_only_its_own_listeners`]
/// starts.
const CLUSTER_ID: &str = "I2eXt9rvSnyhct8BYmW6-w";

/// How long a node has to log its startup line. A broker-only node waits for
/// its first unfence, which a few heartbeats bring.
const STARTUP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(90);

/// The line that a node logs when its start is complete.
const STARTED: &str = "krabka-broker started";

/// A free loopback port. Something else can take it between this call and
/// the broker's bind, as in [`boots_with_config_file_listener`].
fn free_port() -> std::net::SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("bind an ephemeral loopback port")
}

/// A running `krabka-broker` process. Dropping it kills the process, so a
/// failed assertion leaves none behind.
struct Process {
    child: std::process::Child,
    /// The stdout lines of the process, one JSON object each.
    lines: std::sync::mpsc::Receiver<String>,
}

impl Process {
    fn spawn(args: &[String]) -> Self {
        let mut child = Command::new(broker_bin())
            .args(args)
            .arg("--metrics-listen-addr=none")
            .arg("--health-listen-addr=none")
            // The startup oracle consumes an INFO event, so request that level
            // explicitly for each child, independent of the test runner's filter.
            .env("RUST_LOG", krabka_broker::config::DEFAULT_LOG_FILTER)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .expect("spawn krabka-broker");
        let stdout = child.stdout.take().expect("piped stdout");
        let (sender, lines) = std::sync::mpsc::channel();
        // The reader drains stdout for the whole life of the process, so the
        // broker never blocks on a full pipe.
        std::thread::spawn(move || {
            use std::io::BufRead as _;
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if sender.send(line).is_err() {
                    return;
                }
            }
        });
        Self { child, lines }
    }

    /// The fields of the startup line. Fails the test when the process does
    /// not log it within [`STARTUP_DEADLINE`], with the lines it did log.
    fn wait_until_started(&self) -> Started {
        let deadline = std::time::Instant::now() + STARTUP_DEADLINE;
        let mut logged = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            let Ok(line) = self.lines.recv_timeout(left) else {
                panic!(
                    "no `{STARTED}` line within {STARTUP_DEADLINE:?}; the process logged:\n{}",
                    logged.join("\n")
                );
            };
            if let Ok(entry) = serde_json::from_str::<serde_json::Value>(&line)
                && entry.get("message").and_then(serde_json::Value::as_str) == Some(STARTED)
            {
                let field = |name: &str| {
                    entry
                        .get(name)
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                };
                return Started {
                    listen_addr: field("listen_addr"),
                    controller_listen_addr: field("controller_listen_addr"),
                };
            }
            logged.push(line);
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The listener addresses that the startup line names.
#[derive(Debug, PartialEq, Eq)]
struct Started {
    listen_addr: Option<String>,
    controller_listen_addr: Option<String>,
}

/// What a port answers.
#[derive(Debug, PartialEq, Eq)]
enum Listener {
    /// The connection is refused: nothing listens on the port.
    Closed,
    /// `ApiVersions` advertises `Produce`: a broker listener.
    Broker,
    /// `ApiVersions` advertises no `Produce`: a controller listener, as Kafka
    /// tags `Produce` for the broker listeners only.
    Controller,
}

fn probe(runtime: &tokio::runtime::Runtime, addr: std::net::SocketAddr) -> Listener {
    match std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(5)) {
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
            return Listener::Closed;
        }
        Err(error) => panic!("connect to {addr}: {error}"),
        Ok(stream) => drop(stream),
    }
    let connection = runtime
        .block_on(krabka_client_core::Connection::connect(
            addr,
            krabka_client_core::ConnectionOptions {
                client_id: "cli-smoke".to_owned(),
                ..krabka_client_core::ConnectionOptions::default()
            },
        ))
        .unwrap_or_else(|error| panic!("ApiVersions on {addr}: {error}"));
    let produce = connection.advertised_api_range(krabka_protocol::owned::produce_request::API_KEY);
    connection.close();
    if produce.is_some() {
        Listener::Broker
    } else {
        Listener::Controller
    }
}

/// What one node logs and what its two ports answer once it has started.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    roles: &'static str,
    started: Started,
    client_port: Listener,
    controller_port: Listener,
}

/// One node of [`each_role_opens_only_its_own_listeners`].
struct Case {
    /// `--process-roles`.
    roles: &'static str,
    node_id: u32,
    /// `--controller-listen-addr`.
    controller: std::net::SocketAddr,
    /// `--controller-quorum-voters` of a node that is not a voter. A node
    /// without it is formatted as the standalone voter of its own quorum.
    voters: Option<String>,
    /// Whether the startup line names the client listener and the controller
    /// listener.
    names: (bool, bool),
    /// What the client port and the controller port answer.
    ports: (Listener, Listener),
}

/// Every `process.roles` value opens the listeners of its roles and no other,
/// as Kafka does. Kafka's `ControllerServer` opens only the listeners that
/// `controller.listener.names` names, and `KafkaConfig` refuses a
/// controller-only node whose `listeners` name another one. So a client cannot
/// reach a controller-only node on the client port, and two controller-only
/// nodes on one host do not compete for that port.
///
/// Each node gets a `--listen-addr` and a `--controller-listen-addr` of its
/// own. The startup line names the listeners that the node opened.
///
/// The controller-only node is the quorum of the broker-only node, which
/// cannot start without one, so the cases run in order and every node runs
/// until the end. The combined node is a standalone quorum of its own.
#[test]
fn each_role_opens_only_its_own_listeners() {
    let runtime = current_thread_runtime();
    let tmp = tempfile::tempdir().expect("tempdir");
    let quorum = free_port();
    let cases = [
        Case {
            roles: "controller",
            node_id: 1,
            controller: quorum,
            voters: None,
            names: (false, true),
            ports: (Listener::Closed, Listener::Controller),
        },
        Case {
            roles: "broker",
            node_id: 2,
            controller: free_port(),
            voters: Some(format!("1@{quorum}")),
            names: (true, false),
            ports: (Listener::Broker, Listener::Closed),
        },
        Case {
            roles: "broker,controller",
            node_id: 3,
            controller: free_port(),
            voters: None,
            names: (true, true),
            ports: (Listener::Broker, Listener::Controller),
        },
    ];

    let mut running = Vec::new();
    let mut observed = Vec::new();
    let mut expected = Vec::new();
    for case in cases {
        let log_dir = tmp.path().join(format!("node-{}", case.node_id));
        let controller = case.controller.to_string();
        let mut quorum_flags = vec!["--cluster-id", CLUSTER_ID];
        if case.voters.is_none() {
            quorum_flags.extend(["--standalone", "--controller-listener", controller.as_str()]);
        }
        format_log_dir(&log_dir, case.node_id, &quorum_flags);
        let client = free_port();
        let mut args = vec![
            format!("--log-dir={}", log_dir.display()),
            format!("--broker-id={}", case.node_id),
            format!("--process-roles={}", case.roles),
            format!("--listen-addr={client}"),
            format!("--controller-listen-addr={controller}"),
        ];
        args.extend(
            case.voters
                .map(|voters| format!("--controller-quorum-voters={voters}")),
        );
        let process = Process::spawn(&args);
        observed.push(Observed {
            roles: case.roles,
            started: process.wait_until_started(),
            client_port: probe(&runtime, client),
            controller_port: probe(&runtime, case.controller),
        });
        expected.push(Observed {
            roles: case.roles,
            started: Started {
                listen_addr: case.names.0.then(|| client.to_string()),
                controller_listen_addr: case.names.1.then(|| controller.clone()),
            },
            client_port: case.ports.0,
            controller_port: case.ports.1,
        });
        running.push(process);
    }
    drop(running);

    assert!(observed == expected);
}
