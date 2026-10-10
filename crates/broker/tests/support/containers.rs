//! Addressing and fixture paths for the suites that drive Kafka containers.
//!
//! A suite that runs a real Kafka container needs three things from here: ports
//! that a concurrent run has not already taken, a container name that a
//! concurrent run has not already taken, and the directory its fixtures were
//! staged in, which is not the same under Cargo and under Bazel.

/// A free TCP port on the loopback interface.
///
/// Bound and immediately dropped, so the port is free when the caller binds it.
/// That leaves a window in which something else could take it; the alternative
/// is a fixed port, which is not a window but a certainty whenever two tests run
/// at once.
#[must_use]
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

/// Runs a Docker fixture command and returns its trimmed stdout.
pub fn docker(args: &[&str]) -> String {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn docker {args:?}: {e}"));
    assert2::assert!(
        out.status.success(),
        "docker {args:?} exited {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// Resolve Docker's `Gateway|Subnet` pair, including daemons that omit the gateway.
/// Docker assigns the bridge the subnet's first address in that case.
pub fn bridge_gateway(rendered: &str) -> Option<String> {
    let (gateway, subnet) = rendered.trim().split_once('|')?;
    if gateway.parse::<std::net::IpAddr>().is_ok() {
        return Some(gateway.to_owned());
    }
    let (base, _prefix) = subnet.split_once('/')?;
    let base: std::net::Ipv4Addr = base.parse().ok()?;
    let first = u32::from(base).checked_add(1)?;
    Some(std::net::Ipv4Addr::from(first).to_string())
}

/// Capture a Docker tool's output after delivering its optional complete stdin.
pub fn docker_output(
    command: &mut std::process::Command,
    stdin: Option<&str>,
    write_context: &str,
) -> std::process::Output {
    use std::{io::Write as _, process::Stdio};

    match stdin {
        None => command
            .stdin(Stdio::null())
            .output()
            .expect("spawn docker run"),
        Some(text) => {
            let mut child = command
                .stdin(Stdio::piped())
                .spawn()
                .expect("spawn docker run");
            child
                .stdin
                .as_mut()
                .expect("the container has a piped stdin")
                .write_all(text.as_bytes())
                .expect(write_context);
            drop(child.stdin.take());
            child.wait_with_output().expect("wait for docker run")
        }
    }
}

/// Listen and advertised addresses for a broker the JVM containers talk to.
///
/// The suites that drive real Kafka containers used to hard-code `9092`/`9093`,
/// which is why they had to run one at a time: two of them, or two tests inside
/// one of them, would race for the same port and the loser reported `Address
/// already in use` as a test failure. Each caller gets its own pair now.
///
/// `advertised` keeps the `host.docker.internal` name. Containers resolve it
/// through `--add-host=host.docker.internal:host-gateway`; the host resolves it
/// through an `/etc/hosts` entry pointing at loopback, which CI adds before
/// running these suites.
pub struct JvmListeners {
    /// What the broker binds, e.g. `0.0.0.0:41551`.
    pub listen: String,
    /// What it advertises and what the containers bootstrap against.
    pub advertised: String,
    /// The controller listener, on its own port.
    pub controller: String,
}

impl JvmListeners {
    /// Allocate a fresh set.
    #[must_use]
    pub fn allocate() -> Self {
        let client = free_port();
        let controller = free_port();
        Self {
            listen: format!("0.0.0.0:{client}"),
            advertised: format!("host.docker.internal:{client}"),
            controller: format!("0.0.0.0:{controller}"),
        }
    }

    /// The controller as containers address it.
    #[must_use]
    pub fn controller_advertised(&self) -> String {
        let port = self
            .controller
            .rsplit(':')
            .next()
            .expect("controller addr has a port");
        format!("host.docker.internal:{port}")
    }
}

/// Listeners shared by the tests in a single JVM test binary.
pub fn jvm_listeners() -> &'static JvmListeners {
    static LISTENERS: std::sync::OnceLock<JvmListeners> = std::sync::OnceLock::new();
    LISTENERS.get_or_init(JvmListeners::allocate)
}

/// Host-loopback address for native clients in a JVM test binary.
pub fn jvm_client_addr() -> &'static str {
    static ADDRESS: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ADDRESS.get_or_init(|| jvm_listeners().listen.replace("0.0.0.0", "127.0.0.1"))
}

/// Read the finalized level from the named feature's own CLI output line.
/// Returns `None` if the feature is absent or shows no finalized level.
pub fn jvm_finalized_level(stdout: &str, feature: &str) -> Option<i64> {
    let line = stdout
        .lines()
        .find(|line| line.contains(&format!("Feature: {feature}")))?;
    let (_, level) = line.split_once("FinalizedVersionLevel:")?;
    level.split_whitespace().next()?.parse().ok()
}

/// Boot a single host broker using this test binary's listeners.
pub async fn start_jvm_single(
    default_filter: &str,
    adjust: impl FnOnce(&mut krabka_broker::BrokerConfig),
) -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    init_jvm_tracing(default_filter);
    let listeners = jvm_listeners();
    let dir = tempfile::tempdir().expect("broker directory");
    let mut config = jvm_single_broker_config(
        dir.path().to_path_buf(),
        crate::support::JvmSingleBrokerSetup {
            listen: &listeners.listen,
            advertised: &listeners.advertised,
            controller: &listeners.controller,
        },
    );
    adjust(&mut config);
    let broker = krabka_broker::Broker::start(config)
        .await
        .expect("start broker");
    eprintln!(
        "KRABKA[test] broker started listen={} advertised={}",
        listeners.listen, listeners.advertised
    );
    (broker, dir)
}

/// Initialize tracing with the suite's default filter.
pub fn init_jvm_tracing(default_filter: &str) {
    super::init_tracing_with(default_filter);
}

use krabka_broker::NodeId;

#[derive(krabka_macros::FieldDefaults)]
pub struct JvmBrokerSetup {
    #[default(NodeId(1))]
    pub node: NodeId,
    #[default("127.0.0.1:0".parse().expect("loopback listener"))]
    pub listen: std::net::SocketAddr,
    #[default("127.0.0.1:0".parse().expect("loopback controller"))]
    pub controller: std::net::SocketAddr,
    #[default("127.0.0.1:0".into())]
    pub advertised: String,
    #[default(vec![(NodeId(1), "127.0.0.1:0".into())])]
    pub voters: Vec<(NodeId, String)>,
}

/// Common configuration for host brokers addressed by Kafka containers.
pub fn jvm_broker_config(
    log_dir: std::path::PathBuf,
    setup: JvmBrokerSetup,
) -> krabka_broker::BrokerConfig {
    let quorum_size = setup.voters.len();
    krabka_broker::BrokerConfig {
        broker_id: i32::try_from(setup.node.0).expect("broker id"),
        listen_addr: setup.listen,
        advertised_listener: setup.advertised,
        log_dir,
        log_config: krabka_log::LogConfig::default(),
        node_id: setup.node,
        controller_listen_addr: setup.controller,
        controller_quorum_voters: setup.voters,
        heartbeat_interval: krabka_units::millis(3_000),
        heartbeat_timeout: krabka_units::millis(9_000),
        replica_lag_time_max: krabka_units::millis(30_000),
        controller_election_timeout: krabka_units::secs(5),
        controller_heartbeat_interval: krabka_units::millis(500),
        bootstrap_mode: krabka_broker::BootstrapMode::Bootstrap,
        ..krabka_broker::BrokerConfig::default().with_internal_topics_for(quorum_size)
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct JvmSingleBrokerSetup<'a> {
    #[default("127.0.0.1:0")]
    pub listen: &'a str,
    #[default("127.0.0.1:0")]
    pub advertised: &'a str,
    #[default("127.0.0.1:0")]
    pub controller: &'a str,
}

/// Common single-voter configuration, with listener strings supplied by a suite.
pub fn jvm_single_broker_config(
    log_dir: std::path::PathBuf,
    setup: JvmSingleBrokerSetup<'_>,
) -> krabka_broker::BrokerConfig {
    let controller: std::net::SocketAddr = setup.controller.parse().expect("controller address");
    jvm_broker_config(
        log_dir,
        JvmBrokerSetup {
            listen: setup.listen.parse().expect("client address"),
            controller,
            advertised: setup.advertised.into(),
            voters: vec![(NodeId(1), controller.to_string())],
            ..Default::default()
        },
    )
}

/// Prepare a tool container with Docker options kept in the caller's order.
pub fn docker_tool_command(image: &str, options: &[&str]) -> std::process::Command {
    use std::process::{Command, Stdio};

    let mut command = Command::new("docker");
    command
        .args(["run", "--rm"])
        .args(options)
        .arg(image)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

/// Prepare a disposable tool container that can reach the host broker.
#[derive(Clone, Copy, Default)]
pub enum ContainerInput {
    #[default]
    Closed,
    Attached,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct JvmDockerSetup<'a> {
    #[default("mirror.gcr.io/apache/kafka:4.3.1")]
    pub image: &'a str,
    pub mounts: &'a [&'a str],
    pub args: &'a [&'a str],
    pub input: ContainerInput,
}

pub fn jvm_docker_command(setup: JvmDockerSetup<'_>) -> std::process::Command {
    let mut options = vec!["--add-host=host.docker.internal:host-gateway"];
    if matches!(setup.input, ContainerInput::Attached) {
        options.push("-i");
    }
    for mount in setup.mounts {
        options.extend(["-v", mount]);
    }
    let mut command = docker_tool_command(setup.image, &options);
    command.args(setup.args);
    command
}

/// Capture both log streams for a container-readiness diagnostic.
pub fn docker_logs(name: &str) -> String {
    let out = std::process::Command::new("docker")
        .args(["logs", name])
        .output()
        .expect("spawn docker logs");
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// Best-effort cleanup, including containers left by a failed test.
pub fn remove_container(name: &str) {
    let _ = std::process::Command::new("docker")
        .args(["rm", "-f", name])
        .output();
}

/// Best-effort cleanup of a container and its anonymous volumes.
pub fn remove_container_with_volumes(name: &str) {
    let _ = std::process::Command::new("docker")
        .args(["rm", "--force", "--volumes", name])
        .output();
}

/// Nonempty, trimmed CLI rows in their original order.
pub fn jvm_output_lines(output: &std::process::Output) -> Vec<String> {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect()
}

#[derive(krabka_macros::FieldDefaults)]
pub struct JvmStaticVoterSetup {
    pub broker: JvmBrokerSetup,
    pub cluster_id: uuid::Uuid,
}

/// A test-default voter in a named static quorum shared with a JVM node.
pub fn jvm_static_voter_config(
    log_dir: &std::path::Path,
    setup: JvmStaticVoterSetup,
) -> krabka_broker::BrokerConfig {
    let mut config = krabka_broker::BrokerConfig::for_tests(log_dir.to_path_buf());
    config.broker_id = i32::try_from(setup.broker.node.0).expect("broker id");
    config.node_id = setup.broker.node;
    config.listen_addr = setup.broker.listen;
    config.advertised_listener = setup.broker.advertised;
    config.controller_listen_addr = setup.broker.controller;
    // The lowest 100 directory ids are reserved by Kafka.
    config.directory_id = uuid::Uuid::from_u64_pair(1, config.node_id.0);
    config.bootstrap_mode = krabka_broker::BootstrapMode::Bootstrap;
    config.controller_quorum_voters = setup.broker.voters;
    config.auto_join = false;
    config.bootstrap_servers = vec![];
    config.cluster_id = Some(setup.cluster_id);
    config
}

/// Format a voter at the Kafka release supported by the oldest quorum member.
pub async fn format_jvm_voter(
    log_dir: &std::path::Path,
    cluster_id: &str,
    node: &krabka_broker::BrokerConfig,
) {
    let argv = vec![
        "krabka-format".to_string(),
        "--log-dir".to_string(),
        log_dir.to_str().unwrap().to_string(),
        "--cluster-id".to_string(),
        cluster_id.to_string(),
        "--node-id".to_string(),
        node.node_id.0.to_string(),
        "--directory-id".to_string(),
        node.directory_id.to_string(),
        "--release-version".to_string(),
        "4.0".to_string(),
    ];
    let code = krabka_format::run_from_args(argv).await;
    assert2::assert!(code == 0, "krabka-format exited {code}");
}

/// Run a JVM tool while leaving success and output assertions to its caller.
pub fn jvm_docker_run(image: &str, args: &[&str]) -> std::process::Output {
    let out = jvm_docker_command(crate::support::JvmDockerSetup {
        image,
        args,
        ..Default::default()
    })
    .output()
    .expect("docker run");
    eprintln!(
        "KRABKA[test] docker {image} {args:?} status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Write a console tool's complete input and capture its output.
pub fn jvm_stdin_output(
    command: &mut std::process::Command,
    payload: &[u8],
) -> std::process::Output {
    use std::{io::Write, process::Stdio};

    let mut child = command
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn JVM tool");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(payload)
        .expect("write stdin");
    drop(child.stdin.take());
    child.wait_with_output().expect("wait JVM tool")
}

/// Start all static voters concurrently on the suite's fixed port pairs.
pub async fn start_jvm_cluster<const N: usize>(
    client_ports: [u16; N],
    controller_ports: [u16; N],
    adjust: impl Fn(&mut krabka_broker::BrokerConfig),
) -> Vec<(krabka_broker::BrokerHandle, tempfile::TempDir)> {
    let voters: Vec<_> = controller_ports
        .iter()
        .enumerate()
        .map(|(index, port)| {
            (
                u64::try_from(index + 1).expect("node id"),
                format!("127.0.0.1:{port}")
                    .parse()
                    .expect("controller address"),
            )
        })
        .collect();
    let starts: Vec<_> = client_ports
        .iter()
        .enumerate()
        .map(|(index, port)| {
            let dir = tempfile::tempdir().expect("broker directory");
            let mut config = jvm_broker_config(
                dir.path().to_path_buf(),
                crate::support::JvmBrokerSetup {
                    node: krabka_broker::NodeId(voters[index].0),
                    listen: format!("0.0.0.0:{port}").parse().expect("client address"),
                    controller: format!("0.0.0.0:{}", controller_ports[index])
                        .parse()
                        .expect("controller address"),
                    advertised: format!("host.docker.internal:{port}"),
                    voters: crate::support::controller_voters(&voters),
                },
            );
            adjust(&mut config);
            (
                tokio::spawn(async move {
                    krabka_broker::Broker::start(config)
                        .await
                        .expect("broker start")
                }),
                dir,
            )
        })
        .collect();
    let mut brokers = Vec::with_capacity(N);
    for (start, dir) in starts {
        brokers.push((start.await.expect("broker start task"), dir));
    }
    brokers
}

/// A container name unlikely to collide with a concurrent run.
///
/// `docker run --name` fails outright when the name is taken, so a fixed name is
/// a second reason these suites could not overlap -- and a stale container from
/// a killed run blocks every later run until someone removes it by hand.
#[must_use]
pub fn unique_container_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// This crate's directory, wherever the test is running from.
///
/// Cargo exports `CARGO_MANIFEST_DIR` to a test process, so under Cargo this is
/// the path `env!` would have produced. It is read rather than expanded because
/// `env!` bakes an absolute build path into the binary, which ties the test to
/// the directory it was compiled in -- `rules_rust` rejects such a binary
/// outright, and under Cargo it only works when launched from that same path.
///
/// Bazel sets no such variable; it stages a target's `data` under
/// `$TEST_SRCDIR/$TEST_WORKSPACE/<package>`. Falling back to that is what lets
/// the TLS suites find their fixtures under both.
///
/// # Panics
///
/// Panics when neither Cargo's variable nor Bazel's pair is set, which means the
/// test was launched by something that stages fixtures differently again.
#[must_use]
pub fn manifest_dir() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("CARGO_MANIFEST_DIR") {
        return std::path::PathBuf::from(dir);
    }
    let srcdir = std::env::var("TEST_SRCDIR")
        .expect("CARGO_MANIFEST_DIR (cargo) or TEST_SRCDIR (bazel) must be set");
    let workspace =
        std::env::var("TEST_WORKSPACE").expect("TEST_WORKSPACE accompanies TEST_SRCDIR");
    std::path::PathBuf::from(srcdir)
        .join(workspace)
        .join("crates/broker")
}

/// A cache directory under the system temp dir whose name carries a digest of
/// the named `tests/fixtures/security/` files.
///
/// The JVM suites stage keystores there and reuse them across runs. Keying the
/// directory on the fixture bytes means a rotated certificate gets a fresh
/// directory instead of the stale artifact built from the old one.
///
/// # Panics
///
/// Panics when a fixture cannot be read.
#[must_use]
pub fn fixture_cache_dir(prefix: &str, fixtures: &[&str]) -> std::path::PathBuf {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for name in fixtures {
        let path = manifest_dir().join("tests/fixtures/security").join(name);
        std::fs::read(&path)
            .unwrap_or_else(|error| panic!("read fixture {}: {error}", path.display()))
            .hash(&mut hasher);
    }
    std::env::temp_dir().join(format!("{prefix}-{:016x}", hasher.finish()))
}

/// Run a blocking container command without occupying a broker runtime worker.
pub async fn docker_run_blocking(args: Vec<String>, context: &'static str) -> std::process::Output {
    tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(&args)
            .output()
            .unwrap_or_else(|error| panic!("{context}: {error}"))
    })
    .await
    .expect("docker run task")
}

/// Command prefix for authenticated admin tools mounted at the shared config path.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct JvmAdminSetup<'a> {
    #[default("mirror.gcr.io/apache/kafka:4.3.1")]
    pub image: &'a str,
    #[default("/tmp/krabka-config:/krabka-config")]
    pub mount: &'a str,
    #[default("/opt/kafka/bin/kafka-configs.sh")]
    pub tool: &'a str,
    #[default("host.docker.internal:9092")]
    pub bootstrap: &'a str,
}

pub fn jvm_admin_args(setup: JvmAdminSetup<'_>) -> Vec<String> {
    let JvmAdminSetup {
        image,
        mount,
        tool,
        bootstrap,
    } = setup;
    [
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
        "-v",
        mount,
        image,
        tool,
        "--bootstrap-server",
        bootstrap,
        "--command-config",
        "/krabka-config/admin.properties",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

/// Last `count` diagnostic lines, printed in the same order as the process.
pub fn print_log_tail(text: &str, count: usize) {
    for line in text
        .lines()
        .rev()
        .take(count)
        .collect::<Vec<_>>()
        .iter()
        .rev()
    {
        eprintln!("{line}");
    }
}

/// A finished process's stdout followed by stderr, including invalid UTF-8 replacement.
pub fn combined_output(output: &std::process::Output) -> String {
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    text
}

/// Host-bound listeners transferred directly into the broker to avoid port races.
pub async fn start_jvm_bound(
    log_dir: std::path::PathBuf,
    customize: impl FnOnce(&mut krabka_broker::BrokerConfig),
) -> (krabka_broker::BrokerHandle, String) {
    let data_plane = tokio::net::TcpListener::bind("0.0.0.0:0")
        .await
        .expect("bind data plane");
    let controller = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind controller");
    let port = data_plane.local_addr().expect("data plane addr").port();
    let bootstrap = format!("host.docker.internal:{port}");
    let controller_addr = controller.local_addr().expect("controller addr");
    let mut config = krabka_broker::BrokerConfig::for_tests(log_dir);
    customize(&mut config);
    config.listen_addr = data_plane.local_addr().expect("data plane addr");
    config.advertised_listener = bootstrap.clone();
    config.controller_listen_addr = controller_addr;
    config.controller_quorum_voters = vec![(config.node_id, controller_addr.to_string())];
    let handle =
        krabka_broker::Broker::start_with_listeners(config, Some(controller), [data_plane])
            .await
            .expect("broker start");
    handle.wait_until_controller_leader().await;
    (handle, bootstrap)
}

/// The long admin timeouts shared by tools whose broker heartbeat must keep running.
pub fn jvm_admin_config() -> &'static std::path::Path {
    static CONFIG: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let dir = tempfile::tempdir().expect("tempdir for command config");
            std::fs::write(
                dir.path().join("admin.properties"),
                "request.timeout.ms=120000\ndefault.api.timeout.ms=240000\n",
            )
            .expect("write command config");
            dir
        })
        .path()
}

/// Allocate a broker set's client ports before allocating any controller ports.
pub fn jvm_client_ports<const N: usize>() -> ([u16; N], [String; N], [String; N]) {
    let ports: [u16; N] = std::array::from_fn(|_| free_port());
    (
        ports,
        ports.map(|port| format!("0.0.0.0:{port}")),
        ports.map(|port| format!("host.docker.internal:{port}")),
    )
}

/// Container-reachable client endpoints, in the supplied broker order.
pub fn jvm_bootstrap_servers(ports: &[u16]) -> String {
    ports
        .iter()
        .map(|port| format!("host.docker.internal:{port}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Parse the final field of a stock kafka-get-offsets row.
pub fn jvm_parse_offset(line: &str) -> i64 {
    line.rsplit(':')
        .next()
        .and_then(|offset| offset.parse::<i64>().ok())
        .unwrap_or_else(|| panic!("kafka-get-offsets row is not an offset: {line}"))
}

/// Spawn an interactive JVM command with all three streams captured.
pub fn jvm_spawn_piped(command: &mut std::process::Command, context: &str) -> std::process::Child {
    use std::process::Stdio;
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect(context)
}

/// The shared acks-all console-producer command; callers retain their write/exit assertions.
pub fn jvm_acks_all_producer(image: &str, bootstrap: &str, topic: &str) -> std::process::Child {
    jvm_spawn_piped(
        &mut jvm_docker_command(crate::support::JvmDockerSetup {
            image,
            args: &[
                "kafka-console-producer",
                "--bootstrap-server",
                bootstrap,
                "--topic",
                topic,
                "--producer-property",
                "acks=all",
            ],
            input: crate::support::ContainerInput::Attached,
            ..Default::default()
        }),
        "spawn JVM producer",
    )
}

/// Capture a plain JVM tool while retaining the suite-specific log prefix.
pub fn jvm_tool_output(image: &str, args: &[&str], log_scope: &str) -> std::process::Output {
    let out = jvm_docker_command(crate::support::JvmDockerSetup {
        image,
        args,
        ..Default::default()
    })
    .output()
    .expect("spawn docker run");
    eprintln!(
        "KRABKA[{log_scope}] docker_run image={image} {args:?} status={} stderr_len={}",
        out.status,
        out.stderr.len()
    );
    out
}

/// Controller logs concatenated in stdout/stderr order and saved for diagnosis.
pub fn save_jvm_logs(container: &str, path: &str) -> String {
    let logs = std::process::Command::new("docker")
        .args(["logs", container])
        .output()
        .expect("docker logs");
    let text = combined_output(&logs);
    let _ = std::fs::write(path, &text);
    text
}

/// Execute a tool inside an existing container, retaining its output for the caller's assertions.
pub fn docker_exec(name: &str, args: &[&str]) -> std::process::Output {
    let mut full = vec!["exec", name];
    full.extend_from_slice(args);
    std::process::Command::new("docker")
        .args(&full)
        .output()
        .expect("spawn docker exec")
}

/// The ordered environment options for a single-node broker and controller oracle.
pub fn kafka_single_node_env_args() -> &'static [&'static str] {
    &[
        "-e",
        "KAFKA_NODE_ID=1",
        "-e",
        "KAFKA_PROCESS_ROLES=broker,controller",
        "-e",
        "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
        "-e",
        "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092",
        "-e",
        "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e",
        "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
        "-e",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "-e",
        "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "-e",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "-e",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "-e",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
    ]
}

/// Set the caller's exact Unix mode so a container user can access a mounted fixture.
#[cfg(unix)]
pub fn chmod_for_container(path: &std::path::Path, mode: u32, context: &str) {
    use std::os::unix::fs::PermissionsExt as _;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect(context);
}
