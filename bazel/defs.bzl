"""Bazel macros for this repository's Cargo workspace members.

Every knob these macros need — crate name, edition, feature set, dependency
labels, workspace lints — already exists in the `@crates` repo that
`crate.from_cargo` generates from `Cargo.toml`/`Cargo.lock`. Reading it from
there rather than restating it per crate keeps the BUILD files from drifting
away from the manifests Cargo still resolves.
"""

load("@crates//:data.bzl", "DEP_DATA")
load("@crates//:defs.bzl", "all_crate_deps", "crate_name", "edition")
load("@package_metadata//rules:package_metadata.bzl", "package_metadata")
load("@rules_rs//rs:rust_binary.bzl", "rust_binary")
load("@rules_rs//rs:rust_library.bzl", "rust_library")
load("@rules_rs//rs:rust_proc_macro.bzl", "rust_proc_macro")
load("@rules_rs//rs:rust_test.bzl", "rust_test")
load("@rules_rs_mutants//mutants:cargo_mutants_test.bzl", "cargo_mutants_test")
load("@rules_rust//rust:defs.bzl", "rust_doc", "rust_doc_test")
load("@rules_shell//shell:sh_test.bzl", "sh_test")
load("//bazel:krabka_image.bzl", "KRABKA_IMAGE_TAG", "KRABKA_IMAGE_TAR")
load("//tools/lint:linters.bzl", "clippy_test")

def docker_test_exec_properties(recycling_key):
    """Execution properties that put a container-driven suite on a microVM.

    The suites need a Docker daemon, which is why they have always run on a
    machine that already had one. A BuildBuddy executor can supply it instead:
    `init-dockerd` starts a daemon inside a Firecracker microVM before the
    action runs, so the test needs nothing from the machine that invoked Bazel.

    Every key carries the `test.` prefix -- Bazel's built-in `test` exec group
    -- so it reaches the test action alone and the actions that build the test
    binary keep the default execution platform. See the `docker-rbe` block in
    //.bazelrc for why those stay on the local machine.

    Ignored outright when there is no remote executor, so a developer running
    `bazel test --config=docker` is unaffected.

    Args:
        recycling_key: what makes two suites willing to share a recycled
            microVM -- in practice the set of images they load. Suites that
            name the same one find those images already loaded, so the
            `docker load` in //bazel:docker_test.sh is the no-op it counts on.
            Keyed rather than shared globally: a VM recycled into a suite that
            wants different images has to load them anyway, and carries the
            previous suite's disk to do it.

    Returns:
        A dict for a test rule's `exec_properties`.
    """
    return {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        # Two constraints pick this image, and only one of them was obvious.
        # `@llvm//:rbe_linux_x86_64` pins `ubuntu:22.04`, which carries no
        # Docker, so `init-dockerd` has nothing to start; a target's properties
        # beat the platform's, and only for this exec group, so the compile
        # actions still resolve against @llvm's. But BuildBuddy's *default*
        # executor image is Ubuntu 16.04, and these binaries are built against
        # `@llvm//constraints/libc:gnu.2.28`, so on that image a suite died
        # before it ran a line:
        #
        #     jvm_tiered_storage_docker_bin: /lib/x86_64-linux-gnu/libc.so.6:
        #     version `GLIBC_2.28' not found
        #
        # This one is Ubuntu 20.04 -- glibc 2.31, which satisfies that
        # constraint -- and installs docker-ce and containerd.io from
        # download.docker.com. Anything substituted here has to keep both
        # properties: `rbe-ubuntu22-04` is newer but ships no Docker.
        "test.container-image": "docker://gcr.io/flame-public/rbe-ubuntu20-04:latest",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "krabka-images:" + recycling_key,
        # A real Kafka cluster, its JVMs, and the suite's own broker. One
        # compute unit is one CPU and 2.5 GB, which none of these fit in.
        "test.EstimatedComputeUnits": "4",
        # The image tarballs are gigabytes before Docker unpacks them, and a
        # recycled VM holds the last suite's as well.
        "test.EstimatedFreeDiskBytes": "20000000000",
    }

# `[workspace.lints.rust] unsafe_code = "forbid"`. rules_rs 0.0.107 does not
# yet plumb Cargo lint tables into the Bazel build, and this is the one lint
# in that table whose guarantee must not lapse under a second build system.
# The clippy tables stay a Cargo-side gate: clippy runs as an aspect here, not
# as part of a normal build, and that aspect sees only Bazel targets. CI's
# `cargo` job runs `cargo clippy --workspace --all-targets` for what it cannot
# reach: the benches under `crates/broker/benches` and `crates/log/benches`,
# which have no target here at all; the test and binary targets `aspect lint`
# narrows away because a change did not touch them; and `krabka-protocol`'s
# build script, which //MODULE.bazel pins with `gen_build_script = "off"` and
# which therefore only runs under Cargo.
WORKSPACE_RUSTC_FLAGS = ["-Funsafe_code"]

# The root `Cargo.toml`'s `[workspace.package] version`. It reaches the release
# artifacts through the purl `crate_library` stamps below, so a release bump
# has to land here as well as in the manifest.
WORKSPACE_VERSION = "1.0.3"

def _features():
    return DEP_DATA[native.package_name()]["crate_features"]

def _aliases(kinds):
    """The renamed-dependency map, narrowed to one set of dependency kinds.

    `@crates//:defs.bzl`'s `aliases()` returns one map covering normal *and*
    dev dependencies. rules_rust treats every key of `aliases` as a dependency,
    so handing the whole map to a `rust_library` links that crate's dev
    dependencies into the library. Where two crates dev-depend on each other --
    `krabka-client-consumer` and `krabka-client-producer` do -- that is a
    dependency cycle Bazel refuses to build, and everywhere else it is dead
    weight. Cargo has no such problem: a lib and its test binaries are separate
    compilations.
    """
    data = DEP_DATA[native.package_name()]
    labels = {}
    for kind in kinds:
        for dep in data.get(kind, []):
            labels[dep] = True
        for platform_deps in data.get(kind + "_by_platform", {}).values():
            for dep in platform_deps:
                labels[dep] = True
    return {
        label: name
        for label, name in data["aliases"].items()
        if label in labels
    }

def crate_library(name, srcs = None, **kwargs):
    """`rust_library` for a workspace member, configured from Cargo metadata."""
    _crate(rust_library, name, srcs, **kwargs)

def crate_proc_macro(name, srcs = None, **kwargs):
    """`rust_proc_macro` for a `proc-macro = true` member, configured like `crate_library`.

    A crate that uses the macros takes this target through `all_crate_deps`
    like any other dependency; rules_rust sorts proc-macro crates out of `deps`.
    """
    _crate(rust_proc_macro, name, srcs, clippy = False, **kwargs)

def _crate(rule, name, srcs, clippy = True, **kwargs):
    """The target, SBOM metadata, Clippy test and rustdoc of one member's library.

    `clippy = False` leaves out the Bazel Clippy test. The rules_lint Clippy
    aspect writes no report for a `rust_proc_macro` target, so `lint_test` has
    nothing to read and fails analysis; `cargo clippy --workspace` still lints
    such a crate.
    """
    metadata = name + "_package_metadata"
    package_metadata(
        name = metadata,
        # The SBOM task checks this against Cargo's resolved version.
        purl = "pkg:cargo/%s@%s" % (crate_name().replace("_", "-"), WORKSPACE_VERSION),
    )
    rule(
        name = name,
        srcs = srcs if srcs != None else native.glob(
            ["src/**/*.rs"],
            exclude = ["src/bin/**"],
        ),
        aliases = _aliases(["deps"]),
        crate_features = _features(),
        crate_name = crate_name(),
        edition = edition(),
        package_metadata = [metadata],
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        visibility = ["//visibility:public"],
        deps = all_crate_deps(normal = True),
        **kwargs
    )

    # Clippy as a test, so `bazel test //...` gates on it the way `cargo clippy
    # -- -D warnings` used to. The aspect alone only writes a report: it is
    # `lint_test` that turns a finding into a failure.
    if clippy:
        clippy_test(
            name = name + "_clippy",
            srcs = [":" + name],
        )

    # `bazel build //crates/<x>:<x>_doc` renders this crate's rustdoc. The
    # rustdoc examples themselves are run by `crate_tests`, which emits a
    # `rust_doc_test`; this is the HTML.
    rust_doc(
        name = name + "_doc",
        crate = ":" + name,
        rustdoc_flags = ["-Dwarnings"],
    )

def crate_binary(name, crate_root, lib, tests = True, **kwargs):
    """`rust_binary` for a `[[bin]]` target that links its own crate's library.

    Args:
      name: the binary target name, matching Cargo's `[[bin]] name`.
      crate_root: the binary's entry point, e.g. `src/bin/broker.rs`.
      lib: the `crate_library` target in this package that it links.
      tests: emit a `rust_test` over the binary's own `#[cfg(test)]` module.
        `cargo test` runs those; without this they are simply not run.
      **kwargs: passed through to `rust_binary`.
    """

    # A binary too large for one file puts its modules in a directory named
    # after the entry point, the layout `crates/raft/src/server.rs` uses. Cargo
    # finds them from the `mod` declarations alone; Bazel needs them named. The
    # `crate_library` glob cannot supply them, because it excludes `src/bin/**`
    # to keep the binaries out of the library.
    module_srcs = native.glob(
        [crate_root[:-len(".rs")] + "/**/*.rs"],
        allow_empty = True,
    )

    rust_binary(
        name = name,
        srcs = [crate_root] + module_srcs,
        aliases = _aliases(["deps"]),
        crate_features = _features(),
        crate_root = crate_root,
        edition = edition(),
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        visibility = ["//visibility:public"],
        deps = all_crate_deps(normal = True) + [lib],
        **kwargs
    )

    if tests:
        rust_test(
            name = name + "_test",
            aliases = _aliases(["deps", "dev_deps"]),
            crate = ":" + name,
            crate_features = _features(),
            edition = edition(),
            rustc_flags = WORKSPACE_RUSTC_FLAGS,
            deps = all_crate_deps(normal_dev = True),
        )

def crate_bench(lib, benches = None, data = None, compile_data = None, env = {}, rustc_env = {}, rustc_flags = None):
    """`rust_binary` per `benches/*.rs`, so Bazel compiles and lints them.

    A criterion bench is declared `harness = false` and gets its `main` from
    `criterion_main!`, so it is a binary, not a test. rules_rust has no
    benchmark rule -- neither the pinned fork nor upstream defines one -- so
    `rust_binary` is the primitive, the same one `crates/parse-benches` uses
    for the benchmark it ships as a runnable target.

    Without this the bench sources are built by nothing: `crate_library`
    globs `src/**`, `crate_tests` globs `tests/*.rs`, and neither reaches
    `benches/`. They still compile under `cargo bench`, which `ci.yml` runs
    nightly, so a break there surfaces a day late and only on that job. Here
    `bazel build //...` compiles them and `clippy_test` gates them the way it
    gates every other target in the workspace.

    The benches are not run by `bazel test`. A criterion run measures
    wall-clock time over many iterations, which is neither hermetic nor quick,
    and the nightly `bench` job already runs them. `bazel run
    //crates/<x>:<stem>_bench` runs one on demand.

    Args:
      lib: the `crate_library` target name in this package.
      benches: bench stems to wire, defaulting to every `benches/*.rs`.
      data: runtime files the bench needs.
      compile_data: files reachable from `include!`/`include_str!`.
      env: runtime environment for the bench binary.
      rustc_env: extra compile-time environment.
      rustc_flags: replaces `WORKSPACE_RUSTC_FLAGS`. Only a crate whose
        `Cargo.toml` opts out of the workspace lints needs it, so that Bazel
        applies the same rules Cargo does. Leave it unset otherwise.
    """
    srcs = native.glob(["benches/*.rs"], allow_empty = True)
    if benches != None:
        wanted = {stem: True for stem in benches}
        srcs = [s for s in srcs if s[len("benches/"):-len(".rs")] in wanted]

    # A bench too large for one file puts its modules alongside it, the way an
    # integration suite puts helpers under `tests/`. Name them for Bazel; Cargo
    # finds them from the `mod` declarations alone.
    helpers = native.glob(
        ["benches/**/*.rs"],
        exclude = ["benches/*.rs"],
        allow_empty = True,
    )

    for src in srcs:
        stem = src[len("benches/"):-len(".rs")]
        name = stem + "_bench"
        rust_binary(
            name = name,
            srcs = [src] + helpers,
            crate_root = src,
            # Dev dependencies, because criterion is one. This crate reaches it
            # under a renamed key -- `codspeed-criterion-compat` published as
            # `criterion` -- which is exactly what the alias map carries.
            aliases = _aliases(["deps", "dev_deps"]),
            compile_data = compile_data or [],
            crate_features = _features(),
            data = data or [],
            edition = edition(),
            env = env,
            rustc_env = rustc_env,
            rustc_flags = WORKSPACE_RUSTC_FLAGS if rustc_flags == None else rustc_flags,
            visibility = ["//visibility:public"],
            # One call, not two concatenated: a bench links the crate's normal
            # *and* dev dependencies, and `all_crate_deps` dedupes a package
            # listed in both tables.
            deps = all_crate_deps(normal = True, normal_dev = True) + [":" + lib],
        )

        clippy_test(
            name = name + "_clippy",
            srcs = [":" + name],
        )

def crate_tests(
        lib,
        data = None,
        compile_data = None,
        cpu_heavy = [],
        docker = {},
        krabka_image = [],
        env = {},
        rustc_env = {},
        unit_rustc_env_files = [],
        manual = [],
        no_harness = [],
        integration_timeouts = {},
        doc_tests = True,
        doc_test_dev_deps = True,
        mutants = True,
        mutants_jobs = 4,
        mutants_shards = 8,
        mutants_timeout = "long",
        unit_tags = []):
    """Unit tests, one target per `tests/*.rs`, and a mutation sweep.

    Args:
      lib: the `crate_library` target name in this package.
      data: runtime files every integration test gets (fixtures, corpora).
      compile_data: files reachable from `include!`/`include_str!` at compile time.
      env: runtime environment for every test target in the package.
      rustc_env: extra compile-time environment, e.g. `CARGO_MANIFEST_DIR`.
      unit_rustc_env_files: `NAME=value` files whose contents become compile-time
        environment for the unit-test target only -- the Bazel counterpart of a
        `cargo::rustc-env` line from a build script. Unit tests alone, because
        the values a build produces are for `src/`; an integration suite that
        needs one can be given `rustc_env` instead.
      krabka_image: test stems that additionally run brokers as real processes
        out of //packaging's broker image. The image's tarball joins whatever
        `docker` names for the stem, so //bazel:docker_test.sh loads it before
        the suite starts, and `KRABKA_BROKER_IMAGE` tells the suite which tag to
        `docker run`. A stem may appear here without a `docker` entry, which is
        a suite that needs the broker image and no Kafka one.
      manual: test stems to tag `manual` — Docker-driven or otherwise
        non-hermetic suites, the Bazel equivalent of their `#[ignore]`.
      no_harness: test stems declared `harness = false` in Cargo.toml.
      integration_timeouts: Bazel timeout overrides keyed by integration-test stem.
      doc_tests: whether to emit a `rust_doc_test`. `cargo test` runs rustdoc
        examples; without this they are simply not run.
      doc_test_dev_deps: whether doctests need the crate's dev dependencies.
      mutants: whether to emit a `cargo_mutants_test` over unit and ordinary
        integration tests.
      mutants_jobs: mutants built and tested concurrently within one shard.
      mutants_shards: Bazel shards the sweep is split across.
      mutants_timeout: Bazel timeout for one shard of the sweep.
      unit_tags: extra tags for the unit-test target.
    """
    unit = lib + "_test"

    # The stems that get a Docker wrapper: those with images to load, plus those
    # that need only the broker image. One dict so the rest of this macro asks a
    # single question.
    docker_stems = dict(docker)
    for stem in krabka_image:
        docker_stems.setdefault(stem, [])
    integration_srcs = native.glob(["tests/*.rs"], allow_empty = True)
    integration_stems = [src[len("tests/"):-len(".rs")] for src in integration_srcs]
    rust_test(
        name = unit,
        aliases = _aliases(["deps", "dev_deps"]),
        crate = ":" + lib,
        compile_data = compile_data or [],
        crate_features = _features(),
        data = data or [],
        edition = edition(),
        env = env,
        rustc_env = rustc_env,
        rustc_env_files = unit_rustc_env_files,
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        tags = unit_tags,
        deps = all_crate_deps(normal_dev = True),
    )

    if doc_tests:
        rust_doc_test(
            name = lib + "_doc_test",
            crate = ":" + lib,
            deps = all_crate_deps(normal_dev = True) if doc_test_dev_deps else all_crate_deps(normal = True),
        )

    if mutants:
        # `manual`: a full sweep rebuilds the crate once per mutant, so it runs
        # from the dedicated workflow rather than on every `bazel test //...`.
        cargo_mutants_test(
            name = lib + "_mutants",
            # Every mutant is a full rebuild of the crate plus a test run, so a
            # sweep is long by nature -- but it must still be bounded. `eternal`
            # is 3600s per shard, which for the small crates is not a timeout at
            # all, it is an hour of a runner before anyone learns something is
            # wrong. `long` is 900s; crates that legitimately need more say so.
            timeout = mutants_timeout,
            integration_tests = [
                ":" + stem + "_test"
                for stem in integration_stems
                if stem not in manual and stem not in docker_stems
            ],
            jobs = mutants_jobs,
            library = ":" + lib,
            shard_count = mutants_shards,
            tags = ["manual"],
            test = ":" + unit,
        )

    # Shared helper modules live in `tests/<name>/mod.rs` and are declared with
    # `mod <name>;` by whichever suites need them, so every integration test
    # gets them in `srcs` and names its own file as the crate root.
    helpers = native.glob(
        ["tests/**/*.rs"],
        exclude = ["tests/*.rs"],
        allow_empty = True,
    )

    for src in integration_srcs:
        stem = src[len("tests/"):-len(".rs")]
        rust_test(
            name = stem + "_test",
            args = ["--ignored", "--test-threads=1"] if stem in manual or stem in docker_stems else [],
            srcs = [src] + helpers,
            crate_root = src,
            aliases = _aliases(["deps", "dev_deps"]),
            compile_data = compile_data or [],
            crate_features = _features(),
            data = data or [],
            edition = edition(),
            env = env,
            rustc_env = rustc_env,
            rustc_flags = WORKSPACE_RUSTC_FLAGS,
            # `flaky`: these assert on wall-clock behaviour, so a loaded runner
            # can fail them without the code being wrong. `cpu:4` keeps Bazel
            # from creating that load locally, but a 4-core CI runner has no
            # headroom to give. A retry separates a timing hiccup from a real
            # break, and Bazel reports the result as FLAKY rather than passing it
            # off as a clean run -- so the flakiness stays visible instead of
            # being hidden by a `#[ignore]`.
            flaky = stem in cpu_heavy,
            timeout = integration_timeouts.get(stem),
            tags = (["manual"] if stem in manual or stem in docker_stems else []) +
                   (["cpu:4", "timing-sensitive"] if stem in cpu_heavy else []),
            use_libtest_harness = stem not in no_harness,
            # One call, not two concatenated: an integration test links the
            # crate's normal *and* dev dependencies, and several crates list the
            # same package in both tables. `all_crate_deps` merges the two specs
            # through a set, so asking for both at once dedupes them.
            deps = all_crate_deps(normal = True, normal_dev = True) + [":" + lib],
        )

        if stem not in docker_stems:
            continue

        # The same sources built again for the digest-pinned Docker wrapper.
        # `manual` keeps this backing test out of `bazel test //...`.
        rust_test(
            name = stem + "_docker_bin",
            args = ["--ignored", "--test-threads=1"],
            srcs = [src] + helpers,
            crate_root = src,
            aliases = _aliases(["deps", "dev_deps"]),
            compile_data = compile_data or [],
            crate_features = _features(),
            data = data or [],
            edition = edition(),
            env = env,
            rustc_env = rustc_env,
            rustc_flags = WORKSPACE_RUSTC_FLAGS,
            tags = ["manual"],
            use_libtest_harness = stem not in no_harness,
            deps = all_crate_deps(normal = True, normal_dev = True) + [":" + lib],
        )

        image_tars = ["//bazel/images:%s_tar" % image for image in docker_stems[stem]]
        krabka_broker_image = stem in krabka_image
        if krabka_broker_image:
            image_tars = image_tars + [KRABKA_IMAGE_TAR]

        docker_env = dict(env)
        docker_env["JAVA_HOME"] = "$(JAVABASE)"
        docker_env["KRABKA_IMAGE_TARS"] = ":".join([
            "$(rootpath %s)" % tar
            for tar in image_tars
        ])
        if krabka_broker_image:
            # Which tag the loaded broker image answers to. The suite carries the
            # same string as its fallback, so this is only worth setting where it
            # is true. See //bazel/krabka_image.bzl.
            docker_env["KRABKA_BROKER_IMAGE"] = KRABKA_IMAGE_TAG

        # The Docker daemon is the one thing here Bazel cannot own, so it is the
        # one thing left undeclared: `no-sandbox` for the socket, and `external`
        # so a pass is never cached against inputs that do not describe the
        # daemon's state. Everything else -- the image bytes above all -- is a
        # declared, digest-pinned input rather than a mid-test network fetch.
        sh_test(
            name = stem + "_docker_test",
            size = "enormous",
            # These form real Kafka clusters and assert that a leader is elected
            # and an ISR populated inside a timeout. On a loaded runner that can
            # miss without the code being wrong -- `jvm_kip320_divergence` failed
            # one job and passed another on the same commit, with `Leader: none`
            # and an empty ISR. A retry separates that from a real break, and
            # Bazel reports FLAKY rather than PASSED, so it stays visible.
            flaky = True,
            srcs = ["//bazel:docker_test.sh"],
            args = ["$(rootpath :%s_docker_bin)" % stem],
            # The caller's `data` comes along because its `env` may name those
            # labels in a `$(rootpath)`, which only resolves for a declared
            # prerequisite of this rule.
            data = [":%s_docker_bin" % stem] + image_tars + (data or []) + [
                "@rules_java//toolchains:current_java_runtime",
            ],
            toolchains = ["@rules_java//toolchains:current_java_runtime"],
            # JAVA_HOME points the wrapper at the hermetic JDK, which it puts on
            # PATH. `jvm_tiered_storage` shells out to `java` directly.
            env = docker_env,
            tags = [
                "docker",
                "external",
                "no-sandbox",
                # A suite that runs the broker image inherits
                # //packaging:image_docker_test's constraint: on a BuildBuddy
                # microVM `image_load` reports success and `docker run` then
                # cannot find the image, because the loader and the daemon do
                # not share an image store there. Unresolved, and the same wall
                # this would hit under `--config=docker-rbe`, so keep these on
                # the invoking machine. The Kafka-only suites are unaffected.
            ] + (["no-remote-exec"] if krabka_broker_image else []),
            # The daemon the comment above calls the one thing Bazel cannot
            # own is the one thing a BuildBuddy executor can. See
            # `docker_test_exec_properties`.
            exec_properties = docker_test_exec_properties(
                ",".join(sorted(docker_stems[stem])),
            ),
        )
