//! Hands the crate's compilation the container-image digests `//MODULE.bazel`
//! pins, one `KRABKA_PINNED_IMAGE_<NAME>` variable per image.
//!
//! `src/codes/tests/kafka_error_table.rs` is a checked-in copy of Kafka's
//! `Errors` enum, extracted once from a pinned image. The guard beside it
//! compares the digest that copy records with the digest still pinned, so an
//! image bump fails the guard until the table is re-derived. Neither reads
//! `MODULE.bazel` at test time: the pin arrives as compile-time environment.
//!
//! A `MODULE.bazel` cannot `load()`, so a Bazel file cannot share the literal
//! either. `//bazel/images:pinned_digests` produces the same variables for the
//! Bazel build, from the same lines of the same file; a disagreement between
//! the two extractors is a build failure here rather than a silent pass, since
//! `env!` refuses to compile against a name that was never emitted.
//!
//! A downstream Cargo workspace that pulls this crate in as a git dependency
//! (`krabka-schema-registry`'s own dev-dependency on this crate, for example)
//! vendors only `crates/broker`, not the sibling `MODULE.bazel` two
//! directories up, so the file is absent there. The one reader of these
//! variables is the `#[cfg(test)]` module above, which a downstream build
//! never compiles, so a missing file in that position emits nothing rather
//! than failing the whole build.

use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR"),
    );
    let module_file = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("the crate directory is two levels below the workspace root")
        .join("MODULE.bazel");
    println!("cargo::rerun-if-changed={}", module_file.display());

    let source = match std::fs::read_to_string(&module_file) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => panic!("reading {}: {error}", module_file.display()),
    };

    let mut emitted = 0_usize;
    for ImagePin { name, digest } in source.lines().filter_map(pinned_image) {
        println!(
            "cargo::rustc-env=KRABKA_PINNED_IMAGE_{}={digest}",
            name.to_uppercase()
        );
        emitted += 1;
    }

    // Silence is the one answer that must not pass: a MODULE.bazel whose pins
    // moved out of the shape `pinned_image` reads would otherwise emit nothing
    // and leave every `env!` on those names to fail with no hint of why. This
    // only applies once the file was actually read; a file the build never
    // found takes the early return above instead.
    if emitted == 0 {
        println!(
            "cargo::error=no container-image pins found in {}",
            module_file.display()
        );
    }
}

/// The name and digest of one pinned container image.
struct ImagePin<'a> {
    name: &'a str,
    digest: &'a str,
}

/// The name and digest of one `pull_image` tuple, if this is one.
///
/// The tuples read `("name", "registry", "repository", "tag", "sha256:...")`,
/// so a line qualifies when it holds exactly five quoted fields and the last is
/// a digest. Anything else -- a comment, the loop header, an unrelated list --
/// is not a pin and is skipped.
fn pinned_image(line: &str) -> Option<ImagePin<'_>> {
    let fields: Vec<&str> = line.split('"').skip(1).step_by(2).collect();
    let [name, _registry, _repository, _tag, digest] = fields[..] else {
        return None;
    };
    digest
        .starts_with("sha256:")
        .then_some(ImagePin { name, digest })
}
