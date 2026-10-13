//! Per-item gating preserves trait declarations, methods, imports, exports and bodies.

#[krabka_macros::sendfile_cfg]
use std::os::fd::AsFd as _;

trait Sink {
    #[krabka_macros::sendfile_cfg]
    fn writable_value(&self, value: i32) -> i32;
}

struct Stream;

impl Sink for Stream {
    #[krabka_macros::sendfile_cfg]
    fn writable_value(&self, value: i32) -> i32 {
        if value > 0 {
            value
                .checked_add(1)
                .expect("incrementing the writable test value must fit in i32")
        } else {
            0
        }
    }
}

#[krabka_macros::sendfile_cfg]
mod platform {
    /// A file-backed operation remains visible through its gated re-export.
    pub(crate) fn file_value(file: &std::fs::File) -> i32 {
        use {
            super::{Sink as _, Stream},
            std::os::fd::AsFd as _,
        };

        let _fd = file.as_fd();
        Stream.writable_value(4)
    }
}

#[krabka_macros::sendfile_cfg]
pub(crate) use platform::file_value;

// The original cfg still removes the entire item, including unresolved bodies.
#[krabka_macros::sendfile_cfg]
#[cfg(any())]
fn excluded_item() -> UnknownPlatformType { unavailable_operation() }

#[test]
fn portable_items_remain_available() {
    let stream = Stream;
    std::hint::black_box(&stream as &dyn Sink);
}

#[krabka_macros::sendfile_cfg]
#[test]
fn supported_platforms_keep_methods_imports_and_exports() {
    let file = std::fs::File::open(std::env::current_exe().unwrap()).unwrap();
    let _fd = file.as_fd();
    assert2::assert!(file_value(&file) == 5);
    assert2::assert!(Stream.writable_value(0) == 0);
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
)))]
mod unsupported {
    use super::{Sink, Stream};
    // If the attribute fails to remove any of these items, this target fails to compile.
    #[krabka_macros::sendfile_cfg]
    use unavailable_platform::FileRegion;

    #[krabka_macros::sendfile_cfg]
    fn unsupported_operation() -> MissingFileRegion { unreachable_platform_operation() }

    #[test]
    fn excluded_methods_leave_a_valid_portable_trait() {
        let stream = Stream;
        std::hint::black_box(&stream as &dyn Sink);
    }
}
