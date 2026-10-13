//! Constants remain public while guard tables preserve declaration order.

use assert2::assert;

mod codes {
    #[derive(Debug, PartialEq, Eq)]
    struct NamedCode {
        name: &'static str,
        code: i16,
    }

    krabka_macros::wire_codes! {
        release RELEASE {
            /// A successful reply.
            NONE = 0;
            UNKNOWN = -1;
            MAX = 32767;
            MIN = -32768;
        }
        trunk TRUNK {
            TOPOLOGY = 135;
            CONTROLLER = 136;
        }
        private PRIVATE {
            BUSY = 1000;
            REFUSED = 1013;
        }
    }

    #[test]
    fn private_guard_tables_keep_every_name_and_value_in_declared_order() {
        use assert2::assert;

        let tables: Vec<Vec<_>> = [RELEASE, TRUNK, PRIVATE]
            .into_iter()
            .map(|table| table.iter().map(|code| (code.name, code.code)).collect())
            .collect();
        assert!(
            tables
                == [
                    vec![
                        ("NONE", 0),
                        ("UNKNOWN", -1),
                        ("MAX", 32767),
                        ("MIN", -32768)
                    ],
                    vec![("TOPOLOGY", 135), ("CONTROLLER", 136)],
                    vec![("BUSY", 1000), ("REFUSED", 1013)],
                ]
        );
    }
}

#[test]
fn generated_constants_are_public_i16_values() {
    let release: [i16; 4] = [codes::NONE, codes::UNKNOWN, codes::MAX, codes::MIN];
    assert!(release == [0, -1, i16::MAX, i16::MIN]);
    assert!(
        [
            codes::TOPOLOGY,
            codes::CONTROLLER,
            codes::BUSY,
            codes::REFUSED
        ] == [135, 136, 1000, 1013]
    );
}
