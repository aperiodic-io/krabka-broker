//! Java's Unicode case rules for `(?iu)`, as explicit sets of code points.
//!
//! `fancy_regex` folds Unicode case with the simple case folding of its `i`
//! flag, and it applies that fold to a whole class or a whole reference. Java
//! compares a different relation: `CASE_INSENSITIVE` with `UNICODE_CASE` makes
//! a character `ch` match a pattern character when `Character.toLowerCase` of
//! `Character.toUpperCase` of the two agree. The relations differ for a few
//! letters, and a class has no way to leave a member out of the fold. So the
//! rewrite never turns `i` on. It writes the set that Java's predicate accepts
//! as code point ranges, and the class that holds them is case sensitive.
//!
//! The predicates are those of `java.util.regex.Pattern` in JDK 17:
//!
//! - `single` and `SingleU` for a character outside a class, and for a class
//!   member that is not in Latin-1.
//! - `BitClass.add` for a class member in Latin-1, except the ten characters
//!   that `bitsOrSingle` sends to `single` instead.
//! - `CIRangeU` for a range in a class.
//!
//! `\w`, `\p{Lower}` and the other classes that Java reads as ASCII are not
//! here. Java does not fold them, and with `i` off neither does the rewrite.
//!
//! The case mapping is Java's simple one, `UnicodeData.txt` without
//! `SpecialCasing.txt`. `char::to_uppercase` and `char::to_lowercase` give the
//! full mapping, so a mapping of more than one char counts as none, apart from
//! the few that have a simple mapping under a longer full one. The table is
//! Rust's version of Unicode, which is newer than the one in JDK 17, so it
//! also folds the letters that Unicode 14 and later added.

use std::{fmt::Write as _, sync::OnceLock};

/// An inclusive range of code points.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct CodeRange {
    pub(super) start: u32,
    pub(super) end: u32,
}

/// Inclusive code point ranges, in order, that neither overlap nor touch.
pub(super) type Ranges = Vec<CodeRange>;

/// A code point that Java's case mapping does not leave as it is.
#[derive(Debug, Clone, Copy)]
struct Mapping {
    code: u32,
    /// `Character.toUpperCase(code)`.
    upper: u32,
    /// `Character.toLowerCase(Character.toUpperCase(code))`, which `SingleU`,
    /// `SliceU` and `CIRangeU` compare.
    key: u32,
}

/// The first and last surrogate code points, which no `char` can be.
const SURROGATES: CodeRange = CodeRange {
    start: 0xD800,
    end: 0xDFFF,
};

/// A class that matches nothing, where a set has no member that a `char` can
/// be.
const NO_CODE_POINT: &str = "[^\\x{0}-\\x{10FFFF}]";

/// The Latin-1 characters that `Pattern.bitsOrSingle` does not put in a
/// `BitClass` under `(?iu)`, because a character outside Latin-1 folds to them.
const NOT_IN_BIT_CLASS: [u32; 10] = [0xFF, 0xB5, 0x49, 0x69, 0x53, 0x73, 0x4B, 0x6B, 0xC5, 0xE5];

/// The one char that `mapped`, a `char::to_uppercase` or `char::to_lowercase`,
/// yields, or `None` for a mapping to more than one.
fn only_char(mut mapped: impl Iterator<Item = char>) -> Option<char> {
    let first = mapped.next()?;
    mapped.next().is_none().then_some(first)
}

/// `Character.toUpperCase(int)`, the simple mapping of `UnicodeData.txt`.
fn simple_upper(code: u32) -> u32 {
    match code {
        // `SpecialCasing.txt` maps these to two chars, and `UnicodeData.txt`
        // to the title case letter that has the iota subscript.
        0x1F80..=0x1F87 | 0x1F90..=0x1F97 | 0x1FA0..=0x1FA7 => code + 8,
        0x1FB3 => 0x1FBC,
        0x1FC3 => 0x1FCC,
        0x1FF3 => 0x1FFC,
        _ => char::from_u32(code)
            .and_then(|c| only_char(c.to_uppercase()))
            .map_or(code, u32::from),
    }
}

/// `Character.toLowerCase(int)`, the simple mapping of `UnicodeData.txt`.
fn simple_lower(code: u32) -> u32 {
    match code {
        // `SpecialCasing.txt` maps this to `i` and a combining dot above.
        0x130 => 0x69,
        _ => char::from_u32(code)
            .and_then(|c| only_char(c.to_lowercase()))
            .map_or(code, u32::from),
    }
}

/// The value that Java compares under `(?iu)`.
fn key(code: u32) -> u32 { simple_lower(simple_upper(code)) }

/// Every code point that the mapping changes, or that `key` sends to another
/// one, in order.
fn table() -> &'static [Mapping] {
    static TABLE: OnceLock<Vec<Mapping>> = OnceLock::new();
    TABLE.get_or_init(|| {
        (0..=u32::from(char::MAX))
            .filter_map(|code| {
                let upper = simple_upper(code);
                let key = simple_lower(upper);
                (upper != code || key != code).then_some(Mapping { code, upper, key })
            })
            .collect()
    })
}

/// The code points that have a case in Java's mapping: those the mapping
/// changes, and those it changes something to.
fn cased() -> &'static [u32] {
    static CASED: OnceLock<Vec<u32>> = OnceLock::new();
    CASED.get_or_init(|| {
        let mut cased: Vec<u32> = table()
            .iter()
            .flat_map(|mapping| [mapping.code, mapping.upper, mapping.key])
            .collect();
        cased.sort_unstable();
        cased.dedup();
        cased
    })
}

/// The code points `x` with `key(x) == wanted`, which is `SliceU`'s test.
fn with_key(wanted: u32) -> impl Iterator<Item = u32> {
    table()
        .iter()
        .filter(move |mapping| mapping.key == wanted)
        .map(|mapping| mapping.code)
        .chain((key(wanted) == wanted).then_some(wanted))
}

/// `Pattern.SingleU(lower)`: `lower`, and the code points with that key.
fn single_u(lower: u32) -> impl Iterator<Item = u32> {
    std::iter::once(lower).chain(with_key(lower))
}

/// Sorts `ranges`, and merges those that overlap or touch.
fn normalize(mut ranges: Ranges) -> Ranges {
    ranges.sort_unstable();
    let mut merged: Ranges = Vec::with_capacity(ranges.len());
    for CodeRange { start, end } in ranges {
        match merged.last_mut() {
            Some(CodeRange { end: last_end, .. }) if start <= last_end.saturating_add(1) => {
                *last_end = (*last_end).max(end);
            }
            _ => merged.push(CodeRange { start, end }),
        }
    }
    merged
}

/// The code points as single-point ranges, normalized.
fn points(codes: impl IntoIterator<Item = u32>) -> Ranges {
    normalize(
        codes
            .into_iter()
            .map(|code| CodeRange {
                start: code,
                end: code,
            })
            .collect(),
    )
}

/// Whether Java's mapping gives `code` a case.
pub(super) fn is_cased(code: u32) -> bool { cased().binary_search(&code).is_ok() }

/// What `code`, a member of a class, matches under `(?iu)`, or `None` for a
/// code point that has no case, which matches only itself.
///
/// Java puts a member in Latin-1 in a `BitClass` that adds the two cases, and
/// any other member in `single`, which adds every code point with the same
/// key. The `BitClass` misses what `single` has for a letter that has a
/// counterpart outside Latin-1, such as `ß`, which `[ß]` does not match `ẞ`
/// for and `[ẞ]` matches `ß` for.
pub(super) fn member(code: u32) -> Option<Ranges> {
    if !is_cased(code) {
        return None;
    }
    if code < 0x100 && !NOT_IN_BIT_CLASS.contains(&code) {
        return Some(points([code, simple_lower(code), simple_upper(code)]));
    }
    Some(single_set(code))
}

/// What `Pattern.single(code)` matches under `(?iu)`: the code points with the
/// key of `code`, unless `code` is already its own key, in which case it is
/// the only one.
fn single_set(code: u32) -> Ranges {
    let (upper, lower) = (simple_upper(code), key(code));
    if upper == lower {
        points([code])
    } else {
        points(single_u(lower))
    }
}

/// What `code`, a character outside a class, matches under `(?iu)`, or `None`
/// for a code point that has no case, which matches only itself.
///
/// Java reads a run of two or more characters as a slice, which `SliceU`
/// compares by key, and a lone character with `single`. The two agree for
/// every letter but `ß`, which has no case of its own: `single` takes it as it
/// is, and `SliceU` also matches `ẞ`, which has the same key. `in_run` says
/// whether `code` is in a run, and is called only for such a letter.
pub(super) fn literal(code: u32, in_run: impl FnOnce() -> bool) -> Option<Ranges> {
    if !is_cased(code) {
        return None;
    }
    let slice = points(with_key(key(code)));
    let single = single_set(code);
    Some(if slice == single || in_run() {
        slice
    } else {
        single
    })
}

/// What the range `lo` to `hi` in a class matches under `(?iu)`, which is
/// `Pattern.CIRangeU`: a code point that is in the range, or whose upper case,
/// or lower case of its upper case, is.
pub(super) fn range(lo: u32, hi: u32) -> Ranges {
    let inside = |code: u32| (lo..=hi).contains(&code);
    let mut ranges = vec![CodeRange { start: lo, end: hi }];
    ranges.extend(
        table()
            .iter()
            .filter(|mapping| inside(mapping.upper) || inside(mapping.key))
            .map(|mapping| CodeRange {
                start: mapping.code,
                end: mapping.code,
            }),
    );
    normalize(ranges)
}

/// Writes `code` as it may stand in a class or outside one: an ASCII letter or
/// digit as it is, and any other code point as `\x{..}`.
fn push_code(out: &mut String, code: u32) {
    match char::from_u32(code).filter(char::is_ascii_alphanumeric) {
        Some(c) => out.push(c),
        None => {
            let _ = write!(out, "\\x{{{code:X}}}");
        }
    }
}

/// The range `start` to `end`, less a surrogate at either end, which no `char`
/// can be and `fancy_regex` refuses to read. A range that has the surrogates
/// in the middle is as it is.
fn without_surrogate_ends(range: CodeRange) -> Option<CodeRange> {
    let CodeRange { start, end } = range;
    let CodeRange {
        start: first,
        end: last,
    } = SURROGATES;
    let is_surrogate = |code: u32| (first..=last).contains(&code);
    let start = if is_surrogate(start) { last + 1 } else { start };
    let end = if is_surrogate(end) { first - 1 } else { end };
    (start <= end).then_some(CodeRange { start, end })
}

/// Writes `ranges` as the members of a class. Where they hold nothing but
/// surrogates the class would be empty, which `fancy_regex` refuses, so the
/// member is an empty class of its own.
///
/// The chars on either side of the surrogates, U+D7FF and U+E000, are next to
/// each other. `regex` negates a class wrongly when one member ends at U+D7FF
/// and the next starts at U+E000, so the ranges of one set that end and start
/// there are joined into one. Two members of a class that do so, such as two
/// ranges of a pattern, are still negated wrongly, as they are when a pattern
/// names the two chars itself.
pub(super) fn push_members(out: &mut String, ranges: &[CodeRange]) {
    let CodeRange {
        start: first,
        end: last,
    } = SURROGATES;
    let mut kept: Ranges = Vec::with_capacity(ranges.len());
    for CodeRange { start, end } in ranges
        .iter()
        .filter_map(|&range| without_surrogate_ends(range))
    {
        match kept.last_mut() {
            Some(CodeRange { end: kept_end, .. })
                if *kept_end == first - 1 && start == last + 1 =>
            {
                *kept_end = end;
            }
            _ => kept.push(CodeRange { start, end }),
        }
    }
    if kept.is_empty() {
        out.push_str(NO_CODE_POINT);
    }
    for CodeRange { start, end } in kept {
        push_code(out, start);
        if end > start {
            out.push('-');
            push_code(out, end);
        }
    }
}

/// Writes `ranges` as an expression that matches one char from them, outside
/// a class.
pub(super) fn push_class(out: &mut String, ranges: &[CodeRange]) {
    if let [CodeRange { start, end }] = ranges
        && start == end
    {
        push_code(out, *start);
    } else {
        out.push('[');
        push_members(out, ranges);
        out.push(']');
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// The ranges as `lo-hi,lo-hi` in hex, a point as `lo` alone, which is how
    /// the tests below give what JDK 17 matches.
    fn text(ranges: &[CodeRange]) -> String {
        ranges
            .iter()
            .map(|&CodeRange { start: lo, end: hi }| {
                if lo == hi {
                    format!("{lo:x}")
                } else {
                    format!("{lo:x}-{hi:x}")
                }
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    fn members(ranges: &[CodeRange]) -> String {
        let mut out = String::new();
        push_members(&mut out, ranges);
        out
    }

    /// Each row is `Character.toUpperCase(code)` and `Character.toLowerCase` of
    /// it on JDK 17. The rows from U+1F80 to U+1FF3 are those that
    /// `char::to_uppercase` gives two chars for, and U+0130 is the one that
    /// `char::to_lowercase` does.
    #[test]
    fn the_case_mapping_is_the_simple_one_of_java() {
        // (code, upper, key)
        let cases = [
            (0x61, 0x41, 0x61),
            (0x41, 0x41, 0x61),
            (0x31, 0x31, 0x31),
            (0xDF, 0xDF, 0xDF),
            (0x1E9E, 0x1E9E, 0xDF),
            (0x130, 0x130, 0x69),
            (0x131, 0x49, 0x69),
            (0x17F, 0x53, 0x73),
            (0x212A, 0x212A, 0x6B),
            (0x212B, 0x212B, 0xE5),
            (0xB5, 0x39C, 0x3BC),
            (0xFF, 0x178, 0xFF),
            (0x1C5, 0x1C4, 0x1C6),
            (0x3C2, 0x3A3, 0x3C3),
            (0x3A3, 0x3A3, 0x3C3),
            (0x345, 0x399, 0x3B9),
            (0x1FBE, 0x399, 0x3B9),
            (0x10428, 0x10400, 0x10428),
            (0x1F80, 0x1F88, 0x1F80),
            (0x1F88, 0x1F88, 0x1F80),
            (0x1F97, 0x1F9F, 0x1F97),
            (0x1FB3, 0x1FBC, 0x1FB3),
            (0x1FBC, 0x1FBC, 0x1FB3),
            (0x1FC3, 0x1FCC, 0x1FC3),
            (0x1FF3, 0x1FFC, 0x1FF3),
        ];
        for (code, upper, expected) in cases {
            check!(simple_upper(code) == upper, "upper of {code:x}");
            check!(key(code) == expected, "key of {code:x}");
        }
    }

    /// Each row is the code points that `Pattern.compile("(?iu)[<code>]")`
    /// matches on JDK 17: a Latin-1 member goes in a bit class, and the others
    /// compare keys.
    #[test]
    fn a_class_member_is_what_java_matches_under_a_unicode_fold() {
        // (code, matches, or None where the member has no case)
        let cases = [
            (0x6B, Some("4b,6b,212a")),
            (0x4B, Some("4b,6b,212a")),
            (0x73, Some("53,73,17f")),
            (0x69, Some("49,69,130-131")),
            (0x49, Some("49,69,130-131")),
            (0xE5, Some("c5,e5,212b")),
            (0xC5, Some("c5,e5,212b")),
            (0xFF, Some("ff,178")),
            (0xB5, Some("b5,39c,3bc")),
            (0xE9, Some("c9,e9")),
            (0xDF, Some("df")),
            (0x1E9E, Some("df,1e9e")),
            (0x3C3, Some("3a3,3c2-3c3")),
            (0x3C2, Some("3a3,3c2-3c3")),
            (0x3A3, Some("3a3,3c2-3c3")),
            (0x1C5, Some("1c4-1c6")),
            (0x1F80, Some("1f80,1f88")),
            (0x1F88, Some("1f80,1f88")),
            (0x130, Some("49,69,130-131")),
            (0x131, Some("49,69,130-131")),
            (0x10400, Some("10400,10428")),
            (0x31, None),
            (0x2D, None),
            (0x4E00, None),
        ];
        for (code, expected) in cases {
            let cases = member(code);
            check!(
                cases.as_deref().map(text).as_deref() == expected,
                "member {code:x}"
            );
        }
    }

    /// Each row is the code points that `Pattern.compile("(?iu)[<lo>-<hi>]")`
    /// matches on JDK 17, which are those that are in the range, or whose
    /// upper case or lower case of the upper case is.
    #[test]
    fn a_class_range_is_what_java_matches_under_a_unicode_fold() {
        // (lo, hi, matches)
        let cases = [
            (0x61, 0x7A, "41-5a,61-7a,130-131,17f,212a"),
            (0x41, 0x5A, "41-5a,61-7a,131,17f"),
            (0x41, 0x4B, "41-4b,61-6b,131"),
            (0x41, 0x63, "41-7a,131,17f"),
            (0x30, 0x39, "30-39"),
            (0x5B, 0x60, "5b-60"),
            (0xC0, 0xDE, "c0-de,e0-f6,f8-fe"),
            (
                0x391,
                0x3A9,
                "b5,345,391-3a9,3b1-3c9,3d0-3d1,3d5-3d6,3f0-3f1,3f5,1fbe",
            ),
            (0xDF, 0xDF, "df,1e9e"),
            (0x1E9E, 0x1E9E, "1e9e"),
            (0x130, 0x130, "130"),
            (0x1F80, 0x1F80, "1f80,1f88"),
            (0x1FB3, 0x1FB3, "1fb3,1fbc"),
            (0x1FBC, 0x1FBC, "1fb3,1fbc"),
        ];
        for (lo, hi, expected) in cases {
            check!(text(&range(lo, hi)) == expected, "range {lo:x}-{hi:x}");
        }
    }

    /// A literal outside a class matches the code points with its key, and a
    /// lone `ß` only itself. Each row is `Pattern.compile` on JDK 17.
    #[test]
    fn a_literal_is_what_java_matches_under_a_unicode_fold() {
        // (code, in a run of literals, matches)
        let cases = [
            (0x6B, false, Some("4b,6b,212a")),
            (0x3C2, false, Some("3a3,3c2-3c3")),
            (0x1F80, false, Some("1f80,1f88")),
            (0x1C5, false, Some("1c4-1c6")),
            (0x130, false, Some("49,69,130-131")),
            (0xDF, false, Some("df")),
            (0xDF, true, Some("df,1e9e")),
            (0x1E9E, false, Some("df,1e9e")),
            (0x31, false, None),
        ];
        for (code, in_run, expected) in cases {
            let cases = literal(code, || in_run);
            check!(
                cases.as_deref().map(text).as_deref() == expected,
                "literal {code:x} in a run: {in_run}"
            );
        }
    }

    /// Java reads a run of literals, and a lone literal, alike but for `ß`, so
    /// the rewrite asks whether a literal is in a run only for that one.
    #[test]
    fn a_literal_in_a_run_and_alone_differ_for_sharp_s_only() {
        let differing: Vec<u32> = cased()
            .iter()
            .copied()
            .filter(|&code| points(with_key(key(code))) != single_set(code))
            .collect();
        check!(differing == [0xDF]);
    }

    #[test]
    fn a_set_is_written_as_the_members_of_a_class() {
        struct Case {
            ranges: &'static [CodeRange],
            members: &'static str,
        }
        let cases: [Case; 11] = [
            Case {
                ranges: &[
                    CodeRange {
                        start: 0x41,
                        end: 0x5A,
                    },
                    CodeRange {
                        start: 0x61,
                        end: 0x7A,
                    },
                ],
                members: "A-Za-z",
            },
            Case {
                ranges: &[CodeRange {
                    start: 0x2D,
                    end: 0x2D,
                }],
                members: "\\x{2D}",
            },
            Case {
                ranges: &[CodeRange {
                    start: 0x5B,
                    end: 0x60,
                }],
                members: "\\x{5B}-\\x{60}",
            },
            Case {
                ranges: &[
                    CodeRange {
                        start: 0x17F,
                        end: 0x17F,
                    },
                    CodeRange {
                        start: 0x212A,
                        end: 0x212A,
                    },
                ],
                members: "\\x{17F}\\x{212A}",
            },
            Case {
                ranges: &[CodeRange {
                    start: 0xD000,
                    end: 0x0010_FFFF,
                }],
                members: "\\x{D000}-\\x{10FFFF}",
            },
            Case {
                ranges: &[CodeRange {
                    start: 0x41,
                    end: 0xDBFF,
                }],
                members: "A-\\x{D7FF}",
            },
            Case {
                ranges: &[CodeRange {
                    start: 0xD800,
                    end: 0xE001,
                }],
                members: "\\x{E000}-\\x{E001}",
            },
            Case {
                ranges: &[CodeRange {
                    start: 0xD7FF,
                    end: 0xE000,
                }],
                members: "\\x{D7FF}-\\x{E000}",
            },
            Case {
                ranges: &[
                    CodeRange {
                        start: 0xD7FF,
                        end: 0xD7FF,
                    },
                    CodeRange {
                        start: 0xE000,
                        end: 0xE000,
                    },
                ],
                members: "\\x{D7FF}-\\x{E000}",
            },
            Case {
                ranges: &[
                    CodeRange {
                        start: 0x61,
                        end: 0xD7FF,
                    },
                    CodeRange {
                        start: 0xE000,
                        end: 0xE001,
                    },
                ],
                members: "a-\\x{E001}",
            },
            Case {
                ranges: &[CodeRange {
                    start: 0xD800,
                    end: 0xDFFF,
                }],
                members: NO_CODE_POINT,
            },
        ];
        for Case {
            ranges,
            members: expected,
        } in cases
        {
            check!(members(ranges) == expected, "{ranges:x?}");
        }
    }
}
