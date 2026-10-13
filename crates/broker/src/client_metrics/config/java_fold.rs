//! Java's case folding, for [`java_to_fancy`](super::java_to_fancy).
//!
//! Java's `CASE_INSENSITIVE` (`(?i)`) folds ASCII case only. It folds Unicode
//! case as well when `UNICODE_CASE` (`(?u)`) is on, and `(?U)`
//! (`UNICODE_CHARACTER_CLASS`) turns that on. `fancy_regex` has one `i` flag.
//! It folds Unicode case with the simple case folding tables, and it folds a
//! whole class or a whole reference at once, with no switch for one member:
//! `(?i)service-` matches `ſervice-` (long s) there, and not in Java, and
//! `(?iu)[\w]` matches U+017F, which Java's `\w` does not. The inline `(?-u)`
//! cannot switch the fold off (`ChangingUnicodeModeUnsupported`), and
//! `RegexBuilder::unicode_mode(false)` is a switch for the whole pattern that
//! also refuses `.`, `\W` and `\p{..}`.
//!
//! So the rewrite never turns `fancy_regex`'s `i` on, except for a
//! backreference. While an `i` is in force it writes the folding out.
//!
//! - Without `u`, an ASCII letter is both its cases, `[aA]`, or `aA` in a
//!   character class, and a class range is the range plus the ranges of the
//!   other case of the letters it covers, which is what Java's `CIRange`
//!   matches. A non-ASCII character is as it is.
//! - With `u`, a letter, a class member and a class range are the sets of code
//!   points that Java's `single`, `BitClass` and `CIRangeU` accept. Those
//!   compare `Character.toLowerCase(Character.toUpperCase(ch))`, which is not
//!   the simple case folding of `fancy_regex`: `İ` (U+0130) and `ı` (U+0131)
//!   match `i` in Java, and a range that has `K` and not `k` does not match
//!   the Kelvin sign (U+212A). [`java_case`](super::java_case) computes the
//!   sets, and the class that holds them is case sensitive.
//!
//! The escapes that name a letter, `\x41`, `\x{41}`, `\uhhhh`, `\0101` and the
//! text of `\Q...\E`, fold as the letter does. Java also widens the properties
//! for a case under `(?i)`: `\p{Lower}` and `\p{Upper}` are the ASCII letters
//! of both cases, and `\p{Lu}`, `\p{Ll}` and `\p{Lt}` are all cased letters.
//! [`property_class`] writes those. Java does not fold `\w`, `\d`, `\s`, or any
//! other property or POSIX class, and with `i` off neither does the rewrite.
//! The POSIX classes are ASCII in Java without `(?U)`, and the Unicode
//! properties with it, where `fancy_regex` reads them as Unicode always.
//!
//! `\Q...\E` is gone before any of that is read. Java's `RemoveQEQuoting`
//! rewrites the text of each one into escaped characters before it parses the
//! pattern, so `[\Qa\E-c]` is the range `[a-c]`, and [`remove_qe_quoting`] does
//! the same.
//!
//! A run of literals is one slice in Java and a lone literal is `single`, and
//! they differ for `ß`: `(?iu)ß` matches `ß` only, and `(?iu)ßß` matches `ẞ`
//! (U+1E9E) too. [`Translator`](super::Translator) tells the two apart, as Java's
//! `atom` does, by the literal chars up to the next token that is not one, less
//! the last of them when a quantifier follows.
//!
//! One thing is left different, because `fancy_regex` has no other way to
//! compare the text of a group. A backreference under a fold is written
//! `(?i:\1)`, and it compares the text it refers to with the text at hand
//! differently from Java's `CIBackRef`:
//!
//! - Under `(?i)` Java compares ASCII case only, and `fancy_regex`'s `i`
//!   compares two texts Unicode-insensitively, unless both are ASCII. The two
//!   agree on every ASCII text. They differ when the group holds a non-ASCII
//!   letter, and the reference matches the same letter in the other case:
//!   `(?i)(é)\1` matches `éÉ` in `fancy_regex`, and not in Java.
//! - Under `(?iu)` Java compares `Character.toLowerCase` of
//!   `Character.toUpperCase`, and `fancy_regex` compares simple case foldings
//!   of two texts that are as long in bytes. They differ for `İ` and `ı`, which
//!   Java's key makes `i`: `(?iu)(i)\1` matches `iİ` in Java, and not in
//!   `fancy_regex`. They differ for a letter and a case that take another number
//!   of bytes, such as `s` and `ſ`, `k` and the Kelvin sign, `ß` and `ẞ`, and `å`
//!   and the angstrom sign: `(?iu)(s)\1` matches `sſ` in Java, and not in
//!   `fancy_regex`. They agree on the other letters.

use {super::Scope, std::fmt::Write as _};

/// A backslash escape, as [`read_escape`] reads it.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Escape {
    /// An escape that stands for one character: `\x41`, `\x{41}`,
    /// `\uhhhh`, `\0101`, `\cA`, `\t`, or a backslash before a character that is not a
    /// letter or a digit, such as `\.`.
    Char(u32),
    /// `\p{name}`, `\P{name}`, or `\pL`.
    Property { name: String, negated: bool },
    /// `\1` to `\9`, or `\k<name>`: the text a group matched.
    BackRef,
    /// `\N{name}`, a character by its Unicode name.
    Named,
    /// Any other escape, such as `\w`, `\b` or `\A`.
    Other,
}

/// Java's `Pattern.RemoveQEQuoting`, which runs over the whole pattern before
/// it is parsed: the text of each `\Q...\E`, or of a `\Q` with no `\E`, up to
/// the end, becomes its characters, and each one that is not a letter, a digit
/// or a non-ASCII character gets a backslash before it. The first digit of the
/// text is written `\x3d`, so that a `\x4` before the `\Q` cannot take it. A
/// backslash is written `\\`, and a `\` before a `\Q` or a `\E` is read as a
/// pair, as it is outside the quote.
///
/// What the parser reads after that has no `\Q` in it, and reads the quoted
/// text as it reads any other characters: `[\Qa\E-c]` is `[a-c]`, a range, and
/// `[a\Q-\Ec]` is `[a\-c]`, which is not.
pub(super) fn remove_qe_quoting(chars: &[char]) -> Vec<char> {
    let mut out = Vec::with_capacity(chars.len());
    let mut at = 0;
    while let Some(&c) = chars.get(at) {
        at += 1;
        match (c, chars.get(at)) {
            ('\\', Some('Q')) => at = quote(chars, at + 1, &mut out),
            ('\\', Some(&escaped)) => {
                out.extend([c, escaped]);
                at += 1;
            }
            _ => out.push(c),
        }
    }
    out
}

/// Writes the quoted text that starts at `chars[at]`, just after a `\Q`, and
/// returns the index after the `\E` that ends it, or the end of the pattern.
fn quote(chars: &[char], mut at: usize, out: &mut Vec<char>) -> usize {
    let mut first = true;
    while let Some(&c) = chars.get(at) {
        at += 1;
        match c {
            '\\' if chars.get(at) == Some(&'E') => return at + 1,
            '\\' => out.extend(['\\', '\\']),
            _ if !c.is_ascii() || c.is_ascii_alphabetic() => out.push(c),
            _ if c.is_ascii_digit() => {
                if first {
                    out.extend(['\\', 'x', '3']);
                }
                out.push(c);
            }
            _ => out.extend(['\\', c]),
        }
        first = false;
    }
    at
}

/// The escape that starts at `chars[at]`, a backslash, and the number of chars
/// it takes. `in_class` is true inside a character class, where `\1` and
/// `\k<name>` are not references.
///
/// An escape Java's `Pattern.compile` refuses, such as `\x` without digits, is
/// [`Escape::Other`] with the length of the backslash and the char after it.
pub(super) fn read_escape(chars: &[char], at: usize, in_class: bool) -> (Escape, usize) {
    let rest = &chars[at + 1..];
    let Some(&kind) = rest.first() else {
        return (Escape::Other, 1);
    };
    let after = &rest[1..];
    let read = match kind {
        't' => Some((Escape::Char(0x09), 2)),
        'n' => Some((Escape::Char(0x0A), 2)),
        'r' => Some((Escape::Char(0x0D), 2)),
        'f' => Some((Escape::Char(0x0C), 2)),
        'a' => Some((Escape::Char(0x07), 2)),
        'e' => Some((Escape::Char(0x1B), 2)),
        'x' => hex_escape(after).map(|(code, len)| (Escape::Char(code), 2 + len)),
        'u' => after
            .get(..4)
            .and_then(parse_hex)
            .map(|code| (Escape::Char(code), 6)),
        '0' => octal_escape(after).map(|(code, len)| (Escape::Char(code), 2 + len)),
        'c' => after
            .first()
            .map(|&control| (Escape::Char(u32::from(control) ^ 64), 3)),
        'p' | 'P' => property_name(after).map(|(name, len)| {
            let negated = kind == 'P';
            (Escape::Property { name, negated }, 2 + len)
        }),
        'k' if !in_class => delimited(after, '<', '>').map(|len| (Escape::BackRef, 2 + len)),
        'N' => delimited(after, '{', '}').map(|len| (Escape::Named, 2 + len)),
        '1'..='9' if !in_class => Some((Escape::BackRef, 2)),
        other if !other.is_ascii_alphanumeric() => Some((Escape::Char(u32::from(other)), 2)),
        _ => None,
    };
    read.unwrap_or((Escape::Other, 2))
}

/// The number `digits` spell in base 16, or `None` for a char that is not an
/// ASCII hex digit or a number past `char::MAX`.
fn parse_hex(digits: &[char]) -> Option<u32> {
    let code = digits.iter().try_fold(0u32, |code, digit| {
        code.checked_mul(16)?.checked_add(digit.to_digit(16)?)
    })?;
    (code <= u32::from(char::MAX)).then_some(code)
}

/// `hh` or `{h...h}`, after `\x`, and the chars it takes.
fn hex_escape(after: &[char]) -> Option<(u32, usize)> {
    if after.first() == Some(&'{') {
        let close = after.iter().position(|&c| c == '}')?;
        let digits = &after[1..close];
        if digits.is_empty() {
            return None;
        }
        Some((parse_hex(digits)?, close + 1))
    } else {
        Some((parse_hex(after.get(..2)?)?, 2))
    }
}

/// One to three octal digits after `\0`, and the chars they take: the third
/// digit only when the first is at most 3, as Java's `Pattern.o` reads it.
fn octal_escape(after: &[char]) -> Option<(u32, usize)> {
    let digit = |at: usize| after.get(at).and_then(|c| c.to_digit(8));
    let first = digit(0)?;
    Some(match (digit(1), digit(2)) {
        (Some(second), Some(third)) if first <= 3 => (first * 64 + second * 8 + third, 3),
        (Some(second), _) => (first * 8 + second, 2),
        _ => (first, 1),
    })
}

/// The name in `{name}`, or the one char, after `\p` or `\P`, and the chars
/// it takes.
fn property_name(after: &[char]) -> Option<(String, usize)> {
    if after.first() == Some(&'{') {
        let close = after.iter().position(|&c| c == '}')?;
        (close > 1).then(|| (after[1..close].iter().collect(), close + 1))
    } else {
        after.first().map(|&c| (c.to_string(), 1))
    }
}

/// The chars that `after` takes for a run from `open` to `close`, where it
/// is not empty.
fn delimited(after: &[char], open: char, close: char) -> Option<usize> {
    let end = after.iter().position(|&c| c == close)?;
    (after.first() == Some(&open) && end > 1).then_some(end + 1)
}

/// The chars at the start of `rest` that Java's `COMMENTS` flag ignores: white
/// space, and `#` comments, each through the line terminator that ends it.
pub(super) fn ignorable_len(rest: &[char], unix_lines: bool) -> usize {
    let is_terminator = |c: char| {
        c == '\n' || (!unix_lines && matches!(c, '\r' | '\u{85}' | '\u{2028}' | '\u{2029}'))
    };
    let mut len = 0;
    while let Some(&c) = rest.get(len) {
        match c {
            ' ' | '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r' => len += 1,
            '#' => {
                len += 1;
                while let Some(&comment) = rest.get(len) {
                    len += 1;
                    if is_terminator(comment) {
                        break;
                    }
                }
            }
            _ => break,
        }
    }
    len
}

/// The code and the length of the char at `chars[at]`, where it is a member of
/// a class or the end of a range in one: a plain char, or an escape that stands
/// for one char.
pub(super) fn read_member(chars: &[char], at: usize) -> Option<(u32, usize)> {
    let &c = chars.get(at)?;
    if c != '\\' {
        return Some((u32::from(c), 1));
    }
    match read_escape(chars, at, true) {
        (Escape::Char(code), len) => Some((code, len)),
        _ => None,
    }
}

/// `code` as an ASCII letter, or `None` for any other code point.
fn ascii_letter(code: u32) -> Option<char> {
    let byte = u8::try_from(code).ok().filter(u8::is_ascii_alphabetic)?;
    Some(char::from(byte))
}

/// Writes `source`, the chars of one character of the pattern that stands for
/// `code`, as `fancy_regex` reads it:
///
/// - An ASCII letter is the letter itself. `fancy_regex` reads `\x41` and
///   `A`, and not the octal `\0101`.
/// - A bare `]`, which is a member only first in a class, is `\]`.
/// - In a class, a bare `-` is `\-`. Java reads a `-` that does not join two
///   members as the character, where `fancy_regex` reads it as a range from
///   the member before it, whatever that one is: the last char of a `\w` or
///   `\s` the rewrite has expanded, or the other case of a folded letter.
/// - In a class, a bare `^` that comes right after the `[` is `\^`. Under
///   `(?x)` it can, with the white space and comments between the two gone,
///   and Java reads it as a member where `fancy_regex` reads a negation.
/// - A bare `[`, which only ends a range, as in `(?x)[+- [b]]`, is `\[`, so
///   that it does not open a class.
/// - `\<` and `\>` are `<` and `>`. Java reads a backslash before either as
///   the character, and `fancy_regex` as the edge of a word.
///
/// Any other character is `source`.
pub(super) fn push_member(out: &mut String, source: &[char], code: u32, in_class: bool) {
    match (ascii_letter(code), source) {
        (Some(letter), _) => out.push(letter),
        (None, [']']) => out.push_str("\\]"),
        (None, ['[']) => out.push_str("\\["),
        (None, ['-']) if in_class => out.push_str("\\-"),
        (None, ['^']) if in_class && out.ends_with('[') => out.push_str("\\^"),
        (None, ['\\', edge @ ('<' | '>')]) => out.push(*edge),
        _ => out.extend(source),
    }
}

/// Writes `source`, the chars of one character of the pattern that stands for
/// `code`, as a set of both its cases when it is an ASCII letter: `[aA]`
/// outside a class, and `aA` inside one. Any other character is written as
/// [`push_member`] writes it.
pub(super) fn push_folded(out: &mut String, source: &[char], code: u32, in_class: bool) {
    let Some(letter) = ascii_letter(code) else {
        push_member(out, source, code, in_class);
        return;
    };
    let (lower, upper) = (letter.to_ascii_lowercase(), letter.to_ascii_uppercase());
    if in_class {
        out.push(lower);
        out.push(upper);
    } else {
        let _ = write!(out, "[{lower}{upper}]");
    }
}

/// Writes, inside a class, the ranges of the other case of the ASCII letters
/// that the range `lo` to `hi` covers. Java's `CIRange` accepts a letter when
/// it, or the other case of it, is in the range.
pub(super) fn push_other_case_ranges(out: &mut String, lo: u32, hi: u32) {
    for (first, last) in [('A', 'Z'), ('a', 'z')] {
        let (start, end) = (lo.max(u32::from(first)), hi.min(u32::from(last)));
        if start <= end {
            let _ = write!(out, "\\x{{{:X}}}-\\x{{{:X}}}", start ^ 0x20, end ^ 0x20);
        }
    }
}

/// The members of the class of `\p{name}` where `fancy_regex` reads the name
/// differently from Java, as the text of a class body.
///
/// - The POSIX names `Lower`, `Upper`, `Alpha`, `Alnum`, `Digit`, `Punct` and
///   the rest are the ASCII characters they name, where `fancy_regex`'s are
///   Unicode. Under `(?U)` they are the Unicode properties that Java gives
///   them, which [`unicode_posix_members`] spells out, so that the rewrite
///   does not depend on what `fancy_regex` makes of the name.
/// - `Lu`, `Ll` and `Lt`, `IsLowercase`, `IsUppercase`, `IsTitlecase`,
///   `IsLower` and `IsUpper` take in all three cases under `(?i)`, whether or
///   not `(?u)` is on. `fancy_regex`'s `i` is off, so the rewrite writes the
///   three cases.
fn property_members(name: &str, scope: Scope) -> Option<&'static str> {
    let bare = !name.contains('=') && !name.starts_with("In") && !name.starts_with("Is");
    if bare {
        let posix = if scope.unicode_classes {
            unicode_posix_members(name, scope.case.ignore)
        } else {
            ascii_posix_members(name, scope.case.ignore)
        };
        if posix.is_some() {
            return posix;
        }
    }
    if !scope.case.ignore {
        return None;
    }
    if is_cased_category(name) {
        Some("\\p{Lu}\\p{Ll}\\p{Lt}")
    } else if is_cased_property(name) {
        Some("\\p{Lowercase}\\p{Uppercase}\\p{Lt}")
    } else {
        None
    }
}

/// Whether `\p{name}` is `Lu`, `Ll` or `Lt`, spelled as Java takes it: bare, with
/// an `Is` prefix, or as the value of `gc=` or `general_category=`.
fn is_cased_category(name: &str) -> bool {
    let value = match name.split_once('=') {
        Some((key, value)) if matches!(key.to_lowercase().as_str(), "gc" | "general_category") => {
            value
        }
        Some(_) => return false,
        None => name.strip_prefix("Is").unwrap_or(name),
    };
    matches!(value, "Lu" | "Ll" | "Lt")
}

/// Whether `\p{name}` is one of the `Is` properties for a case, which Java
/// matches in any name case.
fn is_cased_property(name: &str) -> bool {
    name.strip_prefix("Is").is_some_and(|short| {
        matches!(
            short.to_uppercase().as_str(),
            "LOWERCASE" | "UPPERCASE" | "TITLECASE" | "LOWER" | "UPPER"
        )
    })
}

/// The members of the POSIX class `\p{name}` as Java reads it without `(?U)`
/// (`CharPredicates.forProperty`): the ASCII characters that the C locale
/// names, and `name` is matched in the case it is written. `Lower` and `Upper`
/// are the ASCII letters of both cases under `(?i)`.
fn ascii_posix_members(name: &str, ignore_case: bool) -> Option<&'static str> {
    Some(match (name, ignore_case) {
        ("Lower" | "Upper", true) | ("Alpha", _) => "a-zA-Z",
        ("Lower", false) => "a-z",
        ("Upper", false) => "A-Z",
        ("Digit", _) => "0-9",
        ("Alnum", _) => "0-9a-zA-Z",
        ("Punct", _) => "!-/:-@\\[-`{-~",
        ("Graph", _) => "!-~",
        ("Print", _) => "\\x20-~",
        ("Blank", _) => "\\x20\\t",
        ("Cntrl", _) => "\\x00-\\x1F\\x7F",
        ("Space", _) => "\\x20\\t\\n\\x0B\\x0C\\r",
        ("XDigit", _) => "0-9a-fA-F",
        _ => return None,
    })
}

/// The members of the POSIX class `\p{name}` as Java reads it under `(?U)`
/// (`CharPredicates.forPOSIXName`): the Unicode property for each name, which is
/// matched in any case. `Lower` and `Upper` take in all three cases under
/// `(?i)`.
fn unicode_posix_members(name: &str, ignore_case: bool) -> Option<&'static str> {
    Some(match (name.to_uppercase().as_str(), ignore_case) {
        ("LOWER" | "UPPER", true) => "\\p{Lowercase}\\p{Uppercase}\\p{Lt}",
        ("LOWER", false) => "\\p{Lowercase}",
        ("UPPER", false) => "\\p{Uppercase}",
        ("ALPHA", _) => "\\p{Alphabetic}",
        ("DIGIT", _) => "\\p{Nd}",
        ("ALNUM", _) => "\\p{Alphabetic}\\p{Nd}",
        ("PUNCT", _) => "\\p{P}",
        ("GRAPH", _) => "[^\\p{Zs}\\p{Zl}\\p{Zp}\\p{Cc}\\p{Cs}\\p{Cn}]",
        ("PRINT", _) => "[^\\p{Zl}\\p{Zp}\\p{Cc}\\p{Cs}\\p{Cn}]",
        ("BLANK", _) => "\\p{Zs}\\t",
        ("CNTRL", _) => "\\p{Cc}",
        ("SPACE", _) => "\\p{White_Space}",
        ("XDIGIT", _) => "\\p{Nd}\\p{Hex_Digit}",
        _ => return None,
    })
}

/// `\p{name}` or `\P{name}`, written for `fancy_regex` as [`property_members`]
/// says, or `None` where the escape is fine as it stands.
pub(super) fn property_class(
    name: &str,
    negated: bool,
    in_class: bool,
    scope: Scope,
) -> Option<String> {
    let members = property_members(name, scope)?;
    Some(match (negated, in_class) {
        (false, true) => members.to_owned(),
        (false, false) => format!("[{members}]"),
        (true, _) => format!("[^{members}]"),
    })
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    fn escape(text: &str, in_class: bool) -> (Escape, usize) {
        let chars: Vec<char> = text.chars().collect();
        read_escape(&chars, 0, in_class)
    }

    /// Each row is the escape Java's `Pattern.escape` reads, and the chars it
    /// takes.
    #[test]
    fn read_escape_reads_what_java_reads() {
        let property = |name: &str, negated| Escape::Property {
            name: name.to_owned(),
            negated,
        };
        // (text, in a class, escape, chars taken)
        let cases = [
            ("\\x41", false, Escape::Char(0x41), 4),
            ("\\x{41}", false, Escape::Char(0x41), 6),
            ("\\x{1F600}", false, Escape::Char(0x1F600), 9),
            ("\\x{110000}", false, Escape::Other, 2),
            ("\\x{}", false, Escape::Other, 2),
            ("\\x4", false, Escape::Other, 2),
            ("\\u0041b", false, Escape::Char(0x41), 6),
            ("\\u004", false, Escape::Other, 2),
            ("\\0101", false, Escape::Char(0x41), 5),
            ("\\0777", false, Escape::Char(0o77), 4),
            ("\\07", false, Escape::Char(7), 3),
            ("\\00", false, Escape::Char(0), 3),
            ("\\0", false, Escape::Other, 2),
            ("\\cA", false, Escape::Char(1), 3),
            ("\\c!", false, Escape::Char(0x61), 3),
            ("\\t", false, Escape::Char(9), 2),
            ("\\.", false, Escape::Char(0x2E), 2),
            ("\\-", true, Escape::Char(0x2D), 2),
            ("\\pL", false, property("L", false), 3),
            ("\\P{Lu}x", false, property("Lu", true), 6),
            ("\\p{}", false, Escape::Other, 2),
            ("\\p{Lu", false, Escape::Other, 2),
            ("\\1", false, Escape::BackRef, 2),
            ("\\1", true, Escape::Other, 2),
            ("\\k<name>", false, Escape::BackRef, 8),
            ("\\k<>", false, Escape::Other, 2),
            ("\\N{LATIN SMALL LETTER A}", false, Escape::Named, 24),
            ("\\w", false, Escape::Other, 2),
            ("\\b", false, Escape::Other, 2),
            ("\\", false, Escape::Other, 1),
        ];
        for (text, in_class, expected, len) in cases {
            check!(escape(text, in_class) == (expected, len), "{text:?}");
        }
    }

    #[test]
    fn a_class_range_gets_the_other_case_of_the_letters_it_covers() {
        // (lo, hi, extra ranges)
        let cases = [
            ('a', 'f', "\\x{41}-\\x{46}"),
            ('B', 'D', "\\x{62}-\\x{64}"),
            ('X', 'c', "\\x{78}-\\x{7A}\\x{41}-\\x{43}"),
            ('+', 'z', "\\x{61}-\\x{7A}\\x{41}-\\x{5A}"),
            ('0', '9', ""),
            ('[', '`', ""),
            ('\u{e9}', '\u{ff}', ""),
        ];
        for (lo, hi, expected) in cases {
            let mut out = String::new();
            push_other_case_ranges(&mut out, u32::from(lo), u32::from(hi));
            check!(out == expected, "{lo:?}-{hi:?}");
        }
    }

    /// Each row is the pattern, and what the JDK 17 `Pattern.RemoveQEQuoting`
    /// leaves of it, read from the private `temp` array it works on.
    #[test]
    fn remove_qe_quoting_rewrites_as_java_does() {
        let cases = [
            ("abc", "abc"),
            ("\\Qa.b\\E", "a\\.b"),
            ("\\Qa.b", "a\\.b"),
            ("[\\Qa\\E-c]", "[a-c]"),
            ("[a\\Q-\\Ec]", "[a\\-c]"),
            ("[\\Q]\\E-c]", "[\\]-c]"),
            ("\\Q\\E", ""),
            ("a\\Q\\E+", "a+"),
            ("\\Q1\\E2", "\\x312"),
            ("\\Qab1\\E", "ab1"),
            ("\\Q12\\E", "\\x312"),
            ("\\Qa\\\\E", "a\\\\"),
            ("\\Q\\\\E", "\\\\"),
            ("\\\\Qa\\E", "\\\\Qa\\E"),
            ("\\\\Q.\\E", "\\\\Q.\\E"),
            ("\\Q.\\E\\Q*\\E", "\\.\\*"),
            ("x\\Qa\\Eb\\Qc\\E", "xabc"),
            ("\\Q\\E\\Q\\E", ""),
            ("\\Qa\\Eb\\E", "ab\\E"),
            ("\\Q<>_ #\\E", "\\<\\>\\_\\ \\#"),
            ("\\Q\\u00e9k\\E", "\\\\u00e9k"),
            ("\\Q\u{e9}.\\E", "\u{e9}\\."),
            ("\\Q.", "\\."),
            ("a\\", "a\\"),
        ];
        for (pattern, expected) in cases {
            let chars: Vec<char> = pattern.chars().collect();
            let rewritten: String = remove_qe_quoting(&chars).into_iter().collect();
            check!(rewritten == expected, "{pattern:?}");
        }
    }

    /// A member is written as `fancy_regex` reads it: `]` first in a class is
    /// `\]`, `\<` and `\>` are the bare characters, an ASCII letter is the
    /// letter whatever escape named it, a `-` in a class is `\-`, a `^` right
    /// after the `[` of a class is `\^`, and a `[` that ends a range is `\[`.
    #[test]
    fn push_member_writes_what_fancy_regex_reads() {
        // (written so far, source, code, in a class, written after)
        let cases = [
            ("", "]", 0x5D, false, "\\]"),
            ("", "\\]", 0x5D, false, "\\]"),
            ("", "\\<", 0x3C, false, "<"),
            ("", "\\>", 0x3E, false, ">"),
            ("", "<", 0x3C, false, "<"),
            ("", "\\.", 0x2E, false, "\\."),
            ("", "\\x41", 0x41, false, "A"),
            ("", "\\0101", 0x41, false, "A"),
            ("", "\\t", 0x09, false, "\\t"),
            ("", "\u{e9}", 0xE9, false, "\u{e9}"),
            ("", "[", 0x5B, true, "\\["),
            ("", "-", 0x2D, true, "\\-"),
            ("[a", "-", 0x2D, true, "[a\\-"),
            ("[0-9A-Za-z_", "-", 0x2D, true, "[0-9A-Za-z_\\-"),
            ("", "-", 0x2D, false, "-"),
            ("", "\\-", 0x2D, true, "\\-"),
            ("[", "^", 0x5E, true, "[\\^"),
            ("[a[", "^", 0x5E, true, "[a[\\^"),
            ("[a", "^", 0x5E, true, "[a^"),
            ("[^", "^", 0x5E, true, "[^^"),
            ("\\[", "^", 0x5E, false, "\\[^"),
            ("", "^", 0x5E, false, "^"),
        ];
        for (before, source, code, in_class, expected) in cases {
            let chars: Vec<char> = source.chars().collect();
            let mut out = before.to_owned();
            push_member(&mut out, &chars, code, in_class);
            check!(out == expected, "{before:?} then {source:?}");
        }
    }
}
