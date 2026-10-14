//! Kafka's `ConfigDef.parseType`, and the `ConfigException` text its
//! validators produce.
//!
//! Kafka parses every config value before a validator sees it. The parser
//! trims the value, reads a boolean without regard to case, reads an `INT` or
//! `LONG` with `Integer.parseInt` or `Long.parseLong`, reads a `DOUBLE` with
//! `Double.parseDouble`, and splits a `LIST` on commas with the whitespace
//! around each comma removed. The validator then checks the parsed value, and
//! a refusal reads `Invalid value <value> for configuration <name>: <reason>`.
//!
//! The validators and every reader of a stored value use these functions, so
//! a value that validation accepts is read back the same way.

/// Java's `String.trim`: every character at or below U+0020 at either end.
pub(crate) fn java_trim(value: &str) -> &str { value.trim_matches(|c: char| c <= ' ') }

/// The whitespace class `\s` of Kafka's `COMMA_WITH_WHITESPACE` split.
fn regex_space(c: char) -> bool { matches!(c, ' ' | '\t' | '\n' | '\u{b}' | '\u{c}' | '\r') }

/// Kafka's `ConfigException(name, value, message)` text.
pub(crate) fn invalid_value(key: &str, value: impl std::fmt::Display, message: &str) -> String {
    format!("Invalid value {value} for configuration {key}: {message}")
}

/// A `BOOLEAN` value: `true` or `false` in any case, after a trim.
pub(crate) fn bool_value(value: &str) -> Option<bool> {
    let trimmed = java_trim(value);
    if trimmed.eq_ignore_ascii_case("true") {
        Some(true)
    } else if trimmed.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

/// An `INT` value, as `Integer.parseInt` reads the trimmed string.
pub(crate) fn int_value(value: &str) -> Option<i32> { java_trim(value).parse().ok() }

/// A `LONG` value, as `Long.parseLong` reads the trimmed string.
pub(crate) fn long_value(value: &str) -> Option<i64> { java_trim(value).parse().ok() }

/// A `DOUBLE` value, as `Double.parseDouble` reads the trimmed string: an
/// optional sign, `NaN`, `Infinity`, or a decimal number with an optional
/// exponent and an optional `f`, `F`, `d` or `D` suffix.
pub(crate) fn double_value(value: &str) -> Option<f64> {
    let trimmed = java_trim(value);
    let (negative, body) = match trimmed.strip_prefix('-') {
        Some(body) => (true, body),
        None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };
    let magnitude = match body {
        "NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        _ => {
            let digits = body.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(body);
            let decimal = digits.starts_with(|c: char| c.is_ascii_digit() || c == '.')
                && digits
                    .bytes()
                    .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'e' | b'E' | b'+' | b'-'));
            if !decimal {
                return None;
            }
            digits.parse::<f64>().ok()?
        }
    };
    Some(if negative { -magnitude } else { magnitude })
}

/// A `LIST` value: empty after a trim, or the trimmed value split on commas,
/// each element with the whitespace next to its commas removed. An empty
/// element is kept, as Kafka's split with limit -1 keeps it.
pub(crate) fn list_value(value: &str) -> Vec<&str> {
    let trimmed = java_trim(value);
    if trimmed.is_empty() {
        return Vec::new();
    }
    trimmed
        .split(',')
        .map(|element| element.trim_matches(regex_space))
        .collect()
}

/// Java's `Double.toString`, which is how Kafka prints a parsed `DOUBLE` in a
/// refusal and in `DescribeConfigs`: `2.0` for `2`, and `1.0E7` from ten
/// million up.
pub(crate) fn java_double(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_owned();
    }
    let magnitude = value.abs();
    if magnitude == 0.0 || (1.0e-3..1.0e7).contains(&magnitude) {
        return format!("{value:?}");
    }
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    if mantissa.contains('.') {
        format!("{mantissa}E{exponent}")
    } else {
        format!("{mantissa}.0E{exponent}")
    }
}

/// Parse a `BOOLEAN` for `key`, with Kafka's refusal.
pub(crate) fn parse_bool(key: &str, value: &str) -> Result<bool, String> {
    bool_value(value)
        .ok_or_else(|| invalid_value(key, value, "Expected value to be either true or false"))
}

/// Parse an `INT` for `key`, with Kafka's refusal.
pub(crate) fn parse_int(key: &str, value: &str) -> Result<i32, String> {
    int_value(value).ok_or_else(|| invalid_value(key, value, "Not a number of type INT"))
}

/// Parse a `SHORT` for `key`, with Kafka's refusal.
pub(crate) fn parse_short(key: &str, value: &str) -> Result<i16, String> {
    java_trim(value)
        .parse()
        .map_err(|_| invalid_value(key, value, "Not a number of type SHORT"))
}

/// Parse a `LONG` for `key`, with Kafka's refusal.
pub(crate) fn parse_long(key: &str, value: &str) -> Result<i64, String> {
    long_value(value).ok_or_else(|| invalid_value(key, value, "Not a number of type LONG"))
}

/// Parse a `DOUBLE` for `key`, with Kafka's refusal.
pub(crate) fn parse_double(key: &str, value: &str) -> Result<f64, String> {
    double_value(value).ok_or_else(|| invalid_value(key, value, "Not a number of type DOUBLE"))
}

/// Kafka's `Range` validator over a parsed number: `Value must be at least
/// <min>` or `Value must be no more than <max>`, with the parsed value
/// printed.
pub(crate) fn check_range<T: PartialOrd + std::fmt::Display>(
    key: &str,
    value: T,
    min: Option<T>,
    max: Option<T>,
) -> Result<T, String> {
    if let Some(min) = min
        && value < min
    {
        return Err(invalid_value(
            key,
            &value,
            &format!("Value must be at least {min}"),
        ));
    }
    if let Some(max) = max
        && value > max
    {
        return Err(invalid_value(
            key,
            &value,
            &format!("Value must be no more than {max}"),
        ));
    }
    Ok(value)
}

/// Kafka's `ValidString`: the trimmed value must be one of `accepted`.
pub(crate) fn check_one_of<'a>(
    key: &str,
    value: &'a str,
    accepted: &[&str],
) -> Result<&'a str, String> {
    let trimmed = java_trim(value);
    if accepted.contains(&trimmed) {
        return Ok(trimmed);
    }
    Err(invalid_value(
        key,
        trimmed,
        &format!("String must be one of: {}", accepted.join(", ")),
    ))
}

/// Kafka's `ValidList`: no repeated element, no empty element, and, when
/// `accepted` is not empty, every element one of `accepted`. `empty_allowed`
/// is `ValidList.in`'s own flag, which is `true` for `cleanup.policy`.
pub(crate) fn check_valid_list<'a>(
    key: &str,
    value: &'a str,
    accepted: &[&str],
    empty_allowed: bool,
) -> Result<Vec<&'a str>, String> {
    let values = list_value(value);
    if !empty_allowed && values.is_empty() {
        let valid = if accepted.is_empty() {
            "any non-empty value".to_owned()
        } else {
            format!("[{}]", accepted.join(", "))
        };
        return Err(format!(
            "Configuration '{key}' must not be empty. Valid values include: {valid}"
        ));
    }
    let distinct: std::collections::BTreeSet<&str> = values.iter().copied().collect();
    if distinct.len() != values.len() {
        return Err(format!(
            "Configuration '{key}' values must not be duplicated."
        ));
    }
    for element in &values {
        if element.is_empty() {
            return Err(format!("Configuration '{key}' values must not be empty."));
        }
        if !accepted.is_empty() {
            check_one_of(key, element, accepted)?;
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn values_parse_the_way_config_def_parses_them() {
        check!(bool_value(" TRUE ") == Some(true));
        check!(bool_value("False") == Some(false));
        check!(bool_value("yes") == None);
        check!(long_value(" 1000 ") == Some(1000));
        check!(long_value("+7") == Some(7));
        check!(int_value("2147483648") == None);
        check!(double_value("0.5d") == Some(0.5));
        check!(double_value(" 2 ") == Some(2.0));
        check!(double_value("-Infinity") == Some(f64::NEG_INFINITY));
        check!(double_value("inf") == None);
        check!(double_value("abc") == None);
        check!(list_value(" ") == Vec::<&str>::new());
        check!(list_value(" compact , delete ") == vec!["compact", "delete"]);
        check!(list_value("delete,") == vec!["delete", ""]);
    }

    #[yare::parameterized(
        integer = { 2.0, "2.0" },
        fraction = { 0.5, "0.5" },
        small_fraction = { 0.001, "0.001" },
        scientific_notation = { 0.0001, "1.0E-4" },
        large_value = { 1.5e10, "1.5E10" },
        negative_zero = { -0.0, "-0.0" },
    )]
    fn a_double_prints_the_way_java_prints_it(value: f64, printed: &str) {
        check!(java_double(value) == printed, "{value}");
    }
}
