//! The `--to-timestamp` parser: epoch milliseconds or an RFC 3339 instant, and
//! the calendar arithmetic behind the second form.
//!
//! The bound is carried as epoch milliseconds everywhere else, so the whole
//! conversion is here, down to `days_from_civil`. It is written out rather than
//! taken from a date library because the only thing the restore needs is one
//! instant, and the zone designator RFC 3339 requires is the one part an
//! operator can get wrong in a way no default should paper over.

/// Parse `--to-timestamp` into epoch milliseconds.
///
/// A value of only digits, with an optional sign, is epoch milliseconds. Every
/// other value is RFC 3339.
pub(super) fn parse_timestamp(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("timestamp must not be empty".into());
    }
    if is_signed_integer(s) {
        return s
            .parse()
            .map_err(|error| format!("epoch milliseconds: {error}"));
    }
    parse_rfc3339_millis(s)
}

fn is_signed_integer(s: &str) -> bool {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

/// Convert an RFC 3339 instant to epoch milliseconds, truncating any precision
/// below a millisecond.
///
/// The zone designator is required, as RFC 3339 requires it. A restore bound
/// with an implied zone is a bound nobody can check.
fn parse_rfc3339_millis(s: &str) -> Result<i64, String> {
    let (date, rest) = s
        .split_at_checked(10)
        .ok_or_else(|| format!("expected an RFC 3339 timestamp, got {s:?}"))?;
    let (separator, rest) = rest
        .split_at_checked(1)
        .ok_or_else(|| format!("expected a time after the date in {s:?}"))?;
    if !matches!(separator, "T" | "t" | " ") {
        return Err(format!(
            "expected T between the date and the time, got {separator:?}"
        ));
    }
    let (year, month, day) = parse_date(date)?;
    let (time, offset_seconds) = split_zone(rest)?;
    let (hour, minute, second, millis) = parse_time(time)?;

    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offset_seconds;
    Ok(seconds * 1_000 + millis)
}

fn parse_date(date: &str) -> Result<(i64, i64, i64), String> {
    let bytes = date.as_bytes();
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return Err(format!("expected a YYYY-MM-DD date, got {date:?}"));
    }
    let year = parse_digits(&date[0..4], "year")?;
    let month = parse_digits(&date[5..7], "month")?;
    let day = parse_digits(&date[8..10], "day")?;
    if year == 0 {
        return Err("year must be 0001 or later".into());
    }
    if !(1..=12).contains(&month) {
        return Err(format!("month must be 01..12, got {month:02}"));
    }
    let last = days_in_month(year, month);
    if !(1..=last).contains(&day) {
        return Err(format!(
            "day must be 01..{last:02} for {year:04}-{month:02}, got {day:02}"
        ));
    }
    Ok((year, month, day))
}

/// Split the zone designator off the end and return the offset in seconds.
fn split_zone(rest: &str) -> Result<(&str, i64), String> {
    if let Some(time) = rest.strip_suffix(['Z', 'z']) {
        return Ok((time, 0));
    }
    let split = rest
        .len()
        .checked_sub(6)
        .and_then(|at| rest.split_at_checked(at));
    let (time, zone) = split.ok_or_else(|| {
        format!("expected a zone designator, Z or +HH:MM or -HH:MM, at the end of {rest:?}")
    })?;
    let bytes = zone.as_bytes();
    let sign = match bytes[0] {
        b'+' => 1,
        b'-' => -1,
        _ => {
            return Err(format!(
                "expected a zone designator, Z or +HH:MM or -HH:MM, got {zone:?}"
            ));
        }
    };
    if bytes[3] != b':' {
        return Err(format!("expected a zone offset of +HH:MM, got {zone:?}"));
    }
    let hours = parse_digits(&zone[1..3], "zone hours")?;
    let minutes = parse_digits(&zone[4..6], "zone minutes")?;
    if hours > 23 || minutes > 59 {
        return Err(format!("zone offset out of range: {zone:?}"));
    }
    Ok((time, sign * (hours * 3_600 + minutes * 60)))
}

fn parse_time(time: &str) -> Result<(i64, i64, i64, i64), String> {
    let (hms, fraction) = match time.split_once('.') {
        Some((hms, fraction)) => (hms, Some(fraction)),
        None => (time, None),
    };
    let bytes = hms.as_bytes();
    if bytes.len() != 8 || bytes[2] != b':' || bytes[5] != b':' {
        return Err(format!("expected a HH:MM:SS time, got {hms:?}"));
    }
    let hour = parse_digits(&hms[0..2], "hour")?;
    let minute = parse_digits(&hms[3..5], "minute")?;
    let second = parse_digits(&hms[6..8], "second")?;
    if hour > 23 {
        return Err(format!("hour must be 00..23, got {hour:02}"));
    }
    if minute > 59 {
        return Err(format!("minute must be 00..59, got {minute:02}"));
    }
    if second > 59 {
        return Err(format!("second must be 00..59, got {second:02}"));
    }
    let millis = match fraction {
        None => 0,
        Some(fraction) => {
            if fraction.is_empty() || fraction.len() > 9 {
                return Err(format!(
                    "fractional second must be 1 to 9 digits, got {fraction:?}"
                ));
            }
            let mut padded = String::from(fraction);
            padded.push_str("000");
            parse_digits(&padded[0..3], "fractional second")?
        }
    };
    Ok((hour, minute, second, millis))
}

fn parse_digits(s: &str, field: &str) -> Result<i64, String> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("{field} must be digits, got {s:?}"));
    }
    s.parse().map_err(|error| format!("{field}: {error}"))
}

fn is_leap_year(year: i64) -> bool { (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 }

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        _ => 28,
    }
}

/// Days between 1970-01-01 and the given civil date, by Howard Hinnant's
/// `days_from_civil`. The era arithmetic holds for every year this parser
/// accepts, so no calendar table is needed.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let shifted = if month <= 2 { year - 1 } else { year };
    let era = if shifted >= 0 { shifted } else { shifted - 399 } / 400;
    let year_of_era = shifted - era * 400;
    let month_from_march = (month + 9) % 12;
    let day_of_year = (153 * month_from_march + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn timestamps_accept_bare_epoch_milliseconds() {
        for (input, expected) in [
            ("0", 0),
            ("1", 1),
            ("+1", 1),
            ("-1", -1),
            ("1713000000000", 1_713_000_000_000),
            (" 1713000000000 ", 1_713_000_000_000),
        ] {
            check!(parse_timestamp(input) == Ok(expected), "{input:?}");
        }
    }

    #[test]
    fn timestamps_accept_rfc3339_instants() {
        for (input, expected) in [
            ("1970-01-01T00:00:00Z", 0),
            ("1970-01-01T00:00:00.001Z", 1),
            ("1970-01-01T00:00:00.000999999Z", 0),
            ("1969-12-31T23:59:59Z", -1_000),
            ("2026-08-24T12:00:00Z", 1_787_572_800_000),
            ("2026-08-24t12:00:00z", 1_787_572_800_000),
            ("2026-08-24 12:00:00Z", 1_787_572_800_000),
            ("2026-08-24T12:00:00+00:00", 1_787_572_800_000),
            ("2026-08-24T14:00:00+02:00", 1_787_572_800_000),
            ("2026-08-24T07:00:00-05:00", 1_787_572_800_000),
            ("2026-08-24T12:00:00.250Z", 1_787_572_800_250),
            ("2026-08-24T12:00:00.2Z", 1_787_572_800_200),
            ("2024-02-29T00:00:00Z", 1_709_164_800_000),
            ("2000-02-29T00:00:00Z", 951_782_400_000),
        ] {
            check!(parse_timestamp(input) == Ok(expected), "{input:?}");
        }
    }

    #[test]
    fn timestamps_reject_impossible_and_zoneless_input() {
        for bad in [
            "",
            "   ",
            "not-a-time",
            "2026-08-24",
            "2026-08-24T12:00:00",
            "2026-08-24X12:00:00Z",
            "2026-13-01T00:00:00Z",
            "2026-00-01T00:00:00Z",
            "2026-08-32T00:00:00Z",
            "2026-08-00T00:00:00Z",
            "2023-02-29T00:00:00Z",
            "1900-02-29T00:00:00Z",
            "0000-01-01T00:00:00Z",
            "2026-08-24T24:00:00Z",
            "2026-08-24T12:60:00Z",
            "2026-08-24T12:00:60Z",
            "2026-08-24T12:00:00.Z",
            "2026-08-24T12:00:00.0000000000Z",
            "2026-08-24T12:00:00+0200",
            "2026-08-24T12:00:00+24:00",
            "2026-08-24T12:00:00+02:60",
            "2026-08-24T12:00:00*02:00",
            "2026/08/24T12:00:00Z",
            "26-08-24T12:00:00Z",
        ] {
            check!(parse_timestamp(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn civil_days_match_known_epochs() {
        for (year, month, day, expected) in [
            (1970, 1, 1, 0),
            (1970, 1, 2, 1),
            (1969, 12, 31, -1),
            (2000, 3, 1, 11_017),
            (2026, 8, 24, 20_689),
            (1600, 1, 1, -135_140),
        ] {
            check!(
                days_from_civil(year, month, day) == expected,
                "{year}-{month}-{day}"
            );
        }
    }
}
