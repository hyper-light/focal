//! Words and numbers as focal shows them.

/// An eyebrow: the site's small mono capitals, tracked out, here a space between letters.
pub fn eyebrow(word: &str) -> String {
    let mut out = String::with_capacity(word.len().saturating_mul(2));
    for (i, c) in word.chars().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.extend(c.to_uppercase());
    }
    out
}

/// Bytes in decimal units, to three figures: `48.1 MB`.
pub fn bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "kB", "MB", "GB", "TB", "PB"];
    let mut v = n as f64;
    let mut unit = 0usize;
    while v >= 999.5 && unit < 5 {
        v /= 1000.0;
        unit = unit.saturating_add(1);
    }
    let name = UNITS.get(unit).copied().unwrap_or("B");
    if unit == 0 {
        format!("{n} {name}")
    } else if v < 9.995 {
        format!("{v:.2} {name}")
    } else if v < 99.95 {
        format!("{v:.1} {name}")
    } else {
        format!("{v:.0} {name}")
    }
}

/// A count, briefly: `950`, `12.4k`, `3.1M`.
pub fn count(n: u64) -> String {
    const UNITS: [&str; 5] = ["", "k", "M", "G", "T"];
    let mut v = n as f64;
    let mut unit = 0usize;
    while v >= 999.5 && unit < 4 {
        v /= 1000.0;
        unit = unit.saturating_add(1);
    }
    let name = UNITS.get(unit).copied().unwrap_or("");
    if unit == 0 {
        format!("{n}")
    } else if v < 99.95 {
        format!("{v:.1}{name}")
    } else {
        format!("{v:.0}{name}")
    }
}

/// A span of time: `640 µs`, `850 ms`, `4.2 s`, `1 m 05 s`, `2 h 03 m`.
pub fn duration(seconds: f64) -> String {
    let s = if seconds.is_nan() {
        0.0
    } else {
        seconds.max(0.0)
    };
    if s < 0.001 {
        format!("{} µs", (s * 1e6).round())
    } else if s < 1.0 {
        format!("{} ms", (s * 1000.0).round())
    } else if s < 10.0 {
        format!("{s:.1} s")
    } else if s < 60.0 {
        format!("{} s", s.round())
    } else if s < 3600.0 {
        let whole = s.round().min(u64::MAX as f64) as u64;
        format!("{} m {:02} s", whole / 60, whole % 60)
    } else {
        let minutes = (s / 60.0).round().min(u64::MAX as f64) as u64;
        format!("{} h {:02} m", minutes / 60, minutes % 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_read_well() {
        assert_eq!(eyebrow("focal"), "F O C A L");
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(999), "999 B");
        assert_eq!(bytes(1_000), "1.00 kB");
        assert_eq!(bytes(48_123_456), "48.1 MB");
        assert_eq!(bytes(653_200_000), "653 MB");
        assert_eq!(count(950), "950");
        assert_eq!(count(12_400), "12.4k");
        assert_eq!(count(3_100_000), "3.1M");
        assert_eq!(duration(0.000_64), "640 µs");
        assert_eq!(duration(0.85), "850 ms");
        assert_eq!(duration(4.21), "4.2 s");
        assert_eq!(duration(65.0), "1 m 05 s");
        assert_eq!(duration(7380.0), "2 h 03 m");
        assert_eq!(duration(f64::NAN), "0 µs");
    }
}
