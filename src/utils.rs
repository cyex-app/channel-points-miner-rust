use std::time::{SystemTime, UNIX_EPOCH};

use rand::{distributions::Alphanumeric, Rng};

/// Unix time in seconds; `0.0` is used throughout as "never".
pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

pub fn percentage(a: i64, b: i64) -> i64 {
    if a == 0 || b == 0 {
        0
    } else {
        ((a as f64 / b as f64) * 100.0) as i64
    }
}

pub fn millify(value: i64) -> String {
    let abs = value.unsigned_abs() as f64;
    let sign = if value < 0 { "-" } else { "" };
    let (scaled, suffix) = match abs {
        a if a >= 1e9 => (a / 1e9, "B"),
        a if a >= 1e6 => (a / 1e6, "M"),
        a if a >= 1e3 => (a / 1e3, "k"),
        a => return format!("{sign}{a}"),
    };
    let text = format!("{scaled:.2}");
    let text = text.trim_end_matches('0').trim_end_matches('.');
    format!("{sign}{text}{suffix}")
}

pub fn random_alnum(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn random_hex(bytes: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..bytes).map(|_| format!("{:02x}", rng.gen::<u8>())).collect()
}

pub fn random_between(min: f64, max: f64) -> std::time::Duration {
    std::time::Duration::from_secs_f64(rand::thread_rng().gen_range(min..max))
}
