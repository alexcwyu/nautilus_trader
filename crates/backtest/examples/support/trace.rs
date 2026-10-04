//! Canonical trace helpers shared by the parity harnesses (std only; identical in both engines' trees).
//!
//! `META` records the configuration that produced a trace, `INPUT` the data it read, so the comparator
//! can refuse to compare runs of different scenarios. Engine-operational variables (journal location,
//! capacities, event limits) are printed on a separate `OPS` line, which the comparator ignores.

use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

fn is_operational(key: &str) -> bool {
    key.starts_with("PARITY_JOURNAL")
        || key.starts_with("PARITY_MAX_EVENTS")
        || key.starts_with("PARITY_ORDER_CAPACITY")
        || key.starts_with("PARITY_RISK_")
        || key == "PARITY_DIAG"
        || key == "PARITY_LOGGING"
}

/// `META engine=<engine> KEY=value ...` over every `PARITY_*` variable, sorted; `OPS ...` for the
/// operational ones.
pub(crate) fn meta(engine: &str) {
    let mut vars: Vec<(String, String)> =
        std::env::vars().filter(|(key, _)| key.starts_with("PARITY_")).collect();
    vars.sort();
    let (ops, scenario): (Vec<_>, Vec<_>) = vars.into_iter().partition(|(key, _)| is_operational(key));
    let render = |vars: &[(String, String)]| {
        vars.iter().map(|(key, value)| format!("{key}={value}")).collect::<Vec<_>>().join(" ")
    };
    println!("META engine={engine} {}", render(&scenario));
    println!("OPS {}", render(&ops));
}

fn last_line(path: &str) -> std::io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    let window = len.min(4096);
    file.seek(SeekFrom::Start(len - window))?;
    let mut tail = String::new();
    file.read_to_string(&mut tail)?;
    Ok(tail.lines().rev().find(|line| !line.trim().is_empty()).unwrap_or("").to_string())
}

/// `INPUT loaded=<n> first_ts=<ns> last_ts=<ns>` from the first and last CSV line of the file named
/// by `path_env`.
pub(crate) fn input_span(path_env: &str, loaded: usize) -> std::io::Result<()> {
    let path = std::env::var(path_env).map_err(|e| std::io::Error::other(format!("{path_env}: {e}")))?;
    let first = BufReader::new(std::fs::File::open(&path)?)
        .lines()
        .find_map(|line| line.ok().filter(|l| !l.trim().is_empty()))
        .unwrap_or_default();
    let ts = |line: &str| line.split(',').next().unwrap_or("").to_string();
    println!("INPUT loaded={loaded} first_ts={} last_ts={}", ts(&first), ts(&last_line(&path)?));
    Ok(())
}
