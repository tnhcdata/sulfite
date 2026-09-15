use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::BufRead;
use sulfite::ObjectInfo;

/// Formats a byte count as a short human-readable string (e.g. `1.99G`, `6.36M`).
pub fn human_size(size: u64) -> String {
    let size_kb = size as f64 / 1024.0;
    let size_mb = size_kb / 1024.0;
    let size_gb = size_mb / 1024.0;
    let size_tb = size_gb / 1024.0;
    if size_tb > 1.0 {
        format!("{:.2}T", size_tb)
    } else if size_gb > 1.0 {
        format!("{:.2}G", size_gb)
    } else if size_mb > 1.0 {
        format!("{:.2}M", size_mb)
    } else {
        format!("{:.2}K", size_kb)
    }
}

/// Prints an object in the multi-line colored style used by `list`. `display_key` is the
/// text shown as the header line; `restore_status` is only printed when present (LIST does
/// not request it, so it is elided there).
pub fn print_object_human(display_key: &str, obj: &ObjectInfo) {
    println!("  {}", display_key.bold());
    let mut line = format!(
        "    {} {} ({}) {} {} {} {}",
        "size:".blue(),
        obj.size,
        human_size(obj.size),
        "timestamp:".blue(),
        obj.timestamp,
        "storage_class:".blue(),
        obj.storage_class.as_deref().unwrap_or(""),
    );
    if let Some(rs) = obj.restore_status.as_deref() {
        line.push_str(&format!(" {} {}", "restore_status:".blue(), rs));
    }
    println!("{line}");
}

/// Warns if a non-empty prefix does not end with '/'. Directory-style S3 keys usually
/// use a trailing slash; omitting it can yield unexpected matches (e.g. "foo" matches
/// "foo" and "fooBar"). Not appropriate when the prefix is intentional and non-path
/// (e.g. "archive-" or "year-2024-"); in those cases the user can ignore the warning.
pub fn warn_prefix_no_trailing_slash(prefix: &str, context: &str) {
    if !prefix.is_empty() && !prefix.ends_with('/') {
        tracing::warn!(
            "WARNING [{}]: Prefix does not end with '/'. Keys may not match directory-style paths. \
             (if intentional, e.g. non-path prefix like 'archive-', ignore.)",
            context
        );
    }
}

pub fn get_line_count(filepath: &str) -> std::io::Result<usize> {
    let reader = std::io::BufReader::new(std::fs::File::open(filepath)?);
    let lines = reader.lines();
    Ok(lines.count())
}

pub fn get_keys_from_csv(
    filepath: &str,
    column_index: usize,
    has_header: bool,
) -> csv::Result<impl Iterator<Item = Result<String, csv::Error>>> {
    let rdr = csv::ReaderBuilder::new()
        .has_headers(has_header)
        .from_path(filepath)?;

    Ok(rdr
        .into_records()
        .map(move |record| record.map(|r| r[column_index].to_string())))
}

pub fn make_progress_bar(total: Option<u64>, visible: bool) -> indicatif::ProgressBar {
    if !visible {
        return ProgressBar::hidden();
    }

    let pb;
    let sty;
    match total {
        Some(total) => {
            pb = ProgressBar::new(total);
            sty = ProgressStyle::with_template(
                "{spinner:.cyan} [{bar:40.cyan/blue}] {pos:>7}/{len:7} [{elapsed_precise}<{eta_precise} {per_sec:.green}] {msg}"
            )
            .expect("valid progress bar template")
            .progress_chars("#>-");
        }
        None => {
            pb = ProgressBar::new_spinner();
            sty = ProgressStyle::with_template(
                "{spinner:.cyan} {pos:>7} [{elapsed_precise} {per_sec:.green}]",
            )
            .expect("valid progress bar template")
            .progress_chars("#>-");
        }
    }
    pb.set_style(sty);
    pb
}
