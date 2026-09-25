use anyhow::Context;
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream};
use std::path::{Component, Path};
use std::time::SystemTime;
use sulfite::{S3Client, S3Error, copy_object_multipart_cross_clients};
use sulfite_tools::utils::{
    get_keys_from_csv, get_line_count, make_progress_bar, print_object_human,
    warn_prefix_no_trailing_slash,
};
use tracing::{debug, error, info, warn};

use crate::{CsvArgs, CsvCommand};

const IN_MEMORY_COPY_THRESHOLD: u64 = 20 * 1024 * 1024;

fn local_path_for_key(local_dir: &str, key: &str) -> anyhow::Result<String> {
    if key.is_empty() {
        anyhow::bail!("CSV key must not be empty");
    }

    let key_path = Path::new(key);
    if key_path.is_absolute()
        || key_path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        anyhow::bail!("CSV key is not a safe relative local path: {key}");
    }

    Path::new(local_dir)
        .join(key_path)
        .into_os_string()
        .into_string()
        .map_err(|path| {
            anyhow::anyhow!(
                "local path is not valid UTF-8: {}",
                Path::new(&path).display()
            )
        })
}

fn inferred_local_s3_match(
    local_size: u64,
    local_mtime: SystemTime,
    remote_size: u64,
    remote_mtime: SystemTime,
) -> bool {
    local_size == remote_size && local_mtime <= remote_mtime
}

fn effective_storage_class(size: u64, requested: Option<&str>) -> Option<&str> {
    if size < 16 * 1024 { None } else { requested }
}

fn inferred_storage_class_matches(
    actual_storage_class: Option<&str>,
    requested_storage_class: Option<&str>,
) -> bool {
    let actual = actual_storage_class.unwrap_or("STANDARD");
    let expected = requested_storage_class.unwrap_or("STANDARD");
    actual.eq_ignore_ascii_case(expected)
}

fn inferred_upload_matches(
    local_size: u64,
    local_mtime: SystemTime,
    remote_size: u64,
    remote_mtime: SystemTime,
    remote_storage_class: Option<&str>,
    requested_storage_class: Option<&str>,
) -> bool {
    inferred_local_s3_match(local_size, local_mtime, remote_size, remote_mtime)
        && inferred_storage_class_matches(remote_storage_class, requested_storage_class)
}

fn inferred_copy_matches(
    source_size: u64,
    destination_size: u64,
    destination_storage_class: Option<&str>,
    requested_storage_class: Option<&str>,
) -> bool {
    source_size == destination_size
        && inferred_storage_class_matches(destination_storage_class, requested_storage_class)
}

async fn destination_copy_matches(
    client: &S3Client,
    bucket: &str,
    key: &str,
    source_size: u64,
    requested_storage_class: Option<&str>,
) -> Result<bool, S3Error> {
    match client.head_object(bucket, key).await {
        Ok(destination) => Ok(inferred_copy_matches(
            source_size,
            destination.size,
            destination.storage_class.as_deref(),
            requested_storage_class,
        )),
        // 404s are not retriable, so they are mapped to AWSS3Error.
        Err(S3Error::AWSS3Error(_, _, _, 404)) => Ok(false),
        Err(error) => Err(error),
    }
}

pub async fn run_csv(
    client: S3Client,
    dst_client: Option<S3Client>,
    args: CsvArgs,
    show_progress: bool,
) -> anyhow::Result<()> {
    if args.workers == 0 {
        anyhow::bail!("CSV workers must be greater than zero");
    }

    if matches!(&args.command, CsvCommand::CopyCrossClients { .. }) {
        let dst_client = dst_client
            .as_ref()
            .context("destination S3 client is not configured")?;
        let src_part_size = client.multipart_part_size();
        let src_workers = client.multipart_workers();
        let multipart_part_size = dst_client.multipart_part_size();
        let multipart_workers = dst_client.multipart_workers();
        if src_part_size != multipart_part_size || src_workers != multipart_workers {
            warn!(
                "Source and destination multipart settings differ: source part_size={} bytes \
                 workers={}, destination part_size={} bytes workers={}. Using destination \
                 settings.",
                src_part_size, src_workers, multipart_part_size, multipart_workers,
            );
        }

        let batch_workers = u64::try_from(args.workers).unwrap_or(u64::MAX);
        let multipart_workers = u64::try_from(multipart_workers).unwrap_or(u64::MAX);
        let mib = 1024 * 1024;
        let max_in_memory_mib = IN_MEMORY_COPY_THRESHOLD
            .saturating_mul(batch_workers)
            .saturating_div(mib);
        let max_multipart_mib = multipart_part_size
            .saturating_mul(batch_workers)
            .saturating_mul(multipart_workers)
            .saturating_div(mib);
        warn!(
            "CSV cross-client copy concurrency: batch_workers={}, multipart_workers={}, \
             multipart_part_size={} MiB. Objects below {} MiB are copied fully in memory, \
             allowing up to approximately {} MiB of payload buffering; multipart copies can \
             buffer approximately {} MiB, plus SDK overhead.",
            args.workers,
            multipart_workers,
            multipart_part_size / mib,
            IN_MEMORY_COPY_THRESHOLD / mib,
            max_in_memory_mib,
            max_multipart_mib,
        );
    }

    if let Some(local_dir) = match &args.command {
        CsvCommand::Download { local_dir, .. } => Some(local_dir.clone()),
        _ => None,
    } {
        std::fs::create_dir_all(local_dir)?;
    }

    // Keys are streamed (iterator); we do a separate file read for line count so the progress bar can show total. Memory stays O(1) per key.
    let keys = get_keys_from_csv(&args.source_path, args.column_idx, args.has_header)?;

    match &args.command {
        CsvCommand::Head { prefix, .. }
        | CsvCommand::Download { prefix, .. }
        | CsvCommand::Upload { prefix, .. }
        | CsvCommand::Delete { prefix, .. }
        | CsvCommand::Restore { prefix, .. } => warn_prefix_no_trailing_slash(prefix, "csv"),
        CsvCommand::Copy {
            src_prefix,
            dst_prefix,
            ..
        }
        | CsvCommand::CopyCrossClients {
            src_prefix,
            dst_prefix,
            ..
        } => {
            warn_prefix_no_trailing_slash(src_prefix, "csv copy (source)");
            warn_prefix_no_trailing_slash(dst_prefix, "csv copy (destination)");
        }
    }

    let is_head = matches!(&args.command, CsvCommand::Head { .. });
    let skip_existing_with_inference = args.skip_existing_with_inference;
    let pb = if is_head {
        None
    } else {
        let total_lines = get_line_count(&args.source_path)? as u64;
        let key_count = total_lines.saturating_sub(if args.has_header { 1 } else { 0 });
        Some(make_progress_bar(Some(key_count), show_progress))
    };

    let failure_count = stream::iter(keys)
        .map(|key_result| {
            let client = client.clone();
            let dst_client = dst_client.clone();
            let command = args.command.clone();
            let pb = pb.clone();
            tokio::spawn(async move {
                let res: Result<(), anyhow::Error> = async {
                    let key = key_result.context("reading key from CSV")?;
                    match command {
                        CsvCommand::Head { bucket, prefix, suffix, .. } => {
                            let full_key = format!("{prefix}{key}{suffix}");
                            let obj = client.head_object(&bucket, &full_key).await
                                .with_context(|| format!("heading key {full_key}"))?;
                            // Display the key as it appears in the CSV plus suffix (prefix omitted), matching `list`.
                            let display_key = format!("{key}{suffix}");
                            print_object_human(&display_key, &obj);
                        }
                        CsvCommand::Download { bucket, prefix, suffix, local_dir, .. } => {
                            let local_path = local_path_for_key(&local_dir, &key)?;
                            let key = format!("{prefix}{key}{suffix}");

                            let obj = client.head_object(&bucket, &key).await
                                .with_context(|| format!("heading key {key}"))?;

                            // S3-authoritative inference: skip when size matches and the S3 object
                            // is at least as new as the local file. This makes downloading immediately
                            // after an upload a no-op. A newer local file is intentionally overwritten
                            // when the user explicitly chooses download.
                            if skip_existing_with_inference {
                                match std::fs::metadata(&local_path) {
                                    Ok(local_file_info) => {
                                        let local_file_size = local_file_info.len();
                                        let local_mtime = local_file_info.modified()?;
                                        let remote_mtime = match SystemTime::try_from(obj.timestamp) {
                                            Ok(t) => t,
                                            Err(e) => {
                                                warn!("key {key}: could not convert remote timestamp to SystemTime ({}), treating as epoch", e);
                                                SystemTime::UNIX_EPOCH
                                            }
                                        };
                                        if inferred_local_s3_match(
                                            local_file_size,
                                            local_mtime,
                                            obj.size,
                                            remote_mtime,
                                        ) {
                                            if let Some(pb) = pb.as_ref() { pb.set_message(format!("{key} already exists locally. Skipping.")); }
                                            return Ok(());
                                        } else {
                                            let local_ts = DateTime::<Utc>::from(local_mtime).format("%Y-%m-%dT%H:%M:%SZ");
                                            info!(
                                                "Object {key} already exists locally but with different size or timestamp.\n  local: {local_file_size} {local_ts}\n  remote: {} {}",
                                                obj.size, obj.timestamp
                                            );
                                        }
                                    }
                                    Err(error)
                                        if error.kind() == std::io::ErrorKind::NotFound =>
                                    {}
                                    Err(error) => {
                                        return Err(error).with_context(|| {
                                            format!("reading metadata for {local_path}")
                                        });
                                    }
                                }
                            }

                            let dirname = std::path::Path::new(&local_path)
                                .parent()
                                .and_then(|p| p.to_str())
                                .ok_or_else(|| anyhow::anyhow!("path has no parent or invalid UTF-8: {local_path}"))?;

                            tokio::fs::create_dir_all(dirname).await
                                .with_context(|| format!("creating directory {dirname}"))?;

                            // < 1 GB → single GET; >= 1 GB → multipart download.
                            if obj.size < 1024 * 1024 * 1024 {
                                client
                                    .download_object(&bucket, &key, &local_path, None)
                                    .await
                            } else {
                                client
                                    .download_object_multipart(
                                        &bucket,
                                        &key,
                                        &local_path,
                                        None::<&indicatif::ProgressBar>,
                                    )
                                    .await
                            }
                            .with_context(|| format!("downloading object {key}"))?;
                            debug!("object {key} downloaded.");
                        }
                        CsvCommand::Upload { bucket, prefix, suffix, local_dir, storage_class, .. } => {
                            let local_path = local_path_for_key(&local_dir, &key)?;
                            let key = format!("{prefix}{key}{suffix}");

                            let local_file_info = std::fs::metadata(&local_path)
                                .with_context(|| format!("reading metadata for {local_path}"))?;
                            let local_file_size = local_file_info.len();
                            let local_mtime = local_file_info.modified()?;
                            let effective_storage_class = effective_storage_class(
                                local_file_size,
                                storage_class.as_deref(),
                            );

                            // S3-authoritative inference: skip when size matches and the S3 object
                            // is at least as new as the local file, and its storage class matches
                            // the effective requested class. A newer local file or class mismatch
                            // is uploaded.
                            if skip_existing_with_inference {
                                match client.head_object(&bucket, key.as_str()).await {
                                    Ok(obj) => {
                                        let remote_mtime = match SystemTime::try_from(obj.timestamp) {
                                            Ok(t) => t,
                                            Err(e) => {
                                                warn!("key {key}: could not convert remote timestamp to SystemTime ({}), treating as epoch", e);
                                                SystemTime::UNIX_EPOCH
                                            }
                                        };
                                        if inferred_upload_matches(
                                            local_file_size,
                                            local_mtime,
                                            obj.size,
                                            remote_mtime,
                                            obj.storage_class.as_deref(),
                                            effective_storage_class,
                                        ) {
                                            if let Some(pb) = pb.as_ref() { pb.set_message(format!("{key} already exists on destination. Skipping.")); }
                                            return Ok(());
                                        } else {
                                            let local_ts = DateTime::<Utc>::from(local_mtime).format("%Y-%m-%dT%H:%M:%SZ");
                                            info!(
                                                "Object {key} already exists on destination but with different size or timestamp.\n  local: {local_file_size} {local_ts}\n  remote: {} {}",
                                                obj.size, obj.timestamp
                                            );
                                        }
                                    }
                                    // 404s are not retriable, so error goes to AWSS3Error.
                                    Err(S3Error::AWSS3Error(_, _, _, 404)) => {}
                                    Err(e) => return Err(e)
                                        .with_context(|| format!("heading key {key} on destination")),
                                }
                            }

                            // For archival tier, small files should still be STANDARD for efficiency.
                            // Upload path by size: < 16 KB → single-part, no storage class (default STANDARD);
                            // >= 16 KB and < 1 GB → single-part with storage_class; >= 1 GB → multipart with storage_class.
                            if local_file_size < 16 * 1024 {
                                client.upload_object(&bucket, &key, &local_path, None).await
                            } else if local_file_size < 1024 * 1024 * 1024 {
                                client
                                    .upload_object(
                                        &bucket,
                                        &key,
                                        &local_path,
                                        effective_storage_class,
                                    )
                                    .await
                            } else {
                                client
                                    .upload_object_multipart(
                                        &bucket,
                                        &key,
                                        &local_path,
                                        effective_storage_class,
                                        None::<&indicatif::ProgressBar>,
                                    )
                                    .await
                            }
                            .with_context(|| format!("uploading object {key}"))?;
                            debug!("object {key} uploaded.");
                        }
                        CsvCommand::Delete { bucket, prefix, suffix, .. } => {
                            let key = format!("{prefix}{key}{suffix}");
                            client.delete_object(&bucket, &key).await
                                .with_context(|| format!("deleting object {key}"))?;
                            debug!("object {key} deleted.");
                        }
                        CsvCommand::Copy { src_bucket, src_prefix, src_suffix, dst_bucket, dst_prefix, dst_suffix, dst_storage_class, .. } => {
                            let src_key = format!("{src_prefix}{key}{src_suffix}");
                            let dst_key = format!("{dst_prefix}{key}{dst_suffix}");

                            let src_obj = client.head_object(&src_bucket, &src_key).await
                                .with_context(|| format!("heading key {src_key} on source"))?;
                            let dst_storage_class = effective_storage_class(
                                src_obj.size,
                                dst_storage_class.as_deref(),
                            );

                            if skip_existing_with_inference
                                && destination_copy_matches(
                                    &client,
                                    &dst_bucket,
                                    &dst_key,
                                    src_obj.size,
                                    dst_storage_class,
                                )
                                .await
                                .with_context(|| format!("heading key {dst_key} on destination"))?
                            {
                                if let Some(pb) = pb.as_ref() { pb.set_message(format!("{dst_key} already exists on destination with matching size and storage class. Skipping.")); }
                                return Ok(());
                            }

                            // Skip only if src and dst are the same key and src is in archival tier.
                            // This happens when you idempotently copy an object into the same destination bucket and key with an archival tier storage class.
                            if let Some(src_storage_class) = src_obj.storage_class
                                && src_bucket == dst_bucket
                                && src_key == dst_key
                                && src_storage_class.as_str()
                                    == dst_storage_class.unwrap_or("STANDARD")
                            {
                                if let Some(pb) = pb.as_ref() { pb.set_message(format!("{src_key} already exists in destination and has the same storage class. Skipping.")); }
                                return Ok(());
                            }

                            // For archival tier, small files should still be STANDARD for efficiency.
                            // Copy path by size: < 16 KB → no storage class (default STANDARD);
                            // >= 16 KB and < 1 GB → single-part with dst_storage_class;
                            // >= 1 GB → multipart with dst_storage_class.
                            if src_obj.size < 16 * 1024 {
                                client
                                    .copy_object(
                                        &src_bucket,
                                        &src_key,
                                        &dst_bucket,
                                        &dst_key,
                                        None,
                                    )
                                    .await
                            } else if src_obj.size < 1024 * 1024 * 1024 {
                                client
                                    .copy_object(
                                        &src_bucket,
                                        &src_key,
                                        &dst_bucket,
                                        &dst_key,
                                        dst_storage_class,
                                    )
                                    .await
                            } else {
                                client
                                    .copy_object_multipart(
                                        &src_bucket,
                                        &src_key,
                                        &dst_bucket,
                                        &dst_key,
                                        dst_storage_class,
                                        None::<&indicatif::ProgressBar>,
                                    )
                                    .await
                            }
                                .with_context(|| format!("copying object {dst_key}"))?;
                            debug!("object {dst_key} copied.");
                        }
                        CsvCommand::CopyCrossClients { src_bucket, src_prefix, src_suffix, dst_bucket, dst_prefix, dst_suffix, dst_storage_class, .. } => {
                            let dst_client = dst_client.as_ref().ok_or_else(|| {
                                anyhow::anyhow!("destination S3 client is not configured")
                            })?;
                            let src_key = format!("{src_prefix}{key}{src_suffix}");
                            let dst_key = format!("{dst_prefix}{key}{dst_suffix}");
                            let src_obj = client.head_object(&src_bucket, &src_key).await
                                .with_context(|| format!("heading key {src_key} on source"))?;

                            // Keep small archival objects in STANDARD, matching csv copy behavior.
                            let dst_storage_class = effective_storage_class(
                                src_obj.size,
                                dst_storage_class.as_deref(),
                            );
                            if skip_existing_with_inference
                                && destination_copy_matches(
                                    dst_client,
                                    &dst_bucket,
                                    &dst_key,
                                    src_obj.size,
                                    dst_storage_class,
                                )
                                .await
                                .with_context(|| format!("heading key {dst_key} on destination"))?
                            {
                                if let Some(pb) = pb.as_ref() { pb.set_message(format!("{dst_key} already exists on destination with matching size and storage class. Skipping.")); }
                                return Ok(());
                            }
                            if src_obj.size < IN_MEMORY_COPY_THRESHOLD {
                                client
                                    .copy_object_cross_clients(
                                        dst_client,
                                        &src_bucket,
                                        &src_key,
                                        &dst_bucket,
                                        &dst_key,
                                        dst_storage_class,
                                    )
                                    .await
                            } else {
                                copy_object_multipart_cross_clients(
                                    &client,
                                    dst_client,
                                    &src_bucket,
                                    &src_key,
                                    &dst_bucket,
                                    &dst_key,
                                    dst_storage_class,
                                    None::<&indicatif::ProgressBar>,
                                )
                                .await
                            }
                            .with_context(|| {
                                format!(
                                    "copying object across clients from {src_key} to {dst_key}"
                                )
                            })?;
                            debug!("object {dst_key} copied across clients.");
                        }
                        CsvCommand::Restore { bucket, prefix, suffix, restore_tier, restore_days, .. } => {
                            let key = format!("{prefix}{key}{suffix}");
                            client
                                .restore_object(&bucket, &key, restore_days, &restore_tier)
                                .await
                                .with_context(|| format!("restoring object {key}"))?;
                            debug!("object {key} restored.");
                        }
                    }
                    Ok(())
                }
                .await;

                if let Some(pb) = pb.as_ref() { pb.inc(1) }
                res
            })
        })
        .buffer_unordered(if is_head { 1 } else { args.workers })
        .fold(0usize, |failure_count, result| async move {
            match result {
                Ok(Ok(())) => failure_count,
                Ok(Err(e)) => {
                    error!("{e:#}");
                    failure_count + 1
                }
                Err(e) => {
                    error!("Spawned task failed: {e:#}");
                    failure_count + 1
                }
            }
        })
        .await;

    if let Some(pb) = pb.as_ref() {
        pb.finish()
    }

    if failure_count > 0 {
        anyhow::bail!("{failure_count} CSV operation(s) failed");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use super::{
        effective_storage_class, inferred_copy_matches, inferred_local_s3_match,
        inferred_upload_matches, local_path_for_key,
    };

    #[test]
    fn local_key_path_stays_under_local_directory() {
        assert_eq!(
            local_path_for_key("/tmp/base", "nested/object.txt").unwrap(),
            "/tmp/base/nested/object.txt"
        );
    }

    #[test]
    fn local_key_path_rejects_escaping_paths() {
        assert!(local_path_for_key("/tmp/base", "").is_err());
        assert!(local_path_for_key("/tmp/base", "../object.txt").is_err());
        assert!(local_path_for_key("/tmp/base", "nested/../../object.txt").is_err());
        assert!(local_path_for_key("/tmp/base", "/tmp/object.txt").is_err());
    }

    #[test]
    fn archival_copy_keeps_tiny_objects_in_standard() {
        assert_eq!(
            effective_storage_class(16 * 1024 - 1, Some("DEEP_ARCHIVE")),
            None
        );
        assert_eq!(
            effective_storage_class(16 * 1024, Some("DEEP_ARCHIVE")),
            Some("DEEP_ARCHIVE")
        );
    }

    #[test]
    fn copy_inference_requires_matching_size_and_storage_class() {
        assert!(inferred_copy_matches(
            42,
            42,
            Some("DEEP_ARCHIVE"),
            Some("deep_archive"),
        ));
        assert!(inferred_copy_matches(42, 42, None, None));
        assert!(!inferred_copy_matches(
            42,
            41,
            Some("DEEP_ARCHIVE"),
            Some("DEEP_ARCHIVE"),
        ));
        assert!(!inferred_copy_matches(42, 42, None, Some("DEEP_ARCHIVE"),));
    }

    #[test]
    fn local_s3_inference_treats_s3_as_authoritative() {
        let local_time = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let later_s3_time = SystemTime::UNIX_EPOCH + Duration::from_secs(20);
        assert!(inferred_local_s3_match(42, local_time, 42, later_s3_time,));
        assert!(inferred_local_s3_match(42, local_time, 42, local_time));
        assert!(!inferred_local_s3_match(42, later_s3_time, 42, local_time,));
        assert!(!inferred_local_s3_match(41, local_time, 42, later_s3_time,));
    }

    #[test]
    fn upload_inference_requires_effective_storage_class() {
        let time = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
        let archival_size = 16 * 1024;
        let archival_class = effective_storage_class(archival_size, Some("DEEP_ARCHIVE"));

        assert!(!inferred_upload_matches(
            archival_size,
            time,
            archival_size,
            time,
            None,
            archival_class,
        ));
        assert!(inferred_upload_matches(
            archival_size,
            time,
            archival_size,
            time,
            Some("DEEP_ARCHIVE"),
            archival_class,
        ));

        let tiny_size = 16 * 1024 - 1;
        assert!(inferred_upload_matches(
            tiny_size,
            time,
            tiny_size,
            time,
            None,
            effective_storage_class(tiny_size, Some("DEEP_ARCHIVE")),
        ));
    }
}
