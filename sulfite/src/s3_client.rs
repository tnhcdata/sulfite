use crate::multipart::{MultipartPlan, validate_content_range};
use crate::retry_strategy::RetryStrategy;
use crate::utils::generate_random_hex;
use aws_config::Region;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    Client as AWSS3Client, Error as AWSS3Error,
    error::{ErrorMetadata, ProvideErrorMetadata, SdkError},
    operation::list_objects_v2::ListObjectsV2Output,
    primitives::{ByteStream, ByteStreamError, DateTime, DateTimeFormat, Length},
    types::{
        CompletedMultipartUpload, CompletedPart, GlacierJobParameters, RestoreRequest,
        StorageClass, Tier,
    },
};
use bytes::Bytes;
use core::str;
use futures::{StreamExt, TryStreamExt, stream};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio_retry::RetryIf;

/// Default read timeout in seconds for the underlying HTTP client (boto default).
pub const DEFAULT_READ_TIMEOUT: u64 = 60;
/// Default maximum number of attempts for our high-level retries (0 means no high-level retries and rely on the underlying SDK retries).
pub const DEFAULT_MAX_RETRIES: usize = 0;
/// Default HTTP status codes treated as retriable client errors (408 Request Timeout, 429 Too Many Requests).
/// Error code SlowDown is also retried.
pub const DEFAULT_RETRIABLE_CLIENT_STATUS_CODES: &[u16] = &[408, 429];
/// Comma-separated default for CLI; must match DEFAULT_RETRIABLE_CLIENT_STATUS_CODES.
pub const DEFAULT_RETRIABLE_CLIENT_STATUS_CODES_STR: &str = "408,429";
/// Region used when the environment/config does not provide one.
pub const FALLBACK_REGION: &str = "us-east-1";
/// Buffer size for upload byte stream (1 MiB).
pub const FILE_BUFFER_SIZE: usize = 1024 * 1024;
/// Part size for multipart upload/download/copy (20 MiB). Uploads and copies constrain the
/// effective size to 5 MiB through 5 GiB and adapt upward to stay within
/// `MULTIPART_MAX_PARTS`.
pub const DEFAULT_MULTIPART_PART_SIZE: u64 = 1024 * 1024 * 20;
/// Number of parallel workers for multipart download/upload/copy when not overridden per call (default: 1).
pub const DEFAULT_MULTIPART_WORKERS: usize = 1;
/// S3 API limit on number of parts per multipart upload (10_000).
pub const MULTIPART_MAX_PARTS: u64 = 10000;
/// S3 minimum size for every multipart upload part except the final part (5 MiB).
pub const MULTIPART_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;
/// S3 maximum size for one multipart upload part (5 GiB).
pub const MULTIPART_MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;
/// S3 multipart object ceiling: 10,000 parts of 5 GiB each (marketed as 50 TB).
pub const MULTIPART_MAX_OBJECT_SIZE: u64 = MULTIPART_MAX_PARTS * MULTIPART_MAX_PART_SIZE;

/// Progress bar for multipart transfer (e.g. part count). Use with [`download_object_multipart`](S3Client::download_object_multipart), [`upload_object_multipart`](S3Client::upload_object_multipart), and [`copy_object_multipart`](S3Client::copy_object_multipart).
/// When the `indicatif` feature is enabled, [`indicatif::ProgressBar`] implements this trait.
pub trait ProgressBar: Send + Sync + Clone {
    /// Set the total number of units (e.g. parts).
    fn set_length(&self, len: u64);
    /// Advance by `delta` units (e.g. one part completed).
    fn inc(&self, delta: u64);
    /// Mark the progress bar as finished.
    fn finish(&self);
}

#[cfg(feature = "indicatif")]
impl ProgressBar for indicatif::ProgressBar {
    fn set_length(&self, len: u64) {
        indicatif::ProgressBar::set_length(self, len);
    }
    fn inc(&self, delta: u64) {
        indicatif::ProgressBar::inc(self, delta);
    }
    fn finish(&self) {
        indicatif::ProgressBar::finish(self);
    }
}

/// No-op progress bar. Use when progress reporting is not needed (e.g. in tests).
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopProgressBar;

impl ProgressBar for NoopProgressBar {
    fn set_length(&self, _len: u64) {}
    fn inc(&self, _delta: u64) {}
    fn finish(&self) {}
}

/// Configuration for the underlying AWS S3 client (region, endpoint, credentials, timeouts).
#[derive(Clone, Debug)]
pub struct S3ClientConfig {
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub profile_name: Option<String>,
    pub access_secret_session_tuple: Option<(String, String, Option<String>)>,
    /// Read timeout in seconds for the HTTP client (default: 60).
    pub read_timeout_secs: u64,
    /// Part size for multipart upload/download/copy in bytes (default: 20 MiB). Uploads and copies
    /// constrain the effective size to 5 MiB through 5 GiB and adapt upward to stay within
    /// `MULTIPART_MAX_PARTS`.
    pub multipart_part_size: u64,
    /// Number of parallel workers for multipart download/upload/copy when not overridden per call (default: 1).
    pub multipart_workers: usize,
}

impl Default for S3ClientConfig {
    fn default() -> Self {
        Self {
            region: None,
            endpoint_url: None,
            profile_name: None,
            access_secret_session_tuple: None,
            read_timeout_secs: DEFAULT_READ_TIMEOUT,
            multipart_part_size: DEFAULT_MULTIPART_PART_SIZE,
            multipart_workers: DEFAULT_MULTIPART_WORKERS,
        }
    }
}

/// Configuration for retry behavior (max retries, strategy, and which client status codes to retry).
/// Use [`RetryConfig::default`] for default retry behavior (no high-level retries).
#[derive(Clone, Debug)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub retry_strategy: RetryStrategy,
    pub retriable_client_status_codes: Vec<u16>,
}

/// Default retry configuration (max_retries=0, exponential backoff strategy, retriable client status codes: 408, 429).
impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            retry_strategy: RetryStrategy::default(),
            retriable_client_status_codes: DEFAULT_RETRIABLE_CLIENT_STATUS_CODES.to_vec(),
        }
    }
}

/// BucketInfo
#[derive(Clone, Debug)]
pub struct BucketInfo {
    pub name: String,
    pub region: Option<String>,
}

/// Metadata for an S3 object (from HEAD or LIST).
#[derive(Clone, Debug)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    /// Last-modified time (AWS SDK `DateTime`).
    pub timestamp: DateTime,
    pub storage_class: Option<String>,
    /// Normalized restore status:
    /// - `None` — no restore status (object not restored and not being restored),
    /// - `Some("ONGOING")` — a restore is currently in progress,
    /// - `Some("EXPIRY:<ts>")` — a temporary restored copy is available until `<ts>` (RFC3339 UTC).
    ///
    /// Normalized identically whether the value came from the LIST or HEAD/GET API.
    pub restore_status: Option<String>,
}

/// A common prefix from a list_objects_v2 response (delimiter-based "directory").
#[derive(Clone, Debug)]
pub struct CommonPrefixInfo {
    pub prefix: String,
}

/// Page-by-page iterator for list_objects_v2. Yields one page at a time; retries are applied
/// per page request, so a failure on one page does not invalidate the iterator.
/// MaxKeys is not set (SDK default, typically 1000 keys per page).
///
/// Uses `Option<Option<String>>` for continuation: `None` = first request not yet made,
/// `Some(None)` = no more pages, `Some(Some(token))` = use token for next request.
pub struct ListObjectsV2PageIter<'a> {
    s3_client: &'a S3Client,
    bucket: &'a str,
    prefix: &'a str,
    delimiter: Option<&'a str>,
    /// None = first page not fetched; Some(None) = exhausted; Some(Some(t)) = next token
    continuation_token: Option<Option<String>>,
}

impl<'a> ListObjectsV2PageIter<'a> {
    /// Fetches the next page. Returns `Ok(None)` when there are no more pages.
    /// Retries (using the client's retry config) are applied to this single page request only.
    pub async fn next_page(&mut self) -> Result<Option<(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)>> {
        if let Some(None) = self.continuation_token {
            return Ok(None);
        }

        let s3_client = self.s3_client;
        let resp = s3_client
            .with_retry(|| async {
                let mut builder = s3_client
                    .inner
                    .list_objects_v2()
                    .bucket(self.bucket)
                    .prefix(self.prefix);
                if let Some(d) = self.delimiter {
                    builder = builder.delimiter(d);
                }
                if let Some(Some(t)) = &self.continuation_token {
                    builder = builder.continuation_token(t);
                }
                builder.send().await.map_err(|e| {
                    map_sdk_error(
                        format!(
                            "<list_objects_v2_paginate_pages> bucket={} prefix={}",
                            self.bucket, self.prefix
                        ),
                        s3_client
                            .retry_config
                            .retriable_client_status_codes
                            .as_slice(),
                        e,
                    )
                })
            })
            .await?;

        let more = resp.is_truncated() == Some(true)
            && resp
                .next_continuation_token()
                .map(|s| !s.is_empty())
                .unwrap_or(false);
        self.continuation_token = Some(if more {
            resp.next_continuation_token().map(String::from)
        } else {
            None
        });

        Ok(Some(page_to_object_and_prefix_lists(&resp)?))
    }
}

/// Normalized restore status from the structured `RestoreStatus` returned by LIST.
/// See [`ObjectInfo::restore_status`] for the possible values.
fn normalize_restore_status_from_list(
    rs: Option<&aws_sdk_s3::types::RestoreStatus>,
) -> Option<String> {
    let rs = rs?;
    if rs.is_restore_in_progress == Some(true) {
        Some("ONGOING".to_string())
    } else if let Some(exp) = rs.restore_expiry_date() {
        let ts = exp
            .fmt(DateTimeFormat::DateTime)
            .unwrap_or_else(|_| exp.to_string());
        Some(format!("EXPIRY:{ts}"))
    } else {
        None
    }
}

/// Normalized restore status from the raw `x-amz-restore` header returned by HEAD/GET.
/// The header looks like `ongoing-request="true"` or
/// `ongoing-request="false", expiry-date="Thu, 18 Jun 2026 00:00:00 GMT"`.
/// See [`ObjectInfo::restore_status`] for the possible values.
fn normalize_restore_status_from_header(restore: Option<&str>) -> Option<String> {
    let restore = restore?;
    if restore.contains("ongoing-request=\"true\"") {
        return Some("ONGOING".to_string());
    }
    // Extract and reformat the expiry-date (an HTTP date) to RFC3339, matching the LIST path.
    let marker = "expiry-date=\"";
    let start = restore.find(marker)? + marker.len();
    let rest = &restore[start..];
    let end = rest.find('"')?;
    let http_date = &rest[..end];
    let dt = DateTime::from_str(http_date, DateTimeFormat::HttpDate).ok()?;
    dt.fmt(DateTimeFormat::DateTime)
        .ok()
        .map(|ts| format!("EXPIRY:{ts}"))
}

/// Converts one SDK list_objects_v2 page into `(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)`.
#[allow(clippy::result_large_err)]
fn page_to_object_and_prefix_lists(
    item: &ListObjectsV2Output,
) -> Result<(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)> {
    let mut objects: Vec<ObjectInfo> = vec![];
    let mut common_prefixes: Vec<CommonPrefixInfo> = vec![];
    item.contents().iter().try_for_each(|object| {
        objects.push(ObjectInfo {
            key: object.key().ok_or(S3Error::FieldNotExist("key"))?.into(),
            size: checked_content_length(object.size())?,
            timestamp: object
                .last_modified()
                .ok_or(S3Error::FieldNotExist("timestamp"))?
                .to_owned(),
            storage_class: object.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: normalize_restore_status_from_list(object.restore_status()),
        });
        Result::Ok(())
    })?;
    item.common_prefixes()
        .iter()
        .try_for_each(|common_prefix| {
            common_prefixes.push(CommonPrefixInfo {
                prefix: common_prefix
                    .prefix()
                    .ok_or(S3Error::FieldNotExist("prefix"))?
                    .into(),
            });
            Result::Ok(())
        })?;
    Ok((objects, common_prefixes))
}

#[derive(Error, Debug)]
pub enum S3Error {
    #[error("{} [ConstructionFailure]", .0)]
    ConstructionFailure(String),
    #[error("{} [TimeoutError]", .0)]
    TimeoutError(String),
    #[error("{} [DispatchFailure]", .0)]
    DispatchFailure(String),
    #[error("{} [ResponseError]", .0)]
    ResponseError(String),
    #[error("{} [RetriableClientError - <{}> <{}> <{}>]", .0, .1, .2, .3)]
    RetriableClientError(String, AWSS3Error, ErrorMetadata, u16),
    #[error("{} [RetriableServerError - <{}> <{}> <{}>]", .0, .1, .2, .3)]
    RetriableServerError(String, AWSS3Error, ErrorMetadata, u16),
    #[error("{} [AWSS3Error - <{}> <{}> <{}>]", .0, .1, .2, .3)]
    AWSS3Error(String, AWSS3Error, ErrorMetadata, u16),
    #[error("{} [OtherSDKError - <{}>]", .0, .1)]
    OtherSDKError(String, AWSS3Error),
    #[error("{} [ByteStreamDownloadError - <{}>]", .0, .1)]
    ByteStreamDownloadError(String, ByteStreamError),
    /// A local upload-body construction or file-read error. High-level retries intentionally do
    /// not retry this variant because retrying cannot repair a missing, unreadable, or changed
    /// local source file.
    #[error("{} [ByteStreamUploadError - <{}>]", .0, .1)]
    ByteStreamUploadError(String, ByteStreamError),
    #[error("{} [UnexpectedContentLength - expected <{}>, received <{}>]", .0, .1, .2)]
    UnexpectedContentLength(String, u64, u64),
    #[error("{} [UnexpectedContentRange - expected <{}>, received <{}>]", .0, .1, .2)]
    UnexpectedContentRange(String, String, String),
    #[error("{} [ValidationError]", .0)]
    ValidationError(String),
    #[error("{} [IOError]", .0)]
    IOError(String),
    #[error("{} [FieldNotExist]", .0)]
    FieldNotExist(&'static str),
    #[error("{} [RuntimeError]", .0)]
    RuntimeError(String),
}

impl From<std::io::Error> for S3Error {
    fn from(e: std::io::Error) -> Self {
        S3Error::IOError(e.to_string())
    }
}

fn map_sdk_error<E>(
    context: String,
    retriable_client_status_codes: &[u16],
    e: SdkError<E>,
) -> S3Error
where
    AWSS3Error: From<SdkError<E>>,
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    match &e {
        SdkError::ConstructionFailure(construction_error) => {
            debug!("[ConstructionFailure] {:?}", construction_error);
            S3Error::ConstructionFailure(context)
        }
        SdkError::TimeoutError(timeout_error) => {
            debug!("[TimeoutError] {:?}", timeout_error);
            S3Error::TimeoutError(context)
        }
        SdkError::DispatchFailure(dispatch_error) => {
            debug!(
                "[DispatchFailure] is_io: {} is_timeout: {} is_user: {} is_other: {} {:?}",
                dispatch_error.is_io(),
                dispatch_error.is_timeout(),
                dispatch_error.is_user(),
                dispatch_error.is_other(),
                dispatch_error
            );
            S3Error::DispatchFailure(context)
        }
        SdkError::ResponseError(response_error) => {
            if let Some(bytes) = response_error.raw().body().bytes()
                && let Ok(raw_content) = str::from_utf8(bytes)
                && !raw_content.is_empty()
            {
                debug!("[ResponseError] raw {}", raw_content);
            }
            S3Error::ResponseError(context)
        }
        SdkError::ServiceError(service_error) => {
            if let Some(bytes) = service_error.raw().body().bytes()
                && let Ok(raw_content) = str::from_utf8(bytes)
                && !raw_content.is_empty()
            {
                debug!("[ServiceError] raw {}", raw_content);
            }

            let error_meta = e.meta().to_owned();
            debug!("[ServiceError] error_meta {:?}", error_meta);

            let status_code = service_error.raw().status().as_u16();
            debug!("[ServiceError] status_code {}", status_code);

            if retriable_client_status_codes.contains(&status_code)
                || error_meta.code() == Some("SlowDown")
            {
                S3Error::RetriableClientError(context, e.into(), error_meta, status_code)
            } else if status_code >= 500 {
                S3Error::RetriableServerError(context, e.into(), error_meta, status_code)
            } else {
                S3Error::AWSS3Error(context, e.into(), error_meta, status_code)
            }
        }
        _ => {
            error!("{context} {:?}", e);
            S3Error::OtherSDKError(context, e.into())
        }
    }
}

fn map_bytestream_download_error(context: String, e: ByteStreamError) -> S3Error {
    debug!("{context} {:?}", e);
    S3Error::ByteStreamDownloadError(context, e)
}

fn map_bytestream_upload_error(context: String, e: ByteStreamError) -> S3Error {
    debug!("{context} {:?}", e);
    S3Error::ByteStreamUploadError(context, e)
}

async fn finalize_download_file(
    temporary_path: &str,
    final_path: &str,
    timestamp: DateTime,
) -> Result<()> {
    let temporary_path_owned = temporary_path.to_owned();
    let final_path_owned = final_path.to_owned();
    let finalize_result = match tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        filetime::set_file_mtime(
            &temporary_path_owned,
            filetime::FileTime::from_unix_time(timestamp.secs(), timestamp.subsec_nanos()),
        )?;
        std::fs::rename(&temporary_path_owned, &final_path_owned)?;
        Ok(())
    })
    .await
    {
        Ok(result) => result.map_err(S3Error::from),
        Err(error) => Err(S3Error::RuntimeError(error.to_string())),
    };

    if finalize_result.is_err()
        && let Err(cleanup_error) = tokio::fs::remove_file(temporary_path).await
    {
        warn!("Failed to remove temporary download file {temporary_path}: {cleanup_error}");
    }
    finalize_result
}

fn should_retry(e: &S3Error) -> bool {
    match e {
        S3Error::TimeoutError(_)
        | S3Error::DispatchFailure(_)
        | S3Error::ResponseError(_)
        | S3Error::RetriableClientError(_, _, _, _)
        | S3Error::RetriableServerError(_, _, _, _)
        | S3Error::ByteStreamDownloadError(_, _)
        | S3Error::UnexpectedContentLength(_, _, _)
        | S3Error::UnexpectedContentRange(_, _, _) => {
            info!("RetryIf: {}. Retrying...", e);
            true
        }
        S3Error::ByteStreamUploadError(_, _) => {
            debug!("RetryIf: local upload-body errors are not retriable: {}", e);
            false
        }
        _ => {
            // other S3Error errors
            debug!("RetryIf: {}. Not retrying...", e);
            false
        }
    }
}

pub type Result<T> = std::result::Result<T, S3Error>;

#[allow(clippy::result_large_err)]
pub(crate) fn checked_content_length(content_length: Option<i64>) -> Result<u64> {
    let content_length = content_length.ok_or(S3Error::FieldNotExist("content_length"))?;
    u64::try_from(content_length).map_err(|_| {
        S3Error::ValidationError(format!(
            "content length must not be negative: {content_length}"
        ))
    })
}

#[derive(Debug, Clone)]
pub struct S3Client {
    pub inner: AWSS3Client,
    retry_config: RetryConfig,
    multipart_part_size: u64,
    multipart_workers: usize,
}

impl S3Client {
    /// Build an S3 client. Use [`RetryConfig::default`] for default AWS client retry behavior (no high-level retries from this crate).
    /// When both high-level (this crate) and low-level (SDK) retries are enabled, logs a warning (double retries).
    pub async fn new(config: S3ClientConfig, retry_config: RetryConfig) -> Self {
        let mut config_loader = aws_config::from_env();
        if let Some(region) = &config.region {
            config_loader = config_loader.region(Region::new(region.clone()))
        }

        if let Some(endpoint_url) = &config.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint_url);
        }

        if let Some(profile_name) = &config.profile_name {
            config_loader = config_loader.profile_name(profile_name);
        }
        if let Some((access_key, secret_key, session_token)) = &config.access_secret_session_tuple {
            config_loader = config_loader.credentials_provider(Credentials::from_keys(
                access_key.clone(),
                secret_key.clone(),
                session_token.clone(),
            ));
        }

        config_loader = config_loader.timeout_config(
            aws_config::timeout::TimeoutConfig::builder()
                .read_timeout(Duration::from_secs(config.read_timeout_secs))
                .build(),
        );

        // if enrolling into high-level retries, disable low-level retries
        if retry_config.max_retries > 0 {
            config_loader = config_loader.retry_config(aws_config::retry::RetryConfig::disabled())
        }

        let sdk_config = config_loader.load().await;
        let mut config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        if sdk_config.region().is_none() {
            info!(
                "Can't resolve region. Using fallback region: {}",
                FALLBACK_REGION
            );
            config_builder = config_builder.region(Region::new(FALLBACK_REGION));
        }
        config_builder = config_builder.force_path_style(true); // this allows http://minio:11000 style endpoint_url

        S3Client {
            inner: AWSS3Client::from_conf(config_builder.build()),
            retry_config,
            multipart_part_size: config.multipart_part_size,
            multipart_workers: config.multipart_workers,
        }
    }

    /// Build from an existing SDK client. Use [`RetryConfig::default`] for default AWS client retry behavior (no high-level retries from this crate).
    /// When both high-level (this crate) and low-level (SDK) retries are enabled, logs a warning (double retries).
    /// Uses [`DEFAULT_MULTIPART_PART_SIZE`] and [`DEFAULT_MULTIPART_WORKERS`] unless overridden.
    pub fn new_with_aws_s3_client(
        aws_s3_client: AWSS3Client,
        retry_config: RetryConfig,
        multipart_part_size: Option<u64>,
        multipart_workers: Option<usize>,
    ) -> Self {
        if retry_config.max_retries > 0 && aws_s3_client.config().retry_config().is_some() {
            warn!("High-level retries are enabled but low-level retries are also enabled.");
        }

        S3Client {
            inner: aws_s3_client,
            retry_config,
            multipart_part_size: multipart_part_size.unwrap_or(DEFAULT_MULTIPART_PART_SIZE),
            multipart_workers: multipart_workers.unwrap_or(DEFAULT_MULTIPART_WORKERS),
        }
    }

    /// Configured multipart part size in bytes.
    pub fn multipart_part_size(&self) -> u64 {
        self.multipart_part_size
    }

    /// Configured number of concurrent multipart workers.
    pub fn multipart_workers(&self) -> usize {
        self.multipart_workers
    }

    /// Maps an AWS SDK operation error using this client's retriable status-code configuration.
    pub fn map_sdk_error<E>(&self, context: impl Into<String>, error: SdkError<E>) -> S3Error
    where
        AWSS3Error: From<SdkError<E>>,
        E: ProvideErrorMetadata + std::fmt::Debug,
    {
        map_sdk_error(
            context.into(),
            &self.retry_config.retriable_client_status_codes,
            error,
        )
    }

    /// Maps an error encountered while collecting a downloaded byte stream.
    pub fn map_bytestream_download_error(
        &self,
        context: impl Into<String>,
        error: ByteStreamError,
    ) -> S3Error {
        map_bytestream_download_error(context.into(), error)
    }

    /// Runs an operation with this client's retry strategy, limit, and transient-error filtering.
    ///
    /// The closure may run more than once and must recreate any consumed request body each time.
    pub async fn with_retry<F, Fut, T>(&self, op: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        RetryIf::spawn(
            self.retry_config
                .retry_strategy
                .clone()
                .delay_iterator_with_jitter(self.retry_config.max_retries),
            op,
            should_retry,
        )
        .await
    }

    pub async fn head_bucket(&self, bucket: &str) -> Result<BucketInfo> {
        self.inner
            .head_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|error| self.map_sdk_error(format!("<head_bucket> bucket={bucket}"), error))?;
        Ok(BucketInfo {
            name: bucket.into(),
            region: self.inner.config().region().map(|r| r.to_string()),
        })
    }

    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.inner
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|error| {
                self.map_sdk_error(format!("<create_bucket> bucket={bucket}"), error)
            })?;
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket: &str) -> Result<()> {
        self.inner
            .delete_bucket()
            .bucket(bucket)
            .send()
            .await
            .map_err(|error| {
                self.map_sdk_error(format!("<delete_bucket> bucket={bucket}"), error)
            })?;
        Ok(())
    }

    async fn _list_objects_v2_paginated(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> Result<(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)> {
        let mut builder = self.inner.list_objects_v2().bucket(bucket).prefix(prefix);

        if let Some(delimiter) = delimiter {
            builder = builder.delimiter(delimiter);
        }

        let mut pagination_stream = builder.into_paginator().send();

        let mut objects: Vec<ObjectInfo> = vec![];
        let mut common_prefixes: Vec<CommonPrefixInfo> = vec![];
        while let Some(item) = pagination_stream.try_next().await.map_err(|error| {
            self.map_sdk_error(
                format!("<list_objects_v2_paginated> bucket={bucket} prefix={prefix}"),
                error,
            )
        })? {
            let (mut objs, mut prefixes) = page_to_object_and_prefix_lists(&item)?;
            objects.append(&mut objs);
            common_prefixes.append(&mut prefixes);
        }
        Ok((objects, common_prefixes))
    }

    pub async fn list_objects_v2_paginated(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> Result<(Vec<ObjectInfo>, Vec<CommonPrefixInfo>)> {
        let (objects, common_prefixes) = self
            .with_retry(|| async {
                self._list_objects_v2_paginated(bucket, prefix, delimiter)
                    .await
            })
            .await?;

        debug!(
            "Prefix {}: Found {} objects and {} common prefixes.",
            prefix,
            objects.len(),
            common_prefixes.len()
        );

        Ok((objects, common_prefixes))
    }

    /// Returns an iterator that yields one list_objects_v2 page at a time. Retries are applied
    /// per page request (each call to `next_page()`), so the iterator is not invalidated by
    /// a transient failure on one page.
    pub fn list_objects_v2_page_iter<'a>(
        &'a self,
        bucket: &'a str,
        prefix: &'a str,
        delimiter: Option<&'a str>,
    ) -> ListObjectsV2PageIter<'a> {
        ListObjectsV2PageIter {
            s3_client: self,
            bucket,
            prefix,
            delimiter,
            continuation_token: None,
        }
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        let resp = self
            .with_retry(|| async {
                self.inner
                    .head_object()
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(|error| {
                        self.map_sdk_error(
                            format!("<head_object> bucket={bucket} key={key}"),
                            error,
                        )
                    })
            })
            .await?;

        let object_info = ObjectInfo {
            key: key.into(),
            size: checked_content_length(resp.content_length())?,
            timestamp: resp
                .last_modified()
                .ok_or(S3Error::FieldNotExist("timestamp"))?
                .to_owned(),
            storage_class: resp.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: normalize_restore_status_from_header(resp.restore()),
        };
        debug!("Found object with key={}", key);
        debug!("Content length: {}", object_info.size);
        debug!("Last modified: {}", object_info.timestamp);
        debug!("Storage class: {:?}", object_info.storage_class);
        debug!(
            "Content type: {}",
            resp.content_type()
                .ok_or(S3Error::FieldNotExist("content_type"))?
        );

        Ok(object_info)
    }

    async fn _get_object(
        &self,
        bucket: &str,
        key: &str,
        start_end_offsets: Option<(usize, usize)>,
    ) -> Result<(ObjectInfo, Vec<u8>)> {
        let mut builder = self.inner.get_object().bucket(bucket).key(key);

        if let Some(start_end_offsets) = start_end_offsets {
            if start_end_offsets.1 <= start_end_offsets.0 {
                return Err(S3Error::ValidationError(format!(
                    "Invalid start_end_offsets, non-positive slice: start {} end {}!",
                    start_end_offsets.0, start_end_offsets.1
                )));
            }
            let range = format!(
                "bytes={}-{}",
                start_end_offsets.0,
                // end_offset is provided exclusive but the "end" of "bytes=start,end" is inclusive
                start_end_offsets.1 - 1
            );
            builder = builder.range(range);
        }

        let resp = builder.send().await.map_err(|error| {
            self.map_sdk_error(format!("<get_object> bucket={bucket} key={key}"), error)
        })?;

        let object_info = ObjectInfo {
            key: key.into(),
            size: checked_content_length(resp.content_length())?,
            timestamp: resp
                .last_modified()
                .ok_or(S3Error::FieldNotExist("timestamp"))?
                .to_owned(),
            storage_class: resp.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: normalize_restore_status_from_header(resp.restore()),
        };

        debug!("Found object with key={}", key);
        debug!("Content length: {}", object_info.size);
        debug!("Last modified: {}", object_info.timestamp);
        debug!("Storage class: {:?}", object_info.storage_class);
        debug!(
            "Content type: {}",
            resp.content_type()
                .ok_or(S3Error::FieldNotExist("content_type"))?
        );

        let content = resp
            .body
            .collect()
            .await
            .map_err(|error| {
                self.map_bytestream_download_error(
                    format!("<get_object> bucket={bucket} key={key}"),
                    error,
                )
            })?
            .into_bytes()
            .to_vec();
        Ok((object_info, content))
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        start_end_offsets: Option<(usize, usize)>,
    ) -> Result<(ObjectInfo, Vec<u8>)> {
        let (object_info, content) = self
            .with_retry(|| async { self._get_object(bucket, key, start_end_offsets).await })
            .await?;

        Ok((object_info, content))
    }

    async fn _download_object(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        start_end_offsets: Option<(usize, usize)>,
    ) -> Result<ObjectInfo> {
        let mut builder = self.inner.get_object().bucket(bucket).key(key);

        if let Some(start_end_offsets) = start_end_offsets {
            if start_end_offsets.1 <= start_end_offsets.0 {
                return Err(S3Error::ValidationError(format!(
                    "Invalid start_end_offsets, non-positive slice: start {} end {}!",
                    start_end_offsets.0, start_end_offsets.1
                )));
            }
            let range = format!(
                "bytes={}-{}",
                start_end_offsets.0,
                // end_offset is provided exclusive but the "end" of "bytes=start,end" is inclusive
                start_end_offsets.1 - 1
            );
            builder = builder.range(range);
        }

        let mut resp = builder.send().await.map_err(|error| {
            self.map_sdk_error(
                format!("<download_object> bucket={bucket} key={key}"),
                error,
            )
        })?;

        let object_info = ObjectInfo {
            key: key.into(),
            size: checked_content_length(resp.content_length())?,
            timestamp: resp
                .last_modified()
                .ok_or(S3Error::FieldNotExist("timestamp"))?
                .to_owned(),
            storage_class: resp.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: normalize_restore_status_from_header(resp.restore()),
        };

        let local_path = local_path.to_owned();
        let timestamp = object_info.timestamp;

        // We create a temporary file to download the object to for atomicity.
        // Temp file is cleaned up on any error (stream, write, flush, mtime, or rename).
        let random_suffix = generate_random_hex(8);
        let local_path_tmp = format!("{local_path}.{random_suffix}");

        let transfer_result: Result<()> = async {
            let mut file = BufWriter::with_capacity(
                FILE_BUFFER_SIZE,
                tokio::fs::File::create(&local_path_tmp).await?,
            );
            while let Some(bytes) = resp.body.try_next().await.map_err(|error| {
                self.map_bytestream_download_error(
                    format!("<download_object> bucket={bucket} key={key}"),
                    error,
                )
            })? {
                file.write_all(&bytes).await?;
            }
            file.flush().await?;
            Ok(())
        }
        .await;

        if let Err(transfer_error) = transfer_result {
            if let Err(cleanup_error) = tokio::fs::remove_file(&local_path_tmp).await {
                warn!("Failed to remove temporary download file {local_path_tmp}: {cleanup_error}");
            }
            return Err(transfer_error);
        }

        finalize_download_file(&local_path_tmp, &local_path, timestamp).await?;
        Ok(object_info)
    }

    pub async fn download_object(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        start_end_offsets: Option<(usize, usize)>,
    ) -> Result<ObjectInfo> {
        let obj = self
            .with_retry(|| async {
                self._download_object(bucket, key, local_path, start_end_offsets)
                    .await
            })
            .await?;

        trace!("Downloaded from s3://{}/{} to {}", bucket, key, local_path);

        Ok(obj)
    }

    pub async fn download_object_multipart<P>(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        pb: Option<&P>,
    ) -> Result<ObjectInfo>
    where
        P: ProgressBar + 'static,
    {
        if self.multipart_workers == 0 {
            return Err(S3Error::ValidationError(
                "multipart workers must be greater than zero".to_owned(),
            ));
        }

        let resp = self
            .with_retry(|| async {
                self.inner
                    .head_object()
                    .bucket(bucket)
                    .key(key)
                    .send()
                    .await
                    .map_err(|error| {
                        self.map_sdk_error(
                            format!("<download_object_multipart> bucket={bucket} key={key}"),
                            error,
                        )
                    })
            })
            .await?;

        let file_size = checked_content_length(resp.content_length())?;
        let timestamp = resp
            .last_modified()
            .ok_or(S3Error::FieldNotExist("timestamp"))?
            .to_owned();

        let object_info = ObjectInfo {
            key: key.into(),
            size: file_size,
            timestamp: timestamp.to_owned(),
            storage_class: resp.storage_class().map(|sc| sc.as_str().to_owned()),
            restore_status: normalize_restore_status_from_header(resp.restore()),
        };

        if file_size == 0 {
            let local_path_tmp = format!("{local_path}.{}", generate_random_hex(8));
            tokio::fs::File::create(&local_path_tmp).await?;
            finalize_download_file(&local_path_tmp, local_path, timestamp).await?;
            debug!("Created blank file at {local_path}");
            return Ok(object_info);
        }

        let source_etag = resp
            .e_tag()
            .ok_or(S3Error::FieldNotExist("etag"))?
            .to_owned();
        let plan = MultipartPlan::for_download(file_size, self.multipart_part_size)?;
        debug!("Part count: {}", plan.part_count);
        if let Some(p) = pb {
            p.set_length(plan.part_count);
        }

        let local_path_tmp = format!("{local_path}.{}", generate_random_hex(8));
        let local_path_tmp_ = local_path_tmp.clone();
        tokio::fs::File::create(&local_path_tmp).await?;

        // parallel download
        let transfer_result: Result<Vec<()>> = stream::iter(plan.parts())
            .map(|part| {
                let client = self.clone();
                let local_path_tmp = local_path_tmp_.clone();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let source_etag = source_etag.clone();
                let pb = pb.cloned();
                let part_index = part.number - 1;

                async move {
                    client
                        .with_retry(|| async {
                            // end_offset is provided exclusive but the "end" of "bytes=start,end" is inclusive
                            let range = format!("bytes={}-{}", part.start, part.end - 1);
                            debug!("Getting part {} with range: {}", part_index, range);
                            let mut resp = client
                                .inner
                                .get_object()
                                .bucket(&bucket)
                                .key(&key)
                                .if_match(&source_etag)
                                .range(range)
                                .send()
                                .await
                                .map_err(|error| {
                                    client.map_sdk_error(
                                        format!(
                                            "<download_object_multipart> bucket={bucket} \
                                             key={key} download_part_index={part_index}"
                                        ),
                                        error,
                                    )
                                })?;
                            debug!("Done getting part {}", part_index);
                            validate_content_range(
                                resp.content_range(),
                                part,
                                file_size,
                                format!(
                                    "<download_object_multipart> bucket={bucket} key={key} \
                                     download_part_index={part_index}"
                                ),
                            )?;

                            let mut file = BufWriter::with_capacity(
                                FILE_BUFFER_SIZE,
                                tokio::fs::OpenOptions::new()
                                    .write(true)
                                    .open(&local_path_tmp)
                                    .await?,
                            );
                            file.seek(std::io::SeekFrom::Start(part.start)).await?;

                            debug!("Streaming part {} to file", part_index);
                            let mut actual_length = 0_u64;
                            while let Some(bytes) = resp.body.try_next().await.map_err(|error| {
                                client.map_bytestream_download_error(
                                    format!(
                                        "<download_object_multipart> bucket={bucket} key={key} \
                                         download_part_index={part_index}"
                                    ),
                                    error,
                                )
                            })? {
                                let bytes_length = u64::try_from(bytes.len()).map_err(|error| {
                                    S3Error::ValidationError(format!(
                                        "downloaded part length cannot be represented as u64: \
                                         {error}"
                                    ))
                                })?;
                                actual_length =
                                    actual_length.checked_add(bytes_length).ok_or_else(|| {
                                        S3Error::UnexpectedContentLength(
                                            format!(
                                                "<download_object_multipart> bucket={bucket} \
                                                 key={key} download_part_index={part_index}"
                                            ),
                                            part.length(),
                                            u64::MAX,
                                        )
                                    })?;
                                file.write_all(&bytes).await?;
                            }
                            file.flush().await?;
                            if actual_length != part.length() {
                                return Err(S3Error::UnexpectedContentLength(
                                    format!(
                                        "<download_object_multipart> bucket={bucket} key={key} \
                                         download_part_index={part_index}"
                                    ),
                                    part.length(),
                                    actual_length,
                                ));
                            }
                            debug!("Done streaming part {} to file", part_index);

                            Ok(())
                        })
                        .await?;

                    if let Some(p) = &pb {
                        p.inc(1);
                    }
                    Ok(())
                }
            })
            .buffer_unordered(self.multipart_workers)
            .try_collect()
            .await;
        if let Err(transfer_error) = transfer_result {
            if let Err(cleanup_error) = tokio::fs::remove_file(&local_path_tmp).await {
                warn!("Failed to remove temporary download file {local_path_tmp}: {cleanup_error}");
            }
            error!("Download of {local_path} failed! Not finalizing the file.");
            return Err(transfer_error);
        }

        finalize_download_file(&local_path_tmp, local_path, timestamp).await?;
        trace!(
            "Downloaded multipart from s3://{}/{} to {}",
            bucket, key, local_path
        );
        Ok(object_info)
    }

    async fn _put_object(
        &self,
        bucket: &str,
        key: &str,
        content: Bytes,
        storage_class: Option<&str>,
    ) -> Result<()> {
        let body = ByteStream::from(content);
        let mut builder = self.inner.put_object().bucket(bucket).key(key).body(body);

        if let Some(storage_class) = storage_class {
            builder = builder.storage_class(StorageClass::from(storage_class));
        }

        builder.send().await.map_err(|error| {
            self.map_sdk_error(format!("<put_object> bucket={bucket} key={key}"), error)
        })?;

        Ok(())
    }

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        content: &[u8],
        storage_class: Option<&str>,
    ) -> Result<()> {
        let content = Bytes::from(content.to_vec());
        self.with_retry(|| async {
            self._put_object(bucket, key, content.clone(), storage_class)
                .await
        })
        .await?;

        trace!("Put from memory to s3://{}/{}", bucket, key);

        Ok(())
    }

    async fn _upload_object(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        storage_class: Option<&str>,
    ) -> Result<()> {
        let body = ByteStream::read_from()
            .path(local_path)
            .buffer_size(FILE_BUFFER_SIZE)
            .build()
            .await
            .map_err(|error| {
                map_bytestream_upload_error(
                    format!("<upload_object> bucket={bucket} key={key}"),
                    error,
                )
            })?;
        let mut builder = self.inner.put_object().bucket(bucket).key(key).body(body);

        if let Some(storage_class) = storage_class {
            builder = builder.storage_class(StorageClass::from(storage_class));
        }

        builder.send().await.map_err(|error| {
            self.map_sdk_error(format!("<upload_object> bucket={bucket} key={key}"), error)
        })?;

        Ok(())
    }

    pub async fn upload_object(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        storage_class: Option<&str>,
    ) -> Result<()> {
        self.with_retry(|| async {
            self._upload_object(bucket, key, local_path, storage_class)
                .await
        })
        .await?;

        trace!("Uploaded from {} to s3://{}/{}", local_path, bucket, key);

        Ok(())
    }

    pub async fn upload_object_multipart<P>(
        &self,
        bucket: &str,
        key: &str,
        local_path: &str,
        storage_class: Option<&str>,
        pb: Option<&P>,
    ) -> Result<()>
    where
        P: ProgressBar + 'static,
    {
        if self.multipart_workers == 0 {
            return Err(S3Error::ValidationError(
                "multipart workers must be greater than zero".to_owned(),
            ));
        }

        let file_size = tokio::fs::metadata(local_path).await?.len();
        if file_size == 0 {
            self.put_object(bucket, key, &[], storage_class).await?;
            return Ok(());
        }

        let plan = MultipartPlan::for_upload(file_size, self.multipart_part_size)?;
        if plan.part_size != self.multipart_part_size {
            info!(
                "Object requires adaptive multipart upload part size {}.",
                plan.part_size
            );
        }

        // CreateMultipartUpload has no idempotency token. Avoid adding high-level retries that can
        // create multiple orphaned uploads when S3 commits the request but its response is lost.
        let mut builder = self.inner.create_multipart_upload().bucket(bucket).key(key);
        if let Some(storage_class) = storage_class {
            builder = builder.storage_class(StorageClass::from(storage_class));
        }
        let create_multipart_upload_output = builder.send().await.map_err(|error| {
            self.map_sdk_error(
                format!("<upload_object_multipart> bucket={bucket} key={key}"),
                error,
            )
        })?;

        let upload_id = create_multipart_upload_output
            .upload_id()
            .ok_or(S3Error::FieldNotExist("upload_id"))?;

        debug!("Part count: {}", plan.part_count);
        if let Some(p) = pb {
            p.set_length(plan.part_count);
        }

        // parallel upload
        let transfer_result: Result<Vec<CompletedPart>> = stream::iter(plan.parts())
            .map(|part| {
                let client = self.clone();
                let local_path = local_path.to_string();
                let bucket = bucket.to_string();
                let key = key.to_string();
                let upload_id = upload_id.to_string();
                let pb = pb.cloned();
                let part_index = part.number - 1;

                async move {
                    let part_number = part.number;
                    let upload_part_output = client
                        .with_retry(|| async {
                            let body = ByteStream::read_from()
                                .path(&local_path)
                                .buffer_size(FILE_BUFFER_SIZE)
                                .offset(part.start)
                                .length(Length::Exact(part.length()))
                                .build()
                                .await
                                .map_err(|error| {
                                    map_bytestream_upload_error(
                                        format!(
                                            "<upload_object_multipart> bucket={bucket} key={key} \
                                             upload_part_index={part_index}"
                                        ),
                                        error,
                                    )
                                })?;

                            client
                                .inner
                                .upload_part()
                                .bucket(&bucket)
                                .key(&key)
                                .upload_id(&upload_id)
                                .body(body)
                                .part_number(part_number)
                                .send()
                                .await
                                .map_err(|error| {
                                    client.map_sdk_error(
                                        format!(
                                            "<upload_object_multipart> bucket={bucket} key={key} \
                                             upload_part_index={part_index}"
                                        ),
                                        error,
                                    )
                                })
                        })
                        .await?;

                    if let Some(p) = &pb {
                        p.inc(1);
                    }
                    Ok(CompletedPart::builder()
                        .e_tag(
                            upload_part_output
                                .e_tag
                                .ok_or(S3Error::FieldNotExist("etag"))?,
                        )
                        .part_number(part_number)
                        .build())
                }
            })
            .buffer_unordered(self.multipart_workers)
            .try_collect()
            .await;
        let mut upload_parts = match transfer_result {
            Ok(parts) => parts,
            Err(error) => {
                error!(
                    "<upload_object_multipart> bucket={bucket} key={key} Failed to upload all parts! Abort multipart upload."
                );
                if let Err(abort_error) = self.abort_multipart_upload(bucket, key, upload_id).await
                {
                    error!(
                        "<upload_object_multipart> Failed to abort multipart upload \
                         bucket={bucket} key={key} upload_id={upload_id}: {abort_error}"
                    );
                }
                return Err(error);
            }
        };

        // sort by part number
        upload_parts.sort_by_key(|part| part.part_number);

        // complete multipart upload
        let client = self.inner.clone();
        let upload_parts_ref = &upload_parts;
        let complete_multipart_upload_res = self
            .with_retry(|| async {
                let complete_multipart_upload_output = client
                    .complete_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(upload_parts_ref.clone()))
                            .build(),
                    )
                    .upload_id(upload_id)
                    .send()
                    .await
                    .map_err(|error| {
                        self.map_sdk_error(
                            format!("<upload_object_multipart> bucket={bucket} key={key}"),
                            error,
                        )
                    })?;
                Ok(complete_multipart_upload_output)
            })
            .await;

        if let Err(e) = complete_multipart_upload_res {
            error!(
                "<upload_object_multipart> bucket={bucket} key={key} Failed to complete multipart upload! Abort multipart upload."
            );
            if let Err(abort_error) = self.abort_multipart_upload(bucket, key, upload_id).await {
                error!(
                    "<upload_object_multipart> Failed to abort multipart upload \
                     bucket={bucket} key={key} upload_id={upload_id}: {abort_error}"
                );
            }
            return Err(e);
        }

        debug!(
            "Uploaded multipart from {} to s3://{}/{}",
            local_path, bucket, key
        );

        Ok(())
    }

    /// Copies an object across independently configured S3 clients through memory.
    pub async fn copy_object_cross_clients(
        &self,
        dst_client: &S3Client,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        dst_storage_class: Option<&str>,
    ) -> Result<()> {
        let (_, content) = self.get_object(src_bucket, src_key, None).await?;
        dst_client
            .put_object(dst_bucket, dst_key, &content, dst_storage_class)
            .await?;

        trace!(
            "Copied through memory from s3://{}/{} to s3://{}/{} (dst_storage_class={:?})",
            src_bucket, src_key, dst_bucket, dst_key, dst_storage_class
        );
        Ok(())
    }

    pub async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        dst_storage_class: Option<&str>,
    ) -> Result<()> {
        self.with_retry(|| async {
            let mut builder = self
                .inner
                .copy_object()
                .bucket(dst_bucket)
                .key(dst_key)
                .copy_source(urlencoding::encode(&format!("{}/{}", src_bucket, src_key)));

            if let Some(dst_storage_class) = dst_storage_class {
                let dst_storage_class = StorageClass::from(dst_storage_class);
                builder = builder.storage_class(dst_storage_class);
            }

            builder.send().await.map_err(|error| {
                self.map_sdk_error(
                    format!(
                        "<copy_object> src_bucket={src_bucket} src_key={src_key} \
                             dst_bucket={dst_bucket} dst_key={dst_key} \
                             dst_storage_class={dst_storage_class:?}"
                    ),
                    error,
                )
            })
        })
        .await?;

        trace!(
            "Copied s3://{}/{} to s3://{}/{} (dst_storage_class={:?})",
            src_bucket, src_key, dst_bucket, dst_key, dst_storage_class
        );

        Ok(())
    }

    /// Copies an object within one S3 backend using server-side ranged multipart copies.
    ///
    /// The object data does not pass through this process. The source must be addressable by the
    /// destination backend and accessible with this client's credentials.
    pub async fn copy_object_multipart<P>(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        dst_storage_class: Option<&str>,
        pb: Option<&P>,
    ) -> Result<()>
    where
        P: ProgressBar + 'static,
    {
        if self.multipart_workers == 0 {
            return Err(S3Error::ValidationError(
                "multipart workers must be greater than zero".to_owned(),
            ));
        }

        let head = self
            .with_retry(|| async {
                self.inner
                    .head_object()
                    .bucket(src_bucket)
                    .key(src_key)
                    .send()
                    .await
                    .map_err(|error| {
                        self.map_sdk_error(
                            format!(
                                "<copy_object_multipart> src_bucket={src_bucket} \
                                 src_key={src_key} dst_bucket={dst_bucket} dst_key={dst_key}"
                            ),
                            error,
                        )
                    })
            })
            .await?;

        let object_size = checked_content_length(head.content_length())?;

        if object_size == 0 {
            self.put_object(dst_bucket, dst_key, &[], dst_storage_class)
                .await?;
            return Ok(());
        }

        let source_etag = head
            .e_tag()
            .ok_or(S3Error::FieldNotExist("etag"))?
            .to_owned();
        let plan = MultipartPlan::for_upload(object_size, self.multipart_part_size)?;
        if plan.part_size != self.multipart_part_size {
            info!(
                "Object requires adaptive multipart copy part size {}.",
                plan.part_size
            );
        }

        // CreateMultipartUpload has no idempotency token; do not add high-level retries.
        let mut builder = self
            .inner
            .create_multipart_upload()
            .bucket(dst_bucket)
            .key(dst_key);
        if let Some(dst_storage_class) = dst_storage_class {
            builder = builder.storage_class(StorageClass::from(dst_storage_class));
        }
        let create_multipart_upload_output = builder.send().await.map_err(|error| {
            self.map_sdk_error(
                format!(
                    "<copy_object_multipart> src_bucket={src_bucket} src_key={src_key} \
                     dst_bucket={dst_bucket} dst_key={dst_key}"
                ),
                error,
            )
        })?;

        let upload_id = create_multipart_upload_output
            .upload_id()
            .ok_or(S3Error::FieldNotExist("upload_id"))?;

        debug!("Part count: {}", plan.part_count);
        if let Some(p) = pb {
            p.set_length(plan.part_count);
        }

        let transfer_result: Result<Vec<CompletedPart>> = stream::iter(plan.parts())
            .map(|part| {
                let client = self.clone();
                let src_bucket = src_bucket.to_owned();
                let src_key = src_key.to_owned();
                let dst_bucket = dst_bucket.to_owned();
                let dst_key = dst_key.to_owned();
                let copy_source =
                    urlencoding::encode(&format!("{src_bucket}/{src_key}")).into_owned();
                let source_etag = source_etag.clone();
                let upload_id = upload_id.to_owned();
                let pb = pb.cloned();
                let part_index = part.number - 1;

                async move {
                    let part_number = part.number;
                    let upload_part_copy_output = client
                        .with_retry(|| async {
                            let range = format!("bytes={}-{}", part.start, part.end - 1);
                            debug!(
                                "Copying part {} with range {} server-side",
                                part_index, range
                            );
                            client
                                .inner
                                .upload_part_copy()
                                .bucket(&dst_bucket)
                                .key(&dst_key)
                                .upload_id(&upload_id)
                                .part_number(part_number)
                                .copy_source(&copy_source)
                                .copy_source_if_match(&source_etag)
                                .copy_source_range(range)
                                .send()
                                .await
                                .map_err(|error| {
                                    client.map_sdk_error(
                                        format!(
                                            "<copy_object_multipart> src_bucket={src_bucket} \
                                             src_key={src_key} dst_bucket={dst_bucket} \
                                             dst_key={dst_key} copy_part_index={part_index}"
                                        ),
                                        error,
                                    )
                                })
                        })
                        .await?;

                    if let Some(p) = &pb {
                        p.inc(1);
                    }
                    Ok(CompletedPart::builder()
                        .e_tag(
                            upload_part_copy_output
                                .copy_part_result()
                                .and_then(|result| result.e_tag())
                                .ok_or(S3Error::FieldNotExist("etag"))?,
                        )
                        .part_number(part_number)
                        .build())
                }
            })
            .buffer_unordered(self.multipart_workers)
            .try_collect()
            .await;
        let mut upload_parts = match transfer_result {
            Ok(parts) => parts,
            Err(error) => {
                error!(
                    "<copy_object_multipart> Failed to copy all parts; aborting multipart upload."
                );
                if let Err(abort_error) = self
                    .abort_multipart_upload(dst_bucket, dst_key, upload_id)
                    .await
                {
                    error!(
                        "<copy_object_multipart> Failed to abort multipart upload \
                         bucket={dst_bucket} key={dst_key} upload_id={upload_id}: {abort_error}"
                    );
                }
                return Err(error);
            }
        };

        upload_parts.sort_by_key(|part| part.part_number);
        let complete_result = self
            .with_retry(|| async {
                self.inner
                    .complete_multipart_upload()
                    .bucket(dst_bucket)
                    .key(dst_key)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(upload_parts.clone()))
                            .build(),
                    )
                    .upload_id(upload_id)
                    .send()
                    .await
                    .map_err(|error| {
                        self.map_sdk_error(
                            format!(
                                "<copy_object_multipart> src_bucket={src_bucket} \
                                 src_key={src_key} dst_bucket={dst_bucket} dst_key={dst_key}"
                            ),
                            error,
                        )
                    })
            })
            .await;

        if let Err(error) = complete_result {
            error!("<copy_object_multipart> Failed to complete copy; aborting multipart upload.");
            if let Err(abort_error) = self
                .abort_multipart_upload(dst_bucket, dst_key, upload_id)
                .await
            {
                error!(
                    "<copy_object_multipart> Failed to abort multipart upload \
                     bucket={dst_bucket} key={dst_key} upload_id={upload_id}: {abort_error}"
                );
            }
            return Err(error);
        }

        trace!(
            "Copied multipart server-side from s3://{}/{} to s3://{}/{}",
            src_bucket, src_key, dst_bucket, dst_key
        );
        Ok(())
    }

    pub async fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<()> {
        self.with_retry(|| async {
            self.inner
                .abort_multipart_upload()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .send()
                .await
                .map_err(|error| {
                    self.map_sdk_error(
                        format!(
                            "<abort_multipart_upload> bucket={bucket} key={key} \
                             upload_id={upload_id}"
                        ),
                        error,
                    )
                })
        })
        .await?;
        Ok(())
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.with_retry(|| async {
            self.inner
                .delete_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|error| {
                    self.map_sdk_error(format!("<delete_object> bucket={bucket} key={key}"), error)
                })
        })
        .await?;

        trace!("Deleted s3://{}/{}", bucket, key);
        Ok(())
    }

    pub async fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        days: i32,
        tier: &str,
    ) -> Result<()> {
        self.with_retry(|| async {
            let restore_request = RestoreRequest::builder()
                .days(days)
                .glacier_job_parameters(
                    GlacierJobParameters::builder()
                        .tier(Tier::from(tier))
                        .build()
                        .map_err(|e| S3Error::ValidationError(e.to_string()))?,
                )
                .build();
            self.inner
                .restore_object()
                .bucket(bucket)
                .key(key)
                .restore_request(restore_request)
                .send()
                .await
                .map_err(|error| {
                    self.map_sdk_error(
                        format!(
                            "<restore_object> bucket={bucket} key={key} days={days} tier={tier}"
                        ),
                        error,
                    )
                })
        })
        .await?;

        trace!(
            "Restored s3://{}/{} (days={}, tier={})",
            bucket, key, days, tier
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::checked_content_length;

    #[test]
    fn validates_content_length() {
        assert_eq!(checked_content_length(Some(42)).unwrap(), 42);
        assert!(checked_content_length(Some(-1)).is_err());
        assert!(checked_content_length(None).is_err());
    }
}
