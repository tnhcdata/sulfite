#![doc = include_str!("../README.md")]

mod multipart;
mod retry_strategy;
mod s3_client;
mod utils;

pub use multipart::copy_object_multipart_cross_clients;
pub use retry_strategy::RetryStrategy;
pub use s3_client::{
    CommonPrefixInfo, DEFAULT_MULTIPART_PART_SIZE, DEFAULT_MULTIPART_WORKERS, DEFAULT_READ_TIMEOUT,
    DEFAULT_RETRIABLE_CLIENT_STATUS_CODES, DEFAULT_RETRIABLE_CLIENT_STATUS_CODES_STR,
    ListObjectsV2PageIter, MULTIPART_MAX_OBJECT_SIZE, MULTIPART_MAX_PART_SIZE, MULTIPART_MAX_PARTS,
    MULTIPART_MIN_PART_SIZE, NoopProgressBar, ObjectInfo, ProgressBar, RetryConfig, S3Client,
    S3ClientConfig, S3Error,
};
pub use utils::generate_random_hex;
