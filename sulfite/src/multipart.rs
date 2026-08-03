use crate::s3_client::{
    MULTIPART_MAX_OBJECT_SIZE, MULTIPART_MAX_PART_SIZE, MULTIPART_MAX_PARTS,
    MULTIPART_MIN_PART_SIZE, ProgressBar, Result, S3Client, S3Error, checked_content_length,
};
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart, StorageClass},
};
use futures::{StreamExt, TryStreamExt, stream};
use log::{debug, error, info, warn};

#[derive(Clone, Copy, Debug)]
pub(crate) struct MultipartPlan {
    pub(crate) part_size: u64,
    pub(crate) part_count: u64,
    object_size: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MultipartPart {
    pub(crate) number: i32,
    pub(crate) start: u64,
    pub(crate) end: u64,
}

impl MultipartPart {
    pub(crate) fn length(&self) -> u64 {
        self.end - self.start
    }
}

impl MultipartPlan {
    #[allow(clippy::result_large_err)]
    pub(crate) fn for_download(object_size: u64, requested_part_size: u64) -> Result<Self> {
        Self::new(object_size, requested_part_size, None)
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn for_upload(object_size: u64, requested_part_size: u64) -> Result<Self> {
        if object_size > MULTIPART_MAX_OBJECT_SIZE {
            return Err(S3Error::ValidationError(format!(
                "object size {object_size} exceeds S3 maximum {MULTIPART_MAX_OBJECT_SIZE}"
            )));
        }

        let requested_part_size =
            requested_part_size.clamp(MULTIPART_MIN_PART_SIZE, MULTIPART_MAX_PART_SIZE);
        let plan = Self::new(object_size, requested_part_size, Some(MULTIPART_MAX_PARTS))?;
        Ok(plan)
    }

    #[allow(clippy::result_large_err)]
    fn new(object_size: u64, requested_part_size: u64, max_parts: Option<u64>) -> Result<Self> {
        if requested_part_size == 0 {
            return Err(S3Error::ValidationError(
                "multipart part size must be greater than zero".to_owned(),
            ));
        }
        let minimum_part_size = match max_parts {
            Some(0) => {
                return Err(S3Error::ValidationError(
                    "multipart maximum part count must be greater than zero".to_owned(),
                ));
            }
            Some(max_parts) => object_size.div_ceil(max_parts),
            None => 0,
        };
        let part_size = requested_part_size.max(minimum_part_size);
        let part_count = object_size.div_ceil(part_size);
        if part_count > i32::MAX as u64 {
            return Err(S3Error::ValidationError(format!(
                "multipart part count {part_count} exceeds supported maximum {}",
                i32::MAX
            )));
        }

        Ok(Self {
            part_size,
            part_count,
            object_size,
        })
    }

    pub(crate) fn parts(&self) -> impl Iterator<Item = MultipartPart> + '_ {
        (0..self.part_count).map(|index| {
            let start = index * self.part_size;
            let end = start.saturating_add(self.part_size).min(self.object_size);
            MultipartPart {
                number: (index + 1) as i32,
                start,
                end,
            }
        })
    }
}

#[allow(clippy::result_large_err)]
pub(crate) fn validate_content_range(
    content_range: Option<&str>,
    part: MultipartPart,
    object_size: u64,
    context: impl Into<String>,
) -> Result<()> {
    let expected = format!("bytes {}-{}/{}", part.start, part.end - 1, object_size);
    let received = content_range.unwrap_or("<missing>");
    if received != expected {
        return Err(S3Error::UnexpectedContentRange(
            context.into(),
            expected,
            received.to_owned(),
        ));
    }
    Ok(())
}

/// Copies an object across independently configured S3 clients through bounded memory.
///
/// Each worker downloads one ranged part into memory and uploads it before taking another part.
/// Steady-state payload memory is approximately `effective_part_size * workers`, with additional
/// transient SDK buffering. Part size and worker count come from `dst_client`; the effective part
/// size may increase when needed to stay within the multipart part-count limit.
#[allow(clippy::too_many_arguments)]
pub async fn copy_object_multipart_cross_clients<P>(
    src_client: &S3Client,
    dst_client: &S3Client,
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
    let part_size = dst_client.multipart_part_size();
    let workers = dst_client.multipart_workers();
    if src_client.multipart_part_size() != part_size || src_client.multipart_workers() != workers {
        warn!(
            "Source and destination multipart settings differ: source part_size={} workers={}, \
             destination part_size={} workers={}. Using destination settings.",
            src_client.multipart_part_size(),
            src_client.multipart_workers(),
            part_size,
            workers
        );
    }
    if workers == 0 {
        return Err(S3Error::ValidationError(
            "multipart workers must be greater than zero".to_owned(),
        ));
    }

    let src_sdk_client = src_client.inner.clone();
    let dst_sdk_client = dst_client.inner.clone();
    let head = src_client
        .with_retry(|| async {
            src_sdk_client
                .head_object()
                .bucket(src_bucket)
                .key(src_key)
                .send()
                .await
                .map_err(|error| {
                    src_client.map_sdk_error(
                        format!(
                            "<copy_object_multipart_cross_clients> source HEAD \
                             bucket={src_bucket} key={src_key}"
                        ),
                        error,
                    )
                })
        })
        .await?;
    let object_size = checked_content_length(head.content_length())?;

    if object_size == 0 {
        dst_client
            .put_object(dst_bucket, dst_key, &[], dst_storage_class)
            .await?;
        return Ok(());
    }

    let source_etag = head
        .e_tag()
        .ok_or(S3Error::FieldNotExist("etag"))?
        .to_owned();
    let plan = MultipartPlan::for_upload(object_size, part_size)?;
    if plan.part_size != part_size {
        info!(
            "Object requires adaptive cross-client multipart copy part size {}.",
            plan.part_size
        );
    }

    // CreateMultipartUpload has no idempotency token; do not add high-level retries.
    let mut request = dst_sdk_client
        .create_multipart_upload()
        .bucket(dst_bucket)
        .key(dst_key);
    if let Some(dst_storage_class) = dst_storage_class {
        request = request.storage_class(StorageClass::from(dst_storage_class));
    }
    let create_output = request.send().await.map_err(|error| {
        dst_client.map_sdk_error(
            format!(
                "<copy_object_multipart_cross_clients> create upload \
                 bucket={dst_bucket} key={dst_key}"
            ),
            error,
        )
    })?;
    let upload_id = create_output
        .upload_id()
        .ok_or(S3Error::FieldNotExist("upload_id"))?
        .to_owned();

    if let Some(progress) = pb {
        progress.set_length(plan.part_count);
    }

    let transfer_result: Result<Vec<CompletedPart>> = stream::iter(plan.parts())
        .map(|part| {
            let src_client = src_client.clone();
            let dst_client = dst_client.clone();
            let src_sdk_client = src_sdk_client.clone();
            let dst_sdk_client = dst_sdk_client.clone();
            let src_bucket = src_bucket.to_owned();
            let src_key = src_key.to_owned();
            let dst_bucket = dst_bucket.to_owned();
            let dst_key = dst_key.to_owned();
            let upload_id = upload_id.clone();
            let source_etag = source_etag.clone();
            let progress = pb.cloned();

            async move {
                let range = format!("bytes={}-{}", part.start, part.end - 1);
                let part_number = part.number;

                let bytes = src_client
                    .with_retry(|| async {
                        debug!("Transferring part {part_number} with source range {range}");
                        let response = src_sdk_client
                            .get_object()
                            .bucket(&src_bucket)
                            .key(&src_key)
                            .if_match(&source_etag)
                            .range(&range)
                            .send()
                            .await
                            .map_err(|error| {
                                src_client.map_sdk_error(
                                    format!(
                                        "<copy_object_multipart_cross_clients> download part \
                                         src_bucket={src_bucket} src_key={src_key} \
                                         part_number={part_number}"
                                    ),
                                    error,
                                )
                            })?;
                        validate_content_range(
                            response.content_range(),
                            part,
                            object_size,
                            format!(
                                "<copy_object_multipart_cross_clients> validate part range \
                                 src_bucket={src_bucket} src_key={src_key} \
                                 part_number={part_number}"
                            ),
                        )?;
                        let bytes = response
                            .body
                            .collect()
                            .await
                            .map_err(|error| {
                                src_client.map_bytestream_download_error(
                                    format!(
                                        "<copy_object_multipart_cross_clients> read part \
                                         src_bucket={src_bucket} src_key={src_key} \
                                         part_number={part_number}"
                                    ),
                                    error,
                                )
                            })?
                            .into_bytes();
                        let actual_length = u64::try_from(bytes.len()).map_err(|error| {
                            S3Error::ValidationError(format!(
                                "downloaded part length cannot be represented as u64: {error}"
                            ))
                        })?;
                        if actual_length != part.length() {
                            return Err(S3Error::UnexpectedContentLength(
                                format!(
                                    "<copy_object_multipart_cross_clients> read part \
                                     src_bucket={src_bucket} src_key={src_key} \
                                     part_number={part_number}"
                                ),
                                part.length(),
                                actual_length,
                            ));
                        }
                        Ok(bytes)
                    })
                    .await?;

                let upload_output = dst_client
                    .with_retry(|| async {
                        dst_sdk_client
                            .upload_part()
                            .bucket(&dst_bucket)
                            .key(&dst_key)
                            .upload_id(&upload_id)
                            .part_number(part_number)
                            .body(ByteStream::from(bytes.clone()))
                            .send()
                            .await
                            .map_err(|error| {
                                dst_client.map_sdk_error(
                                    format!(
                                        "<copy_object_multipart_cross_clients> upload part \
                                         dst_bucket={dst_bucket} dst_key={dst_key} \
                                         part_number={part_number}"
                                    ),
                                    error,
                                )
                            })
                    })
                    .await?;

                if let Some(progress) = progress {
                    progress.inc(1);
                }
                Ok(CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(
                        upload_output
                            .e_tag()
                            .ok_or(S3Error::FieldNotExist("etag"))?,
                    )
                    .build())
            }
        })
        .buffer_unordered(workers)
        .try_collect()
        .await;

    let mut completed_parts = match transfer_result {
        Ok(parts) => parts,
        Err(error) => {
            if let Err(abort_error) = dst_client
                .abort_multipart_upload(dst_bucket, dst_key, &upload_id)
                .await
            {
                error!(
                    "<copy_object_multipart_cross_clients> Failed to abort multipart upload \
                     bucket={dst_bucket} key={dst_key} upload_id={upload_id}: {abort_error}"
                );
            }
            return Err(error);
        }
    };

    completed_parts.sort_by_key(|part| part.part_number());
    let multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();
    let complete_result = dst_client
        .with_retry(|| {
            let multipart_upload = multipart_upload.clone();
            async {
                dst_sdk_client
                    .complete_multipart_upload()
                    .bucket(dst_bucket)
                    .key(dst_key)
                    .upload_id(&upload_id)
                    .multipart_upload(multipart_upload)
                    .send()
                    .await
                    .map_err(|error| {
                        dst_client.map_sdk_error(
                            format!(
                                "<copy_object_multipart_cross_clients> complete upload \
                                 dst_bucket={dst_bucket} dst_key={dst_key}"
                            ),
                            error,
                        )
                    })
            }
        })
        .await;

    if let Err(error) = complete_result {
        if let Err(abort_error) = dst_client
            .abort_multipart_upload(dst_bucket, dst_key, &upload_id)
            .await
        {
            error!(
                "<copy_object_multipart_cross_clients> Failed to abort multipart upload \
                 bucket={dst_bucket} key={dst_key} upload_id={upload_id}: {abort_error}"
            );
        }
        return Err(error);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_partition(object_size: u64, requested_part_size: u64, max_parts: Option<u64>) {
        let plan = MultipartPlan::new(object_size, requested_part_size, max_parts).unwrap();
        assert_eq!(plan.part_count, object_size.div_ceil(plan.part_size));
        if let Some(max_parts) = max_parts {
            assert!(plan.part_count <= max_parts);
        }
        if object_size == 0 {
            assert_eq!(plan.part_count, 0);
            return;
        }

        let first = plan.parts().next().unwrap();
        let last = plan.parts().last().unwrap();
        assert_eq!(first.start, 0);
        assert_eq!(last.end, object_size);
        assert_eq!(first.number, 1);
        assert_eq!(u64::try_from(last.number).unwrap(), plan.part_count);

        for index in [0, plan.part_count / 2, plan.part_count - 1] {
            let part = plan.parts().nth(index as usize).unwrap();
            assert_eq!(part.start, index * plan.part_size);
            assert!(part.length() > 0);
            assert!(part.length() <= plan.part_size);
            if index + 1 < plan.part_count {
                assert_eq!(
                    part.end,
                    plan.parts().nth(index as usize + 1).unwrap().start
                );
            }
        }
    }

    #[test]
    fn validates_part_size_and_part_limit() {
        assert!(MultipartPlan::new(100, 0, Some(10)).is_err());
        assert!(MultipartPlan::new(100, 10, Some(0)).is_err());
        assert_eq!(
            MultipartPlan::for_upload(100, MULTIPART_MIN_PART_SIZE - 1)
                .unwrap()
                .part_size,
            MULTIPART_MIN_PART_SIZE
        );
        assert_eq!(
            MultipartPlan::for_upload(100, 0).unwrap().part_size,
            MULTIPART_MIN_PART_SIZE
        );
        assert_eq!(
            MultipartPlan::for_upload(100, MULTIPART_MAX_PART_SIZE + 1)
                .unwrap()
                .part_size,
            MULTIPART_MAX_PART_SIZE
        );
        assert!(
            MultipartPlan::for_upload(MULTIPART_MAX_OBJECT_SIZE + 1, MULTIPART_MAX_PART_SIZE)
                .is_err()
        );
    }

    #[test]
    fn partitions_exact_and_partial_final_parts() {
        assert_partition(40, 20, None);
        assert_partition(45, 20, None);
        assert_partition(1, 20, None);
        assert_partition(0, 20, None);
    }

    #[test]
    fn adapts_to_part_limit_without_overflow() {
        assert_partition(201, 20, Some(10));
        assert_partition(u64::MAX, 20, Some(10_000));
        assert_partition(u64::MAX, u64::MAX - 1, None);
    }

    #[test]
    fn validates_content_range() {
        let plan = MultipartPlan::for_download(42, 20).unwrap();
        let part = plan.parts().nth(1).unwrap();
        assert!(validate_content_range(Some("bytes 20-39/42"), part, 42, "test").is_ok());
        assert!(validate_content_range(Some("bytes 20-38/42"), part, 42, "test").is_err());
        assert!(validate_content_range(None, part, 42, "test").is_err());
    }
}
