use anyhow::Context;
use sulfite::{S3Client, copy_object_multipart_cross_clients};

use crate::ObjCommand;
use sulfite_tools::utils::{make_progress_bar, print_object_human};

pub async fn run_obj(
    client: S3Client,
    command: ObjCommand,
    show_progress: bool,
) -> anyhow::Result<()> {
    match command {
        ObjCommand::Head(a) => {
            let obj = client.head_object(&a.bucket, &a.key).await?;
            print_object_human(&a.key, &obj);
        }
        ObjCommand::Download(a) => {
            let local_path = match &a.local_path {
                Some(p) => p.as_str(),
                None => std::path::Path::new(&a.key)
                    .file_name()
                    .and_then(|os_str| os_str.to_str())
                    .context("key has no file name")?,
            };
            let start_end_offsets = a.start_offset.zip(a.end_offset);
            client
                .download_object(&a.bucket, &a.key, local_path, start_end_offsets)
                .await?;
        }
        ObjCommand::DownloadMultipart(a) => {
            let local_path = match &a.local_path {
                Some(p) => p.as_str(),
                None => std::path::Path::new(&a.key)
                    .file_name()
                    .and_then(|os_str| os_str.to_str())
                    .context("key has no file name")?,
            };
            let pb = make_progress_bar(Some(0), show_progress);
            client
                .download_object_multipart(&a.bucket, &a.key, local_path, Some(&pb))
                .await?;
            pb.finish();
        }
        ObjCommand::Upload(a) => {
            client
                .upload_object(&a.bucket, &a.key, &a.local_path, a.storage_class.as_deref())
                .await?;
        }
        ObjCommand::UploadMultipart(a) => {
            let pb = make_progress_bar(Some(0), show_progress);
            client
                .upload_object_multipart(
                    &a.bucket,
                    &a.key,
                    &a.local_path,
                    a.storage_class.as_deref(),
                    Some(&pb),
                )
                .await?;
            pb.finish();
        }
        ObjCommand::Delete(a) => {
            client.delete_object(&a.bucket, &a.key).await?;
        }
        ObjCommand::Copy(a) => {
            client
                .copy_object(
                    &a.src_bucket,
                    &a.src_key,
                    &a.dst_bucket,
                    &a.dst_key,
                    a.dst_storage_class.as_deref(),
                )
                .await?;
        }
        ObjCommand::CopyMultipart(a) => {
            let pb = make_progress_bar(Some(0), show_progress);
            client
                .copy_object_multipart(
                    &a.src_bucket,
                    &a.src_key,
                    &a.dst_bucket,
                    &a.dst_key,
                    a.dst_storage_class.as_deref(),
                    Some(&pb),
                )
                .await?;
            pb.finish();
        }
        ObjCommand::CopyMultipartCrossClients { args, dst_client } => {
            let pb = make_progress_bar(Some(0), show_progress);
            copy_object_multipart_cross_clients(
                &client,
                &dst_client,
                &args.src_bucket,
                &args.src_key,
                &args.dst_bucket,
                &args.dst_key,
                args.dst_storage_class.as_deref(),
                Some(&pb),
            )
            .await?;
            pb.finish();
        }
        ObjCommand::Restore(a) => {
            client
                .restore_object(&a.bucket, &a.key, a.restore_days, &a.restore_tier)
                .await?;
        }
    }

    Ok(())
}
