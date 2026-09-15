mod args;
mod csv;
mod list;
mod obj;

pub use args::*;
use clap::Parser;
use std::process::ExitCode;
use sulfite::{RetryConfig, S3Client, S3ClientConfig};

#[tokio::main]
async fn main() -> ExitCode {
    let args = Cli::parse();
    let filter =
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into());
    if args.json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .json()
            .with_ansi(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    }

    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Cli) -> anyhow::Result<()> {
    let show_progress = !args.json;

    // Create the primary client once before command dispatch.
    let client = S3Client::new(
        S3ClientConfig {
            region: args.region.clone(),
            endpoint_url: args.endpoint_url.clone(),
            profile_name: args.profile.clone(),
            read_timeout_secs: args.read_timeout,
            multipart_part_size: args.multipart_part_size,
            multipart_workers: args.multipart_workers,
            ..Default::default()
        },
        RetryConfig {
            max_retries: args.max_retries,
            retriable_client_status_codes: args.retriable_client_status_codes.clone(),
            ..Default::default()
        },
    )
    .await;

    match args.command {
        Command::List(a) => list::run_list(client, a).await,
        Command::Head(a) => obj::run_obj(client, ObjCommand::Head(a), show_progress).await,
        Command::Download(a) => obj::run_obj(client, ObjCommand::Download(a), show_progress).await,
        Command::DownloadMultipart(a) => {
            obj::run_obj(client, ObjCommand::DownloadMultipart(a), show_progress).await
        }
        Command::Upload(a) => obj::run_obj(client, ObjCommand::Upload(a), show_progress).await,
        Command::UploadMultipart(a) => {
            obj::run_obj(client, ObjCommand::UploadMultipart(a), show_progress).await
        }
        Command::Delete(a) => obj::run_obj(client, ObjCommand::Delete(a), show_progress).await,
        Command::Copy(a) => obj::run_obj(client, ObjCommand::Copy(a), show_progress).await,
        Command::CopyMultipart(a) => {
            obj::run_obj(client, ObjCommand::CopyMultipart(a), show_progress).await
        }
        Command::CopyMultipartCrossClients(a) => {
            let dst_client = S3Client::new(
                S3ClientConfig {
                    region: a.dst_region.clone(),
                    endpoint_url: a.dst_endpoint_url.clone(),
                    profile_name: a.dst_profile.clone(),
                    read_timeout_secs: args.read_timeout,
                    multipart_part_size: args.multipart_part_size,
                    multipart_workers: args.multipart_workers,
                    ..Default::default()
                },
                RetryConfig {
                    max_retries: args.max_retries,
                    retriable_client_status_codes: args.retriable_client_status_codes,
                    ..Default::default()
                },
            )
            .await;
            obj::run_obj(
                client,
                ObjCommand::CopyMultipartCrossClients {
                    args: a,
                    dst_client,
                },
                show_progress,
            )
            .await
        }
        Command::Restore(a) => obj::run_obj(client, ObjCommand::Restore(a), show_progress).await,
        Command::Csv(a) => {
            let dst_client = match &a.command {
                CsvCommand::CopyCrossClients {
                    dst_endpoint_url,
                    dst_region,
                    dst_profile,
                    ..
                } => Some(
                    S3Client::new(
                        S3ClientConfig {
                            region: dst_region.clone(),
                            endpoint_url: dst_endpoint_url.clone(),
                            profile_name: dst_profile.clone(),
                            read_timeout_secs: args.read_timeout,
                            multipart_part_size: args.multipart_part_size,
                            multipart_workers: args.multipart_workers,
                            ..Default::default()
                        },
                        RetryConfig {
                            max_retries: args.max_retries,
                            retriable_client_status_codes: args
                                .retriable_client_status_codes
                                .clone(),
                            ..Default::default()
                        },
                    )
                    .await,
                ),
                _ => None,
            };
            csv::run_csv(client, dst_client, a, show_progress).await
        }
    }
}
