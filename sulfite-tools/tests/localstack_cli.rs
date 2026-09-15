//! LocalStack integration tests for the sulfite CLI.
//!
//! Requires LocalStack running (e.g. `docker run --rm -it -p 4566:4566 localstack/localstack`).
//! Set `LOCALSTACK_ENDPOINT` to override default `http://localhost:4566`.
//!
//! Each test uses a random hex run id so tests are independent. Setup (bucket, put object) uses the
//! sulfite library; assertions run the sulfite CLI and check exit status and output.

use std::process::Command;
use std::sync::Once;

use sulfite::{RetryConfig, S3Client, S3ClientConfig, generate_random_hex};

const DEFAULT_LOCALSTACK_ENDPOINT: &str = "http://localhost:4566";
const TEST_BUCKET: &str = "sulfite-test-bucket";
const RANDOM_HEX_LEN: usize = 32;

static TRACING_SUBSCRIBER: Once = Once::new();

fn localstack_endpoint() -> String {
    std::env::var("LOCALSTACK_ENDPOINT").unwrap_or_else(|_| DEFAULT_LOCALSTACK_ENDPOINT.into())
}

fn cli_base_args() -> Vec<String> {
    vec![
        "--endpoint-url".into(),
        localstack_endpoint(),
        "--region".into(),
        "us-east-1".into(),
    ]
}

fn run_cli(args: &[&str]) -> (bool, String, String) {
    TRACING_SUBSCRIBER.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "warn".into()),
            )
            .with_test_writer()
            .try_init();
    });
    let exe = env!("CARGO_BIN_EXE_sulfite");
    let mut cmd = Command::new(exe);
    cmd.env("AWS_ACCESS_KEY_ID", "test");
    cmd.env("AWS_SECRET_ACCESS_KEY", "test");
    cmd.env("AWS_REGION", "us-east-1");
    cmd.args(cli_base_args());
    cmd.args(args);
    let out = cmd.output().expect("run sulfite CLI");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    (out.status.success(), stdout, stderr)
}

async fn make_client() -> S3Client {
    let endpoint = localstack_endpoint();
    let config = S3ClientConfig {
        endpoint_url: Some(endpoint),
        region: Some("us-east-1".into()),
        access_secret_session_tuple: Some(("test".into(), "test".into(), None)),
        ..S3ClientConfig::default()
    };
    S3Client::new(config, RetryConfig::default()).await
}

async fn ensure_bucket(client: &S3Client, bucket: &str) {
    let _ = client.create_bucket(bucket).await;
}

// Put object via library, then CLI head
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test -p sulfite-tools --test localstack_cli -- --ignored)"]
async fn cli_head_after_put() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let key = format!("cli-head/{}/obj", run);
    let body = b"hello cli head";

    client
        .put_object(TEST_BUCKET, &key, body, None)
        .await
        .expect("put_object");

    let (ok, stdout, stderr) = run_cli(&["head", "--bucket", TEST_BUCKET, "--key", &key]);
    assert!(ok, "cli head failed: stderr={}", stderr);
    assert!(
        stdout.contains(&key) || stdout.contains("key") || stdout.contains("size"),
        "stdout: {}",
        stdout
    );

    let _ = client.delete_object(TEST_BUCKET, &key).await;
}

// Put object via library, CLI download to file, assert content
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test -p sulfite-tools --test localstack_cli -- --ignored)"]
async fn cli_download_after_put() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let key = format!("cli-download/{}/obj", run);
    let body = b"download me";

    client
        .put_object(TEST_BUCKET, &key, body, None)
        .await
        .expect("put_object");

    let tmp = std::env::temp_dir().join(format!("sulfite_cli_test_{}", run));
    let path = tmp.to_str().unwrap();
    let (ok, _stdout, stderr) = run_cli(&[
        "download",
        "--bucket",
        TEST_BUCKET,
        "--key",
        &key,
        "--local-path",
        path,
    ]);
    assert!(ok, "cli download failed: stderr={}", stderr);
    let got = std::fs::read(&tmp).expect("read downloaded file");
    assert_eq!(got.as_slice(), body);

    let _ = std::fs::remove_file(&tmp);
    let _ = client.delete_object(TEST_BUCKET, &key).await;
}

// Put objects via library, CLI list with prefix, assert count in output
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test -p sulfite-tools --test localstack_cli -- --ignored)"]
async fn cli_list_after_put() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let prefix = format!("cli-list/{}/", run);
    let n = 5u32;

    for i in 0..n {
        let key = format!("{}k{:02}", prefix, i);
        client
            .put_object(TEST_BUCKET, &key, format!("body-{}", i).as_bytes(), None)
            .await
            .expect("put_object");
    }

    let (ok, stdout, stderr) = run_cli(&["list", "--bucket", TEST_BUCKET, "--prefix", &prefix]);
    assert!(ok, "cli list failed: stderr={}", stderr);
    assert!(
        stdout.contains("Found 5 objects") || stdout.contains("5 objects"),
        "stdout: {}",
        stdout
    );

    for i in 0..n {
        let _ = client
            .delete_object(TEST_BUCKET, &format!("{}k{:02}", prefix, i))
            .await;
    }
}

// Create temp file, CLI upload, then head via library
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test -p sulfite-tools --test localstack_cli -- --ignored)"]
async fn cli_upload_then_head() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let key = format!("cli-upload/{}/obj", run);
    let body = b"uploaded via cli";

    let tmp = std::env::temp_dir().join(format!("sulfite_cli_upload_{}", run));
    std::fs::write(&tmp, body).expect("write temp file");
    let path = tmp.to_str().unwrap();

    let (ok, _stdout, stderr) = run_cli(&[
        "upload",
        "--bucket",
        TEST_BUCKET,
        "--key",
        &key,
        "--local-path",
        path,
    ]);
    assert!(ok, "cli upload failed: stderr={}", stderr);

    let info = client
        .head_object(TEST_BUCKET, &key)
        .await
        .expect("head_object");
    assert_eq!(info.key, key);
    assert_eq!(info.size, body.len() as u64);

    let _ = std::fs::remove_file(&tmp);
    let _ = client.delete_object(TEST_BUCKET, &key).await;
}

// Put object via library, CLI copy, head copy via library
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test -p sulfite-tools --test localstack_cli -- --ignored)"]
async fn cli_copy_then_head() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let src_key = format!("cli-copy/{}/src", run);
    let dst_key = format!("cli-copy/{}/dst", run);
    let body = b"copy me";

    client
        .put_object(TEST_BUCKET, &src_key, body, None)
        .await
        .expect("put_object");

    let (ok, _stdout, stderr) = run_cli(&[
        "copy",
        "--src-bucket",
        TEST_BUCKET,
        "--src-key",
        &src_key,
        "--dst-bucket",
        TEST_BUCKET,
        "--dst-key",
        &dst_key,
    ]);
    assert!(ok, "cli copy failed: stderr={}", stderr);

    let info = client
        .head_object(TEST_BUCKET, &dst_key)
        .await
        .expect("head copy");
    assert_eq!(info.key, dst_key);
    assert_eq!(info.size, body.len() as u64);

    let _ = client.delete_object(TEST_BUCKET, &src_key).await;
    let _ = client.delete_object(TEST_BUCKET, &dst_key).await;
}

// Put a multi-part object via the library, copy it across clients with concurrent ranged
// transfers, then verify the destination bytes.
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test -p sulfite-tools --test localstack_cli -- --ignored)"]
async fn cli_copy_multipart_cross_clients() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let src_key = format!("cli-copy-multipart/{}/src", run);
    let dst_key = format!("cli-copy-multipart/{}/dst", run);
    let body: Vec<u8> = (0..(21 * 1024 * 1024 + 123))
        .map(|index| (index % 251) as u8)
        .collect();

    client
        .put_object(TEST_BUCKET, &src_key, &body, None)
        .await
        .expect("put source object");

    let destination_endpoint = localstack_endpoint();
    let (ok, _stdout, stderr) = run_cli(&[
        "--multipart-part-size",
        "5242880",
        "--multipart-workers",
        "2",
        "copy-multipart-cross-clients",
        "--src-bucket",
        TEST_BUCKET,
        "--src-key",
        &src_key,
        "--dst-bucket",
        TEST_BUCKET,
        "--dst-key",
        &dst_key,
        "--dst-endpoint-url",
        &destination_endpoint,
        "--dst-region",
        "us-east-1",
    ]);
    assert!(ok, "CLI multipart copy failed: stderr={stderr}");

    let (_, copied) = client
        .get_object(TEST_BUCKET, &dst_key, None)
        .await
        .expect("get copied object");
    assert_eq!(copied, body);

    let _ = client.delete_object(TEST_BUCKET, &src_key).await;
    let _ = client.delete_object(TEST_BUCKET, &dst_key).await;
}

// Put object via library, CLI delete, head via library must 404
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test -p sulfite-tools --test localstack_cli -- --ignored)"]
async fn cli_delete_then_head_404() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let key = format!("cli-delete/{}/obj", run);
    let body = b"delete me";

    client
        .put_object(TEST_BUCKET, &key, body, None)
        .await
        .expect("put_object");

    let (ok, _stdout, stderr) = run_cli(&["delete", "--bucket", TEST_BUCKET, "--key", &key]);
    assert!(ok, "cli delete failed: stderr={}", stderr);

    let err = client.head_object(TEST_BUCKET, &key).await.unwrap_err();
    if let sulfite::S3Error::AWSS3Error(_, _, _, code) = err {
        assert_eq!(code, 404);
    } else {
        panic!("expected 404, got {:?}", err);
    }
}

// Put in GLACIER via library, CLI download fails, CLI restore, CLI download succeeds (best-effort on LocalStack)
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test -p sulfite-tools --test localstack_cli -- --ignored)"]
async fn cli_glacier_put_restore_get() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let key = format!("cli-glacier/{}/archived", run);
    let body = b"cold storage content";

    client
        .put_object(TEST_BUCKET, &key, body, Some("GLACIER"))
        .await
        .expect("put_object GLACIER");

    let tmp = std::env::temp_dir().join(format!("sulfite_cli_glacier_{}", run));
    let path = tmp.to_str().unwrap();

    let (ok_before, _stdout, stderr_before) = run_cli(&[
        "download",
        "--bucket",
        TEST_BUCKET,
        "--key",
        &key,
        "--local-path",
        path,
    ]);
    assert!(
        !ok_before,
        "download before restore should fail: stderr={}",
        stderr_before
    );

    let (ok_restore, _stdout, stderr_restore) = run_cli(&[
        "restore",
        "--bucket",
        TEST_BUCKET,
        "--key",
        &key,
        "--restore-tier",
        "Expedited",
        "--restore-days",
        "1",
    ]);
    assert!(ok_restore, "cli restore failed: stderr={}", stderr_restore);

    let (ok_after, _stdout, stderr_after) = run_cli(&[
        "download",
        "--bucket",
        TEST_BUCKET,
        "--key",
        &key,
        "--local-path",
        path,
    ]);
    assert!(
        ok_after,
        "download after restore failed: stderr={}",
        stderr_after
    );
    let got = std::fs::read(&tmp).expect("read downloaded file");
    assert_eq!(got.as_slice(), body);

    let _ = std::fs::remove_file(&tmp);
    let _ = client.delete_object(TEST_BUCKET, &key).await;
}

// CSV workflow: put 10 objects via library, list to CSV, csv download to dir, verify 10 files
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test -p sulfite-tools --test localstack_cli -- --ignored)"]
async fn cli_csv_list_then_download() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let prefix = format!("cli-csv-dl/{}/", run);
    let n = 10u32;

    for i in 0..n {
        let key = format!("{}k{:02}", prefix, i);
        let body = format!("body-{}", i);
        client
            .put_object(TEST_BUCKET, &key, body.as_bytes(), None)
            .await
            .expect("put_object");
    }

    let base = std::env::temp_dir().join("sulfite_cli_csv").join(&run);
    let _ = std::fs::create_dir_all(&base).ok();
    let manifest = base.join("manifest.csv");
    let out_dir = base.join("out");
    let _ = std::fs::create_dir_all(&out_dir).ok();
    let manifest_s = manifest.to_str().unwrap();
    let out_dir_s = out_dir.to_str().unwrap();

    let (ok_list, _stdout, stderr_list) = run_cli(&[
        "list",
        "--bucket",
        TEST_BUCKET,
        "--prefix",
        &prefix,
        "--output-path",
        manifest_s,
    ]);
    assert!(ok_list, "cli list failed: stderr={}", stderr_list);

    let (ok_dl, _stdout, stderr_dl) = run_cli(&[
        "csv",
        manifest_s,
        "--has-header",
        "download",
        "--bucket",
        TEST_BUCKET,
        "--prefix",
        &prefix,
        "--local-dir",
        out_dir_s,
    ]);
    assert!(ok_dl, "cli csv download failed: stderr={}", stderr_dl);

    for i in 0..n {
        let key_file = out_dir.join(format!("k{:02}", i));
        let got = std::fs::read_to_string(&key_file).expect("read downloaded file");
        assert_eq!(got, format!("body-{}", i), "file k{:02}", i);
    }

    for i in 0..n {
        let _ = client
            .delete_object(TEST_BUCKET, &format!("{}k{:02}", prefix, i))
            .await;
    }
    let _ = std::fs::remove_dir_all(&base).ok();
}

// CSV workflow: create 10 local files, write CSV of keys, csv upload, then head each via library
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test -p sulfite-tools --test localstack_cli -- --ignored)"]
async fn cli_csv_upload_then_head() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let prefix = format!("cli-csv-ul/{}/", run);
    let n = 10u32;

    let base = std::env::temp_dir().join("sulfite_cli_csv_ul").join(&run);
    let _ = std::fs::create_dir_all(&base).ok();
    let mut csv_content = String::from("key\n");
    for i in 0..n {
        let name = format!("k{:02}", i);
        let body = format!("uploaded-{}", i);
        std::fs::write(base.join(&name), &body).expect("write file");
        csv_content.push_str(&name);
        csv_content.push('\n');
    }
    let manifest = base.join("keys.csv");
    std::fs::write(&manifest, &csv_content).expect("write CSV");
    let manifest_s = manifest.to_str().unwrap();
    let base_s = base.to_str().unwrap();

    let (ok_ul, _stdout, stderr_ul) = run_cli(&[
        "csv",
        manifest_s,
        "--has-header",
        "upload",
        "--bucket",
        TEST_BUCKET,
        "--prefix",
        &prefix,
        "--local-dir",
        base_s,
    ]);
    assert!(ok_ul, "cli csv upload failed: stderr={}", stderr_ul);

    for i in 0..n {
        let key = format!("{}k{:02}", prefix, i);
        let info = client
            .head_object(TEST_BUCKET, &key)
            .await
            .expect("head_object");
        assert_eq!(info.key, key);
        assert_eq!(info.size, format!("uploaded-{}", i).len() as u64);
        let _ = client.delete_object(TEST_BUCKET, &key).await;
    }
    let _ = std::fs::remove_dir_all(&base).ok();
}

// CSV workflow: copy small objects through independently configured clients.
#[tokio::test]
#[ignore = "requires LocalStack (run with: cargo test -p sulfite-tools --test localstack_cli -- --ignored)"]
async fn cli_csv_copy_cross_clients() {
    let client = make_client().await;
    ensure_bucket(&client, TEST_BUCKET).await;
    let run = generate_random_hex(RANDOM_HEX_LEN);
    let src_prefix = format!("cli-csv-copy-cross/{run}/src/");
    let dst_prefix = format!("cli-csv-copy-cross/{run}/dst/");
    let keys = ["first", "second"];

    for key in keys {
        client
            .put_object(
                TEST_BUCKET,
                &format!("{src_prefix}{key}"),
                format!("body-{key}").as_bytes(),
                None,
            )
            .await
            .expect("put source object");
    }

    let base = std::env::temp_dir()
        .join("sulfite_cli_csv_copy_cross_clients")
        .join(&run);
    std::fs::create_dir_all(&base).expect("create test directory");
    let manifest = base.join("keys.csv");
    std::fs::write(&manifest, "key\nfirst\nsecond\n").expect("write CSV");
    let endpoint = localstack_endpoint();
    let (ok, _stdout, stderr) = run_cli(&[
        "csv",
        manifest.to_str().unwrap(),
        "--has-header",
        "copy-cross-clients",
        "--src-bucket",
        TEST_BUCKET,
        "--src-prefix",
        &src_prefix,
        "--dst-bucket",
        TEST_BUCKET,
        "--dst-prefix",
        &dst_prefix,
        "--dst-endpoint-url",
        &endpoint,
        "--dst-region",
        "us-east-1",
    ]);
    assert!(ok, "CLI CSV cross-client copy failed: stderr={stderr}");

    for key in keys {
        let (_, copied) = client
            .get_object(TEST_BUCKET, &format!("{dst_prefix}{key}"), None)
            .await
            .expect("get copied object");
        assert_eq!(copied, format!("body-{key}").as_bytes());
        let _ = client
            .delete_object(TEST_BUCKET, &format!("{src_prefix}{key}"))
            .await;
        let _ = client
            .delete_object(TEST_BUCKET, &format!("{dst_prefix}{key}"))
            .await;
    }
    let _ = std::fs::remove_dir_all(&base);
}
