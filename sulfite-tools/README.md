# sulfite-tools

[![Crates.io](https://img.shields.io/crates/v/sulfite-tools)](https://crates.io/crates/sulfite-tools)
[![Docs.rs](https://docs.rs/sulfite-tools/badge.svg)](https://docs.rs/sulfite-tools)
[![License](https://img.shields.io/crates/l/sulfite-tools)](#license)

CLI for S3 built on the [sulfite](https://crates.io/crates/sulfite) library. Supports listing, single-object ops, and batch ops from CSV.

## Overview

`sulfite` is a high-level S3 client built on [AWS SDK for Rust](https://awslabs.github.io/aws-sdk-rust/) for even better ease of use, reliability, and bandwidth saturation (>50 Gbps).

*The name*: `SO3^2-`, an anion, implying a companion to some other cation (application), is commonly used as a preservative in wines and dried fruits (preserve to S3). It's `S3` with an `O` in the middle, a play on [oxidization](https://wiki.mozilla.org/Oxidation).

## Installation

```bash
cargo install sulfite-tools
```

## Usage

Global options (can be used with any subcommand):

- `--region`, `-r` — AWS region (or region of custom endpoint)
- `--endpoint-url`, `-e` — S3 endpoint URL (e.g. for MinIO)
- `--profile` — AWS profile for the primary/source client
- `--max-retries` — Maximum retries per request (default: 3)
- `--retriable-client-status-codes` — HTTP status codes to treat as retriable (comma-separated; default: 408,429)
- `--read-timeout` — Read timeout in seconds for the HTTP client (default: 60)

### Subcommands

| Command | Description |
|--------|-------------|
| `list` | List objects in a bucket (optional prefix/suffix), output keys to CSV or stdout |
| `head` | Get metadata (HEAD) for one object |
| `download` | Download one object (single request) |
| `download-multipart` | Download one object (multipart transfer) |
| `upload` | Upload one object (single request) |
| `upload-multipart` | Upload one object (multipart transfer) |
| `delete` | Delete one object |
| `copy` | Copy one object from source to destination |
| `copy-multipart` | Copy within one S3 backend using server-side multipart ranges |
| `copy-multipart-cross-clients` | Copy across independently configured S3 backends through bounded memory |
| `restore` | Restore one object from archival storage (e.g. Glacier) |
| `csv` | Run one operation per key from a CSV file (batch) |

### Examples

```bash
# List keys in a bucket with prefix, write to CSV
--> sulfite list -b my-bucket -p my-prefix/ -o keys.csv
Found 3 objects.
Listing first 3...
  my-object-1.txt
    size: 276480 (270.00K) timestamp: 2025-06-15T15:06:42Z storage_class: DEEP_ARCHIVE
  my-object-2.txt
    size: 20480 (20.00K) timestamp: 2025-07-06T13:11:54Z storage_class: STANDARD
  my-object-3.txt
    size: 81559 (79.65K) timestamp: 2025-06-15T15:10:21Z storage_class: STANDARD
```

```bash
# Get metadata (HEAD) for one object
--> sulfite head -b my-bucket -k my-object-1.txt
  my-object-1.txt
    size: 276480 (270.00K) timestamp: 2025-06-15T15:06:42Z storage_class: DEEP_ARCHIVE
# If the object is being restored from archival storage, a restore_status is appended, e.g.:
#     ... storage_class: DEEP_ARCHIVE restore_status: ONGOING
#     ... storage_class: DEEP_ARCHIVE restore_status: EXPIRY:2026-06-18T00:00:00Z
```

```bash
# Download with multipart (large files)
--> sulfite download-multipart -b my-bucket -k my-large-object.txt -l my-large-object.txt
⠤ [#>--------------------------------------]     130/3104    [00:00:07<00:03:09 15.6583/s]
```

```bash
# Upload with multipart (large files)
sulfite upload-multipart -b my-bucket -k path/to/object -l local-file

# Use custom endpoint (e.g. MinIO)
sulfite -e https://minio.example.com list -b my-bucket -p ""

# Copy across two S3-compatible backends without local disk
sulfite --endpoint-url https://source.example.com --profile source \
  copy-multipart-cross-clients --src-bucket source-bucket --src-key path/object \
  --dst-endpoint-url https://destination.example.com --dst-profile destination \
  --dst-bucket destination-bucket --dst-key path/object
```

### CSV workflow

Use `list` to write a manifest of keys to a CSV file, then use the `csv` subcommand to run batch operations (e.g. download all objects into a directory):

```bash
# 1. List keys under a prefix and write to a manifest CSV
sulfite list -b my-bucket -p my/prefix/ -o manifest.csv

# 2. Download every key in the manifest to a local directory (keys become paths under that dir)
sulfite csv manifest.csv --has-header download -b my-bucket -p my/prefix/ -l ./downloaded
```

Use `--column-idx/-c N` if the key column is not the first (0-based index).

To copy a manifest across independently configured S3 clients, use `csv ... copy-cross-clients` with the source bucket/prefix and destination bucket/prefix. Destination connection options are `--dst-endpoint-url`, `--dst-region`, and `--dst-profile`.

**CSV skip behavior** — By default, CSV operations overwrite an existing destination. Pass `--skip-existing-with-inference` before the operation subcommand to infer unchanged items. Local/S3 transfers use S3-authoritative inference: download and upload skip when sizes match and the S3 timestamp is equal to or newer than the local mtime. Upload also requires the object's storage class to match the effective requested class. This makes a download immediately after an upload a no-op; if the local file is newer, the selected operation proceeds (upload publishes it, while download intentionally replaces it from S3). Copies compare source and destination sizes plus the effective destination storage class; tiny archival uploads and copies are expected to remain `STANDARD`. For example: `sulfite csv manifest.csv --has-header --skip-existing-with-inference copy ...`.

**Multipart activation** — In CSV batch mode, download uses a single GET for objects under 1 GB and multipart for ≥ 1 GB. Upload uses single-part for under 1 GB and multipart for ≥ 1 GB. Cross-client copy uses an in-memory GET followed by PUT for objects under 20 MiB and bounded-memory multipart transfer for ≥ 20 MiB. The single-object commands `download` / `upload` always use one request; `download-multipart` / `upload-multipart` always use multipart.

**Archival and small files** — When you specify an archival storage class with `--storage-class GLACIER` on `csv upload`, or `--dst-storage-class GLACIER` on `csv copy` and `csv copy-cross-clients`, objects under 16 KB are stored as STANDARD instead of the requested class, for efficiency.

Run `sulfite --help` or `sulfite <command> --help` for full options.

## Testing

Integration tests run the CLI against [LocalStack](https://localstack.cloud/). Start LocalStack (e.g. `docker run --rm -it -p 4566:4566 localstack/localstack`), then:

```bash
cargo test -p sulfite-tools --test localstack_cli -- --ignored
```

Set `LOCALSTACK_ENDPOINT` to override the default `http://localhost:4566`.
