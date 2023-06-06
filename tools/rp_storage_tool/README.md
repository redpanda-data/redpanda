# rp-storage-tool

## Purpose

This low level tool is for offline use by Redpanda Engineering when diagnosing
faults.

This tool is **not** for everyday use on live clusters.

## Quickstart

    # Get a rust toolchain
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

    # Compile and run
    cargo run --release -- --backend=<aws|gcp|azure> scan --source=<bucket name>

## Installation

To run the tool outside a built tree, you may simply copy the statically linked binary.

    # Compile
    cargo build --release

    # Grab the binary
    cp target/release/rp-storage-tool /usr/local/bin

## Usage

### Typical usage

The following examples assume you are running on an AWS instance with IAM roles configured
for access to your bucket. If you need to specify static keys, see the "Working with local object stores"
section below.

    # Scan the bucket, store metadata in a file, and report any metadata anomalies
    $ rp-storage-tool --backend aws scan-metadata --source=my-bucket --meta-file my-bucket.json

    # Scan the data within a topic of interest, and report any anomalies.  Use the metadata
    # file from the last step to avoid re-scanning all objects in the bucket.
    $ rp-storage-tool --filter kafka/my-topic/*_* --backend aws scan-data --source=my-bucket --meta-file my-bucket.json

    # Having identified an issue, extract the data from a partition for further analysis offline
    $ mkdir my-data-dump
    $ rp-storage-tool --filter kafka/my-topic/13_63 --backend aws extract --source=my-bucket --meta-file my-bucket.json --sink ./my-data-dump/

### Generic parameters

* The `--backend` argument selects the cloud storage backend. This has a default (AWS) for convenience when working with
  commands that don't use cloud storage, but ordinarily you should be specifying it.
* The `--filter` argument controls which topics & partitions will be examined. When scanning metadata we
  always scan all objects, but will only store+analyze metadata for partitions matching the filter.
* If you are running on a node with authentication already set up (e.g. IAM Roles on AWS), this will Just Work.
  Otherwise you may need to set the appropriate access key/secret environment variables for the cloud platform you are
  connecting to.

### Subcommands

#### `scan-metadata`

This walks the objects in a bucket used by Redpanda Tiered Storage and reports on any inconsistencies.
**Not all issues this tool reports are harmful**, for example `segments_outside_manifest` may contain objects harmlessly
left behind when Redpanda was restarted during an upload.

#### `scan-data`

This walks the data within segments (you probably want to use `--filter` to only scan segments for certain NTPS)
to check that it is readable, and that the metadata in the partition manifest is consistent with the data.

#### `extract`

For NTPs matching the filter, copy all metadata and data objects into a local folder.

## Working with local object stores

This tool uses environment variables for any special configuration of storage backends required for working outside of
real cloud environments.

For example, to use the tool with a bucket called `data` in a minio cluster at `aplite:9000`:

    AWS_ALLOW_HTTP=1 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin AWS_REGION=us-east-1 \
      AWS_ENDPOINT=http://aplite:9000 cargo run --release -- --backend aws scan --source data

### Building a portable binary

If you are building on a workstation and copying the binary to a remote machine, and your
workstation doesn't happen to run the same linux distro as the remote machine, it is
useful to build a statically linked binary that will work on any distro.

    # A dependency requires this to build: adjust as needed if not on an RPM distro
    sudo dnf install -y musl-gcc

    # Install the Rust toolchain for musl (statically linked libc)
    rustup target add x86_64-unknown-linux-musl

    # Build with the musl toolchain
    cargo build --release --target=x86_64-unknown-linux-musl

    # Output in target/release/rp-storage-tool

