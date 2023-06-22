extern crate deltafor;
extern crate redpanda_records;

mod batch_crc;
mod batch_reader;
mod batch_writer;
mod bucket_reader;
mod error;
mod fundamental;
mod ntp_mask;
mod remote_types;
mod repair;
mod segment_writer;
mod util;
mod varint;

use crate::segment_writer::SegmentWriter;
use bytes::Bytes;
use futures::{pin_mut, StreamExt};
use log::{debug, info, warn};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

use crate::batch_reader::DumpError;
use crate::batch_writer::reserialize_batch;
use crate::bucket_reader::{AnomalyStatus, BucketReader};
use crate::error::BucketReaderError;
use crate::fundamental::{RawOffset, Timestamp, NTPR};
use crate::ntp_mask::NTPFilter;
use crate::remote_types::{PartitionManifest, PartitionManifestSegment, SegmentNameFormat};
use batch_reader::BatchStream;
use clap::{Parser, Subcommand};
use object_store::ObjectStore;
use redpanda_records::{RecordBatchType, RecordOwned};
use tokio_util::io::StreamReader;

/// Parser for use with `clap` argument parsing
pub fn ntpr_mask_parser(input: &str) -> Result<NTPFilter, String> {
    NTPFilter::from_str(input).map_err(|e| e.to_string())
}

#[derive(clap::ValueEnum, Clone)]
enum Backend {
    AWS,
    GCP,
    Azure,
}

impl Display for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::AWS => f.write_str("aws"),
            Backend::GCP => f.write_str("gcp"),
            Backend::Azure => f.write_str("azure"),
        }
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(short, long, default_value_t = Backend::AWS)]
    backend: Backend,

    #[arg(short, long, value_parser = ntpr_mask_parser, default_value_t = NTPFilter::match_all())]
    filter: NTPFilter,
}

#[derive(Subcommand)]
enum Commands {
    Rewrite {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        force: bool,
    },
    Scrub {
        #[arg(short, long)]
        source: String,
    },
    Compact {
        #[arg(short, long)]
        source: String,
    },
    RebuildManifest {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        sink: String,
        #[arg(short, long)]
        meta_file: Option<String>,
    },
    FilterMetadata {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        meta_in: String,
        #[arg(short, long)]
        meta_out: String,
        #[arg(short, long)]
        inject_manifest_path: Option<String>,
    },
}

async fn filter_metadata(
    cli: &Cli,
    source: &str,
    meta_in: &str,
    meta_out: &str,
    inject_manifest_path: Option<&str>,
) -> Result<(), BucketReaderError> {
    info!("Loading metadata from {}...", meta_in);

    let mut bucket_reader = make_bucket_reader(cli, source, Some(meta_in)).await;
    bucket_reader.filter(&cli.filter);

    if let Some(p) = inject_manifest_path {
        info!("Injecting manifest from {}...", p);
        let mut file = File::open(p).await.unwrap();
        let mut buf: String = String::new();
        file.read_to_string(&mut buf).await?;
        bucket_reader
            .inject_partition_manifest(&p, bytes::Bytes::from(buf))
            .await;
    }

    bucket_reader.to_file(meta_out).await?;
    Ok(())
}

async fn rebuild_manifest(
    cli: &Cli,
    source: &str,
    sink: &str,
    meta_file: Option<&str>,
) -> Result<(), BucketReaderError> {
    info!("Loading metadata from {}...", source);

    let bucket_reader = make_bucket_reader(cli, source, meta_file).await;

    for (ntpr, partition_objects) in &bucket_reader.partitions {
        if !cli.filter.match_ntpr(&ntpr) {
            continue;
        }
        info!("Rebuilding manifest for partition {}", ntpr);

        // TODO: implement binary encoding
        let manifest_path = PartitionManifest::manifest_key(&ntpr, "json");

        // TODO: if PartitionObjects has an ambiguous dropped objects, now is the time to
        // try and validate/true-up the content.

        // Initially assume that at the start of the partition kafka offset == raw offset:
        // we cannot know any better from the raw segment data.  Later, we will adjust deltas
        // if we have an existing partition manifest that can tell us about the delta.
        let mut offset_delta: i64 = 0;

        // Build a manifest
        let mut manifest = PartitionManifest::new(ntpr.clone());
        for (_base_offset, segment) in &partition_objects.segment_objects {
            // Stream of Bytes chunks of data in the segment
            let segment_chunk_stream = bucket_reader.stream_one(&segment.key).await?;

            // Stream of bytes in the segment
            let segment_byte_stream = StreamReader::new(segment_chunk_stream);

            // Stream of batches in the segment
            let mut batch_stream = BatchStream::new(segment_byte_stream);

            let mut min_offset: Option<RawOffset> = None;
            let mut max_offset: Option<RawOffset> = None;
            let mut min_ts: Option<Timestamp> = None;
            let mut max_ts: Option<Timestamp> = None;
            let mut compacted: bool = false;

            let base_offset_delta = offset_delta;

            while let Ok(batch) = batch_stream.read_batch_buffer().await {
                let base_offset: RawOffset = batch.header.base_offset as RawOffset;
                let committed_offset: RawOffset = batch.header.base_offset as RawOffset
                    + batch.header.last_offset_delta as RawOffset;

                // TODO: behavior on overlapping segments: when we see a batch that is <=
                // the highest batch already seen, we should either be dropping this segment,
                // or truncating the previous segment, or consulting `dropped_objects_ambiguous`

                // TODO; rules for other data types like Tx batches
                let is_data = batch.header.record_batch_type == RecordBatchType::RaftData as i8;

                debug!(
                    "Seg {:012x}-... Batch data={:?} {:012x}-{:012x}",
                    segment.base_offset, is_data, base_offset, committed_offset
                );

                if let Some(max_offset) = max_offset {
                    if batch.header.base_offset as i64 != max_offset + 1 {
                        // If we see a gap between batches, infer that this segment was compacted.
                        compacted = true;
                        // TODO: also set compacted if we see a gap between records: currently
                        // not doing this because we don't have ability to read inside
                        // compressed batches
                    }
                }

                if !is_data {
                    offset_delta += batch.header.record_count as i64;
                }

                let batch_max_ts = batch.header.max_timestamp as Timestamp;
                if min_ts.is_none() {
                    min_ts = Some(batch.header.first_timestamp as Timestamp);
                }
                if max_ts.is_none() || max_ts.unwrap() < batch_max_ts {
                    max_ts = Some(batch_max_ts);
                }

                if min_offset.is_none() {
                    min_offset = Some(base_offset);
                }
                if max_offset.is_none() || max_offset.unwrap() < committed_offset {
                    max_offset = Some(committed_offset);
                }
            }

            // TODO: validate behavior if last batch is a config batch: this is a sensitive
            // area in the logic for deltas
            let next_offset_delta = offset_delta;

            let pms = PartitionManifestSegment {
                base_offset: min_offset.unwrap(),
                committed_offset: max_offset.unwrap(),
                is_compacted: compacted,
                // TODO: cross ref segment's declared size with HEAD of object and sum of
                // batch sizes, and warn on discrepancies
                size_bytes: segment.size_bytes as i64,
                archiver_term: segment.upload_term,
                delta_offset: Some(base_offset_delta as u64),
                base_timestamp: Some(min_ts.unwrap()),
                max_timestamp: Some(max_ts.unwrap()),
                ntp_revision: Some(ntpr.revision_id as u64),
                sname_format: Some(SegmentNameFormat::V3 as u32),
                segment_term: Some(segment.original_term),
                delta_offset_end: Some(next_offset_delta as u64),
            };

            manifest.push(pms);
        }

        // If an original manifest is present, use it to correct our offset deltas: there
        // is no way to recover these from the raw segments alone.
        if let Some(pm) = bucket_reader.partition_manifests.get(ntpr) {
            if let Some(original_manifest) = &pm.head_manifest {
                info!(
                    "Trying to infer offset delta for {} from existing manifest...",
                    ntpr
                );
                let original_segments = &original_manifest.segments;
                let mut original_segments: Vec<&PartitionManifestSegment> = original_segments
                    .values()
                    .filter(|s| s.delta_offset.is_some())
                    .collect();
                original_segments.sort_by_key(|s| s.base_offset);

                let hint_segment = original_segments.get(0).unwrap();
                let delta_hint = (hint_segment.base_offset, hint_segment.delta_offset.unwrap());

                let mut delta_adjustment: Option<i64> = None;
                for segment in manifest.segments.values() {
                    if segment.base_offset == delta_hint.0 {
                        delta_adjustment =
                            Some(delta_hint.1 as i64 - segment.delta_offset.unwrap() as i64);
                        info!(
                            "Discovered partition {} delta {}",
                            ntpr,
                            delta_adjustment.unwrap()
                        );
                        break;
                    }
                }

                if let Some(delta_adjustment) = delta_adjustment {
                    for segment in manifest.segments.values_mut() {
                        *(segment.delta_offset.as_mut().unwrap()) += delta_adjustment as u64;
                        *(segment.delta_offset_end.as_mut().unwrap()) += delta_adjustment as u64;
                    }
                }
            }
        }

        // TODO: generalized URI-ish things so that callers can use object stores as sinks
        let sink_client = object_store::local::LocalFileSystem::new_with_prefix(sink)?;

        // Serialize manifest to sink
        let manifest_json = serde_json::to_vec_pretty(&manifest).unwrap();

        info!(
            "Rebuilt manifest for partition {} at {}",
            ntpr, manifest_path
        );

        // TODO: option to output V1 JSON manifests for fixing 23.1 and 22.3 clusters

        // Write manifest to sink bucket
        sink_client
            .put(
                &object_store::path::Path::from(manifest_path),
                Bytes::from(manifest_json),
            )
            .await?;
    }

    Ok(())
}

async fn make_bucket_reader(cli: &Cli, source: &str, meta_file: Option<&str>) -> BucketReader {
    let client = build_client(cli, source);
    if let Some(path) = meta_file {
        info!("Loading metadata from {}", path);
        BucketReader::from_file(path, client).await.unwrap()
    } else {
        info!("Scanning bucket...");
        let mut reader = BucketReader::new(client).await;
        reader.scan(&cli.filter).await.unwrap();
        reader
    }
}

/**
 * Safe scrubbing work: remove stray segment files not referenced by
 * any manifest
 */
async fn scrub(cli: &Cli, source: &str) {
    let client = build_client(cli, source);
    let mut reader = BucketReader::new(client).await;
    reader.scan(&cli.filter).await.unwrap();

    // TODO: drop out if there are any anomalies _other than_ segments outside the manifest

    if !reader.anomalies.segments_outside_manifest.is_empty() {
        if let AnomalyStatus::Corrupt = reader.anomalies.status() {
            // If there are corrupt manifests, then we cannot reliably say which objects
            // are referred to by a manifest: maybe some segments _are_ correct data, but
            // something went wrong reading the manifest, and we must not risk deleting
            // segments in this case.
            warn!(
                "Skipping erase of {} keys outside manifest, because bucket is corrupt",
                reader.anomalies.segments_outside_manifest.len()
            );
        } else {
            info!("Erasing keys outside manifests...");
            for key in reader.anomalies.segments_outside_manifest {
                match reader
                    .client
                    .delete(&object_store::path::Path::from(key.as_str()))
                    .await
                {
                    Ok(_) => {
                        info!("Deleted {}", key);
                    }
                    Err(e) => {
                        warn!("Failed to delete {}: {}", key, e);
                    }
                }
            }
        }
    }

    info!("Scrub complete.")
}

/**
 * Rewrite data like-for-like, re-segmenting as we go.
 */
async fn rewrite(cli: &Cli, source: &str, force: bool) -> Result<(), BucketReaderError> {
    let client = build_client(cli, source);

    let mut reader = BucketReader::new(client).await;
    reader.scan(&cli.filter).await?;

    match reader.anomalies.status() {
        AnomalyStatus::Clean => {
            info!("Scan of bucket {}:\n{}", source, reader.anomalies.report());
        }
        _ => {
            // Report on any unclean bucket contents.
            warn!(
                "Anomalies detected in bucket {}:\n{}",
                source,
                reader.anomalies.report()
            );
        }
    }

    // The scan process opportunistically detects any inconsistencies.  If we found any,
    // then pause ouf of abundance of caution.  We permit Dirty state because it is not
    // harmful, just indicates a few stray objects.
    if let AnomalyStatus::Corrupt = reader.anomalies.status() {
        if !force {
            warn!("Anomalies detected, not proceeding with operation.");
            return Ok(());
        }
    }

    // Okay, we know about all the segments and all the manifests, yey.
    for (ntpr, _) in &reader.partitions {
        let dir = std::path::PathBuf::from(format!(
            "/tmp/dump/{}/{}/{}_{}",
            ntpr.ntp.namespace, ntpr.ntp.topic, ntpr.ntp.partition_id, ntpr.revision_id
        ));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let mut writer = SegmentWriter::new(1024 * 1024, dir);

        info!("Reading partition {:?}", ntpr);
        let data_stream = reader.stream(ntpr, None);
        pin_mut!(data_stream);
        while let Some(segment_stream_struct) = data_stream.next().await {
            let (segment_stream_r, _segment_obj) = segment_stream_struct.into_parts();
            let segment_stream = segment_stream_r?;
            let byte_stream = StreamReader::new(segment_stream);
            let mut batch_stream = BatchStream::new(byte_stream);
            while let Ok(bb) = batch_stream.read_batch_buffer().await {
                info!(
                    "Read batch {} bytes, header {:?}",
                    bb.bytes.len(),
                    bb.header
                );
                writer.write_batch(&bb).await.unwrap();
            }
        }

        writer.flush_close().await.unwrap();
    }

    Ok(())
}

async fn compact(cli: &Cli, source: &str) {
    let client = build_client(cli, source);
    let mut reader = BucketReader::new(client).await;
    reader.scan(&cli.filter).await.unwrap();

    // TODO: generalize the "stop if corrupt" check and apply it here, as we do in rewrite

    for (ntpr, _) in &reader.partitions {
        // TODO: inspect topic manifest to see if it is meant to be compacted.
        if !cli.filter.match_ntpr(ntpr) {
            debug!("Skipping {}, doesn't match NTPR mask", ntpr);
            continue;
        }

        let dir = std::path::PathBuf::from(format!(
            "/tmp/dump/{}/{}/{}_{}",
            ntpr.ntp.namespace, ntpr.ntp.topic, ntpr.ntp.partition_id, ntpr.revision_id
        ));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let mut writer = SegmentWriter::new(1024 * 1024, dir);

        info!("Reading partition {:?}", ntpr);
        let data_stream = reader.stream(ntpr, None);
        pin_mut!(data_stream);
        while let Some(segment_stream_struct) = data_stream.next().await {
            let (segment_stream_r, _segment_obj) = segment_stream_struct.into_parts();
            let segment_stream = segment_stream_r.unwrap();

            info!("Compacting segment...");
            let mut dropped_count: u64 = 0;
            let mut retained_count: u64 = 0;

            // This is just basic self-compaction of each segment.
            let mut seen_keys: HashMap<Option<Vec<u8>>, u64> = HashMap::new();
            let tmp_file_path = "/tmp/compact.bin";
            let mut tmp_file = File::create(tmp_file_path).await.unwrap();
            let byte_stream = StreamReader::new(segment_stream);
            let mut batch_stream = BatchStream::new(byte_stream);
            while let Ok(bb) = batch_stream.read_batch_buffer().await {
                for r in bb.iter() {
                    // TODO: check the rules on compactino of records with no key
                    // TODO: only compact data records and/or do special rules for
                    // other record types, like dropping configuration batches and
                    // dropping aborted transactions.
                    seen_keys.insert(
                        r.key.map(|v| Vec::from(v)),
                        bb.header.base_offset + r.offset_delta as u64,
                    );
                }
                tmp_file.write_all(&bb.bytes).await.unwrap();
            }
            tmp_file.flush().await.unwrap();
            drop(tmp_file);
            drop(batch_stream);

            let mut reread_batch_stream =
                BatchStream::new(File::open(tmp_file_path).await.unwrap());
            while let Ok(bb) = reread_batch_stream.read_batch_buffer().await {
                let mut retain_records: Vec<RecordOwned> = vec![];
                for r in bb.iter() {
                    let offset = bb.header.base_offset + r.offset_delta as u64;
                    // Unwrap because we exhaustively populated seen_keys above
                    // TODO: efficiency: constructing a Vec for every key we check
                    if offset >= *seen_keys.get(&r.key.map(|v| Vec::from(v))).unwrap() {
                        retain_records.push(r.to_owned());
                        retained_count += 1;
                    } else {
                        dropped_count += 1;
                    }
                }
                // TODO: dont' reserialize if we didn't change anything
                let _new_batch = reserialize_batch(&bb.header, &retain_records);

                // TODO: hook into a streamwriter
            }

            info!(
                "Compacted segment, dropped {}, retained {}",
                dropped_count, retained_count
            );
        }
        writer.flush_close().await.unwrap();
    }
}

/// Construct an object store client based on the CLI flags
fn build_client(cli: &Cli, bucket: &str) -> Arc<dyn object_store::ObjectStore> {
    match cli.backend {
        Backend::AWS => {
            let mut client_builder = object_store::aws::AmazonS3Builder::from_env();
            client_builder = client_builder.with_bucket_name(bucket);
            Arc::new(client_builder.build().unwrap())
        }
        Backend::GCP => Arc::new(
            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()
                .unwrap(),
        ),
        Backend::Azure => {
            let client = object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_container_name(bucket)
                .build()
                .unwrap();
            Arc::new(client)
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let cli = Cli::parse();
    match &cli.command {
        Some(Commands::Rewrite { source, force }) => {
            rewrite(&cli, source, *force).await.unwrap();
        }
        Some(Commands::Scrub { source }) => {
            scrub(&cli, source).await;
        }
        Some(Commands::Compact { source }) => {
            compact(&cli, source).await;
        }
        Some(Commands::RebuildManifest {
            source,
            sink,
            meta_file,
        }) => rebuild_manifest(&cli, source, sink, meta_file.as_ref().map(|s| s.as_str()))
            .await
            .unwrap(),
        Some(Commands::FilterMetadata {
            source,
            meta_in,
            meta_out,
            inject_manifest_path,
        }) => {
            filter_metadata(
                &cli,
                source,
                meta_in,
                meta_out,
                inject_manifest_path.as_ref().map(|s| s.as_str()),
            )
            .await
            .unwrap();
        }
        None => {}
    }
}

/**
 * Stream design:
 * - The class that decodes batches needs to be cued on when a segment starts, because
 *   it will stop reading once it sees dead bytes at the end of an open segment.
 * - Downloads can fail partway through!  Whatever does the downloading needs to know
 *   how far it got writing into the batch decoder, so that it can pick up approxiastely
 *   where it left off.
 * - Same goes for uploads: if we fail partway through uploading a segment, we need
 *   to be able to rewind the reader/decoder (or restart from a known offset).
 */

async fn _dump_one(filename: &str) -> Result<(), DumpError> {
    info!("Reading {}", filename);

    let file = File::open(filename).await?;
    let _file_size = file.metadata().await?.len();
    let reader = BufReader::new(file);
    let mut stream = BatchStream::new(reader);

    loop {
        match stream.read_batch_buffer().await {
            Ok(bb) => {
                info!(
                    "Read batch {} bytes, header {:?}",
                    bb.bytes.len(),
                    bb.header
                );
            }
            Err(e) => {
                info!("Stream complete: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}
