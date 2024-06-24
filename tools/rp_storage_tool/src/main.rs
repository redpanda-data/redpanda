extern crate deltafor;
extern crate redpanda_records;

mod batch_crc;
mod batch_reader;
mod bucket_reader;
mod error;
mod fundamental;
mod ntp_mask;
mod remote_types;
mod repair;
mod util;
mod varint;

use log::{debug, error, info, trace, warn};
use remote_types::RpSerde;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::bucket_reader::{AnomalyStatus, BucketReader, MetadataGap, PartitionObjects};
use crate::error::BucketReaderError;
use crate::fundamental::{
    raw_to_kafka, KafkaOffset, LabeledNTPR, LabeledNTR, RaftTerm, RawOffset, NTPR, NTR,
};
use crate::ntp_mask::NTPFilter;
use crate::remote_types::{
    segment_shortname, LifecycleMarker, PartitionManifest, PartitionManifestSegment, RemoteLabel,
    TopicManifest,
};
use crate::repair::{
    DataAddNullSegment, ManifestEditAlterSegment, ManifestSegmentDiff, RepairEdit,
};
use batch_reader::BatchStream;
use clap::{Parser, Subcommand};
use futures::pin_mut;
use futures::StreamExt;
use object_store::ObjectStore;
use redpanda_records::RecordBatchType;
use serde::Serialize;
use tokio_util::io::StreamReader;

/// Parser for use with `clap` argument parsing
pub fn ntp_filter_parser(input: &str) -> Result<NTPFilter, String> {
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

    #[arg(short, long, value_parser = ntp_filter_parser, default_value_t = NTPFilter::match_all())]
    filter: NTPFilter,
}

#[derive(Subcommand)]
enum Commands {
    ScanMetadata {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        meta_file: Option<String>,
    },
    AnalyzeMetadata {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        meta_file: String,
    },
    ScanData {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        meta_file: Option<String>,
        #[arg(short, long)]
        start_offset: Option<RawOffset>,
        #[arg(short, long)]
        max_offset: Option<RawOffset>,
    },
    ScanGaps {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        meta_file: Option<String>,
    },
    DecodePartitionManifest {
        #[arg(short, long)]
        path: String,
    },
    DecodeLifecycleMarker {
        #[arg(short, long)]
        path: String,
    },
    Extract {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        sink: String,
        #[arg(short, long)]
        meta_file: Option<String>,
        #[arg(short, long, default_value_t = false)]
        metadata_only: bool,
    },
}

/// Construct an object store client based on the CLI flags
fn build_client(
    cli: &Cli,
    bucket: &str,
) -> Result<Arc<dyn object_store::ObjectStore>, object_store::Error> {
    let c: Arc<dyn object_store::ObjectStore> = match cli.backend {
        Backend::AWS => {
            let mut client_builder = object_store::aws::AmazonS3Builder::from_env();
            client_builder = client_builder.with_bucket_name(bucket);
            Arc::new(client_builder.build()?)
        }
        Backend::GCP => Arc::new(
            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()?,
        ),
        Backend::Azure => {
            let client = object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_container_name(bucket)
                .build()
                .unwrap();
            Arc::new(client)
        }
    };

    Ok(c)
}

/// If a meta_file is provided, the resulting BucketReader will _not_ have its anomalies
/// populated (you must call analyze_metadata() if you want that).  If a meta_file is not
/// provided, the BucketReader will scan the bucket and also popualate its anomalies.
async fn make_bucket_reader(
    cli: &Cli,
    source: &str,
    meta_file: Option<&str>,
) -> Result<BucketReader, BucketReaderError> {
    let client = build_client(cli, source)?;
    if let Some(path) = meta_file {
        info!("Loading metadata for {} from {}", source, path);
        let mut reader = BucketReader::from_file(path, client).await?;
        reader.filter(&cli.filter);
        Ok(reader)
    } else {
        info!("Scanning bucket {}...", source);
        let mut reader = BucketReader::new(client).await;
        reader.scan(&cli.filter).await?;

        // Whether the caller wanted the analysis or not, this has the side effect of tidying
        // up the list of partition objects to match the manifest if there are multiple
        // objects per base offset
        reader.analyze_metadata(&cli.filter).await?;

        Ok(reader)
    }
}

#[derive(Serialize)]
pub struct NTPDataScanResult {
    /// Counters from scan
    pub records: u64,
    pub batches: u64,
    pub bytes: u64,

    /// Manifest for NTP does not exist
    pub metadata_missing: bool,

    /// How many offsets are present in segments not yet
    /// in manifest.  A nonzero value here is normal: it indicates
    /// that the topic was being written to at the time we scanned.
    pub metadata_lag: u64,

    /// Segments without entries in the manifest
    pub segments_without_metadata: Vec<String>,

    /// One or more segments has disagreement between data and metadata
    /// for Kafka offsets
    pub kafka_offset_mismatch: bool,

    /// Apparent data overlap (we saw raw offset jump backwards)
    pub overlap_offsets: bool,

    /// Apparent data loss (we saw raw offset jump forwards)
    pub missing_offsets: bool,

    /// Segment contains more records than the manifest claims it should
    /// (a nice problem to have, compared with it being too small)
    pub segment_oversized: bool,

    /// Segment contained fewer records than the manifest said it should
    pub segment_undersized: bool,

    /// Segments which are not found in the manifest, but it is tolerable because
    /// they are prior to the start of the manifest (i.e retention has removed them)
    pub segments_before_metadata: Vec<String>,

    /// Segments which are not found in the manifest, but it is tolerable because
    /// they are ahead of the end of the manifest (i.e. the manifest is pending update)
    pub segments_after_metadata: Vec<String>,

    /// Were any segment compacted?
    pub compaction: bool,

    /// Were any transaction batches seen?
    pub transactions: bool,

    /// Proposed changes to make the partition readable: scan will never make changes itself
    pub proposed_repairs: Vec<RepairEdit>,
}

#[derive(Serialize)]
pub struct DataScanTopicSummary {
    pub size_bytes: u64,
    pub size_batches: u64,
    pub size_records: u64,
    pub compaction: bool,
    pub transactions: bool,
    pub damaged: bool,
    pub data_loss: bool,
}

impl DataScanTopicSummary {
    fn new() -> Self {
        Self {
            size_bytes: 0,
            size_batches: 0,
            size_records: 0,
            compaction: false,
            transactions: false,
            damaged: false,
            data_loss: false,
        }
    }
}

#[derive(Serialize)]
pub struct DataScanReport {
    ntps: BTreeMap<LabeledNTPR, NTPDataScanResult>,
    summary: BTreeMap<LabeledNTR, DataScanTopicSummary>,
}

impl NTPDataScanResult {
    fn new() -> Self {
        Self {
            records: 0,
            batches: 0,
            bytes: 0,
            metadata_missing: false,
            metadata_lag: 0,
            segments_without_metadata: vec![],
            segments_before_metadata: vec![],
            segments_after_metadata: vec![],
            missing_offsets: false,
            overlap_offsets: false,
            kafka_offset_mismatch: false,
            segment_oversized: false,
            segment_undersized: false,
            compaction: false,
            transactions: false,
            proposed_repairs: vec![],
        }
    }

    fn damaged(&self) -> bool {
        if self.metadata_missing {
            return true;
        }

        if !self.segments_without_metadata.is_empty() {
            return true;
        }

        if self.overlap_offsets
            || self.kafka_offset_mismatch
            || self.missing_offsets
            || self.segment_oversized
            || self.segment_undersized
        {
            return true;
        }

        if !self.proposed_repairs.is_empty() {
            return true;
        }

        false
    }

    fn data_loss(&self) -> bool {
        if self.missing_offsets {
            return true;
        }

        false
    }
}

async fn seek(
    ntpr: &LabeledNTPR,
    objects: &PartitionObjects,
    manifest: &PartitionManifest,
    bounds: (RawOffset, RawOffset),
) -> Option<(RawOffset, KafkaOffset)> {
    let (start_offset, end_offset) = bounds;
    // Look up first segment before start offset
    let mut range = objects.segment_objects.range(start_offset..=end_offset);
    if let Some((seg_base_offset, seg)) = range.next() {
        // This segment will be where we start streaming data from
        if let Some(manifest_seg) = manifest.get_segment(*seg_base_offset, seg.original_term) {
            if let Some(manifest_seg_delta) = manifest_seg.delta_offset {
                let raw_offset = *seg_base_offset as RawOffset;
                let kafka_offset =
                    (seg_base_offset - manifest_seg_delta as RawOffset) as KafkaOffset;
                Some((raw_offset, kafka_offset))
            } else {
                warn!(
                    "[{}] Cannot seek, legacy manifest with no delta offset at {}",
                    ntpr, seg_base_offset
                );
                None
            }
        } else {
            warn!(
                "[{}] Cannot seek, no segment at {} in manifest",
                ntpr, seg_base_offset
            );
            None
        }
    } else {
        warn!(
            "[{}] Cannot seek, no segment found ahead of {}",
            ntpr, start_offset
        );
        None
    }
}

async fn scan_data_ntp(
    ntpr: &LabeledNTPR,
    objects: &PartitionObjects,
    bucket_reader: &BucketReader,
    bounds: Option<(RawOffset, RawOffset)>,
) -> Result<NTPDataScanResult, BucketReaderError> {
    let mut ntp_report = NTPDataScanResult::new();

    let metadata_opt = bucket_reader.partition_manifests.get(ntpr);
    let manifest_opt = if let Some(metadata) = metadata_opt {
        if let Some(manifest) = &metadata.head_manifest {
            Some(manifest)
        } else {
            warn!("No head manifest found for NTP {}", ntpr);
            ntp_report.metadata_missing = true;
            None
        }
    } else {
        warn!("No metadata found for NTP {}", ntpr);
        ntp_report.metadata_missing = true;
        None
    };

    let (mut raw_offset, mut kafka_offset) = if let Some(manifest) = manifest_opt {
        if let Some(bounds) = bounds {
            seek(ntpr, objects, manifest, bounds)
                .await
                .unwrap_or_else(|| {
                    warn!("[{}] Failed to seek, starting from beginning", ntpr);
                    manifest.start_offsets()
                })
        } else {
            manifest.start_offsets()
        }
    } else {
        warn!("[{}] No manifest, cannot seek", ntpr);
        (0 as RawOffset, 0 as KafkaOffset)
    };

    let max_offset = bounds.map(|b| b.1).unwrap_or(RawOffset::MAX);

    let mut offset_delta = raw_offset as u64 - kafka_offset as u64;
    info!(
        "[{}] Reconciling data & metadata, starting at raw={} kafka={}",
        ntpr, raw_offset, kafka_offset
    );

    let mut estimate_ntp_size = 0;
    for o in objects.segment_objects.values() {
        estimate_ntp_size += o.size_bytes;
    }

    let status_interval = std::time::Duration::from_secs(10);
    let mut last_status = std::time::SystemTime::now();

    let data_stream = bucket_reader.stream(ntpr, Some(raw_offset));
    pin_mut!(data_stream);
    let mut prev_segment_meta: Option<PartitionManifestSegment> = None;
    while let Some(segment_stream_struct) = data_stream.next().await {
        let (segment_stream_data_r, segment_obj) = segment_stream_struct.into_parts();

        let segment_stream_data = match segment_stream_data_r {
            Ok(stream) => stream,
            Err(e) => {
                warn!("Cannot read segment {}: {}", segment_obj.key, e);
                // TODO: gracefully handle 404s by reloading manifest and checking again
                continue;
            }
        };

        let byte_stream = StreamReader::new(segment_stream_data);

        let mut current_segment_meta: Option<PartitionManifestSegment> = None;

        // Start of segment, compare offset with manifest
        let meta_seg_opt = manifest_opt
            .map(|m| m.get_segment(segment_obj.base_offset, segment_obj.original_term))
            .unwrap_or(None);
        if let Some(meta_seg) = meta_seg_opt {
            if let Some(seg_meta_delta) = meta_seg.delta_offset {
                let seg_meta_kafka_base = raw_to_kafka(meta_seg.base_offset, seg_meta_delta);
                if seg_meta_kafka_base != kafka_offset {
                    warn!("[{}] Offset translation issue!  At offset {}, but segment meta says {} (segment {})",
                            ntpr, kafka_offset, seg_meta_kafka_base, segment_obj.key
                        );
                    ntp_report.kafka_offset_mismatch = true;

                    // Take the data as authoritative and correct the metadata
                    let alteration = ManifestEditAlterSegment {
                        old_key: segment_shortname(
                            meta_seg.base_offset as RawOffset,
                            meta_seg.segment_term.unwrap() as RaftTerm,
                        ),
                        new_key: segment_shortname(
                            meta_seg.base_offset as RawOffset,
                            meta_seg.segment_term.unwrap() as RaftTerm,
                        ),
                        diff: ManifestSegmentDiff {
                            delta_offset: Some((raw_offset - kafka_offset) as u64),
                            delta_offset_end: None,
                            base_offset: None,
                            committed_offset: None,
                            segment_term: None,
                            archiver_term: None,
                            size_bytes: None,
                        },
                    };
                    ntp_report
                        .proposed_repairs
                        .push(RepairEdit::AlterSegment(alteration));
                }
            }

            if meta_seg.is_compacted {
                ntp_report.compaction = true;
            }

            current_segment_meta = Some(meta_seg.clone());
        } else {
            if let Some(manifest) = manifest_opt {
                let mut tolerate = false;
                if let Some(manifest_start_offset) = manifest.start_offset {
                    if segment_obj.base_offset < manifest_start_offset {
                        info!(
                            "[{}] Segment before metadata start ({} < {}): {}",
                            ntpr, segment_obj.base_offset, manifest_start_offset, segment_obj.key
                        );
                        ntp_report
                            .segments_before_metadata
                            .push(segment_obj.key.clone());
                        tolerate = true;
                    }
                }

                if segment_obj.base_offset > manifest.last_offset {
                    info!(
                        "[{}] Segment after metadata end ({} > {}): {}",
                        ntpr, segment_obj.base_offset, manifest.last_offset, segment_obj.key
                    );
                    ntp_report
                        .segments_after_metadata
                        .push(segment_obj.key.clone());
                    tolerate = true;
                }

                if !tolerate {
                    warn!("[{}] Segment not in manifest: {}", ntpr, segment_obj.key);
                    ntp_report
                        .segments_without_metadata
                        .push(segment_obj.key.clone());
                } else {
                    // There is no manifest, we already logged metadata_missing, so no
                    // need to remind for each segment that we cannot find the metadata
                }
            }
        }

        let mut batch_stream = BatchStream::new(byte_stream);
        while let Ok(bb) = batch_stream.read_batch_buffer().await {
            if (last_status.elapsed().unwrap()) > status_interval {
                info!(
                    "[{}] Scanning... offset {} {}/{}MiB",
                    ntpr,
                    raw_offset,
                    ntp_report.bytes / (1024 * 1024),
                    estimate_ntp_size / (1024 * 1024)
                );
                last_status = std::time::SystemTime::now();
            }

            ntp_report.batches += 1;
            ntp_report.bytes += bb.header.size_bytes as u64;

            if bb.header.record_batch_type == RecordBatchType::TxPrepare as i8
                || bb.header.record_batch_type == RecordBatchType::TxFence as i8
            {
                ntp_report.transactions = true;
            }

            if raw_offset > bb.header.base_offset as RawOffset {
                let header_base_offset = bb.header.base_offset;
                warn!(
                    "[{}] Offset went backward {} -> {} in {}",
                    ntpr, raw_offset, header_base_offset, segment_obj.key
                );
                ntp_report.overlap_offsets = true;
                raw_offset = bb.header.base_offset as RawOffset;
                kafka_offset = raw_offset - offset_delta as RawOffset;
            } else {
                if ntp_report.compaction {
                    // Compaction: tolerate gaps
                    raw_offset = bb.header.base_offset as RawOffset;
                    kafka_offset = raw_offset - offset_delta as RawOffset;
                } else {
                    // We expect offsets to be contiguous, flag if they are not
                    if raw_offset != bb.header.base_offset as RawOffset {
                        let header_base_offset = bb.header.base_offset;
                        warn!(
                            "[{}] Skipped offsets ({} -> {})",
                            ntpr, raw_offset, header_base_offset
                        );

                        // Propose to fill the gap by creating a segment full of no-op batches
                        let gap_begin = raw_offset;
                        let gap_end = (header_base_offset - 1) as RawOffset;
                        // Is this a "clean" inter-segment gap?
                        if let Some(prev_seg) = &prev_segment_meta {
                            if let Some(curr_seg) = &current_segment_meta {
                                if gap_begin == prev_seg.committed_offset as RawOffset + 1
                                    && gap_end == curr_seg.base_offset as RawOffset - 1
                                {
                                    info!(
                                        "[{}] Detected clean data gap betwen segments {} and {}",
                                        ntpr, prev_seg.base_offset, curr_seg.base_offset
                                    );
                                    let non_data_records = curr_seg.delta_offset.unwrap_or(0)
                                        as i64
                                        - prev_seg.delta_offset.unwrap_or(0) as i64;
                                    let data_records =
                                        (gap_end - gap_begin) + 1 - non_data_records as i64;
                                    if data_records < 0 || non_data_records < 0 {
                                        warn!("[{}] Cannot infer record counts for missing segment, will not repair", ntpr);
                                    } else {
                                        let null_seg = PartitionManifestSegment {
                                            base_offset: gap_begin,
                                            committed_offset: gap_end,
                                            is_compacted: false,
                                            size_bytes: 0, // Rely on whatever injects the
                                            // segment to fix this up
                                            delta_offset: Some(offset_delta as u64),
                                            delta_offset_end: curr_seg.delta_offset,
                                            max_timestamp: curr_seg.base_timestamp,
                                            base_timestamp: prev_seg.max_timestamp,
                                            ntp_revision: Some(ntpr.ntpr.revision_id as u64),
                                            sname_format: curr_seg.sname_format,
                                            segment_term: curr_seg.segment_term,
                                            archiver_term: curr_seg.archiver_term,
                                        };

                                        let key = segment_shortname(
                                            null_seg.base_offset as RawOffset,
                                            // FIXME: this will barf on ancient metadata
                                            null_seg.segment_term.unwrap() as RaftTerm,
                                        );

                                        // Unwrap safe because we know we have current_segment_meta
                                        let remote_label = RemoteLabel::from_string(&ntpr.label);
                                        let object_key = manifest_opt
                                            .unwrap()
                                            .segment_key(&null_seg, &remote_label)
                                            .unwrap();

                                        ntp_report.proposed_repairs.push(
                                            RepairEdit::AddNullSegment(DataAddNullSegment {
                                                key,
                                                object_key,
                                                body: null_seg,
                                                data_records: data_records as u64,
                                                non_data_records: non_data_records as u64,
                                            }),
                                        );
                                    }
                                }
                            }
                        }

                        ntp_report.missing_offsets = true;
                        raw_offset = header_base_offset as RawOffset;
                        kafka_offset = raw_offset - offset_delta as RawOffset;
                    } else {
                    }
                }
            };

            trace!("[{}] Batch {}", ntpr, bb.header);
            if !bb.header.is_compressed() {
                // TODO: decompression
                for record in bb.iter() {
                    ntp_report.records += 1;
                    trace!(
                        "[{}] Record o={} s={}",
                        ntpr,
                        bb.header.base_offset + record.offset_delta as u64,
                        record.len
                    );
                }
            } else {
                ntp_report.records += bb.header.record_count as u64;
            }

            if !bb.header.is_kafka_data() {
                offset_delta += bb.header.record_count as u64;
            } else {
                kafka_offset += bb.header.record_count as KafkaOffset;
            }
            raw_offset += bb.header.record_count as RawOffset;
            assert_eq!(kafka_offset, raw_offset - offset_delta as RawOffset);
        }

        // End of segment, compare offset with manifest
        if let Some(meta_seg) = meta_seg_opt {
            if raw_offset - 1 > meta_seg.committed_offset as RawOffset {
                warn!(
                    "[{}] Data overruns metadata {} > {} in segment {}",
                    ntpr,
                    raw_offset - 1,
                    meta_seg.committed_offset,
                    segment_obj.key
                );
                ntp_report.segment_oversized = true;

                let key = segment_shortname(
                    meta_seg.base_offset as RawOffset,
                    // FIXME: this will barf on ancient metadata
                    meta_seg.segment_term.unwrap() as RaftTerm,
                );

                ntp_report.proposed_repairs.push(RepairEdit::AlterSegment(
                    ManifestEditAlterSegment {
                        old_key: key.clone(),
                        new_key: key,
                        diff: ManifestSegmentDiff {
                            delta_offset: None,
                            delta_offset_end: None,
                            base_offset: None,
                            committed_offset: Some(raw_offset - 1),
                            segment_term: None,
                            archiver_term: None,
                            // TODO: worth checking size_bytes if offset count was wrong
                            size_bytes: None,
                        },
                    },
                ));
            } else if raw_offset - 1 < meta_seg.committed_offset as RawOffset {
                warn!(
                    "[{}] Data underruns metadata {} < {} in segment {}",
                    ntpr,
                    raw_offset - 1,
                    meta_seg.committed_offset,
                    segment_obj.key
                );
                ntp_report.segment_undersized = true;
            }

            if let Some(seg_meta_delta_end) = meta_seg.delta_offset_end {
                if seg_meta_delta_end != offset_delta {
                    warn!(
                        "[{}] Bad delta end {} != {} in {}",
                        ntpr, seg_meta_delta_end, offset_delta, segment_obj.key
                    );
                    ntp_report.kafka_offset_mismatch = true;

                    let key = segment_shortname(
                        meta_seg.base_offset as RawOffset,
                        meta_seg.segment_term.unwrap() as RaftTerm,
                    );

                    // Take the data as authoritative and correct the metadata
                    ntp_report.proposed_repairs.push(RepairEdit::AlterSegment(
                        ManifestEditAlterSegment {
                            old_key: key.clone(),
                            new_key: key,
                            diff: ManifestSegmentDiff {
                                delta_offset: None,
                                delta_offset_end: Some(offset_delta),
                                base_offset: None,
                                committed_offset: None,
                                segment_term: None,
                                archiver_term: None,
                                size_bytes: None,
                            },
                        },
                    ));
                }
            }
        }

        if raw_offset >= max_offset {
            info!(
                "[{}] Reached max offset {} >= {}, stopping scan",
                ntpr, raw_offset, max_offset
            );
            break;
        }

        prev_segment_meta = current_segment_meta;
    }

    info!(
        "[{}] Scanned {} records, HWM raw={} kafka={}",
        ntpr, ntp_report.records, raw_offset, kafka_offset
    );

    Ok(ntp_report)
}

/**
 * Walk the data in NTPs matching filter, compare with metadata
 * report anomalies.
 */
async fn scan_data(
    cli: &Cli,
    source: &str,
    meta_file: Option<&str>,
    bounds: Option<(RawOffset, RawOffset)>,
) -> Result<(), BucketReaderError> {
    let mut bucket_reader = make_bucket_reader(cli, source, meta_file).await?;

    // Run metadata analysis to get metadata gap detection
    bucket_reader.analyze_metadata(&cli.filter).await?;

    // TODO: wire up the batch/record read to consider any EOFs etc as errors
    // when reading from S3, and set failed=true here

    let mut report: BTreeMap<LabeledNTPR, NTPDataScanResult> = BTreeMap::new();

    let ntprs: Vec<LabeledNTPR> = bucket_reader
        .partitions
        .keys()
        .filter(|k| cli.filter.match_lntpr(k))
        .map(|k| k.clone())
        .collect();

    for ntpr in ntprs {
        let gaps = bucket_reader
            .anomalies
            .metadata_offset_gaps
            .get(&ntpr)
            .map(|v| v.clone())
            .unwrap_or(Vec::new());

        let initial_repairs = bucket_reader.repair_manifest_ntp(&gaps, &ntpr).await?;

        let objects = match bucket_reader.partitions.get(&ntpr) {
            Some(o) => o,
            None => {
                // We can't scan data if there isn't any
                continue;
            }
        };

        let mut ntp_report = scan_data_ntp(&ntpr, objects, &bucket_reader, bounds).await?;
        ntp_report.proposed_repairs.extend(initial_repairs);

        report.insert(ntpr.clone(), ntp_report);
    }

    // TODO: validate index objects
    // TODO: validate tx manifest objects

    let mut topic_summaries: BTreeMap<LabeledNTR, DataScanTopicSummary> = BTreeMap::new();
    for (ntpr, ntp_report) in &report {
        let ntr = ntpr.to_ntr();
        if !topic_summaries.contains_key(&ntr) {
            topic_summaries.insert(ntr.clone(), DataScanTopicSummary::new());
        }

        let mut topic_summary = topic_summaries.get_mut(&ntr).unwrap();

        topic_summary.size_bytes += ntp_report.bytes;
        topic_summary.size_batches += ntp_report.batches;
        topic_summary.size_records += ntp_report.records;

        topic_summary.compaction = topic_summary.compaction || ntp_report.compaction;
        topic_summary.transactions = topic_summary.transactions || ntp_report.transactions;
        topic_summary.damaged = topic_summary.damaged || ntp_report.damaged();
        topic_summary.data_loss = topic_summary.data_loss || ntp_report.data_loss();
    }

    let report = DataScanReport {
        summary: topic_summaries,
        ntps: report,
    };

    println!("{}", serde_json::to_string_pretty(&report).unwrap());

    Ok(())
}

/// This is a specialized routine for historical bugs that would leave offset gaps
/// in metadata, which may or may not correspond to actual gaps in the underlying data.
async fn scan_gaps(
    cli: &Cli,
    source: &str,
    meta_file: Option<&str>,
) -> Result<(), BucketReaderError> {
    let mut reader = make_bucket_reader(cli, source, meta_file).await?;

    reader.analyze_metadata(&cli.filter).await?;

    #[derive(Serialize)]
    struct GapScanPartitionReport {
        repairs: Vec<RepairEdit>,
        gap_scans: Vec<GapScan>,
    }

    #[derive(Serialize)]
    struct GapScan {
        gap: MetadataGap,
        scan_result: NTPDataScanResult,
    }

    let mut results: HashMap<LabeledNTPR, GapScanPartitionReport> = HashMap::new();

    let mut offset_gaps = HashMap::new();
    std::mem::swap(&mut offset_gaps, &mut reader.anomalies.metadata_offset_gaps);

    for (ntpr, gaps) in offset_gaps {
        let initial_repairs = reader.repair_manifest_ntp(&gaps, &ntpr).await?;

        let objects = if let Some(o) = reader.partitions.get(&ntpr) {
            o
        } else {
            // This shouldn't happen: a partition with no objects would not have found
            // a gap (although in extremis, if there were no data objects at all in the bucket
            // but a buggy manifest, this could happen)
            warn!("[{}] Unexpected: no objects for ntpr reporting gap", ntpr);
            continue;
        };

        let mut partition_report = GapScanPartitionReport {
            repairs: initial_repairs,
            gap_scans: vec![],
        };

        for gap in gaps {
            let bounds = if let Some(next_seg) = objects.segment_objects.get(&gap.next_seg_base) {
                Some((gap.prev_seg_base, next_seg.base_offset + 1))
            } else {
                warn!(
                    "[{}] Unexpected: next seg after gap {} not found",
                    ntpr, gap.next_seg_base
                );
                continue;
            };
            let ntp_scan_result = scan_data_ntp(&ntpr, objects, &reader, bounds).await?;
            partition_report.gap_scans.push(GapScan {
                gap,
                scan_result: ntp_scan_result,
            });
        }

        results.insert(ntpr.clone(), partition_report);
    }

    println!("{}", serde_json::to_string_pretty(&results).unwrap());

    Ok(())
}

/// Return true if corruption is found
fn report_anomalies(source: &str, reader: BucketReader) -> bool {
    let summary = reader.get_summary();
    let mut failed = false;
    match summary.anomalies.status() {
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

            failed = true;
        }
    }

    println!("{}", serde_json::to_string_pretty(&summary).unwrap());

    return failed;
}

/**
 * Brute-force listing of bucket, read-only scan of metadata,
 * report anomalies.
 */
async fn scan_metadata(
    cli: &Cli,
    source: &str,
    meta_file: Option<&str>,
) -> Result<(), BucketReaderError> {
    let reader = make_bucket_reader(cli, source, None).await?;

    if let Some(out_file) = meta_file {
        reader.to_file(out_file).await.unwrap();
    }

    let failed = report_anomalies(source, reader);

    if failed {
        error!("Issues detected in bucket");
        std::process::exit(-1);
    } else {
        Ok(())
    }
}

async fn analyze_metadata(
    cli: &Cli,
    source: &str,
    meta_file: &str,
) -> Result<(), BucketReaderError> {
    let mut reader = make_bucket_reader(cli, source, Some(meta_file)).await?;
    reader.analyze_metadata(&cli.filter).await?;
    report_anomalies(source, reader);
    Ok(())
}

async fn extract(
    cli: &Cli,
    source: &str,
    sink: &str,
    meta_file: Option<&str>,
    metadata_only: bool,
) -> Result<(), BucketReaderError> {
    let bucket_reader = make_bucket_reader(cli, source, meta_file).await?;

    // TODO: generalized URI-ish things so that callers can use object stores as sinks
    let sink_client = object_store::local::LocalFileSystem::new_with_prefix(sink)?;

    for (ntpr, _objects) in bucket_reader.partitions.iter() {
        if !cli.filter.match_lntpr(ntpr) {
            // If metadata was loaded from a file, it might not be filtered
            // in a way that lines up with cli.filter: re-filter so that one
            // can have a monolithic metadata file but extract individual partitions
            // on demand
            continue;
        }

        let manifest_paths: Vec<object_store::path::Path> = vec!["bin", "json"]
            .iter()
            .map(|e| PartitionManifest::manifest_key(&ntpr, e))
            .map(|s| object_store::path::Path::from(s))
            .collect();

        for path in &manifest_paths {
            debug!("Trying to download manifest {}", path);
            match bucket_reader.client.get(path).await {
                Ok(get_result) => {
                    let bytes = get_result.bytes().await?;
                    sink_client.put(path, bytes).await?;
                    info!("Downloaded manifest {}", path);
                }
                Err(e) => {
                    match e {
                        object_store::Error::NotFound { path: _, source: _ } => {
                            // Normal that one or other of the manifest paths is missing
                            debug!("Didn't fetch {}: {}", path, e);
                        }
                        _ => {
                            warn!("Unexpected error fetching {}: {}", path, e);
                            return Err(e.into());
                        }
                    }
                }
            };
        }
    }

    for (ntr, _tp_man) in &bucket_reader.topic_manifests {
        if !cli.filter.match_lntr(ntr) {
            continue;
        }

        let path = object_store::path::Path::from(TopicManifest::manifest_key(
            &ntr.ntr.namespace,
            &ntr.ntr.topic,
        ));
        let get_r = bucket_reader.client.get(&path).await?;
        let bytes = get_r.bytes().await?;
        sink_client.put(&path, bytes).await?;
    }

    if !metadata_only {
        for (ntpr, objects) in bucket_reader.partitions.iter() {
            if !cli.filter.match_lntpr(ntpr) {
                continue;
            }

            for key in objects.all_keys() {
                info!("Copying {}", key);
                // TODO; make bucket reader return an object_store::Error?
                let mut stream = bucket_reader.stream_one(&key).await.unwrap();

                let (_, mut sink_stream) = sink_client
                    .put_multipart(&object_store::path::Path::from(key.as_str()))
                    .await?;

                while let Some(chunk) = stream.next().await {
                    sink_stream.write(chunk.unwrap().as_ref()).await?;
                }
                sink_stream.shutdown().await?;
            }
        }
    } else {
        info!("Extract: skipping data objects for metadata-only mode");
    }
    Ok(())
}

async fn decode_partition_manifest(path: &str) {
    let mut f = tokio::fs::File::open(path).await.unwrap();
    let mut buf: Vec<u8> = vec![];
    f.read_to_end(&mut buf).await.unwrap();

    let manifest = PartitionManifest::from_bytes(bytes::Bytes::from(buf)).unwrap();
    serde_json::to_writer_pretty(&::std::io::stdout(), &manifest).unwrap();
}

async fn decode_lifecycle_marker(path: &str) {
    let mut f = tokio::fs::File::open(path).await.unwrap();
    let mut buf: Vec<u8> = vec![];
    f.read_to_end(&mut buf).await.unwrap();
    let mut cursor = std::io::Cursor::new(buf.as_slice());
    let lm = LifecycleMarker::from_bytes(&mut cursor).unwrap();
    serde_json::to_writer_pretty(&::std::io::stdout(), &lm).unwrap();
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let cli = Cli::parse();
    match &cli.command {
        Some(Commands::ScanData {
            source,
            meta_file,
            start_offset,
            max_offset,
        }) => {
            let bounds = if let Some(start) = *start_offset {
                Some((start, (*max_offset).unwrap_or(RawOffset::MAX)))
            } else {
                None
            };
            let r = scan_data(&cli, source, meta_file.as_ref().map(|s| s.as_str()), bounds).await;
            if let Err(e) = r {
                error!("Error: {:?}", e);
                std::process::exit(-1);
            }
        }
        Some(Commands::ScanGaps { source, meta_file }) => {
            let r = scan_gaps(&cli, source, meta_file.as_ref().map(|s| s.as_str())).await;
            if let Err(e) = r {
                error!("Error: {:?}", e);
                std::process::exit(-1);
            }
        }
        Some(Commands::ScanMetadata { source, meta_file }) => {
            let r = scan_metadata(&cli, source, meta_file.as_ref().map(|s| s.as_str())).await;
            if let Err(e) = r {
                error!("Error: {:?}", e);
                std::process::exit(-1);
            }
        }
        Some(Commands::AnalyzeMetadata { source, meta_file }) => {
            let r = analyze_metadata(&cli, source, meta_file).await;
            if let Err(e) = r {
                error!("Error: {:?}", e);
                std::process::exit(-1);
            }
        }
        Some(Commands::Extract {
            source,
            sink,
            meta_file,
            metadata_only,
        }) => {
            let r = extract(
                &cli,
                source,
                sink,
                meta_file.as_ref().map(|s| s.as_str()),
                *metadata_only,
            )
            .await;

            if let Err(e) = r {
                error!("Error: {:?}", e);
                std::process::exit(-1);
            }
        }
        Some(Commands::DecodePartitionManifest { path }) => {
            decode_partition_manifest(path).await;
        }
        Some(Commands::DecodeLifecycleMarker { path }) => {
            decode_lifecycle_marker(path).await;
        }

        None => {}
    }
}
