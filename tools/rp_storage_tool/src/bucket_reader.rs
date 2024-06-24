use crate::batch_reader::BatchStream;
use crate::error::BucketReaderError;
use crate::fundamental::{
    raw_to_kafka, KafkaOffset, LabeledNTPR, LabeledNTR, RaftTerm, RawOffset, Timestamp, NTP, NTPR,
    NTR,
};
use crate::ntp_mask::NTPFilter;
use crate::remote_types::{
    parse_segment_shortname, ArchivePartitionManifest, ClusterMetadataManifest, LifecycleMarker,
    LifecycleStatus, PartitionManifest, PartitionManifestSegment, RemoteLabel, RpSerde,
    TopicManifest,
};
use crate::repair::{maybe_adjust_manifest, project_repairs, RepairEdit};
use async_stream::stream;
use chrono::Utc;
use futures::stream::{BoxStream, Stream};
use futures::{pin_mut, StreamExt};
use lazy_static::lazy_static;
use log::{debug, info, warn};
use object_store::{GetResult, ObjectMeta, ObjectStore};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamMap;
use tokio_util::io::StreamReader;

/// A segment object
#[derive(Clone, Serialize, Deserialize)]
pub struct SegmentObject {
    pub key: String,
    pub base_offset: RawOffset,
    pub upload_term: RaftTerm,
    pub original_term: RaftTerm,
    pub size_bytes: u64,
}

/// Ground truth about a segment object obtained by fully reading it.  This
/// may be different from what the manifest metadata claims about the object
pub struct SegmentDataSummary {
    pub size_bytes: u64,
    pub base_offset: RawOffset,
    pub committed_offset: RawOffset,
    pub delta_end: u64,
}

/// The raw objects for a NTP, discovered by scanning the bucket: this is distinct
/// from the partition manifest, but on a health system the two should be very similar.
#[derive(Clone, Serialize, Deserialize)]
pub struct PartitionObjects {
    pub segment_objects: BTreeMap<RawOffset, SegmentObject>,

    // Segments not included in segment_objects because they conflict with another
    // segment, e.g. two uploads with the same base offset but different terms.
    // These dropped objects _might_ be preferable to some objects with the same
    // base_offset that are present in `segment_objects`: when reconstructing metadata,
    // it may be necessary to fully read segments to make a decision about which to
    // use.  If this vector is empty, then `segment_objects` may be treated as a robust
    // source of truth for the list of segments to include in a reconstructed partition
    // manifest.
    pub dropped_objects: Vec<SegmentObject>,
}

impl PartitionObjects {
    fn new() -> Self {
        Self {
            segment_objects: BTreeMap::new(),
            dropped_objects: Vec::new(),
        }
    }

    pub fn find_dropped(&self, base_offset: RawOffset) -> Vec<&SegmentObject> {
        let mut result = Vec::new();
        for o in &self.dropped_objects {
            if o.base_offset == base_offset {
                result.push(o);
            }
        }
        result
    }

    /// Find the SegmentObject at this offset.  If the SegmentObject was in a dropped_* list,
    /// then swap it into the main list and demote whoever previously held that offset.
    /// Calling this for all segments in manifests results in a PartitionObjects whose
    /// segment_objects should be a superset of the segments in the manifest.
    fn get_and_adjust(
        &mut self,
        base_offset: RawOffset,
        expect_key: &str,
    ) -> Option<&SegmentObject> {
        if self.segment_objects.contains_key(&base_offset) {
            if let Some(found) = self.segment_objects.get_mut(&base_offset) {
                if found.key != expect_key {
                    // Go looking for a better match.
                    for dropped_obj in &mut self.dropped_objects {
                        if dropped_obj.key == expect_key {
                            // Something is wrong with our key parsing if this isn't the case
                            assert_eq!(dropped_obj.base_offset, base_offset);

                            info!(
                                "Promoting segment object at offset {}: {}",
                                dropped_obj.base_offset, dropped_obj.key
                            );

                            // Swap the dropped object into segment_objects
                            std::mem::swap(dropped_obj, found);
                            return Some(found);
                        }
                    }

                    return None;
                } else {
                    return Some(found);
                }
            } else {
                unreachable!();
            }
        } else {
            // Maybe we dropped the segment referred to in the manifest?
            let mut found: Option<usize> = None;
            for (i, dropped_obj) in self.dropped_objects.iter_mut().enumerate() {
                if dropped_obj.key == expect_key {
                    // Something is wrong with our key parsing if this isn't the case
                    assert_eq!(dropped_obj.base_offset, base_offset);

                    found = Some(i);
                    break;
                }
            }

            if let Some(i) = found {
                let o = self.dropped_objects.remove(i);
                info!(
                    "Promoting segment object at offset {}: {}",
                    o.base_offset, o.key
                );
                let replaced = self.segment_objects.insert(o.base_offset, o);

                // We already checked for the expected offset in the earlier branch
                assert!(replaced.is_none());

                return Some(self.segment_objects.get(&base_offset).unwrap());
            } else {
                return None;
            }
        }
    }

    fn push(&mut self, obj: SegmentObject) {
        let existing = self.segment_objects.get(&obj.base_offset);
        //let mut ambiguous = false;
        if let Some(existing) = existing {
            // if existing.upload_term > obj.upload_term {
            //     self.dropped_objects.push(obj.key);
            //     return;
            // } else if existing.upload_term == obj.upload_term {
            //     // Ambiguous case: two objects at same base offset uploaded in the same term can be:
            //     // - An I/O error, then uploading a larger object later because there's more data
            //     // - Compaction re-upload, where a smaller object replaces a larger one
            //     // - Adjacent segment compaction, where a larger object replaces a smaller one.
            //     //
            //     // It is safer to prefer larger objects, as this avoids any possibility of
            //     // data loss if we get it wrong, and a reader can intentionally stop reading
            //     // when it gets to an offset that should be provided by the following segment.
            //     //
            //     // The output from reading a partition based on this class's reconstruction
            //     // may therefore see duplicate batches, and should account for that.
            //     if existing.size_bytes > obj.size_bytes {
            //         self.dropped_objects_ambiguous.push(obj.key);
            //         return;
            //     } else {
            //         ambiguous = true;
            //     }
            // }

            // TODO: reinstate distinction between ambiguous and non

            if existing.upload_term > obj.upload_term
                || (existing.upload_term == obj.upload_term && existing.size_bytes > obj.size_bytes)
            {
                self.dropped_objects.push(obj);
                return;
            }

            // Fall through and permit the input object to replace the existing
            // object at the same base offset.
        }

        let replaced = self.segment_objects.insert(obj.base_offset, obj);
        if let Some(replaced) = replaced {
            self.dropped_objects.push(replaced)
        }
    }

    /// All the objects we know about, including those that may be redundant/orphan.
    pub fn all_keys(&self) -> impl Iterator<Item = String> + '_ {
        let i1 = self.dropped_objects.iter().map(|o| o.key.clone());
        let i2 = self
            .segment_objects
            .values()
            .into_iter()
            .map(|o| o.key.clone());
        // let i3 = self.dropped_objects_ambiguous.iter().map(|o| o.clone());
        // i3.chain(i2.chain(i1))
        i2.chain(i1)
    }
}

#[derive(Serialize, Clone)]
pub struct MetadataGap {
    pub prev_seg_base: RawOffset,
    pub prev_seg_committed: RawOffset,
    pub kafka_gap_begin: Option<KafkaOffset>,
    pub kafka_gap_end: Option<KafkaOffset>,
    pub next_seg_base: RawOffset,
    pub next_seg_ts: Timestamp,
}

#[derive(Serialize, Clone, Default)]
pub struct Anomalies {
    /// Segment objects not mentioned in their manifest
    pub segments_outside_manifest: HashSet<String>,

    /// Archive manifests not referenced by a head manifest
    pub archive_manifests_outside_manifest: HashSet<String>,

    /// PartitionManifest that could not be loaded
    pub malformed_manifests: HashSet<String>,

    /// LifecycleMarker that could not be loaded
    pub malformed_lifecycle_markers: HashSet<String>,

    /// TopicManifest that could not be loaded
    pub malformed_topic_manifests: HashSet<String>,

    /// NTPR that had segment objects, but no partition manifest
    pub ntpr_no_manifest: HashSet<LabeledNTPR>,

    /// NTR that had segment objects and/or partition manifests, but no topic manifest
    pub ntr_no_topic_manifest: HashSet<LabeledNTR>,

    /// Keys that do not look like any object we expect
    pub unknown_keys: HashSet<String>,

    /// Segments referenced by a manifest, which do not exist in the bucket
    pub missing_segments: HashSet<String>,

    /// NTPR that failed consistency checks on its segments' metadata
    pub ntpr_bad_deltas: HashSet<LabeledNTPR>,

    /// Consistency checks found overlapping segments, which may be readable but
    /// indicate a bug in the code that wrote them.
    pub ntpr_overlap_offsets: HashSet<LabeledNTPR>,

    /// Where a partition manifest has two segments whose commited+base offsets
    /// are discontinuous.  This gap is reported as an anomaly, and may also be
    /// used to cue subsequent data scans.
    /// Ref Incident 259
    pub metadata_offset_gaps: HashMap<LabeledNTPR, Vec<MetadataGap>>,

    /// Files referenced by the cluster manifest with the highest metadata ID which do not exist in
    /// the bucket.
    pub inconsistent_cluster_metadata: Vec<ClusterMetadata>,

    /// ClusterMetadataManifest that could not be loaded
    pub malformed_cluster_manifests: Vec<String>,

    /// Controller snapshot that could not be loaded
    pub malformed_controller_snapshot: Vec<String>,
}
/// A convenience for human beings who would like to know things like the total amount of
/// data in each partition
#[derive(Serialize)]
pub struct PartitionMetadataSummary {
    pub bytes: u64,
    pub raw_start_offset: RawOffset,
    pub raw_last_offset: RawOffset,

    // kafka offsets may only be reported for non-empty manifests
    pub kafka_lwm: Option<KafkaOffset>,
    pub kafka_hwm: Option<KafkaOffset>,

    pub lifecycle_status: Option<LifecycleStatus>,
}

/// A convenience for human beings who would like to know things like the total amount of
/// data in each partition
#[derive(Serialize)]
pub struct MetadataSummary {
    pub anomalies: Anomalies,
    pub partitions: BTreeMap<LabeledNTPR, PartitionMetadataSummary>,
}

impl Anomalies {
    pub fn status(&self) -> AnomalyStatus {
        if !self.malformed_manifests.is_empty()
            || !self.malformed_lifecycle_markers.is_empty()
            || !self.malformed_topic_manifests.is_empty()
            || !self.missing_segments.is_empty()
            || !self.ntpr_bad_deltas.is_empty()
            || !self.metadata_offset_gaps.is_empty()
            || !self.malformed_cluster_manifests.is_empty()
            || !self.malformed_controller_snapshot.is_empty()
            || !self.inconsistent_cluster_metadata.is_empty()
        {
            AnomalyStatus::Corrupt
        } else if !self.segments_outside_manifest.is_empty()
            || !self.ntpr_no_manifest.is_empty()
            || !self.ntr_no_topic_manifest.is_empty()
            || !self.unknown_keys.is_empty()
            || !self.ntpr_overlap_offsets.is_empty()
        {
            AnomalyStatus::Dirty
        } else {
            AnomalyStatus::Clean
        }
    }

    fn report_line<
        I: Iterator<Item = J> + ExactSizeIterator,
        J: std::fmt::Display,
        T: IntoIterator<IntoIter = I, Item = J>,
    >(
        desc: &str,
        coll: T,
    ) -> String {
        let mut result = String::new();
        result.push_str(&format!("{}: ", desc));
        let mut first = true;
        for i in coll {
            if first {
                result.push_str("\n");
                first = false;
            }
            result.push_str(&format!("  {}\n", i));
        }
        if first {
            // No items.
            result.push_str("OK\n");
        }
        result
    }

    pub fn report(&self) -> String {
        let mut result = String::new();
        result.push_str(&Self::report_line(
            "Segments outside manifest",
            &self.segments_outside_manifest,
        ));
        result.push_str(&Self::report_line(
            "Archive manifests outside manifest",
            &self.archive_manifests_outside_manifest,
        ));
        result.push_str(&Self::report_line(
            "Malformed partition manifests",
            &self.malformed_manifests,
        ));
        result.push_str(&Self::report_line(
            "Malformed lifecycle markers",
            &self.malformed_lifecycle_markers,
        ));
        result.push_str(&Self::report_line(
            "Malformed topic manifests",
            &self.malformed_topic_manifests,
        ));
        result.push_str(&Self::report_line(
            "Partitions with segments but no manifest",
            &self.ntpr_no_manifest,
        ));
        result.push_str(&Self::report_line(
            "Topics with segments but no topic manifest",
            &self.ntr_no_topic_manifest,
        ));
        result.push_str(&Self::report_line(
            "Segments referenced in manifest but not found",
            &self.missing_segments,
        ));
        result.push_str(&Self::report_line(
            "NTPs with inconsistent offset deltas, possible bad kafka offsets",
            &self.ntpr_bad_deltas,
        ));
        result.push_str(&Self::report_line(
            "Overlapping offset ranges, possible upload bug",
            &self.ntpr_overlap_offsets,
        ));
        result.push_str(&Self::report_line(
            "NTPs with offset gaps",
            self.metadata_offset_gaps.keys(),
        ));
        result.push_str(&Self::report_line("Unexpected keys", &self.unknown_keys));
        result
    }

    pub fn merge(&mut self, other: Anomalies) {
        self.segments_outside_manifest
            .extend(other.segments_outside_manifest.into_iter());
        self.archive_manifests_outside_manifest
            .extend(other.archive_manifests_outside_manifest.into_iter());
        self.malformed_manifests
            .extend(other.malformed_manifests.into_iter());
        self.malformed_lifecycle_markers
            .extend(other.malformed_lifecycle_markers.into_iter());
        self.malformed_topic_manifests
            .extend(other.malformed_topic_manifests.into_iter());
        self.ntpr_no_manifest
            .extend(other.ntpr_no_manifest.into_iter());
        self.ntr_no_topic_manifest
            .extend(other.ntr_no_topic_manifest.into_iter());
        self.unknown_keys.extend(other.unknown_keys.into_iter());
        self.missing_segments
            .extend(other.missing_segments.into_iter());
        self.ntpr_bad_deltas
            .extend(other.ntpr_bad_deltas.into_iter());
        self.ntpr_overlap_offsets
            .extend(other.ntpr_overlap_offsets.into_iter());
        self.metadata_offset_gaps
            .extend(other.metadata_offset_gaps.into_iter());
    }
}

pub enum AnomalyStatus {
    // Everything lines up: every segment is in a manifest, every partition+topic has a manifest
    Clean,
    // An expected situation requiring cleanup, such as segments outside the manifest
    Dirty,
    // Something has happened that should never happen (e.g. unreadable manifest), or that prevents us knowing
    // quite how to handle the data (e.g. no topic manifest)
    Corrupt,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    // This field is not logically optional for a well-formed partition's metadata, but is
    // physically optional here because we may discover archive manifests prior to discovering
    // the head manifest.
    pub head_manifest: Option<PartitionManifest>,
    pub archive_manifests: Vec<ArchivePartitionManifest>,
}

impl PartitionMetadata {
    pub fn contains_segment(&self, seg: &SegmentObject) -> bool {
        let shortname = format!("{}-{}-v1.log", seg.base_offset, seg.original_term);

        if let Some(hm) = &self.head_manifest {
            if hm.contains_segment_shortname(&shortname) {
                return true;
            }
        }

        for am in &self.archive_manifests {
            if am.manifest.contains_segment_shortname(&shortname) {
                return true;
            }
        }

        false
    }
}

async fn list_parallel<'a>(
    client: &'a dyn ObjectStore,
    parallelism: usize,
) -> Result<impl Stream<Item = object_store::Result<ObjectMeta>> + 'a, object_store::Error> {
    assert!(parallelism == 1 || parallelism == 16 || parallelism == 256);

    Ok(stream! {
        let mut stream_map = StreamMap::new();

        for i in 0..parallelism {
            let prefix = if parallelism == 1 {
                "".to_string()
            } else if parallelism == 16 {
                format!("{:1x}", i)
            } else if parallelism == 256 {
                format!("{:02x}", i)
            } else {
                panic!();
            };

            let stream_key = prefix.clone();
            let prefix_path = object_store::path::Path::from(prefix);
            match client.list(Some(&prefix_path)).await {
                Ok(s) => {
                    debug!("Streaming keys for prefix '{}'", prefix_path);
                    stream_map.insert(stream_key, s);
                },
                Err(e) => {
                    warn!("Error streaming keys for prefix '{}'", prefix_path);
                    yield Err(e);
                }
            };
        }

        while let Some(item) = stream_map.next().await {
            debug!("Yielding item...");
            yield item.1;
        }
    })
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct ClusterMetadata {
    pub manifests: BTreeMap<i64, ClusterMetadataManifest>,
    pub controller_snapshots: HashSet<String>,
}

/// Find all the partitions and their segments within a bucket
pub struct BucketReader {
    pub partitions: HashMap<LabeledNTPR, PartitionObjects>,
    pub partition_manifests: HashMap<LabeledNTPR, PartitionMetadata>,
    pub topic_manifests: HashMap<LabeledNTR, TopicManifest>,
    pub lifecycle_markers: HashMap<LabeledNTR, LifecycleMarker>,
    pub cluster_metadata: HashMap<String, ClusterMetadata>,
    pub anomalies: Anomalies,
    pub client: Arc<dyn ObjectStore>,
}

#[derive(Serialize, Deserialize)]
struct SavedBucketReader {
    pub partitions: HashMap<LabeledNTPR, PartitionObjects>,
    pub partition_manifests: HashMap<LabeledNTPR, PartitionMetadata>,
    pub topic_manifests: HashMap<LabeledNTR, TopicManifest>,
    pub lifecycle_markers: HashMap<LabeledNTR, LifecycleMarker>,
    pub cluster_metadata: HashMap<String, ClusterMetadata>,
}

pub struct SegmentStream {
    pub stream: Result<BoxStream<'static, object_store::Result<bytes::Bytes>>, object_store::Error>,
    pub object: SegmentObject,
}

impl SegmentStream {
    pub fn into_parts(
        self,
    ) -> (
        Result<BoxStream<'static, object_store::Result<bytes::Bytes>>, object_store::Error>,
        SegmentObject,
    ) {
        (self.stream, self.object)
    }
}

#[derive(Clone)]
enum FetchKey {
    PartitionManifest(String),
    ArchiveManifest(String),
    TopicManifest(String),
    TopicLifecycleMarker(String),
}

impl FetchKey {
    fn as_str(&self) -> &str {
        match self {
            FetchKey::PartitionManifest(s) => s,
            FetchKey::TopicManifest(s) => s,
            FetchKey::ArchiveManifest(s) => s,
            FetchKey::TopicLifecycleMarker(s) => s,
        }
    }
}

impl BucketReader {
    pub async fn from_file(
        path: &str,
        client: Arc<dyn ObjectStore>,
    ) -> Result<Self, tokio::io::Error> {
        let mut file = tokio::fs::File::open(path).await.unwrap();
        let mut buf: String = String::new();
        file.read_to_string(&mut buf).await?;
        let saved_state = serde_json::from_str::<SavedBucketReader>(&buf).unwrap();
        Ok(Self {
            partitions: saved_state.partitions,
            partition_manifests: saved_state.partition_manifests,
            topic_manifests: saved_state.topic_manifests,
            lifecycle_markers: saved_state.lifecycle_markers,
            anomalies: Default::default(),
            cluster_metadata: saved_state.cluster_metadata,
            client,
        })
    }

    pub fn filter(&mut self, filter: &NTPFilter) {
        self.partitions = self
            .partitions
            .drain()
            .filter(|i| filter.match_lntpr(&i.0))
            .collect();

        self.partition_manifests = self
            .partition_manifests
            .drain()
            .filter(|i| filter.match_lntpr(&i.0))
            .collect();

        self.topic_manifests = self
            .topic_manifests
            .drain()
            .filter(|i| filter.match_lntr(&i.0))
            .collect();
    }

    pub async fn to_file(&self, path: &str) -> Result<(), tokio::io::Error> {
        let saved_state = SavedBucketReader {
            partitions: self.partitions.clone(),
            partition_manifests: self.partition_manifests.clone(),
            topic_manifests: self.topic_manifests.clone(),
            lifecycle_markers: self.lifecycle_markers.clone(),
            cluster_metadata: self.cluster_metadata.clone(),
        };

        let buf = serde_json::to_vec(&saved_state).unwrap();

        let mut file = tokio::fs::File::create(path).await.unwrap();
        file.write_all(&buf).await?;
        info!("Wrote {} bytes to {}", buf.len(), path);
        Ok(())
    }

    pub async fn new(client: Arc<dyn ObjectStore>) -> Self {
        Self {
            partitions: HashMap::new(),
            partition_manifests: HashMap::new(),
            topic_manifests: HashMap::new(),
            lifecycle_markers: HashMap::new(),
            cluster_metadata: HashMap::new(),
            anomalies: Default::default(),
            client,
        }
    }

    async fn load_manifests(
        &mut self,
        manifest_keys: Vec<FetchKey>,
    ) -> Result<(), BucketReaderError> {
        fn getter_stream(
            client: Arc<dyn ObjectStore>,
            keys: Vec<FetchKey>,
        ) -> impl Stream<
            Item = impl std::future::Future<
                Output = (FetchKey, Result<bytes::Bytes, object_store::Error>),
            >,
        > {
            stream! {
                for key in keys {
                    let client_clone = client.clone();
                    let raw_key = match &key {
                        FetchKey::PartitionManifest(s) => s.clone(),
                        FetchKey::TopicManifest(s) => s.clone(),
                        FetchKey::ArchiveManifest(s) => s.clone(),
                        FetchKey::TopicLifecycleMarker(s) => s.clone()
                    };
                    yield async move {
                        let output_key = key.clone();
                        let response_result = client_clone
                                    .get(&object_store::path::Path::from(raw_key))
                                    .await;
                        match response_result {
                            Err(e) => (output_key, Err(e)),
                            Ok(response) => {
                                let bytes_result = response.bytes().await;
                                match bytes_result {
                                    Ok(bytes) => {
                                        (output_key, Ok(bytes))
                                    },
                                    Err(e) => {
                                        (output_key, Err(e))
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let buffered = getter_stream(self.client.clone(), manifest_keys).buffer_unordered(16);
        pin_mut!(buffered);
        while let Some(result) = buffered.next().await {
            let (key, body_r) = result;
            let body = match body_r {
                Ok(b) => b,
                Err(e) => {
                    if let object_store::Error::NotFound {
                        path: _path,
                        source: _source,
                    } = e
                    {
                        // This is legal: users may delete topics, and that may happen between
                        // our object listing and our manifest fetching
                        info!(
                            "Manifest key {} removed between object listing and fetch",
                            key.as_str()
                        );
                        continue;
                    } else {
                        return Err(BucketReaderError::from(e));
                    }
                }
            };
            match key {
                FetchKey::PartitionManifest(key) => {
                    debug!(
                        "Parsing {} bytes from partition manifest key {}",
                        body.len(),
                        key
                    );
                    self.ingest_partition_manifest(&key, body).await?;
                }
                FetchKey::TopicManifest(key) => {
                    debug!(
                        "Parsing {} bytes from topic manifest key {}",
                        body.len(),
                        key
                    );
                    self.ingest_topic_manifest(&key, body).await?;
                }
                FetchKey::ArchiveManifest(key) => {
                    debug!(
                        "Parsing {} bytes from archive partition manifest key {}",
                        body.len(),
                        key
                    );
                    self.ingest_archive_manifest(&key, body).await?;
                }
                FetchKey::TopicLifecycleMarker(key) => {
                    debug!(
                        "Parsing {} bytes from lifecycle marker key {}",
                        body.len(),
                        key
                    );
                    self.ingest_topic_lifecycle_marker(&key, body).await?;
                }
            }
        }

        debug!(
            "Loaded {} partition manifests",
            self.partition_manifests.len()
        );
        debug!("Loaded {} topic manifests", self.topic_manifests.len());
        debug!("Loaded {} lifecycle markers", self.lifecycle_markers.len());

        Ok(())
    }

    pub fn get_summary(&self) -> MetadataSummary {
        let mut partitions = BTreeMap::new();
        for (lntpr, partition_meta) in &self.partition_manifests {
            if let Some(manifest) = &partition_meta.head_manifest {
                let ntr = NTR {
                    namespace: manifest.namespace.clone(),
                    topic: manifest.namespace.clone(),
                    revision_id: manifest.revision,
                };
                let labeled_ntr = LabeledNTR {
                    ntr,
                    label: lntpr.label.clone(),
                };

                let kafka_offsets = manifest.kafka_watermarks();
                partitions.insert(
                    lntpr.clone(),
                    PartitionMetadataSummary {
                        bytes: manifest.get_size_bytes(),
                        raw_start_offset: manifest.start_offsets().0,
                        raw_last_offset: manifest.last_offset,
                        kafka_lwm: kafka_offsets.map(|x| x.0),
                        kafka_hwm: kafka_offsets.map(|x| x.1),
                        lifecycle_status: self
                            .lifecycle_markers
                            .get(&labeled_ntr)
                            .and_then(|m| Some(m.status.clone())),
                    },
                );
            } else {
                // No manifest, don't include it in summary.  Its objects will still show up
                // in the list of anomalies is ntp_no_manifest
            }
        }

        MetadataSummary {
            anomalies: self.anomalies.clone(),
            partitions,
        }
    }

    fn filter_old_revisions(&mut self) {
        // If we see multiple revisions for the same NTP, then all older revisions correspond
        // to deleted topics.  To avoid making output hard to read when filtering on NTP (without R),
        // suppress old revisions.
        let mut latest_revision: HashMap<(String, String), i64> = HashMap::new();
        for lntpr in self.partitions.keys() {
            // FIXME: these clones are gratuitous
            let nt = (
                lntpr.ntpr.ntp.namespace.clone(),
                lntpr.ntpr.ntp.topic.clone(),
            );
            if let Some(current_max) = latest_revision.get(&nt) {
                if current_max >= &lntpr.ntpr.revision_id {
                    continue;
                }
            }

            latest_revision.insert(nt, lntpr.ntpr.revision_id);
        }

        let match_ntpr = |lntpr: &LabeledNTPR| {
            // FIXME: these clones are gratuitous
            let nt = (
                lntpr.ntpr.ntp.namespace.clone(),
                lntpr.ntpr.ntp.topic.clone(),
            );
            if let Some(latest) = latest_revision.get(&nt) {
                lntpr.ntpr.revision_id == *latest
            } else {
                true
            }
        };

        let match_ntr = |lntr: &LabeledNTR| {
            // FIXME: these clones are gratuitous
            let nt = (lntr.ntr.namespace.clone(), lntr.ntr.topic.clone());
            if let Some(latest) = latest_revision.get(&nt) {
                lntr.ntr.revision_id == *latest
            } else {
                true
            }
        };

        self.partitions = self
            .partitions
            .drain()
            .filter(|i| match_ntpr(&i.0))
            .collect();

        self.partition_manifests = self
            .partition_manifests
            .drain()
            .filter(|i| match_ntpr(&i.0))
            .collect();

        self.topic_manifests = self
            .topic_manifests
            .drain()
            .filter(|i| match_ntr(&i.0))
            .collect();

        // TODO: re-expose the things we've dropped as candidates for cleanup, and/or add a flag
        // to optionally disable this culling if user is interested in inspecting data for
        // partitions they know where deleted+replaced
    }

    pub async fn repair_manifest_ntp(
        &mut self,
        gaps: &Vec<MetadataGap>,
        lntpr: &LabeledNTPR,
    ) -> Result<Vec<RepairEdit>, BucketReaderError> {
        let initial_repairs = maybe_adjust_manifest(&lntpr, &gaps, self).await?;
        info!(
            "[{}] Found {} repairs to manifest, projecting.",
            lntpr,
            initial_repairs.len()
        );

        let objects = if let Some(o) = self.partitions.get_mut(&lntpr) {
            o
        } else {
            return Ok(Vec::new());
        };

        if let Some(metadata) = self.partition_manifests.get_mut(&lntpr) {
            if let Some(manifest) = metadata.head_manifest.as_mut() {
                project_repairs(manifest, &initial_repairs);

                let remote_label = RemoteLabel::from_string(&lntpr.label);
                for seg in manifest.segments.values() {
                    if let Some(segment_key) = manifest.segment_key(seg, &remote_label) {
                        // Our repair might mean that a segment from the 'dropped' list
                        // is now referenced by the manifest: use the 'adjust' side effect
                        // of this function to swap that segment into the main list.:w
                        objects.get_and_adjust(seg.base_offset as RawOffset, &segment_key);
                    }
                }
            }
        }

        Ok(initial_repairs)
    }

    pub async fn analyze_metadata(&mut self, filter: &NTPFilter) -> Result<(), BucketReaderError> {
        // In case caller calls it twice
        self.anomalies = Default::default();

        self.filter_old_revisions();

        // During manifest validation, we may stat() some objects that didn't exist
        // during the initial bucket scan.  Accumulate them here for ingesting afterwards.
        let mut discovered_objects: Vec<ObjectMeta> = vec![];

        for (lntpr, partition_objects) in &mut self.partitions {
            if !filter.match_lntpr(lntpr) {
                continue;
            }

            if lntpr.ntpr.ntp.partition_id == 0 {
                let t_manifest_o = self.topic_manifests.get(&lntpr.to_ntr());
                if let None = t_manifest_o {
                    self.anomalies.ntr_no_topic_manifest.insert(lntpr.to_ntr());
                }
            }

            let p_metadata_o = self.partition_manifests.get(lntpr);
            match p_metadata_o {
                None => {
                    // The manifest may be missing because we couldn't load it, in which
                    // case that is already tracked in malformed_manifests
                    let manifest_key_bin = PartitionManifest::manifest_key(&lntpr, "bin");
                    let manifest_key_json = PartitionManifest::manifest_key(&lntpr, "json");
                    if self
                        .anomalies
                        .malformed_manifests
                        .contains(&manifest_key_bin)
                        || self
                            .anomalies
                            .malformed_manifests
                            .contains(&manifest_key_json)
                    {
                        debug!("Not reporting {} as missing because it's already reported as malformed", lntpr);
                    } else {
                        self.anomalies.ntpr_no_manifest.insert(lntpr.clone());
                    }
                }
                Some(p_metadata) => {
                    for o in partition_objects.segment_objects.values() {
                        if !p_metadata.contains_segment(&o) {
                            self.anomalies
                                .segments_outside_manifest
                                .insert(o.key.clone());
                        }
                    }
                }
            }

            // TODO: also mutate the lists of objects, to simplify
            //       subsequent processing:
            // - Drop segments that are outside the manifest, unless they are
            //   at an offset higher than the tip of the manifest.
            // - Drop segments that overlap: retain the one that is mentioned
            //   in the manifest, or whichever appears to come from a newer term.
        }

        let mut new_anomalies: Anomalies = Default::default();
        for (lntpr, partition_metadata) in &self.partition_manifests {
            let mut raw_objects = self.partitions.get_mut(&lntpr);

            if !filter.match_lntpr(&lntpr) {
                continue;
            }

            // We will validate the manifest.  If there is no head manifest, that is an anomaly.
            if partition_metadata.head_manifest.is_none()
                && partition_metadata.archive_manifests.len() > 0
            {
                for am in &partition_metadata.archive_manifests {
                    self.anomalies
                        .archive_manifests_outside_manifest
                        .insert(am.key(&lntpr));
                }
            }

            if partition_metadata.head_manifest.is_some() {
                let anomalies = Self::analyze_manifest(
                    self.client.clone(),
                    &lntpr,
                    partition_metadata.head_manifest.as_ref().unwrap(),
                    &mut raw_objects,
                    &mut discovered_objects,
                    None,
                )
                .await?;
                new_anomalies.merge(anomalies);
            }

            for archive_manifest in &partition_metadata.archive_manifests {
                let anomalies = Self::analyze_archive_manifest(
                    self.client.clone(),
                    &lntpr,
                    partition_metadata.head_manifest.as_ref().unwrap(),
                    archive_manifest,
                    &mut raw_objects,
                    &mut discovered_objects,
                )
                .await?;
                new_anomalies.merge(anomalies);
            }
        }

        self.anomalies.merge(new_anomalies);

        for (lntpr, _) in &mut self.partition_manifests {
            if !filter.match_lntpr(lntpr) {
                continue;
            }
            let t_manifest_o = self.topic_manifests.get(&lntpr.to_ntr());
            if let None = t_manifest_o {
                self.anomalies.ntr_no_topic_manifest.insert(lntpr.to_ntr());
            }
        }

        for object_meta in discovered_objects {
            self.ingest_segment(
                object_meta.location.as_ref(),
                filter,
                object_meta.size as u64,
            )
        }
        debug!(
            "Loaded metadata from {} clusters",
            self.cluster_metadata.len()
        );
        for (cluster_uuid, meta) in &self.cluster_metadata {
            debug!(
                "Loaded {} cluster metadata manifests and {} controller snapshots from cluster {}",
                meta.manifests.len(),
                meta.controller_snapshots.len(),
                cluster_uuid
            );
            let highest_manifest = if let Some((_, manifest)) =
                meta.manifests.iter().max_by_key(|(_, m)| m.metadata_id)
            {
                manifest
            } else {
                debug!("No manifests for cluster {}", cluster_uuid);
                continue;
            };
            if highest_manifest.controller_snapshot_path.is_empty() {
                continue;
            }
            if !meta
                .controller_snapshots
                .contains(&highest_manifest.controller_snapshot_path)
            {
                warn!(
                    "Cluster {} manifest points at snapshot {} but it doesn't exist in bucket",
                    cluster_uuid, highest_manifest.controller_snapshot_path
                );
                self.anomalies
                    .inconsistent_cluster_metadata
                    .push(meta.clone());
            }
        }
        Ok(())
    }

    async fn analyze_archive_manifest(
        client: Arc<dyn object_store::ObjectStore>,
        lntpr: &LabeledNTPR,
        head_manifest: &PartitionManifest,
        archive_manifest: &ArchivePartitionManifest,
        raw_objects: &mut Option<&mut PartitionObjects>,
        discovered: &mut Vec<ObjectMeta>,
    ) -> Result<Anomalies, BucketReaderError> {
        // TODO(vlad): validate parsed name against contents
        Self::analyze_manifest(
            client,
            lntpr,
            &archive_manifest.manifest,
            raw_objects,
            discovered,
            head_manifest.archive_start_offset,
        )
        .await
    }

    async fn analyze_manifest(
        client: Arc<dyn object_store::ObjectStore>,
        lntpr: &LabeledNTPR,
        partition_manifest: &PartitionManifest,
        raw_objects: &mut Option<&mut PartitionObjects>,
        discovered: &mut Vec<ObjectMeta>,
        archive_start_offset: Option<RawOffset>,
    ) -> Result<Anomalies, BucketReaderError> {
        let mut anomalies: Anomalies = Default::default();

        // For all segments in the manifest, check they were found in the bucket
        debug!(
            "Checking {} ({} segments)",
            partition_manifest.ntp(),
            partition_manifest.segments.len()
        );
        let manifest_segments = &partition_manifest.segments;
        for (segment_short_name, segment) in manifest_segments {
            if let Some(so) = partition_manifest.start_offset {
                if segment.committed_offset < so {
                    debug!(
                        "Not checking {} {}, it is below start offset",
                        partition_manifest.ntp(),
                        segment_short_name
                    );
                    continue;
                }
            }

            debug!(
                "Checking {} {}",
                partition_manifest.ntp(),
                segment_short_name
            );
            let remote_label = RemoteLabel::from_string(&lntpr.label);
            if let Some(expect_key) = partition_manifest.segment_key(segment, &remote_label) {
                debug!("Calculated segment {}", expect_key);
                let so = archive_start_offset.unwrap_or(RawOffset::MIN);
                let bo = segment.base_offset;
                if bo >= so
                    && !Self::check_existence(
                        client.clone(),
                        raw_objects,
                        segment.base_offset as RawOffset,
                        &expect_key,
                        discovered,
                    )
                    .await?
                {
                    anomalies.missing_segments.insert(expect_key.to_string());
                    // TODO: we should re-read manifest in case the segment
                    // was legitimately GC'd while we were scanning
                }
            }
        }

        // Inspect the manifest's offsets:
        // - Deltas should be monotonic
        // - Segment offsets should be continuous
        let mut last_committed_offset: Option<RawOffset> = None;
        let mut last_base_offset: Option<RawOffset> = None;
        let mut last_max_timestamp = None;
        let mut last_delta: Option<u64> = None;
        let mut sorted_segments: Vec<&PartitionManifestSegment> =
            manifest_segments.values().collect();
        sorted_segments.sort_by_key(|s| s.base_offset);

        for segment in sorted_segments {
            if let Some(last_delta) = last_delta {
                match segment.delta_offset {
                    None => {
                        // After some segments have a delta offset, subsequent ones must
                        // as well.
                        warn!(
                            "[{}] Segment {} has missing delta_offset",
                            lntpr, segment.base_offset
                        );
                        anomalies.ntpr_bad_deltas.insert(lntpr.clone());
                    }
                    Some(seg_delta) => {
                        if seg_delta < last_delta {
                            warn!(
                                "[{}] Segment {} has delta lower than previous",
                                lntpr, segment.base_offset
                            );
                            anomalies.ntpr_bad_deltas.insert(lntpr.clone());
                        }
                    }
                }
            }

            if segment.delta_offset.is_some() && segment.delta_offset_end.is_some() {
                let d_off = segment.delta_offset.unwrap();
                let d_off_end = segment.delta_offset_end.unwrap();
                if d_off > d_off_end {
                    warn!(
                        "[{}] Segment {} has end delta lower than base delta",
                        lntpr, segment.base_offset
                    );
                    anomalies.ntpr_bad_deltas.insert(lntpr.clone());
                }
            }

            if let Some(last_committed_offset) = last_committed_offset {
                if segment.base_offset as RawOffset > last_committed_offset + 1 {
                    let ts = SystemTime::UNIX_EPOCH.add(Duration::from_millis(
                        segment.base_timestamp.unwrap_or(0) as u64,
                    ));
                    let dt: chrono::DateTime<Utc> = ts.into();

                    warn!(
                            "[{}] Segment {} has gap between base offset and previous segment's committed offset ({}).  Missing {} records, from ts {} to ts {} ({})",
                            lntpr,
                            segment.base_offset,
                            last_committed_offset,
                            segment.base_offset as RawOffset - (last_committed_offset + 1),
                            last_max_timestamp.unwrap_or(0),
                            segment.base_timestamp.unwrap_or(0),
                            dt.to_rfc3339());
                    let gap_list = anomalies
                        .metadata_offset_gaps
                        .entry(lntpr.clone())
                        .or_insert_with(|| Vec::new());

                    let kafka_gap_begin =
                        last_delta.map(|d| (last_committed_offset - d as i64) as KafkaOffset);
                    let kafka_gap_end = segment
                        .delta_offset
                        .map(|d| raw_to_kafka(segment.base_offset, d));

                    gap_list.push(MetadataGap {
                        next_seg_base: segment.base_offset as RawOffset,
                        next_seg_ts: segment.base_timestamp.unwrap_or(0) as Timestamp,
                        prev_seg_committed: last_committed_offset,
                        prev_seg_base: last_base_offset.unwrap(),
                        kafka_gap_begin,
                        kafka_gap_end,
                    });
                } else if (segment.base_offset as RawOffset) < last_committed_offset + 1 {
                    warn!(
                            "[{}] Segment {} has overlap between base offset and previous segment's committed offset ({})",
                            lntpr, segment.base_offset, last_committed_offset
                        );
                    anomalies.ntpr_overlap_offsets.insert(lntpr.clone());
                }
            }

            last_delta = segment.delta_offset_end;
            last_base_offset = Some(segment.base_offset as RawOffset);
            last_committed_offset = Some(segment.committed_offset as RawOffset);
            last_max_timestamp = segment.max_timestamp;
        }

        Ok(anomalies)
    }

    async fn check_existence(
        client: Arc<dyn object_store::ObjectStore>,
        raw_objects: &mut Option<&mut PartitionObjects>,
        check_offset: RawOffset,
        expect_key: &str,
        discovered: &mut Vec<ObjectMeta>,
    ) -> Result<bool, object_store::Error> {
        let found_obj = if let Some(raw_objects) = raw_objects {
            raw_objects.get_and_adjust(check_offset, &expect_key)
        } else {
            None
        };

        if found_obj.is_some() {
            return Ok(true);
        }

        // Object not found in PartitionObjects from scan: do a HEAD to see if it's really
        // missing
        match client
            .head(&object_store::path::Path::from(expect_key))
            .await
        {
            Err(e) => {
                match e {
                    object_store::Error::NotFound { path: _, source: _ } => {
                        info!("Confirmed missing segment with HEAD: {}", expect_key);
                        // Confirmed, the segment really doesn't exist
                        Ok(false)
                    }
                    _ => Err(e),
                }
            }
            Ok(stat) => {
                // The object isn't really missing, it just wasn't
                // written when we did our full scan of the bucket.
                // Load it into the PartitionObjects.
                discovered.push(stat);
                Ok(true)
            }
        }
    }

    pub async fn scan(&mut self, filter: &NTPFilter) -> Result<(), BucketReaderError> {
        // TODO: for this to work at unlimited scale, we need:
        //  - load the manifests first, and only bother storing extra vectors
        //    of segments if those segments aren't in the manifest
        //  - or use a disk-spilling database for all this state.

        // Must clone because otherwise we hold immutable reference to `self` while
        // iterating through list results
        let client = self.client.clone();

        // TODO: we may estimate the total number of objects in the bucket by
        // doing a listing with delimiter at the base of the bucket.  (1000 / (The highest
        // hash prefix we see)) * 4E9 -> approximate object count

        // =======
        // Phase 1: List all objects in the bucket
        // =======

        let list_stream = list_parallel(client.as_ref(), 16).await?;
        pin_mut!(list_stream);
        let mut manifest_keys: Vec<FetchKey> = vec![];

        fn maybe_stash_partition_key(keys: &mut Vec<FetchKey>, k: FetchKey, filter: &NTPFilter) {
            lazy_static! {
                static ref META_NTP_PREFIX: Regex =
                    Regex::new("[-a-f0-9]+/meta/([^]]+)/([^]]+)/(\\d+)_(\\d+)/.+").unwrap();
            }
            if let Some(grps) = META_NTP_PREFIX.captures(k.as_str()) {
                let ns = grps.get(1).unwrap().as_str();
                let topic = grps.get(2).unwrap().as_str();
                // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
                let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
                let partition_revision = grps.get(4).unwrap().as_str().parse::<i64>().unwrap();

                if filter.match_parts(ns, topic, Some(partition_id), Some(partition_revision)) {
                    debug!("Stashing partition manifest key {}", k.as_str());
                    keys.push(k);
                } else {
                    debug!("Dropping filtered-out manifest key {}", k.as_str());
                }
            } else {
                // Drop it.
                warn!("Dropping malformed manifest key {}", k.as_str());
            }
        }

        fn maybe_stash_topic_key(keys: &mut Vec<FetchKey>, k: FetchKey, filter: &NTPFilter) {
            lazy_static! {
                static ref META_TP_PREFIX: Regex =
                    Regex::new("[a-f0-9]+/meta/([^]]+)/([^]]+)/.+").unwrap();
            }
            if let Some(grps) = META_TP_PREFIX.captures(k.as_str()) {
                let ns = grps.get(1).unwrap().as_str();
                let topic = grps.get(2).unwrap().as_str();

                if filter.match_parts(ns, topic, None, None) {
                    debug!("Stashing topic manifest key {}", k.as_str());
                    keys.push(k);
                } else {
                    debug!("Dropping filtered-out topic key {}", k.as_str());
                }
            } else {
                // Drop it.
                warn!("Dropping malformed topic key {}", k.as_str());
            }
        }

        let mut object_k = 0;
        while let Some(o_r) = list_stream.next().await {
            let o = o_r?;

            let key = o.location.to_string();
            if key.ends_with("/manifest.json") || key.ends_with("/manifest.bin") {
                maybe_stash_partition_key(
                    &mut manifest_keys,
                    FetchKey::PartitionManifest(key),
                    filter,
                );
            } else if key.ends_with("/topic_manifest.json") {
                maybe_stash_topic_key(&mut manifest_keys, FetchKey::TopicManifest(key), filter);
            } else if key.ends_with("lifecycle.bin") {
                maybe_stash_topic_key(
                    &mut manifest_keys,
                    FetchKey::TopicLifecycleMarker(key),
                    filter,
                );
            } else if key.ends_with("cluster_manifest.json") {
                debug!("Parsing cluster metadata manifest key {}", key);
                self.ingest_cluster_metadata_manifest(&key).await?;
            } else if key.ends_with("controller.snapshot") {
                debug!("Parsing controller snapshot key {}", key);
                self.ingest_controller_snapshot(&key);
            } else if key.contains("manifest.json.") || key.contains("manifest.bin.") {
                maybe_stash_partition_key(
                    &mut manifest_keys,
                    FetchKey::ArchiveManifest(key),
                    filter,
                );
            } else if key.ends_with(".index") {
                // TODO: do something with index files: currently ignore them as they are
                // somewhat disposable.  Should track .tx and .index files within
                // PartitionObjects: we need .tx files to implement read, and we need
                // both when doing a dump of an ntp for debug.
                debug!("Ignoring index key {}", key);
            } else {
                debug!("Parsing segment key {}", key);
                self.ingest_segment(&key, &filter, o.size as u64);
            }

            object_k += 1;
            if object_k % 10000 == 0 {
                info!("Scan progress: {} objects", object_k);
            }
        }

        // =======
        // Phase 2: Fetch all the manifests
        // =======

        self.load_manifests(manifest_keys).await?;

        // Clean up segment_term fields on legacy-format segments
        for (_ntpr, partition_metadata) in &mut self.partition_manifests {
            if let Some(manifest) = &mut partition_metadata.head_manifest {
                for (segment_shortname, segment) in &mut manifest.segments {
                    if segment.segment_term.is_none() {
                        let parsed = parse_segment_shortname(segment_shortname);
                        if let Some(parsed) = parsed {
                            segment.segment_term = Some(parsed.1);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Yield a byte stream for each segment
    pub fn stream(
        &self,
        lntpr: &LabeledNTPR,
        seek: Option<RawOffset>, //) -> Pin<Box<dyn Stream<Item = Result<BoxStream<'static, object_store::Result<bytes::Bytes>>, BucketReaderError> + '_>>
    ) -> impl Stream<Item = SegmentStream> + '_ {
        // TODO error handling for parittion DNE

        // TODO go via metadata: if we have no manifest, we should synthesize one and validate
        // rather than just stepping throuhg objects naively.
        let partition_objects = self.partitions.get(lntpr).unwrap();
        // Box::pin(
        //     futures::stream::iter(0..partition_objects.segment_objects.len())
        //         .then(|i| self.stream_one(&partition_objects.segment_objects[i])),
        // )
        // Box::pin(futures::stream::iter(
        //     partition_objects
        //         .segment_objects
        //         .values()
        //         .map(|so| self.stream_one(&so.key)),
        // ))
        stream! {
            for so in partition_objects.segment_objects.values() {
                if let Some(seek) = seek {
                    if so.base_offset < seek {
                        continue;
                    }
                }
                yield SegmentStream{
                    stream: self
                .stream_one(&so.key).await,
                // TOOD make object a ref
                object:so.clone()
                    };
            }
        }
    }

    // TODO: return type should include name of the segment we're streaming, so that
    // caller can include it in logs.
    pub async fn stream_one(
        &self,
        key: &String,
    ) -> Result<BoxStream<'static, object_store::Result<bytes::Bytes>>, object_store::Error> {
        // TOOD Handle request failure
        debug!("stream_one: {}", key);
        let key: &str = &key;
        let path = object_store::path::Path::from(key);
        let get_result = self.client.get(&path).await?;
        match get_result {
            // This code is currently only for use with object storage
            GetResult::File(_, _) => unreachable!(),
            GetResult::Stream(s) => Ok(s),
        }
    }

    pub async fn summarize_data_segment(
        &self,
        o: &SegmentObject,
        delta: u64,
    ) -> Result<SegmentDataSummary, BucketReaderError> {
        info!("Reading data segment {} ({} bytes)", o.key, o.size_bytes);
        let head = self
            .client
            .head(&object_store::path::Path::from(o.key.as_str()))
            .await?;

        let mut summary = SegmentDataSummary {
            size_bytes: head.size as u64,
            base_offset: i64::MAX,
            committed_offset: i64::MIN,
            delta_end: delta,
        };

        let stream = self.stream_one(&o.key).await?;
        pin_mut!(stream);
        let byte_stream = StreamReader::new(stream);
        let mut batch_stream = BatchStream::new(byte_stream);
        while let Ok(bb) = batch_stream.read_batch_buffer().await {
            summary.base_offset =
                std::cmp::min(summary.base_offset, bb.header.base_offset as RawOffset);
            summary.committed_offset = std::cmp::max(
                summary.committed_offset,
                (bb.header.base_offset + bb.header.record_count as u64 - 1) as RawOffset,
            );
            if !bb.header.is_kafka_data() {
                summary.delta_end += bb.header.record_count as u64;
            }
        }

        Ok(summary)
    }

    fn decode_partition_manifest(
        key: &str,
        buf: bytes::Bytes,
    ) -> Result<PartitionManifest, BucketReaderError> {
        if key.ends_with(".json") || key.contains(".json.") {
            Ok(serde_json::from_slice(&buf)?)
        } else if key.ends_with(".bin") || key.contains(".bin.") {
            Ok(PartitionManifest::from_bytes(buf)?)
        } else {
            Err(BucketReaderError::SyntaxError("Malformed key".to_string()))
        }
    }

    pub async fn inject_partition_manifest(
        &mut self,
        // Path is not an object path: it's a filename, only used to figure out if
        // the input is meant to be binary or JSON based on extension
        path: &str,
        body: bytes::Bytes,
    ) {
        // Decode it to derive the key
        let manifest = match Self::decode_partition_manifest(path, body.clone()) {
            Ok(m) => m,
            Err(e) => {
                panic!("Error parsing partition manifest {}: {:?}", path, e);
            }
        };

        let extension = if path.ends_with(".bin") {
            ".bin"
        } else {
            ".json"
        };

        let key = PartitionManifest::manifest_key(
            &LabeledNTPR {
                ntpr: NTPR {
                    ntp: NTP {
                        namespace: manifest.namespace,
                        topic: manifest.topic,
                        partition_id: manifest.partition,
                    },
                    revision_id: manifest.revision,
                },
                label: None,
            },
            extension,
        );

        self.ingest_partition_manifest(&key, body).await.unwrap()
    }

    async fn ingest_partition_manifest(
        &mut self,
        key: &str,
        body: bytes::Bytes,
    ) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref PARTITION_MANIFEST_KEY: Regex =
                Regex::new("([-a-f0-9]+)/meta/([^]]+)/([^]]+)/(\\d+)_(\\d+)/manifest.(json|bin)")
                    .unwrap();
        }
        if let Some(grps) = PARTITION_MANIFEST_KEY.captures(key) {
            let prefix = grps.get(1).unwrap().as_str().to_string();
            // Group::get calls are safe to unwrap() because regex always has those groups if it matched
            let ns = grps.get(2).unwrap().as_str().to_string();
            let topic = grps.get(3).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(4).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(5).unwrap().as_str().parse::<i64>().unwrap();
            let ntpr = NTPR {
                ntp: NTP {
                    namespace: ns,
                    topic,
                    partition_id,
                },
                revision_id: partition_revision,
            };
            let labeled_ntpr = LabeledNTPR::from_ntpr_and_prefix(ntpr, prefix);
            let manifest = match Self::decode_partition_manifest(key, body) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Error parsing partition manifest {}: {:?}", key, e);
                    self.anomalies.malformed_manifests.insert(key.to_string());
                    return Ok(());
                }
            };

            // Note: assuming memory is sufficient for manifests
            match self.partition_manifests.get_mut(&labeled_ntpr) {
                Some(meta) => {
                    // Avoid overwriting a binary manifest with a JSON manifest
                    if meta.head_manifest.is_none()
                        || meta.head_manifest.is_some() && key.ends_with(".bin")
                    {
                        meta.head_manifest = Some(manifest);
                    }
                }
                None => {
                    self.partition_manifests.insert(
                        labeled_ntpr,
                        PartitionMetadata {
                            head_manifest: Some(manifest),
                            archive_manifests: vec![],
                        },
                    );
                }
            }
        } else {
            warn!("Malformed partition manifest key {}", key);
            self.anomalies.malformed_manifests.insert(key.to_string());
        }
        Ok(())
    }

    async fn ingest_archive_manifest(
        &mut self,
        key: &str,
        body: bytes::Bytes,
    ) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref PARTITION_MANIFEST_KEY: Regex =
                Regex::new("([-a-f0-9]+)/meta/([^]]+)/([^]]+)/(\\d+)_(\\d+)/manifest.(?:json|bin)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)").unwrap();
        }
        if let Some(grps) = PARTITION_MANIFEST_KEY.captures(key) {
            let prefix = grps.get(1).unwrap().as_str().to_string();
            // Group::get calls are safe to unwrap() because regex always has those groups if it matched
            let ns = grps.get(2).unwrap().as_str().to_string();
            let topic = grps.get(3).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(4).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(5).unwrap().as_str().parse::<i64>().unwrap();

            let base_offset = grps.get(6).unwrap().as_str().parse::<u64>().unwrap();
            let committed_offset = grps.get(7).unwrap().as_str().parse::<u64>().unwrap();
            let base_kafka_offset = grps.get(8).unwrap().as_str().parse::<u64>().unwrap();
            let next_kafka_offset = grps.get(9).unwrap().as_str().parse::<u64>().unwrap();
            let base_ts = grps.get(10).unwrap().as_str().parse::<u64>().unwrap();
            let last_ts = grps.get(11).unwrap().as_str().parse::<u64>().unwrap();

            let ntpr = NTPR {
                ntp: NTP {
                    namespace: ns,
                    topic,
                    partition_id,
                },
                revision_id: partition_revision,
            };
            let labeled_ntpr = LabeledNTPR::from_ntpr_and_prefix(ntpr, prefix);

            // Note: assuming memory is sufficient for manifests
            debug!(
                "Storing archive manifest for {} from key {}",
                labeled_ntpr, key
            );

            let manifest: PartitionManifest = match Self::decode_partition_manifest(key, body) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Error parsing partition archive manifest {}: {:?}", key, e);
                    self.anomalies.malformed_manifests.insert(key.to_string());
                    // This is OK because we cleanly logged anomaly.
                    return Ok(());
                }
            };

            let archive_manifest = ArchivePartitionManifest {
                manifest,
                base_offset,
                committed_offset,
                base_kafka_offset,
                next_kafka_offset,
                base_ts,
                last_ts,
            };

            // Note: assuming memory is sufficient for manifests
            match self.partition_manifests.get_mut(&labeled_ntpr) {
                Some(meta) => {
                    meta.archive_manifests.push(archive_manifest);
                }
                None => {
                    self.partition_manifests.insert(
                        labeled_ntpr,
                        PartitionMetadata {
                            head_manifest: None,
                            archive_manifests: vec![archive_manifest],
                        },
                    );
                }
            }
        } else {
            warn!("Malformed partition archive manifest key {}", key);
            self.anomalies.malformed_manifests.insert(key.to_string());
        }
        Ok(())
    }

    fn ingest_controller_snapshot(&mut self, key: &str) {
        lazy_static! {
            static ref CONTROLLER_SNAPSHOT_KEY: Regex =
                Regex::new("cluster_metadata/([-a-f0-9]+)/[^/]+/controller.snapshot").unwrap();
        }
        if let Some(grps) = CONTROLLER_SNAPSHOT_KEY.captures(key) {
            let cluster_uuid = grps.get(1).unwrap().as_str().to_string();
            let cluster_meta = self.cluster_metadata.entry(cluster_uuid).or_default();
            cluster_meta.controller_snapshots.insert(key.to_string());
        } else {
            self.anomalies
                .malformed_controller_snapshot
                .push(key.to_string());
        }
    }

    async fn ingest_cluster_metadata_manifest(
        &mut self,
        key: &str,
    ) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref CLUSTER_METADATA_MANIFEST_KEY: Regex =
                Regex::new("cluster_metadata/([-a-f0-9]+)/manifests/([^]]+)/cluster_manifest.json")
                    .unwrap();
        }
        if let Some(grps) = CLUSTER_METADATA_MANIFEST_KEY.captures(key) {
            let cluster_uuid = grps.get(1).unwrap().as_str().to_string();
            let meta_id_res = grps.get(2).unwrap().as_str().parse::<i64>();
            let meta_id = if let Ok(id) = meta_id_res {
                id
            } else {
                warn!("Malformed cluster metadata manifest metadata ID {}", key);
                self.anomalies
                    .malformed_cluster_manifests
                    .push(key.to_string());
                return Ok(());
            };

            let cluster_meta = self.cluster_metadata.entry(cluster_uuid).or_default();
            let path = object_store::path::Path::from(key);
            let body = self.client.get(&path).await?.bytes().await?;
            if let Ok(manifest) = serde_json::from_slice::<ClusterMetadataManifest>(&body) {
                cluster_meta.manifests.insert(meta_id, manifest);
            } else {
                warn!("Error parsing JSON cluster metadata manifest {}", key);
                self.anomalies
                    .malformed_cluster_manifests
                    .push(key.to_string());
            }
        } else {
            warn!("Malformed cluster metadata manifest key {}", key);
            self.anomalies
                .malformed_cluster_manifests
                .push(key.to_string());
        }
        Ok(())
    }

    async fn ingest_topic_manifest(
        &mut self,
        key: &str,
        body: bytes::Bytes,
    ) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref TOPIC_MANIFEST_KEY: Regex =
                Regex::new("[a-f0-9]+/meta/([^]]+)/([^]]+)/topic_manifest.json").unwrap();
        }
        if let Some(grps) = TOPIC_MANIFEST_KEY.captures(key) {
            // Group::get calls are safe to unwrap() because regex always has those groups if it matched
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();

            if let Ok(manifest) = serde_json::from_slice::<TopicManifest>(&body) {
                let ntr = NTR {
                    namespace: ns,
                    topic,
                    revision_id: manifest.revision_id as i64,
                };
                // TODO: this tool doesn't support binary format manifests at all.
                let labeled_ntr = LabeledNTR {
                    ntr: ntr.clone(),
                    label: None,
                };

                debug!(
                    "Storing topic manifest for {} from key {}",
                    &labeled_ntr, key
                );

                if self.topic_manifests.insert(labeled_ntr, manifest).is_some() {
                    warn!("Two topic manifests for same NTR seen ({})", key);
                }
            } else {
                warn!("Error parsing JSON topic manifest {}", key);
                self.anomalies
                    .malformed_topic_manifests
                    .insert(key.to_string());
            }
        } else {
            warn!("Malformed topic manifest key {}", key);
            self.anomalies
                .malformed_topic_manifests
                .insert(key.to_string());
        }
        Ok(())
    }

    async fn ingest_topic_lifecycle_marker(
        &mut self,
        key: &str,
        body: bytes::Bytes,
    ) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref LIFECYCLE_MARKER_KEY: Regex =
                Regex::new("[a-f0-9]+/meta/([^]]+)/([^]]+)/(\\d+)_lifecycle.bin").unwrap();
        }
        if let Some(grps) = LIFECYCLE_MARKER_KEY.captures(key) {
            // Group::get calls are safe to unwrap() because regex always has those groups if it matched
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            let initial_revision = grps
                .get(3)
                .unwrap()
                .as_str()
                .to_string()
                .parse::<u64>()
                .unwrap();

            let ntr = NTR {
                namespace: ns,
                topic,
                revision_id: initial_revision as i64,
            };
            // TODO: this tool doesn't support binary format manifests at all.
            let labeled_ntr = LabeledNTR {
                ntr: ntr.clone(),
                label: None,
            };

            let mut cursor = std::io::Cursor::new(body.as_ref());
            let marker = match LifecycleMarker::from_bytes(&mut cursor) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Error parsing lifecycle marker {}: {:?}", key, e);
                    self.anomalies
                        .malformed_lifecycle_markers
                        .insert(key.to_string());
                    return Ok(());
                }
            };

            debug!("Storing lifecycle marker for {} from key {}", ntr, key);

            if self.lifecycle_markers.insert(labeled_ntr, marker).is_some() {
                warn!("Two lifecycle markers for same NTR seen ({})", key);
            }
        } else {
            warn!("Malformed lifecycle marker key {}", key);
            self.anomalies
                .malformed_lifecycle_markers
                .insert(key.to_string());
        }
        Ok(())
    }

    fn ingest_segment(&mut self, key: &str, filter: &NTPFilter, object_size: u64) {
        lazy_static! {
            // e.g. 8606-92-v1.log.92
            // TODO: combine into one regex
            static ref SEGMENT_V1_KEY: Regex = Regex::new(
                "[a-f0-9]+/([^]]+)/([^]]+)/(\\d+)_(\\d+)/(\\d+)-(\\d+)-v1.log.(\\d+)"
            )
            .unwrap();
        }

        lazy_static! {
            static ref SEGMENT_KEY: Regex = Regex::new(
                "([-a-f0-9]+)/([^]]+)/([^]]+)/(\\d+)_(\\d+)/(\\d+)-(\\d+)-(\\d+)-(\\d+)-v1.log.(\\d+)"
            )
            .unwrap();
        }
        let (lntpr, segment) = if let Some(grps) = SEGMENT_KEY.captures(key) {
            let prefix = grps.get(1).unwrap().as_str().to_string();
            let ns = grps.get(2).unwrap().as_str().to_string();
            let topic = grps.get(3).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(4).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(5).unwrap().as_str().parse::<i64>().unwrap();
            let start_offset = grps.get(6).unwrap().as_str().parse::<RawOffset>().unwrap();
            let _committed_offset = grps.get(7).unwrap().as_str();
            let size_bytes = grps.get(8).unwrap().as_str().parse::<u64>().unwrap();
            let original_term = grps.get(9).unwrap().as_str().parse::<RaftTerm>().unwrap();
            let upload_term = grps.get(10).unwrap().as_str().parse::<RaftTerm>().unwrap();
            debug!(
                "ingest_segment v2+ {}/{}/{} {} (key {}",
                ns, topic, partition_id, start_offset, key
            );

            let ntpr = NTPR {
                ntp: NTP {
                    namespace: ns,
                    topic,
                    partition_id,
                },
                revision_id: partition_revision,
            };
            let labeled_ntpr = LabeledNTPR::from_ntpr_and_prefix(ntpr, prefix);
            if !filter.match_ntp(&labeled_ntpr.ntpr.ntp) {
                return;
            }

            (
                labeled_ntpr,
                SegmentObject {
                    key: key.to_string(),
                    base_offset: start_offset,
                    upload_term,
                    original_term,
                    size_bytes,
                },
            )
        } else if let Some(grps) = SEGMENT_V1_KEY.captures(key) {
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(4).unwrap().as_str().parse::<i64>().unwrap();
            let start_offset = grps.get(5).unwrap().as_str().parse::<RawOffset>().unwrap();
            let original_term = grps.get(6).unwrap().as_str().parse::<RaftTerm>().unwrap();
            let upload_term = grps.get(7).unwrap().as_str().parse::<RaftTerm>().unwrap();
            debug!(
                "ingest_segment v1 {}/{}/{} {} (key {}",
                ns, topic, partition_id, start_offset, key
            );

            let ntpr = NTPR {
                ntp: NTP {
                    namespace: ns,
                    topic,
                    partition_id,
                },
                revision_id: partition_revision,
            };
            let labeled_ntpr = LabeledNTPR { ntpr, label: None };

            if !filter.match_ntp(&labeled_ntpr.ntpr.ntp) {
                return;
            }

            (
                labeled_ntpr,
                SegmentObject {
                    key: key.to_string(),
                    base_offset: start_offset,
                    upload_term,
                    original_term,
                    // V1 segment name omits size, use the size from the object store listing
                    size_bytes: object_size,
                },
            )
        } else {
            debug!("Ignoring non-segment-like key {}", key);
            self.anomalies.unknown_keys.insert(key.to_string());
            return;
        };

        let values = self
            .partitions
            .entry(lntpr)
            .or_insert_with(|| PartitionObjects::new());
        values.push(segment);
    }
}
