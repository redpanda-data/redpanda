use crate::bucket_reader::{BucketReader, MetadataGap, SegmentObject};
use crate::error::BucketReaderError;
use crate::fundamental::{DeltaOffset, LabeledNTPR, RaftTerm, RawOffset, NTPR};
use crate::remote_types::{
    segment_shortname, PartitionManifest, PartitionManifestSegment, RemoteLabel,
};
use log::{info, warn};
use serde::Serialize;

#[derive(Serialize)]
pub struct DataAddNullSegment {
    pub key: String,
    pub object_key: String,
    pub data_records: u64,
    pub non_data_records: u64,
    pub body: PartitionManifestSegment,
}

#[derive(Serialize)]
pub struct ManifestEditAddSegment {
    pub key: String,
    pub body: PartitionManifestSegment,
}

/// None in this struct means "do not change".  It does not mean reset the
/// field to None.
#[derive(Serialize)]
pub struct ManifestSegmentDiff {
    pub delta_offset: Option<DeltaOffset>,
    pub delta_offset_end: Option<DeltaOffset>,
    pub base_offset: Option<RawOffset>,
    pub committed_offset: Option<RawOffset>,
    pub segment_term: Option<RaftTerm>,
    pub archiver_term: Option<RaftTerm>,
    pub size_bytes: Option<u64>,
}

impl ManifestSegmentDiff {
    fn apply(&self, seg: &mut PartitionManifestSegment) {
        if let Some(v) = self.delta_offset {
            seg.delta_offset = Some(v);
        }

        if let Some(v) = self.delta_offset_end {
            seg.delta_offset_end = Some(v);
        }

        if let Some(v) = self.base_offset {
            seg.base_offset = v;
        }

        if let Some(v) = self.committed_offset {
            seg.committed_offset = v;
        }

        if let Some(v) = self.segment_term {
            seg.segment_term = Some(v);
        }

        if let Some(v) = self.archiver_term {
            seg.archiver_term = v;
        }

        if let Some(v) = self.size_bytes {
            seg.size_bytes = v as i64;
        }
    }
}

#[derive(Serialize)]
pub struct ManifestEditAlterSegment {
    pub old_key: String,
    pub new_key: String,
    pub diff: ManifestSegmentDiff,
}

#[derive(Serialize)]
pub enum RepairEdit {
    AddSegment(ManifestEditAddSegment),
    AlterSegment(ManifestEditAlterSegment),
    AddNullSegment(DataAddNullSegment),
}

/// Apply repairs to in-memory state
pub fn project_repairs(manifest: &mut PartitionManifest, repairs: &Vec<RepairEdit>) {
    for r in repairs {
        match r {
            RepairEdit::AddSegment(_) => todo!(),
            RepairEdit::AddNullSegment(_) => todo!(),
            RepairEdit::AlterSegment(alter) => {
                // TODO: make safer against empty segments
                let segments = &mut manifest.segments;
                let mut value = segments.remove(&alter.old_key).unwrap();
                alter.diff.apply(&mut value);
                segments.insert(alter.new_key.clone(), value);
            }
        }
    }
}

/// Substitute segments into manifest if they appear to solve metadata gaps
pub async fn maybe_adjust_manifest(
    ntpr: &LabeledNTPR,
    gaps: &Vec<MetadataGap>,
    bucket_reader: &BucketReader,
) -> Result<Vec<RepairEdit>, BucketReaderError> {
    let metadata_opt = bucket_reader.partition_manifests.get(ntpr);
    let manifest = if let Some(metadata) = metadata_opt {
        if let Some(manifest) = &metadata.head_manifest {
            manifest
        } else {
            warn!("No head manifest found for NTP {}", ntpr);
            return Ok(vec![]);
        }
    } else {
        warn!("No metadata found for NTP {}", ntpr);
        return Ok(vec![]);
    };

    let objects = if let Some(o) = bucket_reader.partitions.get(&ntpr) {
        o
    } else {
        warn!("No objects found for NTP {}", ntpr);
        return Ok(Vec::new());
    };

    let mut repairs: Vec<RepairEdit> = Vec::new();

    for gap in gaps {
        let prev_seg = gap.prev_seg_base;

        // unwrap() is safe because a gap will only be reported if there is a previous
        // segment's metadata
        let current_metadata = manifest.get_segment_by_offset(prev_seg).unwrap();

        let remote_label = RemoteLabel::from_string(&ntpr.label);
        let current_segment = SegmentObject {
            key: manifest
                .segment_key(current_metadata, &remote_label)
                .unwrap(),
            size_bytes: current_metadata.size_bytes as u64,
            base_offset: current_metadata.base_offset as i64,
            upload_term: current_metadata.archiver_term as RaftTerm,
            original_term: current_metadata.segment_term.unwrap() as RaftTerm,
        };

        // Check if the current segment at this offset provides enough offsets to fill the gap
        let ground_truth = bucket_reader
            .summarize_data_segment(&current_segment, current_metadata.delta_offset.unwrap_or(0))
            .await?;

        if ground_truth.committed_offset != current_metadata.committed_offset as RawOffset {
            let alteration = ManifestEditAlterSegment {
                old_key: segment_shortname(
                    current_metadata.base_offset as RawOffset,
                    current_metadata.segment_term.unwrap() as RaftTerm,
                ),
                new_key: segment_shortname(
                    current_metadata.base_offset as RawOffset,
                    current_metadata.segment_term.unwrap() as RaftTerm,
                ),
                diff: ManifestSegmentDiff {
                    committed_offset: Some(ground_truth.committed_offset),
                    delta_offset_end: Some(ground_truth.delta_end),
                    size_bytes: Some(ground_truth.size_bytes),
                    base_offset: None,
                    delta_offset: None,
                    segment_term: None,
                    archiver_term: None,
                },
            };
            repairs.push(RepairEdit::AlterSegment(alteration));
            info!(
                "[{}] Discovered segment is longer than manifest claimed ({} {} > {})",
                ntpr,
                current_segment.base_offset,
                ground_truth.committed_offset,
                current_metadata.committed_offset
            );
        }

        if ground_truth.committed_offset < gap.next_seg_base - 1 {
            // The current segment in the manifest is not long enough
            // to get past the gap.  See if any other segments not linked
            // into the manifest are
            let candidate_replacements = objects.find_dropped(prev_seg);
            for replacement in candidate_replacements {
                let ground_truth = bucket_reader
                    .summarize_data_segment(&replacement, current_metadata.delta_offset.unwrap())
                    .await?;

                // TODO: replacement might have different sname_format

                if ground_truth.committed_offset >= gap.next_seg_base - 1 {
                    info!(
                        "[{}] Found segment that fills gap! New seg: {}",
                        ntpr, replacement.key
                    );
                    let alteration = ManifestEditAlterSegment {
                        old_key: segment_shortname(
                            current_metadata.base_offset as RawOffset,
                            current_metadata.segment_term.unwrap() as RaftTerm,
                        ),
                        new_key: segment_shortname(
                            replacement.base_offset as RawOffset,
                            replacement.original_term as RaftTerm,
                        ),
                        diff: ManifestSegmentDiff {
                            delta_offset: None,
                            delta_offset_end: Some(ground_truth.delta_end),
                            base_offset: Some(replacement.base_offset as RawOffset),
                            committed_offset: Some(ground_truth.committed_offset as RawOffset),
                            size_bytes: Some(ground_truth.size_bytes),
                            segment_term: Some(replacement.original_term),
                            archiver_term: Some(replacement.upload_term),
                        },
                    };
                    repairs.push(RepairEdit::AlterSegment(alteration));
                }
            }
        }
    }
    Ok(repairs)
}
