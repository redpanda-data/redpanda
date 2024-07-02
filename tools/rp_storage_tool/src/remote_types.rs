use crate::error::BucketReaderError;
use crate::fundamental::{
    raw_to_kafka, DeltaOffset, KafkaOffset, LabeledNTPR, RaftTerm, RawOffset, Timestamp, NTP, NTPR,
    NTR,
};
use deltafor::envelope::{SerdeEnvelope, SerdeEnvelopeContext};
use deltafor::{DeltaAlg, DeltaDelta, DeltaFORDecoder, DeltaXor};
use lazy_static::lazy_static;
use log::warn;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::collections::HashMap;
use std::marker::PhantomData;
use xxhash_rust::xxh32::xxh32;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteLabel {
    pub cluster_uuid: String,
}

impl RemoteLabel {
    pub fn from_string(s_opt: &Option<String>) -> Option<RemoteLabel> {
        s_opt.as_ref().map(|s| RemoteLabel {
            cluster_uuid: s.clone(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterMetadataManifest {
    pub version: u32,
    pub compat_version: u32,
    pub upload_time_since_epoch: i64,
    pub cluster_uuid: String,
    pub metadata_id: i64,
    pub controller_snapshot_offset: i64,
    pub controller_snapshot_path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionManifestSegment {
    // Mandatory fields: always set, since v22.1.x
    pub base_offset: RawOffset,
    pub committed_offset: RawOffset,
    pub is_compacted: bool,
    pub size_bytes: i64,
    pub archiver_term: RaftTerm,

    // Since v22.1.x, only set if non-default value
    pub delta_offset: Option<DeltaOffset>,
    pub base_timestamp: Option<Timestamp>,
    pub max_timestamp: Option<Timestamp>,
    pub ntp_revision: Option<u64>,

    // Since v22.3.x, only set if != to segment_name_format::v1
    pub sname_format: Option<u32>,

    // Since v22.3.x, always set.
    pub segment_term: Option<RaftTerm>,

    // Since v22.3.x, only set if sname_format==segment_name_format::v2
    pub delta_offset_end: Option<DeltaOffset>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(into = "PartitionManifestSegment")]
pub struct SpilloverManifestMeta {
    size_bytes: i64,
    start_offset: RawOffset,
    last_offset: RawOffset,
    start_timestamp: Timestamp,
    last_timestamp: Timestamp,
    delta_offset: DeltaOffset,
    delta_offset_end: DeltaOffset,
    ntp_revision: u64,
    start_term: RaftTerm,
    last_term: RaftTerm,
}

impl From<SpilloverManifestMeta> for PartitionManifestSegment {
    fn from(spill: SpilloverManifestMeta) -> Self {
        PartitionManifestSegment {
            base_offset: spill.start_offset,
            committed_offset: spill.last_offset,
            is_compacted: false,
            size_bytes: spill.size_bytes,
            archiver_term: spill.start_term,
            delta_offset: Some(spill.delta_offset),
            base_timestamp: Some(spill.start_timestamp),
            max_timestamp: Some(spill.last_timestamp),
            ntp_revision: Some(spill.ntp_revision),
            sname_format: Some(3),
            segment_term: Some(spill.last_term),
            delta_offset_end: Some(spill.delta_offset_end),
        }
    }
}

impl TryFrom<PartitionManifestSegment> for SpilloverManifestMeta {
    type Error = BucketReaderError;

    fn try_from(seg: PartitionManifestSegment) -> Result<Self, Self::Error> {
        let sname_format = seg.sname_format.ok_or(Self::Error::SyntaxError(format!(
            "sname_format not present in {seg:?}"
        )))?;

        // Spillover manifests encoded in segment meta should be segment_name_format::v3
        if sname_format != 3 {
            return Err(Self::Error::SyntaxError(format!(
                "expected segment_name_format::v3 in {seg:?}"
            )));
        }

        Ok(SpilloverManifestMeta {
            size_bytes: seg.size_bytes,
            start_offset: seg.base_offset,
            last_offset: seg.committed_offset,
            start_timestamp: seg.base_timestamp.ok_or(Self::Error::SyntaxError(format!(
                "base_timestamp not present in {seg:?}"
            )))?,
            last_timestamp: seg.max_timestamp.ok_or(Self::Error::SyntaxError(format!(
                "base_timestamp not present in {seg:?}"
            )))?,
            delta_offset: seg.delta_offset.ok_or(Self::Error::SyntaxError(format!(
                "delta_offset not present in {seg:?}"
            )))?,
            delta_offset_end: seg
                .delta_offset_end
                .ok_or(Self::Error::SyntaxError(format!(
                    "delta_offset_end not present in {seg:?}"
                )))?,
            ntp_revision: seg.ntp_revision.ok_or(Self::Error::SyntaxError(format!(
                "ntp_revision not present in {seg:?}"
            )))?,
            start_term: seg.archiver_term,
            last_term: seg.segment_term.ok_or(Self::Error::SyntaxError(format!(
                "segment term not present in {seg:?}"
            )))?,
        })
    }
}

struct ColumnReader<A: DeltaAlg + 'static> {
    values: Vec<i64>,
    marker: PhantomData<&'static A>,
}

/// Legagcy manifest JSON format stores segments in maps where the key
/// is derived from the segment using this mapping.  This is equivalent
/// to SegmentNameFormat::V1
pub fn segment_shortname(base_offset: RawOffset, segment_term: RaftTerm) -> String {
    format!("{}-{}-v1.log", base_offset, segment_term)
}

pub fn parse_segment_shortname(shortname: &str) -> Option<(RawOffset, RaftTerm)> {
    lazy_static! {
        static ref SHORTNAME: Regex = Regex::new("(\\d+)-(\\d+)-v1.log").unwrap();
    }
    if let Some(grps) = SHORTNAME.captures(shortname) {
        let base_offset = grps.get(1).unwrap().as_str().parse::<i64>().unwrap();
        let segment_term = grps.get(2).unwrap().as_str().parse::<i64>().unwrap();
        Some((base_offset as RawOffset, segment_term as RaftTerm))
    } else {
        None
    }
}

impl<A: DeltaAlg> ColumnReader<A> {
    /// May panic: caller is responsible for ensuring index is within bounds
    pub fn get(&self, i: usize) -> i64 {
        *(self.values.get(i).unwrap())
    }

    fn from(mut cursor: &mut std::io::Cursor<&[u8]>) -> Result<Self, BucketReaderError> {
        let row_width = 16;

        let mut column_values: Vec<i64> = vec![];

        let column_envelope = SerdeEnvelopeContext::from(0, &mut cursor)?;
        assert_eq!(column_envelope.envelope.version, 0);

        let frame_count = read_u32(&mut cursor)?;
        for _frame_i in 0..frame_count {
            let frame_envelope = SerdeEnvelopeContext::from(0, &mut cursor)?;
            assert_eq!(frame_envelope.envelope.version, 0);

            // Read the _head buffer, fixed size array
            let discard_16 = read_u32(&mut cursor)?;
            assert_eq!(discard_16, row_width);
            let mut head_row: Vec<i64> = vec![];
            for _i in 0..row_width {
                let row_i = read_i64(&mut cursor)?;
                head_row.push(row_i);
            }

            // Read the 'tail' object, a deltafor_encoder
            let has_tail = read_bool(&mut cursor)?;

            let mut frame_values = if has_tail {
                // This is the bulk of the data in the frame
                let encoder_envelope = SerdeEnvelopeContext::from(0, &mut cursor)?;

                // TODO: generalize to make use_nttp_deltastep optional,
                // it is always true for partition manifests, and it
                // determines which fields are encoded

                // Encoder has fields
                // _initial, _last, _data, _cnt);

                // The initial value required to decode the deltas in `data`
                let initial = read_i64(&mut cursor)?;

                // Last is an optimization for readers, we do not need it when exhaustively
                // decoding the array from the start.
                let _last = read_i64(&mut cursor)?;

                // Raw DeltaFOR encoded buffer
                let data = read_iobuf(&mut cursor)?;

                // Number of row_width wide rows stored in `data`
                let cnt = read_u32(&mut cursor)?;

                encoder_envelope.end(cursor);

                let mut decoder = DeltaFORDecoder::<A>::new(cnt as u64, initial, None);
                // TODO: read inline instead of copying out to a Vec<u8> first
                let mut decoder_cursor = std::io::Cursor::new(data.as_slice());
                let mut decoded_values: Vec<i64> = vec![];
                for _ in 0..cnt {
                    let mut buf: [i64; 16] = [0; 16];
                    decoder.read_row(&mut buf, &mut decoder_cursor)?;
                    for j in 0..row_width as usize {
                        decoded_values.push(buf[j]);
                    }
                }

                decoded_values
            } else {
                vec![]
            };

            let frame_size = read_u64(&mut cursor)?;

            // The values in the head row are only readable if the main encoded
            // body is too short to fulfil the expected frame size.
            if frame_values.len() < frame_size as usize {
                frame_values.extend_from_slice(&head_row);
            }

            if frame_size as usize > frame_values.len() {
                return Err(BucketReaderError::SyntaxError(format!(
                    "Decode values list too short {} (vs frame size {})",
                    frame_values.len(),
                    frame_size
                )));
            }
            frame_values.truncate(frame_size as usize);

            let has_last_row = read_bool(&mut cursor)?;
            if has_last_row {
                // last_row is a deltafor_stream_pos_t
                let _ignore = DeltaFORStreamPos::from(&mut cursor);
            }

            frame_envelope.end(cursor);

            column_values.extend_from_slice(&frame_values);

            continue;
        }

        column_envelope.end(cursor);

        Ok(Self {
            values: column_values,
            marker: PhantomData,
        })
    }
}

pub fn decode_colstore(
    buf: Vec<u8>,
) -> Result<HashMap<String, PartitionManifestSegment>, BucketReaderError> {
    // Segments binary format:
    // Envelope: segment_meta_cstore::impl
    // Envelope: column store
    // Columns:
    // gauge_col_t _is_compacted{};
    // gauge_col_t _size_bytes{};
    // counter_col_t _base_offset{};
    // gauge_col_t _committed_offset{};
    // gauge_col_t _base_timestamp{};
    // gauge_col_t _max_timestamp{};
    // gauge_col_t _delta_offset{};
    // gauge_col_t _ntp_revision{};
    // /// The archiver term is not strictly monotonic in manifests
    // /// generated by old redpanda versions
    // gauge_col_t _archiver_term{};
    // gauge_col_t _segment_term{};
    // gauge_col_t _delta_offset_end{};
    // gauge_col_t_sname_format{};
    // gauge_col_t _metadata_size_hint{};

    // gauge columns are int64_xor
    // counter columns are int64_delta

    // Other fields in column store:
    //    _hints
    //     using hint_t = deltafor_stream_pos_t<int64_t>;
    //     using hint_vec_t = std::array<hint_t, 13>;
    //     absl::btree_map<int64_t, std::optional<hint_vec_t>, greater>;

    // Each column is also an envelope
    //  Then
    //   - u32 number of frames
    //   - frames, whose encoding depends on the algo

    // Each frame is like:
    // buffer_depth is a constant, 16.
    // std::array<value_t, buffer_depth> _head{};
    // std::optional<encoder_t> _tail{std::nullopt};
    // size_t _size{0};
    // std::optional<hint_t> _last_row{std::nullopt};

    // The "tail" is where the bulk of the data lives.
    // Each tail is ALSO an envelope (deltafor_encoder):
    //

    let mut cursor = std::io::Cursor::new(buf.as_slice());
    let segment_meta_cstore_envelope = SerdeEnvelopeContext::from(0, &mut cursor)?;
    let column_store_envelope = SerdeEnvelopeContext::from(0, &mut cursor)?;

    // TODO: define error type for not-understood version (this tool should
    // always be newer code than the data it is analyzing, so should always
    // understand the latest versions)
    assert_eq!(segment_meta_cstore_envelope.envelope.version, 0);
    assert_eq!(column_store_envelope.envelope.version, 0);

    let is_compacted: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;
    let size_bytes: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;
    let base_offset: ColumnReader<DeltaDelta> = ColumnReader::from(&mut cursor)?;
    let committed_offset: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;
    let base_timestamp: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;
    let max_timestamp: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;
    let delta_offset: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;
    let ntp_revision: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;
    let archiver_term: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;
    let segment_term: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;
    let delta_offset_end: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;
    let sname_format: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;
    let _metadata_size_hint: ColumnReader<DeltaXor> = ColumnReader::from(&mut cursor)?;

    let hint_map_size = read_u32(&mut cursor)?;
    for _hint_i in 0..hint_map_size {
        read_i64(&mut cursor)?;
        let opt_set = read_bool(&mut cursor)?;
        if opt_set {
            // One deltafor_stream_pos_t per column
            let hint_col_count = read_u32(&mut cursor)?;
            for _hint_col_i in 0..hint_col_count {
                let _ignore = DeltaFORStreamPos::from(&mut cursor);
            }
        }
    }

    column_store_envelope.end(&cursor);
    segment_meta_cstore_envelope.end(&cursor);

    let mut segment_map = HashMap::new();
    for i in 0..is_compacted.values.len() {
        let seg_base_offset = base_offset.get(i);
        let seg_segment_term = segment_term.get(i);
        let shortname =
            segment_shortname(seg_base_offset as RawOffset, seg_segment_term as RaftTerm);

        // For optional fields which are represented as uint64_t minimum value when
        // they are unset: these are unset when the cluster's metadata pre-dates the version
        // when the field was added.
        fn some_if_positive<T: TryFrom<i64>>(v: i64) -> Option<T> {
            if v >= 0 {
                T::try_from(v).ok()
            } else {
                None
            }
        }

        segment_map.insert(
            shortname,
            PartitionManifestSegment {
                base_offset: seg_base_offset,
                committed_offset: committed_offset.get(i),
                is_compacted: is_compacted.get(i) == 1,
                size_bytes: size_bytes.get(i),
                archiver_term: archiver_term.get(i),
                delta_offset: some_if_positive(delta_offset.get(i)),
                base_timestamp: some_if_positive(base_timestamp.get(i)),
                max_timestamp: some_if_positive(max_timestamp.get(i)),
                ntp_revision: some_if_positive(ntp_revision.get(i)),
                sname_format: some_if_positive(sname_format.get(i)),
                segment_term: some_if_positive(segment_term.get(i)),
                delta_offset_end: some_if_positive(delta_offset_end.get(i)),
            },
        );
    }

    return Ok(segment_map);
}

#[repr(u8)]
pub enum SegmentNameFormat {
    V1 = 1,
    V2 = 2,
    V3 = 3,
}

#[repr(u8)]
pub enum PartitionManifestFormat {
    V1 = 1,
    // >=23.2 serde-enabled format
    V2 = 2,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionManifest {
    // Mandatory fields: always set, since v22.1.x
    pub version: u32, // This field is explicit in JSON, in serde it is the envelope version
    pub namespace: String,
    pub topic: String,
    pub partition: u32,
    pub revision: i64,
    pub last_offset: i64,

    // Since v22.1.x, only Some if collection has length >= 1
    // `segments` is logically a vector, but stored as a map for convenient conversion with
    // legacy JSON encoding which uses a map.
    #[serde(default)]
    pub segments: HashMap<String, PartitionManifestSegment>,

    // >> Since v22.3.x, only set if non-default value
    #[serde(skip_serializing_if = "offset_has_default_value")]
    pub insync_offset: Option<i64>,
    #[serde(skip_serializing_if = "offset_has_default_value")]
    pub last_uploaded_compacted_offset: Option<i64>,
    #[serde(skip_serializing_if = "offset_has_default_value")]
    pub start_offset: Option<i64>,
    // `replaced` is logically a vector, but stored as a map for convenient conversion with
    // legacy JSON encoding which uses a map.
    pub replaced: Option<HashMap<String, LwSegment>>, // When decoding JSON, this is only set if non-empty
    // << Since v22.3.x

    // >> Since v23.2.x, in manifest format v2
    pub cloud_log_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "offset_has_default_value")]
    pub archive_start_offset: Option<i64>,
    #[serde(skip_serializing_if = "offset_has_default_value")]
    pub archive_start_offset_delta: Option<i64>,
    #[serde(skip_serializing_if = "offset_has_default_value")]
    pub archive_clean_offset: Option<i64>,
    #[serde(skip_serializing_if = "offset_has_default_value")]
    pub start_kafka_offset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive_size_bytes: Option<u64>, // << Since v23.2.x

    #[serde(default)] // If missing, deserialize as an empty Vec
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub spillover: Vec<SpilloverManifestMeta>,
}

impl PartitionManifest {
    fn get_sorted_segments(&self) -> Vec<&PartitionManifestSegment> {
        // TODO we should just hold segments in sorted order (currently a hashmap)
        if self.segments.is_empty() {
            vec![]
        } else {
            let mut segments: Vec<&PartitionManifestSegment> = self.segments.values().collect();
            segments.sort_by_key(|s| s.base_offset);
            segments
        }
    }

    pub fn start_offsets(&self) -> (RawOffset, KafkaOffset) {
        let segments = self.get_sorted_segments();
        if segments.is_empty() {
            // We cannot report a Kafka offset because an empty segment
            // set doesn't provide a delta offset.
            (
                self.start_offset.unwrap_or(0) as RawOffset,
                0 as KafkaOffset,
            )
        } else {
            // Guarantee to exist because we checked for empty at start of fn
            let base_segment = segments.get(0).unwrap();

            (
                base_segment.base_offset as RawOffset,
                raw_to_kafka(
                    base_segment.base_offset,
                    base_segment.delta_offset.unwrap_or(0),
                ),
            )
        }
    }

    pub fn kafka_watermarks(&self) -> Option<(KafkaOffset, KafkaOffset)> {
        let segments = self.get_sorted_segments();
        if segments.is_empty() {
            None
        } else {
            let first = segments.get(0).unwrap();
            let last = segments.get(segments.len() - 1).unwrap();
            if first.delta_offset.is_none() || last.delta_offset_end.is_none() {
                None
            } else {
                let lwm = raw_to_kafka(first.base_offset, first.delta_offset.unwrap());
                let hwm = raw_to_kafka(last.committed_offset + 1, last.delta_offset_end.unwrap());
                Some((lwm as KafkaOffset, hwm as KafkaOffset))
            }
        }
    }

    pub fn get_size_bytes(&self) -> u64 {
        if let Some(csb) = self.cloud_log_size_bytes {
            if let Some(asb) = self.archive_size_bytes {
                csb + asb
            } else {
                csb
            }
        } else {
            let mut n: u64 = 0;
            for s in self.segments.values() {
                n += s.size_bytes as u64;
            }
            n
        }
    }

    pub fn get_segment(
        &self,
        base_offset: RawOffset,
        segment_term: RaftTerm,
    ) -> Option<&PartitionManifestSegment> {
        let shortname = segment_shortname(base_offset, segment_term);
        self.segments.get(shortname.as_str())
    }

    // FIXME: slow path because the segment map is indexed by shortname, which
    // requires caller to know term.  That map key is only there for legacy
    // json compat.
    pub fn get_segment_by_offset(
        &self,
        base_offset: RawOffset,
    ) -> Option<&PartitionManifestSegment> {
        for seg in self.segments.values() {
            if seg.base_offset == base_offset {
                return Some(seg);
            }
        }

        None
    }
}

fn offset_has_default_value(offset: &Option<i64>) -> bool {
    match offset {
        None => true,
        Some(o) if *o == i64::MIN => true,
        _ => false,
    }
}

fn read_string(mut cursor: &mut dyn std::io::Read) -> Result<String, BucketReaderError> {
    let len = read_u32(&mut cursor)?;
    let mut bytes: Vec<u8> = vec![0; len as usize];
    cursor.read_exact(bytes.as_mut_slice())?;
    Ok(String::from_utf8(bytes).unwrap())
}
fn read_u64(cursor: &mut dyn std::io::Read) -> Result<u64, BucketReaderError> {
    let mut raw: [u8; 8] = [0; 8];
    cursor.read_exact(&mut raw)?;
    Ok(u64::from_le_bytes(raw))
}

fn read_i64(cursor: &mut dyn std::io::Read) -> Result<i64, BucketReaderError> {
    let mut raw: [u8; 8] = [0; 8];
    cursor.read_exact(&mut raw)?;
    Ok(i64::from_le_bytes(raw))
}

fn read_u32(cursor: &mut dyn std::io::Read) -> Result<u32, BucketReaderError> {
    let mut raw: [u8; 4] = [0; 4];
    cursor.read_exact(&mut raw)?;
    Ok(u32::from_le_bytes(raw))
}

fn read_bool(cursor: &mut dyn std::io::Read) -> Result<bool, BucketReaderError> {
    let mut raw: [u8; 1] = [0; 1];
    cursor.read_exact(&mut raw)?;
    match raw[0] {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(BucketReaderError::SyntaxError(
            "Invalid bool byte".to_string(),
        )),
    }
}

fn read_iobuf(mut cursor: &mut dyn std::io::Read) -> Result<Vec<u8>, BucketReaderError> {
    let len = read_u32(&mut cursor)?;
    let mut bytes: Vec<u8> = vec![0; len as usize];
    cursor.read_exact(bytes.as_mut_slice())?;
    Ok(bytes)
}

fn decode_envelope<T, F>(
    cursor: &mut std::io::Cursor<&[u8]>,
    my_version: u8,
    f: F,
) -> Result<T, BucketReaderError>
where
    F: FnOnce(&SerdeEnvelopeContext, &mut std::io::Cursor<&[u8]>) -> Result<T, BucketReaderError>,
{
    let env = SerdeEnvelopeContext::from(my_version, cursor)?;
    let result = f(&env, cursor);
    if let Err(e) = result {
        warn!(
            "Envelope decode error at cursor position 0x{:08x} ({:?})",
            cursor.position(),
            e
        );
        Err(e)
    } else {
        env.end(cursor);
        result
    }
}

pub struct DeltaFORStreamPos {
    _initial: i64,
    _offset: u32,
    _num_rows: u32,
}

impl DeltaFORStreamPos {
    pub fn from(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Self, BucketReaderError> {
        decode_envelope(cursor, 0, |_env, mut cursor| {
            let initial = read_i64(&mut cursor)?;
            let offset = read_u32(&mut cursor)?;
            let num_rows = read_u32(&mut cursor)?;
            Ok(Self {
                _initial: initial,
                _offset: offset,
                _num_rows: num_rows,
            })
        })
    }
}

pub trait RpSerde {
    fn from_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Self, BucketReaderError>
    where
        Self: Sized;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LwSegment {
    pub ntp_revision: u64,
    pub base_offset: i64,
    pub committed_offset: u64,
    pub archiver_term: u64,
    pub segment_term: i64,
    pub size_bytes: u64,
    pub sname_format: u32,
}

impl RpSerde for LwSegment {
    fn from_bytes(mut cursor: &mut std::io::Cursor<&[u8]>) -> Result<Self, BucketReaderError> {
        let _envelope = SerdeEnvelope::new().read(&mut cursor)?;
        let ntp_revision = read_u64(&mut cursor)?;
        let base_offset = read_i64(&mut cursor)?;
        let committed_offset = read_u64(&mut cursor)?;
        let archiver_term = read_u64(&mut cursor)?;
        let segment_term = read_i64(&mut cursor)?;
        let size_bytes = read_u64(&mut cursor)?;
        let sname_format = read_u32(&mut cursor)?;

        Ok(LwSegment {
            ntp_revision,
            base_offset,
            committed_offset,
            archiver_term,
            segment_term,
            size_bytes,
            sname_format,
        })

        // TODO: respect envelope + skip any unread bytes
    }
}

fn read_vec<T: RpSerde>(
    mut cursor: &mut std::io::Cursor<&[u8]>,
) -> Result<Vec<T>, BucketReaderError> {
    let len = read_u32(&mut cursor)?;
    let mut result: Vec<T> = vec![];
    result.reserve(len as usize);
    for _ in 0..len {
        result.push(T::from_bytes(cursor)?);
    }
    Ok(result)
}

impl PartitionManifest {
    pub fn new(ntpr: NTPR) -> Self {
        // This initial state is not meant to be a valid manifest: we expect at least one
        // segment insertion to properly initialize offsets.
        Self {
            version: PartitionManifestFormat::V2 as u32,
            namespace: ntpr.ntp.namespace,
            topic: ntpr.ntp.topic,
            partition: ntpr.ntp.partition_id,
            revision: ntpr.revision_id,
            last_offset: 0,
            segments: HashMap::new(),
            insync_offset: None,
            last_uploaded_compacted_offset: Some(i64::MIN),
            start_offset: None,
            replaced: Some(HashMap::new()),
            cloud_log_size_bytes: Some(0),
            archive_start_offset: Some(i64::MIN),
            archive_start_offset_delta: Some(i64::MIN),
            archive_clean_offset: Some(i64::MIN),
            start_kafka_offset: None,
            archive_size_bytes: Some(0),
            spillover: Vec::new(),
        }
    }

    pub fn push(&mut self, segment: PartitionManifestSegment) {
        let shortname = segment_shortname(
            segment.base_offset as i64,
            segment.segment_term.unwrap() as i64,
        );

        fn opt_max(a: &mut Option<i64>, b: i64) {
            match a {
                Some(v) => *v = b.max(*v),
                None => *a = Some(b),
            }
        }

        self.last_offset = self.last_offset.max(segment.committed_offset as i64);
        opt_max(&mut self.insync_offset, segment.committed_offset as i64);
        if segment.is_compacted {
            opt_max(
                &mut self.last_uploaded_compacted_offset,
                segment.committed_offset as i64,
            );
        }

        if self.start_offset.is_none() {
            self.start_offset = Some(segment.base_offset as i64);
        }

        if self.cloud_log_size_bytes.is_none() {
            self.cloud_log_size_bytes = Some(segment.size_bytes as u64);
        } else {
            *(self.cloud_log_size_bytes.as_mut().unwrap()) += segment.size_bytes as u64;
        }

        let replaced = self.segments.insert(shortname, segment);

        // Caller is responsible for managing any colliding segments, if they are
        // passed into this function then the result would be a corrupt manifest.
        // TODO: a nice LogicalError error type instead of an assertion
        assert!(replaced.is_none());
    }

    pub fn contains_segment_shortname(&self, short_name: &str) -> bool {
        self.segments.contains_key(short_name)
    }

    pub fn from_bytes(bytes: bytes::Bytes) -> Result<Self, BucketReaderError> {
        let mut reader = std::io::Cursor::new(bytes.as_ref());
        let envelope = SerdeEnvelope::from(&mut reader)?;

        // model::ntp _ntp;

        let namespace = read_string(&mut reader)?;
        let topic = read_string(&mut reader)?;
        let partition = read_u32(&mut reader)?;

        // model::initial_revision_id _rev;
        let revision = read_i64(&mut reader)?;

        // iobuf _segments_serialized;
        let segments_serialized = read_iobuf(&mut reader)?;

        let segments = decode_colstore(segments_serialized)?;

        let replaced = read_vec::<LwSegment>(&mut reader)?;

        // Convert replaced list to a map, the struct uses a map for convenient encoding to
        // the legacy manifest v1 JSON format which uses a map.
        let replaced_map = replaced
            .into_iter()
            .map(|seg| (segment_shortname(seg.base_offset, seg.segment_term), seg))
            .collect();

        let last_offset = read_i64(&mut reader)?;
        let start_offset = read_i64(&mut reader)?;
        let last_uploaded_compacted_offset = read_i64(&mut reader)?;
        let insync_offset = read_i64(&mut reader)?;

        let cloud_log_size_bytes = read_u64(&mut reader)?;
        let archive_start_offset = read_i64(&mut reader)?;
        let archive_start_offset_delta = read_i64(&mut reader)?;
        let archive_clean_offset = read_i64(&mut reader)?;
        let start_kafka_offset = read_i64(&mut reader)?;

        let archive_size_bytes = if (reader.position() as u32) < envelope.size {
            Some(read_u64(&mut reader)?)
        } else {
            None
        };

        let spillover = if (reader.position() as u32) < envelope.size {
            let spills_deserialized = read_iobuf(&mut reader)?;
            let mut spills_as_segments = decode_colstore(spills_deserialized)?;

            let mut spills: Vec<SpilloverManifestMeta> = spills_as_segments
                .into_values()
                .map(|seg| SpilloverManifestMeta::try_from(seg))
                .collect::<Result<Vec<_>, _>>()?;
            spills.sort_by(|lhs, rhs| lhs.start_offset.cmp(&rhs.start_offset));

            spills
        } else {
            Vec::new()
        };

        Ok(PartitionManifest {
            version: envelope.version as u32,
            namespace,
            topic,
            partition,
            revision,
            last_offset,
            segments,
            replaced: Some(replaced_map),
            insync_offset: Some(insync_offset),
            last_uploaded_compacted_offset: Some(last_uploaded_compacted_offset),
            start_offset: Some(start_offset),
            cloud_log_size_bytes: Some(cloud_log_size_bytes),
            archive_start_offset: Some(archive_start_offset),
            archive_start_offset_delta: Some(archive_start_offset_delta),
            archive_clean_offset: Some(archive_clean_offset),
            start_kafka_offset: Some(start_kafka_offset),
            archive_size_bytes: archive_size_bytes,
            spillover: spillover,
        })

        // TODO: respect envelope + skip any unread bytes
    }

    pub fn ntp(&self) -> NTP {
        NTP {
            namespace: self.namespace.clone(),
            topic: self.topic.clone(),
            partition_id: self.partition,
        }
    }

    pub fn manifest_key(ntpr: &LabeledNTPR, extension: &str) -> String {
        let path = format!(
            "{}/{}/{}_{}",
            ntpr.ntpr.ntp.namespace,
            ntpr.ntpr.ntp.topic,
            ntpr.ntpr.ntp.partition_id,
            ntpr.ntpr.revision_id
        );
        let bitmask = 0xf0000000;
        let hash = xxh32(path.as_bytes(), 0);
        let prefix = if let Some(label) = &ntpr.label {
            label.clone()
        } else {
            format!("{:08x}/meta", hash & bitmask)
        };
        format!("{}/meta/{}/manifest.{}", prefix, path, extension)
    }
    pub fn segment_key(
        &self,
        segment: &PartitionManifestSegment,
        label: &Option<RemoteLabel>,
    ) -> Option<String> {
        let sname_format = match segment.sname_format {
            None => SegmentNameFormat::V1,
            Some(1) => SegmentNameFormat::V1,
            Some(2) => SegmentNameFormat::V2,
            Some(3) => SegmentNameFormat::V3,
            Some(v) => {
                warn!("Unknown segment name format {}", v);
                return None;
            }
        };

        let segment_term = match segment.segment_term {
            Some(t) => t,
            None => {
                // bucket_reader should have cleaned up metadata to substitute segment_term
                // from the key in the manifest dict via parse_segment_shortname: this should
                // not happen.
                warn!("Segment without segment_term set");
                return None;
            }
        };

        let name = match sname_format {
            SegmentNameFormat::V1 => {
                format!("{}-{}-v1.log", segment.base_offset, segment_term)
            }
            SegmentNameFormat::V2 => {
                format!(
                    "{}-{}-{}-{}-v1.log",
                    segment.base_offset, segment.committed_offset, segment.size_bytes, segment_term
                )
            }
            SegmentNameFormat::V3 => {
                format!(
                    "{}-{}-{}-{}-v1.log",
                    segment.base_offset, segment.committed_offset, segment.size_bytes, segment_term
                )
            }
        };

        let path = format!(
            "{}/{}/{}_{}/{}",
            self.namespace, self.topic, self.partition, self.revision, name
        );
        if let Some(remote_label) = label {
            return Some(format!(
                "{}/{}.{}",
                remote_label.cluster_uuid, path, segment.archiver_term
            ));
        }

        let hash = xxh32(path.as_bytes(), 0);

        Some(format!("{:08x}/{}.{}", hash, path, segment.archiver_term))
    }
}

/// Metadata spilled from the head partition manifest: this includes a full manifest of
/// its own, plus additional fields that are encoded in the key
#[derive(Clone, Serialize, Deserialize)]
pub struct ArchivePartitionManifest {
    pub manifest: PartitionManifest,
    pub base_offset: u64,
    pub committed_offset: u64,
    pub base_kafka_offset: u64,
    pub next_kafka_offset: u64,
    pub base_ts: u64,
    pub last_ts: u64,
}

impl ArchivePartitionManifest {
    pub fn key(&self, ntpr: &LabeledNTPR) -> String {
        let path = format!(
            "{}/{}/{}_{}",
            ntpr.ntpr.ntp.namespace,
            ntpr.ntpr.ntp.topic,
            ntpr.ntpr.ntp.partition_id,
            ntpr.ntpr.revision_id
        );
        let bitmask = 0xf0000000;
        let hash = xxh32(path.as_bytes(), 0);
        let prefix = if let Some(label) = &ntpr.label {
            label.clone()
        } else {
            format!("{:08x}/meta", hash & bitmask)
        };
        format!(
            "{}/{}/manifest.json_{}_{}_{}_{}_{}_{}",
            prefix,
            path,
            self.base_offset,
            self.committed_offset,
            self.base_kafka_offset,
            self.next_kafka_offset,
            self.base_ts,
            self.last_ts
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopicManifest {
    pub version: u32,
    pub namespace: String,
    pub topic: String,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub revision_id: u64,
    pub cleanup_policy_bitflags: String,
    // TODO: following fields are null in captured examples...
    pub compaction_strategy: Option<String>,
    pub compression: Option<String>,
    // FIXME (in redpanda): it's not super useful for these to be encoded as "null means
    // default" when that means any cloud reader has to be able to read the cluster
    // configuration in order to interpret that.
    // https://github.com/redpanda-data/redpanda/issues/10667
    pub timestamp_type: Option<String>,
    pub segment_size: Option<u64>,
    pub retention_bytes: Option<u64>,
    pub retention_duration: Option<u64>,
}

impl TopicManifest {
    pub fn manifest_key(ns: &str, topic: &str) -> String {
        let path = format!("{}/{}", ns, topic);
        let hash = xxh32(path.as_bytes(), 0);
        let bitmask = 0xf0000000;

        format!("{:08x}/meta/{}/topic_manifest.json", hash & bitmask, path)
    }
}

#[derive(Eq, PartialEq, Clone, Debug, Serialize_repr, Deserialize_repr)]
#[repr(u8)]
pub enum LifecycleStatus {
    Live = 1,
    Purging = 2,
    Purged = 3,
    Offloaded = 4,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LifecycleMarker {
    /// ID of the cluster that wrote this marker, in case multiple clusters
    // /// are addressing the same bucket
    // ss::sstring cluster_id;
    //
    // /// The unique identify of the topic-revision.  This will also be present
    // /// in the object key, but including it in the body is convenient for
    // /// readers.
    // cluster::nt_revision topic;
    //
    // lifecycle_status status;
    cluster_id: String,
    ntr: NTR,
    pub status: LifecycleStatus,
}

impl RpSerde for LifecycleMarker {
    fn from_bytes(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Self, BucketReaderError> {
        decode_envelope(cursor, 0, |_env, mut cursor| {
            let cluster_id = read_string(&mut cursor)?;

            let ntr = decode_envelope(cursor, 0, |_env, mut cursor| {
                let namespace = read_string(&mut cursor)?;
                let topic = read_string(&mut cursor)?;
                let revision_id = read_i64(&mut cursor)?;
                Ok(NTR {
                    namespace,
                    topic,
                    revision_id,
                })
            })?;

            let status_raw = read_u32(&mut cursor)?;

            let status = match status_raw {
                1 => LifecycleStatus::Live,
                2 => LifecycleStatus::Purging,
                3 => LifecycleStatus::Purged,
                4 => LifecycleStatus::Offloaded,
                _ => {
                    return Err(BucketReaderError::SyntaxError(format!(
                        "Invalid lifecycle status {}",
                        status_raw
                    )))
                }
            };

            Ok(Self {
                cluster_id,
                ntr,
                status,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::env;
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    async fn read_json<T: for<'a> serde::Deserialize<'a>>(path: &str) -> T {
        let cargo_path = env::var("CARGO_MANIFEST_DIR").unwrap();
        let filename = cargo_path + path;
        let mut file = File::open(filename).await.unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).await.unwrap();
        serde_json::from_str(&contents).unwrap()
    }

    async fn read_bytes(path: &str) -> Bytes {
        let cargo_path = env::var("CARGO_MANIFEST_DIR").unwrap();
        let filename = cargo_path + path;
        let mut file = File::open(filename).await.unwrap();
        let mut data: Vec<u8> = vec![];

        file.read_to_end(&mut data).await.unwrap();
        Bytes::from(data)
    }

    async fn read_manifest(path: &str) -> PartitionManifest {
        read_json(path).await
    }

    async fn read_cluster_manifest(path: &str) -> ClusterMetadataManifest {
        read_json(path).await
    }

    #[test_log::test(tokio::test)]
    async fn test_manifest_decode() {
        let manifest = read_manifest("/resources/test/manifest.json").await;
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.segments.len(), 3);
        assert_eq!(manifest.start_offset.unwrap(), 3795);
        assert_eq!(manifest.namespace, "kafka");
        assert_eq!(manifest.topic, "tiered");
        assert_eq!(manifest.partition, 4);
        assert_eq!(manifest.insync_offset, Some(15584));
    }

    #[test_log::test(tokio::test)]
    async fn test_empty_manifest_decode() {
        let manifest = read_manifest("/resources/test/manifest_empty.json").await;
        assert_eq!(manifest.version, 1);
        assert!(manifest.segments.is_empty());
        assert_eq!(manifest.start_offset, None);
        assert_eq!(manifest.namespace, "kafka");
        assert_eq!(manifest.topic, "acme-ticker-cd-s");
        assert_eq!(manifest.partition, 9);
        assert_eq!(manifest.insync_offset, Some(40));
    }

    #[test_log::test(tokio::test)]
    async fn test_nocompact_manifest_decode() {
        let manifest = read_manifest("/resources/test/manifest_nocompact.json").await;
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.segments.len(), 4);
        assert_eq!(manifest.start_offset.unwrap(), 0);
        assert_eq!(manifest.namespace, "kafka");
        assert_eq!(manifest.topic, "acme-ticker-d-d");
        assert_eq!(manifest.partition, 15);
        assert_eq!(manifest.insync_offset, Some(32));
    }

    #[test_log::test(tokio::test)]
    async fn test_short_manifest_decode() {
        let manifest = read_manifest("/resources/test/manifest_short.json").await;
        assert_eq!(manifest.version, 1);
        assert!(manifest.segments.is_empty());
        assert_eq!(manifest.start_offset, None);
        assert_eq!(manifest.namespace, "kafka");
        assert_eq!(manifest.topic, "si_test_topic");
        assert_eq!(manifest.partition, 0);
        assert_eq!(manifest.insync_offset, None);
    }

    #[test_log::test(tokio::test)]
    async fn test_no_maxa_timestamp_manifest_decode() {
        let manifest = read_manifest("/resources/test/manifest_no_max_timestamp.json").await;
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.segments.len(), 30);
        assert_eq!(manifest.start_offset, Some(0));
        assert_eq!(manifest.namespace, "kafka");
        assert_eq!(manifest.topic, "panda-topic");
        assert_eq!(manifest.partition, 0);
        assert_eq!(manifest.insync_offset, Some(30493));
    }

    async fn read_topic_manifest(path: &str) -> TopicManifest {
        read_json(path).await
    }

    #[test_log::test(tokio::test)]
    async fn test_topic_manifest_decode() {
        let topic_manifest = read_topic_manifest("/resources/test/topic_manifest.json").await;
        assert_eq!(topic_manifest.version, 1);
        assert_eq!(topic_manifest.namespace, "kafka");
        assert_eq!(topic_manifest.topic, "acme-ticker-cd-s");
        assert_eq!(topic_manifest.partition_count, 16);
        assert_eq!(topic_manifest.replication_factor, 3);
        assert_eq!(topic_manifest.revision_id, 29);
        assert_eq!(topic_manifest.cleanup_policy_bitflags, "compact,delete");
    }

    #[test_log::test(tokio::test)]
    async fn test_binary_manifest_decode() {
        let b = read_bytes("/resources/test/manifest_23_2_binary.bin").await;

        let manifest = PartitionManifest::from_bytes(b).unwrap();
        assert_eq!(manifest.namespace, "kafka");
        assert_eq!(manifest.topic, "test");
        assert_eq!(manifest.partition, 0);
        assert_eq!(manifest.revision, 8);
        assert_eq!(manifest.segments.len(), 3654);
        // Sanity check some missing field serialization.
        assert!(
            offset_has_default_value(&manifest.start_kafka_offset),
            "{:?}",
            manifest.start_kafka_offset
        );
        let json_manifest = serde_json::to_string(&manifest).unwrap();
        assert_eq!(json_manifest.find("start_kafka_offset"), None);
    }

    #[test_log::test(tokio::test)]
    async fn test_binary_manifest_decode_2() {
        // This is an exampe of a manifest that happesn to have values large enough
        // to trigger a previously-fixed bug in the deltafor code.
        let b = read_bytes("/resources/test/manifest_23_2_binary_2.bin").await;

        let manifest = PartitionManifest::from_bytes(b).unwrap();
        assert_eq!(manifest.namespace, "kafka");
        assert_eq!(manifest.topic, "topic-lppzmjltwl");
        assert_eq!(manifest.partition, 5);
        assert_eq!(manifest.revision, 51);

        // Reproducer for issue with DeltaFOR decode
        assert_eq!(manifest.segments.len(), 97);
        assert_eq!(
            manifest.segments.get("1225-1-v1.log").unwrap().size_bytes,
            1573496
        );
        // Sanity check some missing field serialization.
        assert!(
            offset_has_default_value(&manifest.start_kafka_offset),
            "{:?}",
            manifest.start_kafka_offset
        );
        let json_manifest = serde_json::to_string(&manifest).unwrap();
        assert_eq!(json_manifest.find("start_kafka_offset"), None);
    }

    #[test_log::test(tokio::test)]
    async fn test_binary_manifest_decode_start_kafka_offset() {
        let b = read_bytes("/resources/test/manifest_23_2_start_kafka_offset.bin").await;

        let manifest = PartitionManifest::from_bytes(b).unwrap();
        assert_eq!(manifest.namespace, "test-ns");
        assert_eq!(manifest.topic, "test-topic");
        assert_eq!(manifest.partition, 42);
        assert_eq!(manifest.revision, 0);

        assert_eq!(manifest.segments.len(), 4);
        assert_eq!(Some(80), manifest.start_kafka_offset);
        let json_manifest = serde_json::to_string(&manifest).unwrap();
        assert_ne!(json_manifest.find("start_kafka_offset"), None);
    }

    #[test_log::test(tokio::test)]
    async fn test_manifest_with_replaced_segments() {
        let b = read_bytes("/resources/test/manifest_with_replaced_segments.bin").await;
        let manifest = PartitionManifest::from_bytes(b).unwrap();
        assert_eq!(manifest.version, 2);
        assert_eq!(manifest.namespace, "kafka");
        assert_eq!(manifest.topic, "test-topic");

        assert_eq!(manifest.replaced.is_some(), true);
        let replaced_segs = manifest.replaced.as_ref().unwrap();
        assert_eq!(replaced_segs["6756-1-v1.log"].base_offset, 6756);
        assert_eq!(replaced_segs["6756-1-v1.log"].sname_format, 3);
        assert_eq!(replaced_segs["6889-1-v1.log"].base_offset, 6889);
        assert_eq!(replaced_segs["6889-1-v1.log"].sname_format, 3);

        assert_eq!(manifest.cloud_log_size_bytes, Some(225998141));
        assert_eq!(manifest.start_offset, Some(0));
        assert_eq!(manifest.last_offset, 7017);
        assert_eq!(
            manifest.last_uploaded_compacted_offset,
            Some(-9223372036854775808)
        );

        let json_manifest = serde_json::to_string(&manifest).unwrap();
        assert_eq!(json_manifest.find("last_uploaded_compacted_offset"), None);
    }

    #[test_log::test(tokio::test)]
    async fn test_cluster_manifest_decode() {
        let manifest = read_cluster_manifest("/resources/test/cluster_manifest.json").await;
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.compat_version, 1);
        assert_eq!(manifest.upload_time_since_epoch, 1668768681875);
        assert_eq!(
            manifest.cluster_uuid,
            "19fd9bd7-29e9-4da4-85cf-7199b2997ad3"
        );
        assert_eq!(manifest.metadata_id, 3);
        assert_eq!(manifest.controller_snapshot_offset, 380);
        assert_eq!(
            manifest.controller_snapshot_path,
            "cluster_metadata/19fd9bd7-29e9-4da4-85cf-7199b2997ad3/380/controller.snapshot"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_lifecycle_marker() {
        let b = read_bytes("/resources/test/lifecycle_marker.bin").await;
        let mut cursor = std::io::Cursor::new(b.as_ref());
        let manifest = LifecycleMarker::from_bytes(&mut cursor).unwrap();
        assert_eq!(
            manifest.cluster_id,
            "9687f8b0-b81a-4a4c-9811-883fed35c1d8".to_string()
        );
        assert_eq!(manifest.ntr.namespace, "kafka");
        assert_eq!(manifest.ntr.topic, "testtopic");
        assert_eq!(manifest.ntr.revision_id, 19);
        assert_eq!(manifest.status, LifecycleStatus::Purged);
    }
}
