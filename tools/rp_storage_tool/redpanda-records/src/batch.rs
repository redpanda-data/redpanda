use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

use bincode::Encode;

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
#[repr(C, packed)]
pub struct RecordBatchHeader {
    pub header_crc: u32,
    pub size_bytes: i32,
    pub base_offset: u64,
    pub record_batch_type: i8,
    pub crc: u32,

    pub record_batch_attributes: u16,
    pub last_offset_delta: i32,
    pub first_timestamp: u64,
    pub max_timestamp: u64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: i32,
}

impl RecordBatchHeader {
    pub fn is_compressed(&self) -> bool {
        return self.record_batch_attributes & 0x7 != 0x0;
    }

    /// 'data' means counted as data for Kafka offset translation purposes.
    pub fn is_kafka_data(&self) -> bool {
        // TODO include other batch types
        self.record_batch_type == RecordBatchType::RaftData as i8
    }
}

pub const BATCH_HEADER_BYTES: usize = std::mem::size_of::<RecordBatchHeader>();

#[derive(Serialize, Encode)]
pub struct UnpackedRecordBatchHeader {
    // FIXME: this is just an un-packed ersion of RecordbatchHeader
    // ...and RecordBatchHeader is only packed because it was convenient
    // to use its size_of.
    pub header_crc: u32,
    pub size_bytes: i32,
    pub base_offset: u64,
    pub record_batch_type: i8,
    pub crc: u32,

    pub record_batch_attributes: u16,
    pub last_offset_delta: i32,
    pub first_timestamp: u64,
    pub max_timestamp: u64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: i32,
}

impl UnpackedRecordBatchHeader {
    pub fn from(other: &RecordBatchHeader) -> Self {
        Self {
            header_crc: other.header_crc,
            size_bytes: other.size_bytes,
            base_offset: other.base_offset,
            record_batch_type: other.record_batch_type,
            crc: other.crc,
            record_batch_attributes: other.record_batch_attributes,
            last_offset_delta: other.last_offset_delta,
            first_timestamp: other.first_timestamp,
            max_timestamp: other.max_timestamp,
            producer_id: other.producer_id,
            producer_epoch: other.producer_epoch,
            base_sequence: other.base_sequence,
            record_count: other.record_count,
        }
    }
}

impl Display for RecordBatchHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "RecordBatchHeader<o={} s={} t={}",
            self.base_offset as u64, self.size_bytes as u64, self.record_batch_type
        ))
    }
}

/// For batch header_crc calculation: little-endian encode this
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Encode)]
pub struct RecordBatchHeaderCrcFirst {
    pub size_bytes: i32,
    pub base_offset: u64,
    pub record_batch_type: i8,
    pub crc: u32,
}

/// For batch crc calculation: big-endian encode this to get the initial bytes
/// for batch CRC (then append record body bytes)
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Encode)]
pub struct RecordBatchHeaderCrcSecond {
    pub record_batch_attributes: u16,
    pub last_offset_delta: i32,
    pub first_timestamp: u64,
    pub max_timestamp: u64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: i32,
}

#[repr(i8)]
#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum RecordBatchType {
    RaftData = 0x1i8,
    RaftConfig = 0x2,
    Controller = 3,
    KVStore = 4,
    Checkpoint = 5,
    TopicManagementCmd = 6,
    GhostBatch = 7,
    IdAllocator = 8,
    TxPrepare = 9,
    TxFence = 10,
    TmUpdate = 11,
    UserManagementCmd = 12,
    AclManagementCmd = 13,
    GroupPrepareTx = 14,
    GroupCommitTx = 15,
    GroupAbortTx = 16,
    NodeManagementCmd = 17,
    DataPolicyManagementCmd = 18,
    ArchivalMetadata = 19,
    ClusterConfigCmd = 20,
    FeatureUpdate = 21,
    ClusterBootstrapCmd = 22,
    Max = 23,
}

/// Record does not implement Deserialize, its encoding is too quirky to make it worthwhile
/// to try and cram into a consistent serde encoding style.
/// This structure does not own its key+value: it is expected to be constructred
/// with references to data inside a record batch.
pub struct Record<'a> {
    pub len: u32,
    pub attrs: i8,
    pub ts_delta: u32,
    pub offset_delta: u32,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: Vec<(&'a [u8], &'a [u8])>,
}

impl<'a> Record<'a> {
    pub fn to_owned(&self) -> RecordOwned {
        RecordOwned {
            len: self.len,
            attrs: self.attrs,
            ts_delta: self.ts_delta,
            offset_delta: self.offset_delta,
            key: self.key.map(|v| Vec::from(v)),
            value: self.value.map(|v| Vec::from(v)),
            headers: self
                .headers
                .iter()
                .map(|(k, v)| (Vec::from(*k), Vec::from(*v)))
                .collect(),
        }
    }
}

// TODO: use Cow<> to unify with Record
pub struct RecordOwned {
    pub len: u32,
    pub attrs: i8,
    pub ts_delta: u32,
    pub offset_delta: u32,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
}
