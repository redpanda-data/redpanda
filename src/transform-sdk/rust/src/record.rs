// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::varint;
use std::time::SystemTime;

use crate::varint::{Decoded, VarintDecodeError};

pub(crate) struct BatchHeader {
    pub base_offset: i64,
    pub record_count: i32,
    pub partition_leader_epoch: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

type KeyValuePair<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);

fn read_kv(payload: &[u8]) -> Result<Decoded<KeyValuePair>, VarintDecodeError> {
    let key = varint::read_sized_buffer(payload)?;
    let payload = &payload[key.read..];
    let value = varint::read_sized_buffer(payload)?;
    Ok(Decoded {
        value: (key.value, value.value),
        read: key.read + value.read,
    })
}

/// A zero-copy record header.
#[derive(Debug)]
pub struct BorrowedHeader<'a> {
    key: &'a [u8],
    value: Option<&'a [u8]>,
}

impl<'a> BorrowedHeader<'a> {
    /// Create a new header without any copies.
    pub fn new(key: &'a [u8], value: Option<&'a [u8]>) -> Self {
        Self { key, value }
    }

    fn read_from_payload(payload: &'a [u8]) -> Result<Decoded<Self>, VarintDecodeError> {
        read_kv(payload).map(|result| {
            result.map(|(key, value)| BorrowedHeader {
                key: key.unwrap_or("".as_bytes()),
                value,
            })
        })
    }

    /// Returns the header's key or `None` if there is no key.
    pub fn key(&self) -> &[u8] {
        self.key
    }

    /// Returns the header's value or `None` if there is no value.
    pub fn value(&self) -> Option<&[u8]> {
        self.value
    }

    /// Clones this header obtaining ownership of the clone.
    pub fn to_owned(&self) -> RecordHeader {
        RecordHeader::new(self.key.to_owned(), self.value.map(|b| b.to_owned()))
    }
}

/// A zero-copy representation of a record within Redpanda.
///
/// BorrowedRecords are handed to [`on_record_written`] event handlers as the record that Redpanda
/// wrote.
#[derive(Debug)]
pub struct BorrowedRecord<'a> {
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
    timestamp: SystemTime,
    headers: Vec<BorrowedHeader<'a>>,
}

impl<'a> BorrowedRecord<'a> {
    /// Create a new record without any copies.
    ///
    /// NOTE: This method is useful for tests to mock out custom events to your transform function.
    pub fn new(key: Option<&'a [u8]>, value: Option<&'a [u8]>, timestamp: SystemTime) -> Self {
        Self {
            key,
            value,
            timestamp,
            headers: Vec::new(),
        }
    }

    /// Create a new record without any copies.
    ///
    /// NOTE: This method is useful for tests to mock out custom events to your transform function.
    pub fn new_with_headers(
        key: Option<&'a [u8]>,
        value: Option<&'a [u8]>,
        timestamp: SystemTime,
        headers: Vec<BorrowedHeader<'a>>,
    ) -> Self {
        Self {
            key,
            value,
            timestamp,
            headers,
        }
    }

    pub(crate) fn read_from_payload(
        payload: &'a [u8],
        timestamp: SystemTime,
    ) -> Result<Self, VarintDecodeError> {
        let kv = read_kv(payload)?;
        let mut payload = &payload[kv.read..];
        let header_count_result = varint::read(payload)?;
        payload = &payload[header_count_result.read..];
        let mut headers: Vec<BorrowedHeader<'a>> =
            Vec::with_capacity(header_count_result.value as usize);
        for _ in 0..header_count_result.value {
            let header_result = BorrowedHeader::read_from_payload(payload)?;
            payload = &payload[header_result.read..];
            headers.push(header_result.value);
        }
        let (key, value) = kv.value;
        Ok(BorrowedRecord {
            key,
            value,
            timestamp,
            headers,
        })
    }

    /// Returns the record's key or `None` if there is no key.
    pub fn key(&self) -> Option<&'a [u8]> {
        self.key
    }

    /// Returns the record's value or `None` if there is no value.
    pub fn value(&self) -> Option<&'a [u8]> {
        self.value
    }

    /// Returns the record's timestamp.
    ///
    /// NOTE: Record timestamps in Redpanda have millisecond resolution.
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// Return the headers for this record.
    pub fn headers(&self) -> &[BorrowedHeader<'a>] {
        &self.headers
    }
}

/// Records may have a collection of headers attached to them.
///
/// Headers are opaque to the broker and are purely a mechanism for the producer and consumers to
/// pass information.
#[derive(Debug)]
pub struct RecordHeader {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

impl RecordHeader {
    /// Create a new `RecordHeader`.
    pub fn new(key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Self { key, value }
    }

    /// Returns the header's key or `None` if there is no key.
    pub fn key(&self) -> &[u8] {
        &self.key[..]
    }

    /// Sets the key for this header.
    pub fn set_key(&mut self, k: Vec<u8>) {
        self.key = k;
    }

    /// Returns the header's value or `None` if there is no value.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_ref().map(|v| &v[..])
    }

    /// Sets the value for this header.
    pub fn set_value(&mut self, v: Vec<u8>) {
        self.value = Some(v);
    }
}

/// An record in Redpanda.
///
/// Records are generated as the result of any transforms that act upon a [`BorrowedRecord`].
#[derive(Debug)]
pub struct Record {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<RecordHeader>,
}

impl Record {
    /// Create a new empty record with no key, no value and no headers.
    pub fn empty() -> Self {
        Self {
            key: None,
            value: None,
            headers: Vec::new(),
        }
    }

    /// Create a new record with the given key and value.
    pub fn new(key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self {
            key,
            value,
            headers: Vec::new(),
        }
    }

    /// Create a new record with the given, key, value and headers.
    pub fn new_with_headers(
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<RecordHeader>,
    ) -> Self {
        Self {
            key,
            value,
            headers,
        }
    }

    /// Returns the record's key or `None` if there is no key.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_ref().map(|k| &k[..])
    }

    /// Sets the key for this record.
    pub fn set_key(&mut self, k: Vec<u8>) {
        self.key = Some(k);
    }

    /// Returns the record's value or `None` if there is no value.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_ref().map(|v| &v[..])
    }

    /// Sets the value for this record.
    pub fn set_value(&mut self, v: Vec<u8>) {
        self.value = Some(v);
    }

    /// Append a header to this record.
    pub fn add_header(&mut self, header: RecordHeader) {
        self.headers.push(header);
    }

    /// Returns a collection of headers for this record.
    pub fn headers(&self) -> &[RecordHeader] {
        &self.headers[..]
    }

    pub(crate) fn write_payload(&self, payload: &mut Vec<u8>) {
        varint::write_sized_buffer(payload, self.key());
        varint::write_sized_buffer(payload, self.value());
        varint::write(payload, self.headers.len() as i64);
        for h in &self.headers {
            varint::write_sized_buffer(payload, Some(h.key()));
            varint::write_sized_buffer(payload, h.value());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use crate::RecordHeader;

    use super::{BorrowedRecord, Record};
    use anyhow::{ensure, Result};
    use rand::prelude::*;

    #[test]
    fn payload_roundtrips() {
        let owned = Record::new_with_headers(
            Some(random_bytes(128)),
            Some(random_bytes(128)),
            vec![
                RecordHeader::new(random_bytes(12), Some(random_bytes(16))),
                RecordHeader::new(random_bytes(16), None),
                RecordHeader::new(random_bytes(16), Some(random_bytes(0))),
            ],
        );
        validate_roundtrip(&owned).unwrap();
        let owned = Record::new_with_headers(None, Some(random_bytes(128)), vec![]);
        validate_roundtrip(&owned).unwrap();
        let owned = Record::new_with_headers(
            Some(random_bytes(0)),
            Some(random_bytes(0)),
            vec![RecordHeader::new(random_bytes(12), Some(random_bytes(16)))],
        );
        validate_roundtrip(&owned).unwrap();
        let owned = Record::new_with_headers(
            Some(random_bytes(128)),
            None,
            vec![
                RecordHeader::new(random_bytes(12), Some(random_bytes(16))),
                RecordHeader::new(random_bytes(16), None),
                RecordHeader::new(random_bytes(16), Some(random_bytes(0))),
            ],
        );
        validate_roundtrip(&owned).unwrap();
    }

    quickcheck! {
        fn roundtrip(key: Option<Vec<u8>>, value: Option<Vec<u8>>, headers: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Result<()> {
            let record = Record::new_with_headers(key, value, headers.into_iter().map(|(k, v)| RecordHeader::new(k, v)).collect());
            validate_roundtrip(&record)
        }
    }

    fn validate_roundtrip(owned: &Record) -> Result<()> {
        let mut buf: Vec<u8> = Vec::new();
        owned.write_payload(&mut buf);
        let borrowed = BorrowedRecord::read_from_payload(&buf, SystemTime::UNIX_EPOCH)?;
        ensure!(borrowed.key() == owned.key(), "keys not equal");
        ensure!(borrowed.value() == owned.value(), "values not equal");
        ensure!(
            borrowed.headers().len() == owned.headers().len(),
            "header count not equal"
        );
        for (b, o) in borrowed.headers.iter().zip(owned.headers().iter()) {
            ensure!(b.key() == o.key(), "header keys not equal");
            ensure!(b.value() == o.value(), "header values not equal");
        }
        Ok(())
    }

    fn random_bytes(n: usize) -> Vec<u8> {
        let mut v = Vec::new();
        v.resize(n, 0);
        rand::thread_rng().fill(&mut v[..]);
        v
    }
}
