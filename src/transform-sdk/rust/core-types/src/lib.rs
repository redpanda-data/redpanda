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

//! An internal crate providing shared types for Redpanda's Data Transforms.
//!
//! If you are looking to use transforms you probably want crate
//! [redpanda-transform-sdk](https://crates.io/crates/redpanda-transform-sdk). These types are
//! re-exported there for usage.

use std::time::SystemTime;

/// An event generated after a write event within the broker.
///
/// These events are asynchronously triggered after the producer's write request has been
/// acknowledged.
#[derive(Debug)]
pub struct WriteEvent<'a> {
    /// The record for which the event was generated for.
    pub record: WrittenRecord<'a>,
}

/// A written [`Record`] within Redpanda.
///
/// A [`WrittenRecord`] is handed to `on_record_written` event handlers as the record that Redpanda
/// wrote. The record contains a key value pair with some headers, along with the record's
/// timestamp.
#[derive(Debug, PartialEq, Eq)]
pub struct WrittenRecord<'a> {
    record: BorrowedRecord<'a>,
    timestamp: SystemTime,
}

impl<'a> WrittenRecord<'a> {
    /// Create a new record without any copies.
    ///
    /// NOTE: This method is useful for tests to mock out custom events to your transform function.
    pub fn from_record(record: impl Into<BorrowedRecord<'a>>, timestamp: SystemTime) -> Self {
        let record = record.into();
        Self { record, timestamp }
    }

    /// Create a new record without any copies.
    ///
    /// NOTE: This method is useful for tests to mock out custom events to your transform function.
    pub fn new(key: Option<&'a [u8]>, value: Option<&'a [u8]>, timestamp: SystemTime) -> Self {
        Self {
            record: BorrowedRecord::new(key, value),
            timestamp,
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
            record: BorrowedRecord::new_with_headers(key, value, headers),
            timestamp,
        }
    }

    /// Returns the record's key or `None` if there is no key.
    pub fn key(&self) -> Option<&'a [u8]> {
        self.record.key()
    }

    /// Returns the record's value or `None` if there is no value.
    pub fn value(&self) -> Option<&'a [u8]> {
        self.record.value()
    }

    /// Returns the record's timestamp.
    ///
    /// NOTE: Record timestamps in Redpanda have millisecond resolution.
    pub fn timestamp(&self) -> SystemTime {
        self.timestamp
    }

    /// Return the headers for this record.
    pub fn headers(&self) -> &[BorrowedHeader<'a>] {
        self.record.headers()
    }
}

impl<'a> AsRef<BorrowedRecord<'a>> for WrittenRecord<'a> {
    fn as_ref(&self) -> &BorrowedRecord<'a> {
        &self.record
    }
}

impl<'a> From<WrittenRecord<'a>> for BorrowedRecord<'a> {
    fn from(r: WrittenRecord<'a>) -> Self {
        r.record
    }
}

impl<'a> From<&'a WrittenRecord<'a>> for BorrowedRecord<'a> {
    fn from(r: &'a WrittenRecord) -> Self {
        r.record.clone()
    }
}

/// Allows you to customize a [`RecordWriter`]'s write.
///
/// For example, use [`WriteOptions`] to customize the output topic to write to.
#[derive(Default, PartialEq, Eq, Clone, Debug)]
pub struct WriteOptions<'a> {
    pub topic: Option<&'a str>,
}

impl<'a> WriteOptions<'a> {
    /// Create a new options struct with the [`Record`]'s destination to an optional `topic`.
    ///
    /// If `topic` is `None`, the record will be written to the first
    /// output topic listed in the configuration.
    pub fn new(topic: Option<&'a str>) -> Self {
        Self { topic }
    }

    /// Create a new options struct with the [`Record`]'s destination to `topic`.
    pub fn to_topic(topic: &'a str) -> Self {
        Self::new(Some(topic))
    }
}

/// An internal trait that can receive a stream of records and output them a destination topic.
///
/// As a user of this framework you should not need to implement this, unless you're writing a mock
/// implementation for testing.
pub trait RecordSink {
    /// Write a record with options, returning any errors.
    fn write(&mut self, r: BorrowedRecord<'_>, opts: WriteOptions<'_>) -> Result<(), WriteError>;
}

/// A struct that writes transformed records to the output topic..
pub struct RecordWriter<'a> {
    sink: &'a mut dyn RecordSink,
}

impl<'a> RecordWriter<'a> {
    // Creates a new [`RecordWriter`] using the specified `sink`.
    pub fn new(sink: &'a mut dyn RecordSink) -> Self {
        Self { sink }
    }

    /// Write a record to the default output topic, returning any errors.
    pub fn write<'b>(&mut self, r: impl Into<BorrowedRecord<'b>>) -> Result<(), WriteError> {
        self.sink.write(r.into(), WriteOptions::default())
    }

    /// Write a record with the given options, returning any errors.
    pub fn write_with_options<'b>(
        &mut self,
        r: impl Into<BorrowedRecord<'b>>,
        opts: WriteOptions<'b>,
    ) -> Result<(), WriteError> {
        self.sink.write(r.into(), opts)
    }
}

/// An error that can occur when writing records to the output topic.
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteError {
    /// Unknown error from the broker with the corresponding error code.
    Unknown(i32),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Unknown(errno) => write!(f, "writing record failed with errno: {}", errno),
        }
    }
}

impl std::error::Error for WriteError {}

/// A zero-copy [`BorrowedRecord`] header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BorrowedHeader<'a> {
    key: &'a [u8],
    value: Option<&'a [u8]>,
}

impl<'a> BorrowedHeader<'a> {
    /// Create a new header without any copies.
    pub fn new(key: &'a [u8], value: Option<&'a [u8]>) -> Self {
        Self { key, value }
    }

    /// Returns the header's key.
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

impl<'a> From<&'a BorrowedHeader<'a>> for BorrowedHeader<'a> {
    fn from(r: &'a BorrowedHeader) -> Self {
        r.clone()
    }
}

/// A zero-copy representation of a [`Record`] within Redpanda.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BorrowedRecord<'a> {
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
    headers: Vec<BorrowedHeader<'a>>,
}

impl<'a> BorrowedRecord<'a> {
    /// Create a new record without any copies.
    pub fn new(key: Option<&'a [u8]>, value: Option<&'a [u8]>) -> Self {
        Self {
            key,
            value,
            headers: Vec::new(),
        }
    }

    /// Create a new record without any copies.
    pub fn new_with_headers(
        key: Option<&'a [u8]>,
        value: Option<&'a [u8]>,
        headers: Vec<BorrowedHeader<'a>>,
    ) -> Self {
        Self {
            key,
            value,
            headers,
        }
    }

    /// Returns the record's key or `None` if there is no key.
    pub fn key(&self) -> Option<&'a [u8]> {
        self.key
    }

    /// Returns the record's value or `None` if there is no value.
    pub fn value(&self) -> Option<&'a [u8]> {
        self.value
    }

    /// Return the headers for this record.
    pub fn headers(&self) -> &[BorrowedHeader<'a>] {
        &self.headers
    }
}

impl<'a> From<&'a BorrowedRecord<'a>> for BorrowedRecord<'a> {
    fn from(r: &'a BorrowedRecord) -> Self {
        r.clone()
    }
}

/// A key value pair attached to a [`Record`].
///
/// Headers are opaque to the broker and are purely a mechanism for the producer and consumers to
/// pass information.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecordHeader {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

impl RecordHeader {
    /// Create a new `RecordHeader`.
    pub fn new(key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Self { key, value }
    }

    /// Returns the header's key.
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

impl<'a> From<&'a RecordHeader> for BorrowedHeader<'a> {
    fn from(header: &'a RecordHeader) -> Self {
        Self::new(header.key(), header.value())
    }
}

/// A record in Redpanda.
///
/// A record is a key-value pair of bytes, along with a collection of [`RecordHeader`].
///
/// Records are generated as the result of any transforms that act upon a [`BorrowedRecord`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
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
    pub fn headers(&self) -> impl ExactSizeIterator<Item = BorrowedHeader> {
        self.headers.iter().map(|h| h.into())
    }
}

impl<'a> From<&'a Record> for BorrowedRecord<'a> {
    fn from(record: &'a Record) -> Self {
        Self::new_with_headers(record.key(), record.value(), record.headers().collect())
    }
}
