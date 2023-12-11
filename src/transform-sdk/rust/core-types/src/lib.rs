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
    pub record: BorrowedRecord<'a>,
}

/// A zero-copy [`BorrowedRecord`] header.
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

/// A zero-copy representation of a [`Record`] within Redpanda.
///
/// A [`BorrowedRecord`] is handed to `on_record_written` event handlers as the record that Redpanda
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

/// A key value pair attached to a [`Record`].
///
/// Headers are opaque to the broker and are purely a mechanism for the producer and consumers to
/// pass information.
#[derive(Debug, Clone, Default)]
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

/// A record in Redpanda.
///
/// A record is a key-value pair of bytes, along with a collection of [`RecordHeader`].
///
/// Records are generated as the result of any transforms that act upon a [`BorrowedRecord`].
#[derive(Debug, Clone, Default)]
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
}
