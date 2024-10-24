// Copyright 2024 Redpanda Data, Inc.
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

//! Redpanda Data Transforms Rust Schema Registry Client library.
//!
//! Access to the Schema Registry built into Redpanda is available within data transforms.
//! This client is the provided interface to read and write schemas within the registry.

use redpanda_transform_sdk_sr_sys::AbiSchemaRegistryClient;
pub use redpanda_transform_sdk_sr_types::*;

extern crate lru;

use lru::LruCache as LruCacheImpl;
use std::cell::RefCell;
use std::hash::Hash as HashT;
use std::num::NonZeroUsize;

/// A client for interacting with the Schema Registry within Redpanda.
pub struct SchemaRegistryClient {
    delegate: Box<dyn SchemaRegistryClientImpl>,
}

impl SchemaRegistryClient {
    /// Create a new default Schema Registry client that connects to Redpanda's Schema Registry
    /// when running within the context of a data transform.
    pub fn new() -> Self {
        Self::new_wrapping(Box::new(CachingSchemaRegistryClient::new(None)))
    }

    /// Create a new custom Schema Registry client wrapping the implementation.
    ///
    /// This is useful in testing when you want to inject a mock client.
    pub fn new_wrapping(delegate: Box<dyn SchemaRegistryClientImpl>) -> Self {
        Self { delegate }
    }

    /// Lookup a schema via its global ID.
    pub fn lookup_schema_by_id(&self, id: SchemaId) -> Result<Schema> {
        self.delegate.lookup_schema_by_id(id)
    }

    /// Lookup a schema for a given subject at a specific version.
    pub fn lookup_schema_by_version(
        &self,
        subject: impl AsRef<str>,
        version: SchemaVersion,
    ) -> Result<SubjectSchema> {
        self.delegate
            .lookup_schema_by_version(subject.as_ref(), version)
    }

    /// Lookup the latest schema for a subject.
    pub fn lookup_latest_schema(&self, subject: impl AsRef<str>) -> Result<SubjectSchema> {
        self.delegate.lookup_latest_schema(subject.as_ref())
    }

    /// Create a schema in the Schema Registry under the given subject, returning the version and
    /// ID.
    ///
    /// If an equivalent schema already exists globally, that schema ID will be reused.
    /// If an equivalent schema already exists within that subject, this will be a noop and the
    /// existing schema version will be returned.
    pub fn create_schema(
        &mut self,
        subject: impl AsRef<str>,
        schema: Schema,
    ) -> Result<SubjectSchema> {
        self.delegate.create_schema(subject.as_ref(), schema)
    }
}

impl Default for SchemaRegistryClient {
    fn default() -> Self {
        Self::new()
    }
}

#[deprecated(
    since = "1.0.3",
    note = "Prefer decode_schema_id to receive both the extracted ID and a byte slice to the rest of the buffer"
)]
pub fn extract_id(buf: &[u8]) -> Result<SchemaId> {
    static MAGIC_BYTES: [u8; 1] = [0x00];
    if !buf.starts_with(&MAGIC_BYTES) || buf.len() < 5 {
        return Err(SchemaRegistryError::BadHeader);
    }
    Ok(SchemaId(i32::from_be_bytes(buf[1..5].try_into().unwrap())))
}

pub fn decode_schema_id(buf: &[u8]) -> Result<(SchemaId, &[u8])> {
    static MAGIC_BYTES: [u8; 1] = [0x00];
    if !buf.starts_with(&MAGIC_BYTES) || buf.len() < 5 {
        return Err(SchemaRegistryError::BadHeader);
    }
    Ok((
        SchemaId(i32::from_be_bytes(buf[1..5].try_into().unwrap())),
        &buf[5..],
    ))
}

pub fn encode_schema_id(id: SchemaId, buf: &[u8]) -> Vec<u8> {
    static MAGIC_BYTES: [u8; 1] = [0x00];
    let id_bytes = id.0.to_be_bytes();
    [&MAGIC_BYTES, &id_bytes[..], buf].concat().to_vec()
}

#[derive(Debug)]
struct LruCache<K: HashT + Eq, V: Clone> {
    underlying: RefCell<LruCacheImpl<K, V>>,
}

impl<Key: HashT + Eq, Value: Clone> LruCache<Key, Value> {
    fn new(max_entries: Option<usize>) -> LruCache<Key, Value> {
        let nz_max = match max_entries {
            Some(v) if v > 0 => NonZeroUsize::new(v),
            _ => NonZeroUsize::new(10),
        };
        Self {
            underlying: RefCell::new(LruCacheImpl::<Key, Value>::new(nz_max.unwrap())),
        }
    }

    fn get(&self, k: &Key) -> Option<Value> {
        if let Some(v) = self.underlying.borrow_mut().get(k) {
            Some((*v).clone())
        } else {
            None
        }
    }

    fn put(&self, k: Key, v: Value) -> Option<Value> {
        self.underlying.borrow_mut().put(k, v)
    }
}

#[derive(Debug)]
struct CachingSchemaRegistryClient {
    underlying: AbiSchemaRegistryClient,
    schema_by_id_cache: LruCache<SchemaId, Schema>,
    schema_by_sv_cache: LruCache<SubjectVersion, SubjectSchema>,
}

/// SubjectVersion is an aggregate type for the registered subject and version of a schema
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct SubjectVersion {
    subject: String,
    version: SchemaVersion,
}

impl SubjectVersion {
    fn new(subject: impl Into<String>, version: SchemaVersion) -> Self {
        Self {
            subject: subject.into(),
            version,
        }
    }
}

impl CachingSchemaRegistryClient {
    pub fn new(max_entries: Option<usize>) -> CachingSchemaRegistryClient {
        // No need for an abi::check() here; that's done in AbiSchemaRegistryClient::new()
        Self {
            underlying: AbiSchemaRegistryClient::new(),
            schema_by_id_cache: LruCache::<SchemaId, Schema>::new(max_entries),
            schema_by_sv_cache: LruCache::<SubjectVersion, SubjectSchema>::new(max_entries),
        }
    }
}

impl SchemaRegistryClientImpl for CachingSchemaRegistryClient {
    fn lookup_schema_by_id(&self, id: SchemaId) -> Result<Schema> {
        if let Some(schema) = self.schema_by_id_cache.get(&id) {
            Ok(schema)
        } else {
            match self.underlying.lookup_schema_by_id(id) {
                Ok(schema) => {
                    self.schema_by_id_cache.put(id, schema.clone());
                    Ok(schema)
                }
                Err(e) => Err(e),
            }
        }
    }

    fn lookup_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<SubjectSchema> {
        if let Some(subj_schema) = self
            .schema_by_sv_cache
            .get(&SubjectVersion::new(subject, version))
        {
            Ok(subj_schema)
        } else {
            match self.underlying.lookup_schema_by_version(subject, version) {
                Ok(subj_schema) => {
                    self.schema_by_sv_cache
                        .put(SubjectVersion::new(subject, version), subj_schema.clone());
                    Ok(subj_schema)
                }
                Err(e) => Err(e),
            }
        }
    }

    fn lookup_latest_schema(&self, subject: &str) -> Result<SubjectSchema> {
        self.lookup_schema_by_version(subject, SchemaVersion(-1))
    }

    fn create_schema(&mut self, subject: &str, schema: Schema) -> Result<SubjectSchema> {
        match self.underlying.create_schema(subject, schema) {
            Ok(subj_schema) => {
                self.schema_by_sv_cache.put(
                    SubjectVersion::new(subject, subj_schema.version()),
                    subj_schema.clone(),
                );
                Ok(subj_schema)
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::{decode_schema_id, encode_schema_id, extract_id, SchemaId};

    use quickcheck::quickcheck;

    quickcheck! {
        fn roundtrip(n: i32) -> bool {
            let id = SchemaId(n);
            let buf = Vec::<u8>::new();
            let (r, _) = decode_schema_id(&encode_schema_id(id, &buf)).unwrap();
            r == id
        }
    }

    quickcheck! {
        fn old_style_roundtrip(n: i32) -> bool {
            let id = SchemaId(n);
            let buf = Vec::<u8>::new();
            let r = extract_id(&encode_schema_id(id, &buf)).unwrap();
            r == id
        }
    }
}
