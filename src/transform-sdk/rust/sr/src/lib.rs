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

/// A client for interacting with the Schema Registry within Redpanda.
pub struct SchemaRegistryClient {
    delegate: Box<dyn SchemaRegistryClientImpl>,
}

impl SchemaRegistryClient {
    /// Create a new default Schema Registry client that connects to Redpanda's Schema Registry
    /// when running within the context of a data transform.
    pub fn new() -> Self {
        Self::new_wrapping(Box::new(AbiSchemaRegistryClient::new()))
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
