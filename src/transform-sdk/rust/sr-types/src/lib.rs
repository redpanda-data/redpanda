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

//! An internal crate providing shared types for Redpanda's Data Transforms Schema Registry client.
//!
//! If you are looking to use Schema Registry within transforms you probably want crate
//! [redpanda-transform-sdk-sr](https://crates.io/crates/redpanda-transform-sdk-sr). These types are
//! re-exported there for usage.

use std::string::FromUtf8Error;

use redpanda_transform_sdk_varint as varint;

/// SchemaFormat is an enum that represents the schema formats supported in the Schema Registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SchemaFormat {
    Avro,
    Protobuf,
    Json,
}

/// SchemaId is a type for the registered ID of a schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaId(pub i32);

/// SchemaVersion is the version of a schema for a subject within the Schema Registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaVersion(pub i32);

/// Reference is a way for one schema to reference another.
///
/// The details for how referencing is done are type-specific; for example, JSON objects
/// that use the key "$ref" can refer to another schema via URL. For more details
/// on references, see the following link:
///
///  <https://docs.redpanda.com/current/manage/schema-reg/schema-reg-api/#reference-a-schema>
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reference {
    name: String,
    subject: String,
    version: SchemaVersion,
}

impl Reference {
    /// Create a new reference from a name, subject and version of a registered schema
    pub fn new(
        name: impl Into<String>,
        subject: impl Into<String>,
        version: SchemaVersion,
    ) -> Self {
        Self {
            name: name.into(),
            subject: subject.into(),
            version,
        }
    }

    /// Get the name of the reference
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the subject of the referenced schema
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Get the version of the referenced schema
    pub fn version(&self) -> SchemaVersion {
        self.version
    }
}

/// Schema is a schema that can be registered within Schema Registry
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    schema: String,
    format: SchemaFormat,
    references: Vec<Reference>,
}

impl Schema {
    /// Create a new schema with a given format and references
    pub fn new(
        schema: impl Into<String>,
        format: SchemaFormat,
        references: Vec<Reference>,
    ) -> Self {
        Self {
            schema: schema.into(),
            format,
            references,
        }
    }

    /// Create a new avro schema with the given references
    pub fn new_avro(schema: String, references: Vec<Reference>) -> Self {
        Self::new(schema, SchemaFormat::Avro, references)
    }

    /// Create a new protobuf schema with the given references
    pub fn new_protobuf(schema: String, references: Vec<Reference>) -> Self {
        Self::new(schema, SchemaFormat::Protobuf, references)
    }

    /// Get the textual format of the schema
    pub fn schema(&self) -> &str {
        self.schema.as_ref()
    }

    /// Get the format of this schema
    pub fn format(&self) -> &SchemaFormat {
        &self.format
    }

    /// Get all the schemas that are referenced in this schema
    pub fn references(&self) -> &[Reference] {
        self.references.as_ref()
    }
}

/// SubjectSchema is a schema along with the subject, version and ID of the schema in the schema
/// registry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubjectSchema {
    schema: Schema,

    subject: String,
    version: SchemaVersion,
    id: SchemaId,
}

impl SubjectSchema {
    /// Create a new schema that has been registered in the Schema Registry under the given
    /// subject/version and ID.
    pub fn new(
        schema: Schema,
        subject: impl Into<String>,
        version: SchemaVersion,
        id: SchemaId,
    ) -> Self {
        Self {
            schema,
            subject: subject.into(),
            version,
            id,
        }
    }

    /// Get the schema that was registered.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// The subject that the schema was registered under.
    pub fn subject(&self) -> &str {
        self.subject.as_ref()
    }

    /// The version that the schema was registered under.
    pub fn version(&self) -> SchemaVersion {
        self.version
    }

    /// The ID that the schema was registered under.
    pub fn id(&self) -> SchemaId {
        self.id
    }
}

/// An error that can occur when interacting with the Schema Registry
#[derive(Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaRegistryError {
    /// Unknown error from the registry with the corresponding error code.
    Unknown(i32),
    DecodingError(varint::DecodeError),
    UnknownFormat(i64),
    InvalidSchema,
    BadHeader,
}

impl std::fmt::Display for SchemaRegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaRegistryError::Unknown(errno) => {
                write!(f, "writing record failed with errno: {}", errno)
            }
            SchemaRegistryError::DecodingError(err) => {
                write!(f, "unable to decode broker data: {}", err)
            }
            SchemaRegistryError::UnknownFormat(id) => {
                write!(f, "unknown format with ID: {}", id)
            }
            SchemaRegistryError::InvalidSchema => {
                write!(f, "invalid utf8 in schema")
            }
            SchemaRegistryError::BadHeader => {
                write!(f, "5 byte header is missing or does not have 0 magic byte")
            }
        }
    }
}

impl std::error::Error for SchemaRegistryError {}

impl From<varint::DecodeError> for SchemaRegistryError {
    fn from(err: varint::DecodeError) -> Self {
        SchemaRegistryError::DecodingError(err)
    }
}

impl From<FromUtf8Error> for SchemaRegistryError {
    fn from(_: FromUtf8Error) -> Self {
        SchemaRegistryError::InvalidSchema
    }
}

/// A result type for the Schema Registry where errors are SchemaRegistryError.
pub type Result<T> = std::result::Result<T, SchemaRegistryError>;

/// A client for interacting with the Schema Registry within Redpanda.
pub trait SchemaRegistryClientImpl {
    /// Lookup a schema via its global ID.
    fn lookup_schema_by_id(&self, id: SchemaId) -> Result<Schema>;

    /// Lookup a schema for a given subject at a specific version.
    fn lookup_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<SubjectSchema>;

    /// Lookup the latest schema for a subject.
    fn lookup_latest_schema(&self, subject: &str) -> Result<SubjectSchema>;

    /// Create a schema in the Schema Registry under the given subject, returning the version and
    /// ID.
    ///
    /// If an equivalent schema already exists globally, that schema ID will be reused.
    /// If an equivalent schema already exists within that subject, this will be a noop and the
    /// existing schema version will be returned.
    fn create_schema(&mut self, subject: &str, schema: Schema) -> Result<SubjectSchema>;
}
