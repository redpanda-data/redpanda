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

//! An internal crate providing the ABI contract for Redpanda's Data Transform Schema Registry
//! Client.
//!
//! If you are looking to use transform's schema registry client you probably want crate
//! [redpanda-transform-sdk-sr](https://crates.io/crates/redpanda-transform-sdk-sr).

#[cfg(target_os = "wasi")]
mod abi;
#[cfg(not(target_os = "wasi"))]
mod stub_abi;
use abi::{get_schema_definition, get_subject_schema};
#[cfg(not(target_os = "wasi"))]
use stub_abi as abi;
mod serde;

use redpanda_transform_sdk_sr_types::*;
use redpanda_transform_sdk_varint as varint;

#[derive(Default, Debug)]
pub struct AbiSchemaRegistryClient {}

impl AbiSchemaRegistryClient {
    pub fn new() -> AbiSchemaRegistryClient {
        unsafe {
            abi::check();
        }
        Self {}
    }
}

impl SchemaRegistryClientImpl for AbiSchemaRegistryClient {
    fn lookup_schema_by_id(&self, id: SchemaId) -> Result<Schema> {
        let mut length: i32 = 0;
        let errno = unsafe { abi::get_schema_definition_len(id.0, &mut length) };
        if errno != 0 {
            return Err(SchemaRegistryError::Unknown(errno));
        }
        let mut buf = vec![0; length as usize];
        let errno_or_amt =
            unsafe { get_schema_definition(id.0, buf.as_mut_ptr(), buf.len() as i32) };
        if errno_or_amt < 0 {
            return Err(SchemaRegistryError::Unknown(errno_or_amt));
        }
        serde::decode_schema_def(&buf[0..errno_or_amt as usize])
    }

    fn lookup_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<SubjectSchema> {
        let mut length: i32 = 0;
        let errno = unsafe {
            abi::get_subject_schema_len(
                subject.as_ptr(),
                subject.len() as i32,
                version.0,
                &mut length,
            )
        };
        if errno != 0 {
            return Err(SchemaRegistryError::Unknown(errno));
        }
        let mut buf = vec![0; length as usize];
        let errno_or_amt = unsafe {
            get_subject_schema(
                subject.as_ptr(),
                subject.len() as i32,
                version.0,
                buf.as_mut_ptr(),
                buf.len() as i32,
            )
        };
        if errno_or_amt < 0 {
            return Err(SchemaRegistryError::Unknown(errno_or_amt));
        }
        serde::decode_schema(subject, &buf[0..errno_or_amt as usize])
    }

    fn lookup_latest_schema(&self, subject: &str) -> Result<SubjectSchema> {
        self.lookup_schema_by_version(subject, SchemaVersion(-1))
    }

    fn create_schema(&mut self, subject: &str, schema: Schema) -> Result<SubjectSchema> {
        let mut buf = Vec::with_capacity(schema.schema().len() + (varint::MAX_LENGTH * 2));
        serde::encode_schema_def(&mut buf, &schema);
        let mut schema_id: i32 = 0;
        // TODO: It was currently an oversight that schema registry does not correctly set the
        // version here (take it as a parameter to this method), we should bump the ABI and
        // expose that method.
        let schema_version: i32 = 0;
        let errno = unsafe {
            abi::create_subject_schema(
                subject.as_ptr(),
                subject.len() as i32,
                buf.as_ptr(),
                buf.len() as i32,
                &mut schema_id,
            )
        };
        if errno != 0 {
            return Err(SchemaRegistryError::Unknown(errno));
        }
        Ok(SubjectSchema::new(
            schema,
            subject,
            SchemaVersion(schema_version),
            SchemaId(schema_id),
        ))
    }
}
