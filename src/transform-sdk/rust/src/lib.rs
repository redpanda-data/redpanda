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

//! Redpanda Data Transforms Rust library.
//!
//! Data transforms let you run common data streaming tasks, like filtering,
//! scrubbing, and transcoding, within Redpanda. For example, you may have consumers
//! that require you to redact credit card numbers or convert JSON to Avro.
//!
//! Data transforms use a WebAssembly (Wasm) engine inside a Redpanda broker.
//! Thus, transforms must compile to WebAssembly via `--target=wasm32-wasi`.
//! A Wasm function acts on a single record in an input topic.
//! You can develop and manage data transforms with `rpk transform` commands.
//!
//! This crate provides a framework for writing transforms.
//!
//! [`on_record_written`]: Transforms individual records after they have been written to an input topic.
//! Written records are output to the destination topic.

use std::fmt::Debug;

pub use redpanda_transform_sdk_types::*;

/// Register a callback to be fired when a record is written to the input topic.
///
/// This callback is triggered after the record has been written and fsynced to disk and the
/// producer has been acknowledged.
///
/// This method blocks and runs forever, it should be called from `main`. Any setup that is
/// needed prior to transforming any records can be done before calling this method.
///
/// # Examples
///
/// ```no_run
/// extern crate redpanda_transform_sdk as redpanda;
///
/// use redpanda::*;
/// use anyhow::Result;
///
/// fn main() {
///   on_record_written(my_transform);
/// }
///
/// // A transform that duplicates the record on the input topic to the output topic.
/// fn my_transform(event: WriteEvent, writer: &mut RecordWriter) -> Result<()> {
///   writer.write(&Record::new(
///     event.record.key().map(|k| k.to_owned()),
///     event.record.value().map(|v| v.to_owned()),
///   ))?;
///   Ok(())
/// }
/// ```
pub fn on_record_written<E, F>(cb: F) -> !
where
    E: Debug,
    F: Fn(WriteEvent, &mut RecordWriter) -> Result<(), E>,
{
    redpanda_transform_sdk_sys::process(cb)
}
