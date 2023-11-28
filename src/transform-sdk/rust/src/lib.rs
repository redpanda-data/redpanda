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
//! Provides a framework for writing in-broker data transforms.
//!
//! [`on_record_written`]: Transforms records after they have been written to an input topic.
//! Resulting records are written to the output topic.
//!
//! Transforms must compile to WebAssembly via `--target=wasm32-wasi`, for the broker to run them.

use std::fmt::Debug;

extern crate redpanda_transform_sdk_types;

pub use redpanda_transform_sdk_types::*;

/// Register a callback to be fired when a record is written to the input topic.
///
/// This callback is triggered after the record has been written and fsynced to disk and the
/// producer has been acknowledged.
///
/// This method blocks and runs forever, it should be called from `main` and any setup that is
/// needed can be done before calling this method.
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
/// fn my_transform(event: WriteEvent) -> Result<Vec<Record>> {
///   Ok(vec![
///     Record::new(
///       event.record.key().map(|k| k.to_owned()),
///       event.record.value().map(|v| v.to_owned()),
///     )
///   ])
/// }
/// ```
pub fn on_record_written<E, F>(cb: F)
where
    E: Debug,
    F: Fn(WriteEvent) -> Result<Vec<Record>, E>,
{
    redpanda_transform_sdk_sys::process(cb)
}
