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

use std::{
    fmt::Debug,
    time::{Duration, SystemTime},
};

#[cfg(target_os = "wasi")]
mod abi;
#[cfg(not(target_os = "wasi"))]
mod stub_abi;
#[cfg(not(target_os = "wasi"))]
use stub_abi as abi;

mod varint;

pub mod record;

pub use record::*;

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

#[cfg(test)]
extern crate rand;

/// An event generated after a write event within the broker.
pub struct WriteEvent<'a> {
    /// The record for which the event was generated for.
    pub record: BorrowedRecord<'a>,
}

/// A callback to process write events and respond with a number of records to write back to the
/// output topic.
pub type OnRecordWritten<E> = fn(WriteEvent) -> Result<Vec<Record>, E>;

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
pub fn on_record_written<E>(cb: OnRecordWritten<E>)
where
    E: Debug,
{
    process(cb)
}

fn process<E>(cb: OnRecordWritten<E>)
where
    E: Debug,
{
    unsafe {
        abi::check_abi();
    }
    loop {
        process_batch(cb);
    }
}

fn process_batch<E>(cb: OnRecordWritten<E>)
where
    E: Debug,
{
    let mut header = record::BatchHeader {
        base_offset: 0,
        record_count: 0,
        partition_leader_epoch: 0,
        attributes: 0,
        last_offset_delta: 0,
        base_timestamp: 0,
        max_timestamp: 0,
        producer_id: 0,
        producer_epoch: 0,
        base_sequence: 0,
    };
    let errno_or_buf_size = unsafe {
        abi::read_batch_header(
            &mut header.base_offset,
            &mut header.record_count,
            &mut header.partition_leader_epoch,
            &mut header.attributes,
            &mut header.last_offset_delta,
            &mut header.base_timestamp,
            &mut header.max_timestamp,
            &mut header.producer_id,
            &mut header.producer_epoch,
            &mut header.base_sequence,
        )
    };
    assert!(
        errno_or_buf_size >= 0,
        "failed to read batch header (errno: {errno_or_buf_size})"
    );
    let buf_size = errno_or_buf_size as usize;
    let mut input_buffer: Vec<u8> = vec![0; buf_size];
    let mut output_buffer: Vec<u8> = Vec::new();
    for _ in 0..header.record_count {
        let mut attr: u8 = 0;
        let mut timestamp: i64 = 0;
        let mut offset: i64 = 0;
        let errno_or_amt = unsafe {
            abi::read_next_record(
                &mut attr,
                &mut timestamp,
                &mut offset,
                input_buffer.as_mut_ptr(),
                input_buffer.len() as u32,
            )
        };
        assert!(
            errno_or_amt >= 0,
            "reading record failed (errno: {errno_or_amt}, buffer_size: {buf_size})"
        );
        let amt = errno_or_amt as usize;
        let ts = SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp as u64);
        let record = BorrowedRecord::read_from_payload(&input_buffer[0..amt], ts)
            .expect("deserializing record failed");
        let transformed = cb(WriteEvent { record }).expect("transforming record failed");
        for record in transformed {
            output_buffer.clear();
            record.write_payload(&mut output_buffer);
            let errno_or_amt =
                unsafe { abi::write_record(output_buffer.as_ptr(), output_buffer.len() as u32) };
            assert!(
                errno_or_amt == output_buffer.len() as i32,
                "writing record failed with errno: {errno_or_amt}"
            );
        }
    }
}
