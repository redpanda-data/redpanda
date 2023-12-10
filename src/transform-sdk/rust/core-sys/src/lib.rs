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

//! An internal crate providing the ABI contract for Redpanda's Data Transforms.
//!
//! If you are looking to use transforms you probably want crate
//! [redpanda-transform-sdk](https://crates.io/crates/redpanda-transform-sdk).

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
mod serde;
mod varint;

extern crate redpanda_transform_sdk_types;

use redpanda_transform_sdk_types::*;

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

#[cfg(test)]
extern crate rand;

pub fn process<E, F>(cb: F) -> !
where
    E: Debug,
    F: Fn(WriteEvent) -> Result<Vec<Record>, E>,
{
    unsafe {
        abi::check_abi();
    }
    loop {
        process_batch(&cb);
    }
}

struct BatchHeader {
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

fn process_batch<E, F>(cb: &F)
where
    E: Debug,
    F: Fn(WriteEvent) -> Result<Vec<Record>, E>,
{
    let mut header = BatchHeader {
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
        let record = serde::read_record_from_payload(&input_buffer[0..amt], ts)
            .expect("deserializing record failed");
        let transformed = cb(WriteEvent { record }).expect("transforming record failed");
        for record in transformed {
            output_buffer.clear();
            serde::write_record_payload(&record, &mut output_buffer);
            let errno_or_amt =
                unsafe { abi::write_record(output_buffer.as_ptr(), output_buffer.len() as u32) };
            assert!(
                errno_or_amt == output_buffer.len() as i32,
                "writing record failed with errno: {errno_or_amt}"
            );
        }
    }
}
