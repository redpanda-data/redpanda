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

// Wasm ABIs are constrainted to only pass simple arguments, so we're limited to what we can do.
#![allow(clippy::too_many_arguments)]

pub(crate) unsafe fn check_abi() {
    panic!("stub");
}

pub(crate) unsafe fn read_batch_header(
    _base_offset: *mut i64,
    _record_count: *mut i32,
    _partition_leader_epoch: *mut i32,
    _attributes: *mut i16,
    _last_offset_delta: *mut i32,
    _base_timestamp: *mut i64,
    _max_timestamp: *mut i64,
    _producer_id: *mut i64,
    _producer_epoch: *mut i16,
    _base_sequence: *mut i32,
) -> i32 {
    panic!("stub");
}

pub(crate) unsafe fn read_next_record(
    _attributes: *mut u8,
    _timestamp: *mut i64,
    _offset: *mut i64,
    _buf: *mut u8,
    _len: u32,
) -> i32 {
    panic!("stub");
}

pub(crate) unsafe fn write_record(_buf: *const u8, _len: u32) -> i32 {
    panic!("stub");
}

pub(crate) unsafe fn write_record_with_options(
    _buf: *const u8,
    _buf_len: u32,
    _opts: *const u8,
    _opts_len: u32,
) -> i32 {
    panic!("stub");
}
