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

#[link(wasm_import_module = "redpanda_transform")]
extern "C" {
    #[link_name = "check_abi_version_2"]
    pub(crate) fn check_abi();

    #[link_name = "read_batch_header"]
    pub(crate) fn read_batch_header(
        base_offset: *mut i64,
        record_count: *mut i32,
        partition_leader_epoch: *mut i32,
        attributes: *mut i16,
        last_offset_delta: *mut i32,
        base_timestamp: *mut i64,
        max_timestamp: *mut i64,
        producer_id: *mut i64,
        producer_epoch: *mut i16,
        base_sequence: *mut i32,
    ) -> i32;

    #[link_name = "read_next_record"]
    pub(crate) fn read_next_record(
        attributes: *mut u8,
        timestamp: *mut i64,
        offset: *mut i64,
        buf: *mut u8,
        len: u32,
    ) -> i32;

    #[link_name = "write_record"]
    pub(crate) fn write_record(buf: *const u8, len: u32) -> i32;

    #[link_name = "write_record_with_options"]
    pub(crate) fn write_record_with_options(
        buf: *const u8,
        buf_len: u32,
        opts: *const u8,
        opts_len: u32,
    ) -> i32;
}
