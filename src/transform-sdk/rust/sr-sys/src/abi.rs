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

#[link(wasm_import_module = "redpanda_schema_registry")]
extern "C" {
    #[link_name = "check_abi_version_0"]
    pub(crate) fn check();

    #[link_name = "get_schema_definition_len"]
    pub(crate) fn get_schema_definition_len(schema_id: i32, len: *mut i32) -> i32;

    #[link_name = "get_schema_definition"]
    pub(crate) fn get_schema_definition(schema_id: i32, buf: *mut u8, len: i32) -> i32;

    #[link_name = "get_subject_schema_len"]
    pub(crate) fn get_subject_schema_len(
        subject: *const u8,
        subject_len: i32,
        version: i32,
        len: *mut i32,
    ) -> i32;

    #[link_name = "get_subject_schema"]
    pub(crate) fn get_subject_schema(
        subject: *const u8,
        subject_len: i32,
        version: i32,
        buf: *mut u8,
        len: i32,
    ) -> i32;

    #[link_name = "create_subject_schema"]
    pub(crate) fn create_subject_schema(
        subject: *const u8,
        subject_len: i32,
        buf: *const u8,
        len: i32,
        schema_id_out: *mut i32,
    ) -> i32;
}
