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

pub(crate) unsafe fn check() {
    panic!();
}

pub(crate) unsafe fn get_schema_definition_len(_schema_id: i32, _len: *mut i32) -> i32 {
    panic!();
}

pub(crate) unsafe fn get_schema_definition(_schema_id: i32, _buf: *mut u8, _len: i32) -> i32 {
    panic!();
}

pub(crate) unsafe fn get_subject_schema_len(
    _subject: *const u8,
    _subject_len: i32,
    _version: i32,
    _len: *mut i32,
) -> i32 {
    panic!();
}

pub(crate) unsafe fn get_subject_schema(
    _subject: *const u8,
    _subject_len: i32,
    _version: i32,
    _buf: *mut u8,
    _len: i32,
) -> i32 {
    panic!();
}

pub(crate) unsafe fn create_subject_schema(
    _subject: *const u8,
    _subject_len: i32,
    _buf: *const u8,
    _len: i32,
    _schema_id_out: *mut i32,
) -> i32 {
    panic!();
}
