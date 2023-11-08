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

//go:build wasip1 || tinygo

package sr

import (
	"unsafe"
)

// These are the host functions that go allows for wasm functions that are imported.
// See: https://github.com/golang/go/issues/59149

//go:wasmimport redpanda_schema_registry get_schema_definition_len
func getSchemaDefinitionLen(schemaId schemaId, length unsafe.Pointer) int32

//go:wasmimport redpanda_schema_registry get_schema_definition
func getSchemaDefinition(schemaId schemaId, buf unsafe.Pointer, len int32) int32

//go:wasmimport redpanda_schema_registry get_subject_schema_len
func getSchemaSubjectLen(subject unsafe.Pointer, subjectLen int32, version int32, lenOut unsafe.Pointer) int32

//go:wasmimport redpanda_schema_registry get_subject_schema
func getSchemaSubject(subject unsafe.Pointer, subjectLen int32, version int32, buf unsafe.Pointer, len int32) int32

//go:wasmimport redpanda_schema_registry create_subject_schema
func createSubjectSchema(subject unsafe.Pointer, subjectLen int32, buf unsafe.Pointer, len int32, schemaIdOut unsafe.Pointer) int32
