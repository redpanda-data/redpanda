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

//go:build !(wasip1 || tinygo)

package redpanda

import (
	"unsafe"
)

// These functions are stubs of the functions documented in `abi.go`, these functions
// exist here so that the code can be compiled in a non-wasm environment, even if they
// will always fail at runtime.

func checkAbiVersion() {
	panic("stub")
}

func readBatchHeader(
	baseOffset unsafe.Pointer,
	recordCount unsafe.Pointer,
	partitionLeaderEpoch unsafe.Pointer,
	attributes unsafe.Pointer,
	lastOffsetDelta unsafe.Pointer,
	baseTimestamp unsafe.Pointer,
	maxTimestamp unsafe.Pointer,
	producerId unsafe.Pointer,
	producerEpoch unsafe.Pointer,
	baseSequence unsafe.Pointer,
) int32 {
	panic("stub")
}

func readNextRecord(
	attributes unsafe.Pointer,
	timestampDelta unsafe.Pointer,
	offsetDelta unsafe.Pointer,
	buf unsafe.Pointer,
	len int32,
) int32 {
	panic("stub")
}

func writeRecord(buf unsafe.Pointer, len int32) int32 {
	panic("stub")
}
