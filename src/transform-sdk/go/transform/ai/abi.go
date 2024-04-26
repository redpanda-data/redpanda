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

package ai

import (
	"unsafe"
)

// These are the host functions that go allows for wasm functions that are imported.
// See: https://github.com/golang/go/issues/59149

//go:wasmimport redpanda_ai load_hf_embeddings
func loadHfEmbeddings(repo unsafe.Pointer, repoLen int32, file unsafe.Pointer, fileLen int32) int32

//go:wasmimport redpanda_ai load_hf_llm
func loadHfLLM(repo unsafe.Pointer, repoLen int32, file unsafe.Pointer, fileLen int32) int32

//go:wasmimport redpanda_ai generate_text
func generateText(handle int32, text unsafe.Pointer, textLen int32, maxTokens int32, out unsafe.Pointer, outLen int32) int32

//go:wasmimport redpanda_ai compute_embeddings
func computeEmbeddings(handle int32, text unsafe.Pointer, textLen int32, out unsafe.Pointer, outLen int32) int32
