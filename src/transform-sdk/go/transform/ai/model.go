// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ai

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"unsafe"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/rwbuf"
)

type modelHandle int32

var buf *rwbuf.RWBuf = rwbuf.New(10240)

// A file from hugging face, only supports gguf file format.
type HuggingFaceModel struct {
	Repo string
	File string
}

// LoadLargeLanguageModel from hugging face
func LoadLargeLanguageModel(m HuggingFaceModel) (*LargeLanguageModel, error) {
	r := loadHfLLM(
		unsafe.Pointer(unsafe.StringData(m.Repo)),
		int32(len(m.Repo)),
		unsafe.Pointer(unsafe.StringData(m.File)),
		int32(len(m.File)),
	)
	if r < 0 {
		return nil, errors.New("unable to load model, errorcode: " + strconv.Itoa(int(r)))
	}
	return &LargeLanguageModel{modelHandle(r)}, nil
}

// LoadEmbeddingsModel from hugging face
func LoadEmbeddingsModel(m HuggingFaceModel) (*EmbeddingsModel, error) {
	r := loadHfEmbeddings(
		unsafe.Pointer(unsafe.StringData(m.Repo)),
		int32(len(m.Repo)),
		unsafe.Pointer(unsafe.StringData(m.File)),
		int32(len(m.File)),
	)
	if r < 0 {
		return nil, errors.New("unable to load model, errorcode: " + strconv.Itoa(int(r)))
	}
	return &EmbeddingsModel{modelHandle(r)}, nil
}

// Wrapper around LLM
type LargeLanguageModel struct {
	handle modelHandle
}

// Generate tokens
type GenerateTextOptions struct {
	MaxTokens int
}

// GenerateText from an LLM
func (llm *LargeLanguageModel) GenerateText(prompt string, opts GenerateTextOptions) (string, error) {
	buf.Reset()
	r := generateText(
		int32(llm.handle),
		unsafe.Pointer(unsafe.StringData(prompt)),
		int32(len(prompt)),
		int32(opts.MaxTokens),
		unsafe.Pointer(buf.WriterBufPtr()),
		int32(buf.WriterLen()),
	)
	if r < 0 {
		return "", errors.New("unable to generate text, errorcode: " + strconv.Itoa(int(r)))
	}
	buf.AdvanceWriter(int(r))
	return string(buf.ReadAll()), nil
}

type EmbeddingsModel struct {
	handle modelHandle
}

// ComputeEmbeddings from a model
func (llm *EmbeddingsModel) ComputeEmbeddings(text string) ([]float32, error) {
	buf.Reset()
	r := computeEmbeddings(
		int32(llm.handle),
		unsafe.Pointer(unsafe.StringData(text)),
		int32(len(text)),
		unsafe.Pointer(buf.WriterBufPtr()),
		int32(buf.WriterLen()/4),
	)
	if r < 0 {
		return nil, errors.New("unable to generate text, errorcode: " + strconv.Itoa(int(r)))
	}
	buf.AdvanceWriter(int(r) * 4)
	b := buf.ReadAll()
	out := make([]float32, int(r)/4)
	for i := 0; i < len(out); i += 1 {
		bits := binary.LittleEndian.Uint32(b[i*4:])
		out[i] = math.Float32frombits(bits)
	}
	return out, nil
}
