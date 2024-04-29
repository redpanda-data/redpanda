package main

import (
	"encoding/json"
	"unsafe"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/ai"
)

var model *ai.EmbeddingsModel

func main() {
	m, err := ai.LoadEmbeddingsModel(ai.HuggingFaceModel{
		Repo: "leliuga/all-MiniLM-L12-v2-GGUF",
		File: "all-MiniLM-L12-v2.Q8_0.gguf",
	})
	if err != nil {
		panic(err)
	}
	model = m
	transform.OnRecordWritten(myTransform)
}

func myTransform(e transform.WriteEvent, w transform.RecordWriter) error {
	// zero copy compute embeddings
	val := e.Record().Value
	str := unsafe.String(&val[0], len(val))
	resp, err := model.ComputeEmbeddings(str)
	if err != nil {
		return err
	}
	// Turn the embeddings into JSON
	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	return w.Write(transform.Record{
		Key:   e.Record().Key,
		Value: b,
	})
}
