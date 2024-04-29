package main

import (
	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/ai"
)

var llm *ai.LargeLanguageModel

func main() {
	m, err := ai.LoadLargeLanguageModel(ai.HuggingFaceModel{
		Repo: "Trelis/TinyLlama-1.1B-intermediate-step-480k-1T-GGUF",
		File: "TinyLlama-1.1B-intermediate-step-480k-1T.Q4_K.gguf",
	})
	if err != nil {
		panic(err)
	}
	llm = m
	transform.OnRecordWritten(myTransform)
}

func myTransform(e transform.WriteEvent, w transform.RecordWriter) error {
	resp, err := llm.GenerateText(
		string(e.Record().Value),
		ai.GenerateTextOptions{
			MaxTokens: 50,
		},
	)
	if err != nil {
		return err
	}
	return w.Write(transform.Record{
		Key:   e.Record().Key,
		Value: []byte(resp),
	})
}
