// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package wasm

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	kafkaMocks "github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka/mocks"
)

func newClientMock(offset int64) *kafkaMocks.MockClient {
	return &kafkaMocks.MockClient{
		MockGetOffset: func(string, int32, int64) (int64, error) {
			return offset, nil
		},
	}
}

func parseLogrusOutput(output string) string {
	plain := strings.TrimLeft(output, "level=info msg=\"")
	plain = strings.Replace(plain, "\\", "", -1)
	plain = strings.Replace(plain, "\n", "", -1)
	plain = strings.TrimSuffix(plain, "\"")
	return plain
}

func compareJSONResults(expected string, observed string) bool {
	expectedParsed := make(map[string]interface{})
	observedParsed := make(map[string]interface{})
	if err := json.Unmarshal([]byte(expected), &expectedParsed); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(observed), &observedParsed); err != nil {
		return false
	}
	return reflect.DeepEqual(expectedParsed, observedParsed)
}

func TestNewListCommand(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		mockClient     kafkaMocks.MockClient
		expectedOutput string
		numberOfEvents int64
		consumer       kafkaMocks.MockConsumer
		pc             kafkaMocks.MockPartitionConsumer
	}{
		{
			name:           "It should return the actual results",
			args:           []string{"--raw"},
			numberOfEvents: 1,
			expectedOutput: "{\"5\": {\"status\":\"up\",\"node_id\":5,\"coprocessors\":{\"22220-name\":{\"input_topics\":[\"bar\"],\"description\":\"Hello World!\"}, \"77778-name\":{\"input_topics\":[\"baz\",\"foo\"],\"description\":\"Hello world again!\"}}}}",
			pc: kafkaMocks.MockPartitionConsumer{
				MockMessages: func() <-chan *sarama.ConsumerMessage {
					nodeId := uint32(5)
					binaryNodeId := make([]byte, 4)
					binary.LittleEndian.PutUint32(binaryNodeId, nodeId)
					messages := make(chan *sarama.ConsumerMessage, 1)
					message := &sarama.ConsumerMessage{
						Key:       binaryNodeId,
						Value:     []byte("{\"status\":\"up\",\"node_id\":5,\"coprocessors\":{\"22220-name\":{\"input_topics\":[\"bar\"],\"description\":\"Hello World!\"}, \"77778-name\":{\"input_topics\":[\"baz\",\"foo\"],\"description\":\"Hello world again!\"}}}"),
						Topic:     kafka.CoprocessorStatusTopic,
						Partition: 0,
						Offset:    0,
					}
					messages <- message
					return messages
				},
				MockErrors: func() <-chan *sarama.ConsumerError {
					return make(chan *sarama.ConsumerError)
				},
				MockHighWaterMarkOffset: func() int64 { return 1 },
				MockAsyncClose:          func() {},
				MockClose:               func() error { return nil },
			},
		},
		{
			name:           "It should not error if there are 0 results",
			args:           []string{"--raw"},
			expectedOutput: "{}",
			numberOfEvents: 0,
			pc: kafkaMocks.MockPartitionConsumer{
				MockMessages: func() <-chan *sarama.ConsumerMessage {
					return make(chan *sarama.ConsumerMessage)
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := func() (sarama.Client, error) {
				return newClientMock(tt.numberOfEvents), nil
			}
			consumer := func(sarama.Client) (sarama.Consumer, error) {
				return &kafkaMocks.MockConsumer{
					MockConsumePartition: func(string, int32, int64) (sarama.PartitionConsumer, error) {
						return tt.pc, nil
					},
				}, nil
			}
			var out bytes.Buffer
			logrus.SetOutput(&out)
			logrus.SetLevel(logrus.InfoLevel)
			logrus.SetFormatter(&logrus.TextFormatter{
				DisableTimestamp: true,
			})
			cmd := NewListCommand(client, consumer)
			cmd.SetArgs(tt.args)
			err := cmd.Execute()
			require.NoError(t, err)
			output := parseLogrusOutput(out.String())
			t.Log(tt.expectedOutput)
			t.Log(output)
			require.Equal(t, compareJSONResults(tt.expectedOutput, output), true)
		})
	}
}
