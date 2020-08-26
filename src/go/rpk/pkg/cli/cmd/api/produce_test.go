package api_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"vectorized/pkg/cli/cmd/api"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

type mockSyncProducer struct {
	sarama.SyncProducer
	sendMessage func(*sarama.ProducerMessage) (int32, int64, error)
}

func (sp *mockSyncProducer) SendMessage(
	msg *sarama.ProducerMessage,
) (partition int32, offset int64, err error) {
	if sp.sendMessage != nil {
		return sp.sendMessage(msg)
	}
	return partition, offset, err
}

func TestProduceCmd(t *testing.T) {
	tests := []struct {
		name           string
		producer       func(bool, int32) (sarama.SyncProducer, error)
		args           []string
		data           string
		expectedOutput []string
		expectedErr    string
	}{
		{
			name: "it should produce a record from stdin input",
			args: []string{"topic-name", "-k", "key", "-j", "-H", "k:v", "-t", "2020-08-09T22:36:34-05:00"},
			data: `{"very":"important", "data": true}`,
			expectedOutput: []string{
				"Sent record to partition 0 at offset 0 with timestamp 2020-08-09 22:36:34 -0500 -05",
				`Data: '{\"very\":\"important\", \"data\": true}'`,
				"Headers: 'k:v'",
			},
		},
		{
			name: "it should work without -t",
			args: []string{"topic-name", "-k", "key", "-j", "-H", "k:v"},
			data: `{"very":"important", "data": true}`,
			expectedOutput: []string{
				"Sent record to partition 0 at offset 0 with timestamp",
				`Data: '{\"very\":\"important\", \"data\": true}'`,
				"Headers: 'k:v'",
			},
		},
		{
			name: "it should work without -j",
			args: []string{"topic-name", "-k", "key", "-H", "k:v"},
			data: `{"very":"important", "data": true}`,
			expectedOutput: []string{
				"Sent record to partition 0 at offset 0 with timestamp",
				`Data: '{\"very\":\"important\", \"data\": true}'`,
				"Headers: 'k:v'",
			},
		},
		{
			name: "it should work without passing headers",
			args: []string{"topic-name", "-k", "key"},
			data: `{"very":"important", "data": true}`,
			expectedOutput: []string{
				"Sent record to partition 0 at offset 0 with timestamp",
				`Data: '{\"very\":\"important\", \"data\": true}'`,
				"Headers: ''",
			},
		},
		{
			name: "it should work passing multiple headers",
			args: []string{"topic-name", "-k", "key", "-H", "k1:v1", "-H", "k2:v2"},
			data: `{"very":"important", "data": true}`,
			expectedOutput: []string{
				"Sent record to partition 0 at offset 0 with timestamp",
				`Data: '{\"very\":\"important\", \"data\": true}'`,
				"Headers: 'k1:v1, k2:v2'",
			},
		},
		{
			name:        "it should omit headers that can't be parsed",
			args:        []string{"topic-name", "-k", "key", "-H", "whatisthis", "-H", "k2:v2"},
			data:        `{"very":"important", "data": true}`,
			expectedErr: "'whatisthis' doesn't conform to the <k>:<v> format",
		},
		{
			name:        "it should fail if no topic is passed",
			args:        []string{"-k", "key"},
			data:        `{"very":"important", "data": true}`,
			expectedErr: "accepts 1 arg(s), received 0",
		},
		{
			name: "it should fail if the producer creation fails",
			producer: func(_ bool, _ int32) (sarama.SyncProducer, error) {
				return nil, errors.New("boom I ain't joking")
			},
			args:        []string{"topic-name", "-k", "key"},
			data:        `{"very":"important", "data": true}`,
			expectedErr: "boom I ain't joking",
		},
		{
			name: "it should fail if sending the message fails",
			producer: func(_ bool, _ int32) (sarama.SyncProducer, error) {
				sp := &mockSyncProducer{
					sendMessage: func(_ *sarama.ProducerMessage) (int32, int64, error) {
						return 0, 0, errors.New("can't send")
					},
				}
				return sp, nil
			},
			args:        []string{"topic-name", "-k", "key"},
			data:        `{"very":"important", "data": true}`,
			expectedErr: "can't send",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer := func(_ bool, _ int32) (sarama.SyncProducer, error) {
				return &mockSyncProducer{}, nil
			}
			if tt.producer != nil {
				producer = tt.producer
			}
			var out bytes.Buffer
			in := strings.NewReader(tt.data)
			cmd := api.NewProduceCommand(producer)
			cmd.SetArgs(tt.args)
			cmd.SetIn(in)
			logrus.SetOutput(&out)
			logrus.SetLevel(logrus.DebugLevel)
			err := cmd.Execute()
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			for _, ln := range tt.expectedOutput {
				require.Contains(t, out.String(), ln)
			}
		})
	}
}
