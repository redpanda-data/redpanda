package wasm

import (
	"bytes"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	kafkaMocks "github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka/mocks"
)

func TestNewRemoveCommand(t *testing.T) {
	tests := []struct {
		name           string
		producer       kafkaMocks.MockProducer
		filename       string
		args           []string
		expectedOutput []string
		expectedErr    string
		admin          kafkaMocks.MockAdmin
	}{
		{
			name: "it should publish a message with correct format",
			args: []string{"filename.js"},
		}, {
			name:        "it should a error if the name arg doesn't set",
			args:        []string{},
			expectedErr: "no wasm script name specified",
		}, {
			name: "it should publish a message with correct format with valid headers",
			args: []string{"filename.js"},
			producer: kafkaMocks.MockProducer{
				MockSendMessage: func(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
					require.Equal(t, msg.Topic, kafka.CoprocessorTopic)
					expectHeader := []sarama.RecordHeader{
						{
							Key:   []byte("action"),
							Value: []byte("remove"),
						}, {
							Key:   []byte("file_name"),
							Value: []byte("filename.js"),
						},
					}
					require.Equal(t, expectHeader, msg.Headers)
					return 0, 0, nil
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			createProduce := func(_ bool, _ int32) (sarama.SyncProducer, error) {
				return tt.producer, nil
			}
			admin := func() (sarama.ClusterAdmin, error) {
				return tt.admin, nil
			}
			var out bytes.Buffer
			logrus.SetOutput(&out)
			logrus.SetLevel(logrus.DebugLevel)
			cmd := NewRemoveCommand(createProduce, admin)
			cmd.SetArgs(tt.args)
			err := cmd.Execute()
			if tt.expectedErr != "" {
				require.Error(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			for _, ln := range tt.expectedOutput {
				require.Contains(t, out.String(), ln)
			}
		})
	}
}
