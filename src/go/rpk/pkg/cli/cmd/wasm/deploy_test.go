package wasm

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	kafkaMocks "github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka/mocks"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/utils"
)

type fileInfo struct {
	name	string
	content	string
}

func TestNewDeployCommand(t *testing.T) {
	tests := []struct {
		name		string
		producer	func(bool, int32) (sarama.SyncProducer, error)
		fileInformation	fileInfo
		args		[]string
		expectedOutput	[]string
		expectedErr	string
		pre		func(fs afero.Fs, fileInformation fileInfo) error
		failSendMessage	bool
		admin		kafkaMocks.MockAdmin
	}{
		{
			name:	"it should publish a message with correct format",
			args:	[]string{"--description", "coprocessor description"},
			fileInformation: fileInfo{
				name:		"fileName.js",
				content:	"let s = 'text'",
			},
			pre: func(fs afero.Fs, fileInformation fileInfo) error {
				return createMockFile(fs, fileInformation.name, fileInformation.content)
			},
		}, {
			name:	"it should fail if the file extension isn't js",
			fileInformation: fileInfo{
				name: "fileName.html",
			},
			expectedErr:	"can't deploy 'fileName.html': only .js files are supported.",
		}, {
			name:	"it should fail if the file doesn't exist",
			fileInformation: fileInfo{
				name: "fileName.js",
			},
			expectedErr:	"open fileName.js: file does not exist",
		}, {
			name:	"it should show an error if there is a error on send message",
			fileInformation: fileInfo{
				name: "fileName.js",
			},
			pre: func(fs afero.Fs, fileInformation fileInfo) error {
				return createMockFile(fs, fileInformation.name, fileInformation.content)
			},
			expectedErr: "Error on deploy fileName.js, message error: kafka mock error," +
				" please check your redpanda instance ",
			failSendMessage:	true,
			admin: kafkaMocks.MockAdmin{
				MockListTopics: func() (map[string]sarama.TopicDetail, error) {
					topics := make(map[string]sarama.TopicDetail)
					topics[kafka.CoprocessorTopic] = sarama.TopicDetail{}
					return topics, nil
				},
			},
		}, {
			name:	"it should create a coprocessor_internal_topic if it doesn't exist",
			fileInformation: fileInfo{
				name: "fileName.js",
			},
			pre: func(fs afero.Fs, fileInformation fileInfo) error {
				return createMockFile(fs, fileInformation.name, fileInformation.content)
			},
			admin: kafkaMocks.MockAdmin{
				MockListTopics: func() (map[string]sarama.TopicDetail, error) {
					return map[string]sarama.TopicDetail{}, nil
				},
				MockCreateTopic: func(topic string, detail *sarama.TopicDetail, b bool) error {
					_, exist := detail.ConfigEntries["cleanup.policy"]
					require.True(t, exist)
					require.Equal(t, kafka.CoprocessorTopic, topic)
					return nil
				},
			},
		}, {
			name:	"it shouldn't create a coprocessor_internal_topic if it exist",
			fileInformation: fileInfo{
				name: "fileName.js",
			},
			pre: func(fs afero.Fs, fileInformation fileInfo) error {
				return createMockFile(fs, fileInformation.name, fileInformation.content)
			},
			admin: kafkaMocks.MockAdmin{
				MockListTopics: func() (map[string]sarama.TopicDetail, error) {
					result := make(map[string]sarama.TopicDetail)
					result[kafka.CoprocessorTopic] = sarama.TopicDetail{}
					return result, nil
				},
				MockCreateTopic: func(topic string, detail *sarama.TopicDetail, b bool) error {
					require.NoError(t, fmt.Errorf("it shouldn't create topic"))
					return nil
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			createProduce := func(_ bool, _ int32) (sarama.SyncProducer, error) {
				produce := mocks.NewSyncProducer(t, nil)
				if !tt.failSendMessage {
					produce.ExpectSendMessageWithCheckerFunctionAndSucceed(func(val []byte) error {
						if tt.fileInformation.content != "" {
							expectValue, _ := utils.WriteString([]byte{}, tt.fileInformation.content)
							require.Equal(t, expectValue, val)
						}
						return nil
					})
				} else {
					// it must fail 3 times for kafka retry policies.
					produce.ExpectSendMessageAndFail(fmt.Errorf(""))
					produce.ExpectSendMessageAndFail(fmt.Errorf(""))
					produce.ExpectSendMessageAndFail(fmt.Errorf("kafka mock error"))
				}
				return produce, nil
			}

			admin := func() (sarama.ClusterAdmin, error) {
				return tt.admin, nil
			}

			fs := afero.NewMemMapFs()
			if tt.pre != nil {
				require.NoError(t, tt.pre(fs, tt.fileInformation))
			}

			var out bytes.Buffer
			logrus.SetOutput(&out)
			logrus.SetLevel(logrus.DebugLevel)
			cmd := NewDeployCommand(fs, createProduce, admin)
			cmd.SetArgs(append([]string{tt.fileInformation.name}, tt.args...))
			err := cmd.Execute()
			if tt.expectedErr != "" {
				require.Errorf(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			for _, ln := range tt.expectedOutput {
				require.Contains(t, out.String(), ln)
			}
		})
	}
}

func createMockFile(fs afero.Fs, fileName string, fileContent string) error {
	file, err := fs.Create(fileName)
	if err != nil {
		return err
	}
	_, err = file.Write([]byte(fileContent))
	if err != nil {
		return err
	}
	return file.Close()
}
