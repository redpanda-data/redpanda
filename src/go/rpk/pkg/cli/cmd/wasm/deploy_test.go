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
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	kafkaMocks "github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka/mocks"
)

type fileInfo struct {
	name    string
	content string
}

func TestNewDeployCommand(t *testing.T) {
	tests := []struct {
		name            string
		producer        kafkaMocks.MockProducer
		fileInformation fileInfo
		args            []string
		expectedOutput  []string
		expectedErr     string
		pre             func(fs afero.Fs, fileInformation fileInfo) error
		admin           kafkaMocks.MockAdmin
	}{
		{
			name: "it should publish a message with correct format",
			args: []string{"--description", "coprocessor description", "--name", "foo"},
			fileInformation: fileInfo{
				name:    "fileName.js",
				content: "let s = 'text'",
			},
			pre: func(fs afero.Fs, fileInformation fileInfo) error {
				return createMockFile(fs, fileInformation.name, fileInformation.content)
			},
		}, {
			name: "it should fail if the file extension isn't js",
			args: []string{"--name", "foo"},
			fileInformation: fileInfo{
				name: "fileName.html",
			},
			expectedErr: "can't deploy 'fileName.html': only .js files are supported.",
		}, {
			name: "it should fail if the file doesn't exist",
			args: []string{"--name", "foo"},
			fileInformation: fileInfo{
				name: "fileName.js",
			},
			expectedErr: "open fileName.js: file does not exist",
		}, {
			name: "it should show an error if there is a error on send message",
			args: []string{"--name", "foo"},
			fileInformation: fileInfo{
				name: "fileName.js",
			},
			pre: func(fs afero.Fs, fileInformation fileInfo) error {
				return createMockFile(fs, fileInformation.name, fileInformation.content)
			},
			expectedErr: "Error on deploy fileName.js, message error: kafka mock error," +
				" please check your redpanda instance ",
			admin: kafkaMocks.MockAdmin{
				MockListTopics: func() (map[string]sarama.TopicDetail, error) {
					topics := make(map[string]sarama.TopicDetail)
					topics[kafka.CoprocessorTopic] = sarama.TopicDetail{}
					return topics, nil
				},
			},
			producer: kafkaMocks.MockProducer{
				MockSendMessage: func(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
					return 0, 0, fmt.Errorf("kafka mock error ")
				},
			},
		}, {
			name: "it should create a coprocessor_internal_topic if it doesn't exist",
			args: []string{"--name", "foo"},
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
			name: "it shouldn't create a coprocessor_internal_topic if it exist",
			args: []string{"--name", "foo"},
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
		}, {
			name: "it should publish a message with correct format with correct header",
			args: []string{"--description", "coprocessor description", "--name", "bar"},
			fileInformation: fileInfo{
				name:    "fileName.js",
				content: "let s = 'text'",
			},
			pre: func(fs afero.Fs, fileInformation fileInfo) error {
				return createMockFile(fs, fileInformation.name, fileInformation.content)
			},
			producer: kafkaMocks.MockProducer{
				MockSendMessage: func(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
					require.Equal(t, msg.Topic, kafka.CoprocessorTopic)
					hashContent := sha256.Sum256([]byte("let s = 'text'"))
					expectHeader := []sarama.RecordHeader{
						{
							Key:   []byte("action"),
							Value: []byte("deploy"),
						}, {
							Key:   []byte("description"),
							Value: []byte("coprocessor description"),
						}, {
							Key:   []byte("sha256"),
							Value: hashContent[:],
						}, {
							Key:   []byte("name"),
							Value: []byte("bar"),
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
