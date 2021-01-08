package wasm

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/utils"
)

func NewDeployCommand(
	fs afero.Fs,
	createProducer func(bool, int32) (sarama.SyncProducer, error),
	adminCreate func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	var description string

	command := &cobra.Command{
		Use:	"deploy <path>",
		Short:	"deploy inline WASM function",
		Args:	cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			path := args[0]
			fullFileName := filepath.Base(path)
			fileExt := filepath.Ext(fullFileName)
			// validate file extension, just allow js extension
			if fileExt != ".js" {
				return fmt.Errorf("can't deploy '%s': only .js files are supported.", path)
			}
			fileName := strings.Replace(fullFileName, fileExt, "", 1)
			fileContent, err := afero.ReadFile(fs, path)
			if err != nil {
				return err
			}
			// create producer
			producer, err := createProducer(false, -1)
			if err != nil {
				return err
			}
			// create admin
			admin, err := adminCreate()
			if err != nil {
				return err
			}

			return deploy(
				fileName,
				fileContent,
				description,
				producer,
				admin,
			)
		},
	}

	command.Flags().StringVar(
		&description,
		"description",
		"",
		"Optional description about what the wasm function does, for reference.",
	)

	return command
}

/**
this function create and publish message for deploying coprocessor
message format:
{
	key: <file name>,
	header: {
		action: "deploy",
		sha256: <file content sha256>,
		description: <file description>
	}
	message: <binary file content>
}
*/
func deploy(
	fileName string,
	fileContent []byte,
	description string,
	producer sarama.SyncProducer,
	admin sarama.ClusterAdmin,
) error {
	exist, err := ExistingTopic(admin, kafka.CoprocessorTopic)
	if err != nil {
		return err
	}
	if !exist {
		err = CreateCoprocessorTopic(admin)
		if err != nil {
			return err
		}
	}
	// create headers
	headers, err := createHeaders("deploy", description, fileContent)
	if err != nil {
		return err
	}
	// create message
	messageContent, err := utils.WriteBuffer([]byte{}, fileContent)
	if err != nil {
		return err
	}
	// publish message
	error := kafka.PublishMessage(producer, messageContent, fileName, kafka.CoprocessorTopic, headers)
	if error != nil {
		return fmt.Errorf("error deploying '%s.js: %v'", fileName, error)
	}
	return nil
}

func createHeaders(
	action string, description string, content []byte,
) ([]sarama.RecordHeader, error) {
	var headersResult []sarama.RecordHeader
	// create simple struct for key string and value string
	value := []struct {
		key	string
		value	string
	}{
		{key: "action", value: action},
		{key: "description", value: description},
	}
	// create RecordHeader and append to headersResult
	for _, v := range value {
		keyH, err := utils.WriteString([]byte{}, v.key)
		if err != nil {
			return nil, err
		}
		valueH, err := utils.WriteString([]byte{}, v.value)
		if err != nil {
			return nil, err
		}
		headersResult = append(headersResult, struct {
			Key	[]byte
			Value	[]byte
		}{Key: keyH, Value: valueH})
	}
	checkSumHeader, err := createCheckSumHeader(content)
	if err != nil {
		return nil, err
	}
	// append checksum header
	headersResult = append(headersResult, checkSumHeader)

	return headersResult, nil
}

func createCheckSumHeader(content []byte) (sarama.RecordHeader, error) {
	// create key for checksum
	keySha, err := utils.WriteString([]byte{}, "checksum")
	if err != nil {
		return sarama.RecordHeader{}, err
	}
	// create sha256 value for content
	shaValue := sha256.Sum256(content)
	// create value for checksum
	valueSha, err := utils.WriteBuffer([]byte{}, shaValue[:])
	if err != nil {
		return sarama.RecordHeader{}, err
	}
	return sarama.RecordHeader{
		Key:	keySha,
		Value:	valueSha,
	}, nil
}
