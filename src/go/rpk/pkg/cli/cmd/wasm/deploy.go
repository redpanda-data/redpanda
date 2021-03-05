package wasm

import (
	"fmt"
	"path/filepath"

	"github.com/Shopify/sarama"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
)

func NewDeployCommand(
	fs afero.Fs,
	createProducer func(bool, int32) (sarama.SyncProducer, error),
	adminCreate func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	var description string

	command := &cobra.Command{
		Use:   "deploy <path>",
		Short: "deploy inline WASM function",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			path := args[0]
			fullFileName := filepath.Base(path)
			fileExt := filepath.Ext(fullFileName)
			// validate file extension, just allow js extension
			if fileExt != ".js" {
				return fmt.Errorf("can't deploy '%s': only .js files are supported.", path)
			}
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
				fullFileName,
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
	// create message
	message := CreateDeployMsg(fileName, description, fileContent)
	// publish message
	err = kafka.PublishMessage(producer, &message)
	if err != nil {
		return fmt.Errorf("error deploying '%s.js: %v'", fileName, err)
	}
	return nil
}
