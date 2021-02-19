package wasm

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
)

func NewRemoveCommand(
	createProduce func(bool, int32) (sarama.SyncProducer, error),
	adminCreate func() (sarama.ClusterAdmin, error),
) *cobra.Command {

	command := &cobra.Command{
		Use:	"remove <name>",
		Short:	"remove inline WASM function",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf(
					"no wasm script name specified",
				)
			}
			return nil
		},
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			producer, err := createProduce(false, -1)
			if err != nil {
				return err
			}
			admin, err := adminCreate()
			if err != nil {
				return err
			}
			return remove(
				name,
				producer,
				admin,
			)
		},
	}

	return command
}

/**
this function create and publish message for removing coprocessor
message format:
{
	key: <file name>,
	header: {
		action: "remove"
	}
}
*/
func remove(
	name string, producer sarama.SyncProducer, admin sarama.ClusterAdmin,
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
	message := CreateRemoveMsg(name)
	//publish message
	return kafka.PublishMessage(producer, &message)
}
