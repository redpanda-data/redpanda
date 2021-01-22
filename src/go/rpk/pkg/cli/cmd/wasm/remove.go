package wasm

import (
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/utils"
)

func NewRemoveCommand(
	createProduce func(bool, int32) (sarama.SyncProducer, error),
	adminCreate func() (sarama.ClusterAdmin, error),
) *cobra.Command {

	command := &cobra.Command{
		Use:	"remove <name>",
		Short:	"remove inline WASM function",
		Args:	cobra.ExactArgs(1),
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
	// create empty message, the remove command doesn't need
	// information on message, just a key value
	var emptyMessage []byte
	header, err := createHeader("remove")
	if err != nil {
		return err
	}
	//publish message
	return kafka.PublishMessage(
		producer,
		emptyMessage,
		name,
		kafka.CoprocessorTopic,
		[]sarama.RecordHeader{header},
	)
}

func createHeader(action string) (sarama.RecordHeader, error) {
	// create key
	key, err := utils.WriteString([]byte{}, "action")
	if err != nil {
		return sarama.RecordHeader{}, err
	}
	// create value
	value, err := utils.WriteString([]byte{}, action)
	if err != nil {
		return sarama.RecordHeader{}, err
	}
	return sarama.RecordHeader{
		Key:	key,
		Value:	value,
	}, nil
}
