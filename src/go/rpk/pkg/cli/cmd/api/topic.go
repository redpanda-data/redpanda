package api

import (
	"errors"
	"vectorized/pkg/kafka"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewTopicCommand(fs afero.Fs) *cobra.Command {
	var admin sarama.ClusterAdmin

	var brokers []string

	root := &cobra.Command{
		Use:              "topic",
		Short:            "Create, delete or update topics",
		TraverseChildren: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			var err error
			if len(brokers) == 0 {
				brokers = []string{"127.0.0.1:9092"}
			}
			client, err := kafka.InitClient(
				brokers...,
			)
			if err != nil {
				return err
			}
			admin, err = sarama.NewClusterAdminFromClient(client)
			return err
		},
	}
	root.PersistentFlags().StringSliceVar(
		&brokers,
		"brokers",
		[]string{},
		"Comma-separated list of broker ip:port pairs",
	)
	root.AddCommand(createTopic(&admin))
	root.AddCommand(deleteTopic(&admin))

	return root
}

func createTopic(admin *sarama.ClusterAdmin) *cobra.Command {
	var (
		partitions int32
		replicas   int16
		compact    bool
	)
	cmd := &cobra.Command{
		Use:   "create <topic name>",
		Short: "Create a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if admin == nil {
				return errors.New("uninitialized API client")
			}
			adm := *admin
			defer adm.Close()
			topicName := args[0]
			cleanupPolicy := "delete"
			if compact {
				cleanupPolicy = "compact"
			}
			err := adm.CreateTopic(
				topicName,
				&sarama.TopicDetail{
					NumPartitions:     partitions,
					ReplicationFactor: replicas,
					ConfigEntries: map[string]*string{
						"cleanup.policy": &cleanupPolicy,
					},
				},
				false,
			)
			if err != nil {
				return err
			}
			log.Infof(
				"Created topic '%s'. Partitions: %d,"+
					" replicas: %d, cleanup policy: '%s'",
				topicName,
				partitions,
				replicas,
				cleanupPolicy,
			)
			return nil
		},
	}
	cmd.Flags().Int32VarP(
		&partitions,
		"partitions",
		"p",
		int32(1),
		"Number of partitions",
	)
	cmd.Flags().Int16VarP(
		&replicas,
		"replicas",
		"r",
		int16(1),
		"Number of replicas",
	)
	cmd.Flags().BoolVar(
		&compact,
		"compact",
		false,
		"Enable topic compaction",
	)
	return cmd
}

func deleteTopic(admin *sarama.ClusterAdmin) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <topic name>",
		Short: "Delete a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if admin == nil {
				return errors.New("uninitialized API client")
			}
			adm := *admin
			defer adm.Close()

			topicName := args[0]
			err := adm.DeleteTopic(topicName)
			if err != nil {
				return err
			}
			log.Infof("Deleted topic '%s'.", topicName)
			return nil
		},
	}
	return cmd
}
