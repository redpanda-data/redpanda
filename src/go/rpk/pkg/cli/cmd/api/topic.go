package api

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"vectorized/pkg/cli/ui"
	"vectorized/pkg/kafka"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewTopicCommand(fs afero.Fs, brokers func() []string) *cobra.Command {
	var admin sarama.ClusterAdmin
	var client sarama.Client

	root := &cobra.Command{
		Use:              "topic",
		Short:            "Create, delete or update topics",
		TraverseChildren: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			var err error
			bs := brokers()
			log.Debugf("Seed brokers: %v", bs)
			client, err = kafka.InitClient(bs...)
			if err != nil {
				return err
			}
			admin, err = sarama.NewClusterAdminFromClient(client)
			return err
		},
	}
	root.AddCommand(createTopic(&admin))
	root.AddCommand(deleteTopic(&admin))
	root.AddCommand(setTopicConfig(&admin))
	root.AddCommand(listTopics(&admin))
	root.AddCommand(describeTopic(&client, &admin))

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

func setTopicConfig(admin *sarama.ClusterAdmin) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-config <topic> <key> [<value>]",
		Short: "Set the topic's config key/value pairs",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			if admin == nil {
				return errors.New("uninitialized API client")
			}
			adm := *admin
			defer adm.Close()

			topicName := args[0]
			key := args[1]
			value := args[2]

			err := adm.AlterConfig(
				sarama.TopicResource,
				topicName,
				map[string]*string{key: &value},
				false,
			)
			if err != nil {
				return err
			}
			log.Infof(
				"Added config '%s'='%s' to topic '%s'.",
				key,
				value,
				topicName,
			)
			return nil
		},
	}
	return cmd
}

func describeTopic(
	client *sarama.Client, admin *sarama.ClusterAdmin,
) *cobra.Command {
	var (
		page              int
		pageSize          int
		includeWatermarks bool
	)
	cmd := &cobra.Command{
		Use:   "describe <topic>",
		Short: "Describe topic",
		Long:  "Describe a topic. Default values of the configuration are omitted.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if admin == nil || client == nil {
				return errors.New("uninitialized API client")
			}
			adm := *admin
			cl := *client
			defer adm.Close()

			topicName := args[0]
			highWatermarks := map[int32]int64{}
			topicDetails, err := adm.DescribeTopics([]string{topicName})
			if err != nil {
				return err
			}
			detail := topicDetails[0]

			if detail.Err == sarama.ErrUnknownTopicOrPartition {
				return fmt.Errorf("topic '%v' not found", topicName)
			}

			cfg, err := adm.DescribeConfig(sarama.ConfigResource{
				Type: sarama.TopicResource,
				Name: topicName,
			})
			if err != nil {
				return err
			}

			cleanupPolicy := ""
			nonDefaultCfg := []sarama.ConfigEntry{}
			for _, e := range cfg {
				if e.Name == "cleanup.policy" {
					cleanupPolicy = e.Value
				}
				if !e.Default {
					nonDefaultCfg = append(nonDefaultCfg, e)
				}
			}

			sort.Slice(detail.Partitions, func(i, j int) bool {
				return detail.Partitions[i].ID < detail.Partitions[j].ID
			})

			t := ui.NewRpkTable(log.StandardLogger().Out)
			t.SetColWidth(80)
			t.SetAutoWrapText(true)
			t.AppendBulk([][]string{
				{"Name", detail.Name},
				{"Internal", fmt.Sprintf("%t", detail.IsInternal)},
				{"Cleanup policy", cleanupPolicy},
			})

			if len(nonDefaultCfg) > 0 {
				t.Append([]string{"Config:"})
				t.Append([]string{"Name", "Value", "Read-only", "Sensitive"})
			}
			for _, entry := range nonDefaultCfg {
				t.Append([]string{
					entry.Name,
					entry.Value,
					strconv.FormatBool(entry.ReadOnly),
					strconv.FormatBool(entry.Sensitive),
				})
			}
			t.Render()
			t.ClearRows()

			pagedPartitions := detail.Partitions
			beginning := 0
			end := len(detail.Partitions)
			if pageSize >= 0 {
				pagedPartitions, beginning, end = pagePartitions(
					detail.Partitions,
					page,
					pageSize,
				)
			}

			t.Append([]string{
				"Partitions",
				fmt.Sprintf(
					"%d - %d out of %d",
					beginning+1,
					end,
					len(detail.Partitions),
				),
			})
			t.Render()
			t.ClearRows()
			partitionHeaders := []string{"Partition", "Leader", "Replicas", "In-Sync Replicas"}
			if includeWatermarks {
				partitionHeaders = append(partitionHeaders, "High Watermark")
			}
			t.Append(partitionHeaders)

			partitions := make([]int32, 0, len(pagedPartitions))
			for _, partition := range pagedPartitions {
				partitions = append(partitions, partition.ID)
			}
			if includeWatermarks {
				highWatermarks, err = kafka.HighWatermarks(cl, topicName, partitions)
				if err != nil {
					return err
				}
			}

			for _, partition := range pagedPartitions {
				sortedReplicas := partition.Replicas
				sort.Slice(sortedReplicas, func(i, j int) bool {
					return sortedReplicas[i] < sortedReplicas[j]
				})

				sortedISR := partition.Isr
				sort.Slice(sortedISR, func(i, j int) bool {
					return sortedISR[i] < sortedISR[j]
				})
				row := []string{
					strconv.Itoa(int(partition.ID)),
					strconv.Itoa(int(partition.Leader)),
					fmt.Sprintf("%v", sortedReplicas),
					fmt.Sprintf("%v", sortedISR),
				}
				if includeWatermarks {
					row = append(
						row,
						strconv.Itoa(int(highWatermarks[partition.ID])),
					)
				}
				t.Append(row)
			}
			t.Render()
			return nil
		},
	}
	cmd.Flags().IntVar(
		&page,
		"page",
		0,
		"The partitions page to display. If negative, all partitions will be shown",
	)
	cmd.Flags().IntVar(
		&pageSize,
		"page-size",
		20,
		"The number of partitions displayed per page",
	)
	cmd.Flags().BoolVar(
		&includeWatermarks,
		"watermarks",
		true,
		"If enabled, will display the topic's partitions' high watermarks",
	)
	return cmd
}

func listTopics(admin *sarama.ClusterAdmin) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List topics",
		Args:    cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if admin == nil {
				return errors.New("uninitialized API client")
			}
			adm := *admin
			defer adm.Close()

			topics, err := adm.ListTopics()
			if err != nil {
				return err
			}
			if len(topics) == 0 {
				log.Info("No topics found.")
				return nil
			}

			sortedTopics := make(
				[]struct {
					name string
					sarama.TopicDetail
				}, len(topics))

			i := 0
			for name, topic := range topics {
				sortedTopics[i].name = name
				sortedTopics[i].TopicDetail = topic
				i++
			}

			sort.Slice(sortedTopics, func(i int, j int) bool {
				return sortedTopics[i].name < sortedTopics[j].name
			})

			t := ui.NewRpkTable(log.StandardLogger().Out)
			t.Append([]string{"Name", "Partitions", "Replicas"})

			for _, topic := range sortedTopics {
				t.Append([]string{
					topic.name,
					strconv.Itoa(int(topic.NumPartitions)),
					strconv.Itoa(int(topic.ReplicationFactor)),
				})
			}
			t.Render()
			return nil
		},
	}
	return cmd
}

func pagePartitions(
	parts []*sarama.PartitionMetadata, page int, pageSize int,
) ([]*sarama.PartitionMetadata, int, int) {
	noParts := len(parts)
	// If the number of partitions is less than the page size,
	// return all the partitions
	if noParts < pageSize {
		return parts, 0, noParts
	}
	noPages := noParts / pageSize
	beginning := page * pageSize
	// If the given page exceeds the number of pages,
	// or the beginning index exceeds the number of partitions,
	// return the last page
	if page > noPages || beginning >= noParts {
		lastPageSize := noParts - noPages*pageSize
		if lastPageSize == 0 {
			lastPageSize = pageSize
		}
		beginning = noParts - lastPageSize
		return parts[beginning:], beginning, noParts
	}
	end := int(math.Min(float64(beginning+pageSize), float64(noParts)))
	return parts[beginning:end], beginning, end
}
