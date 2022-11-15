// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

func newMetadataCommand(fs afero.Fs) *cobra.Command {
	var (
		cluster  bool
		brokers  bool
		topics   bool
		internal bool
		detailed bool
	)
	cmd := &cobra.Command{
		Use:     "metadata",
		Aliases: []string{"status", "info"},
		Short:   "Request broker metadata",
		Long: `Request broker metadata.

The Kafka protocol's metadata contains information about brokers, topics, and
the cluster as a whole.

This command only runs if specific sections of metadata are requested. There
are currently three sections: the cluster, the list of brokers, and the topics.
If no section is specified, this defaults to printing all sections.

If the topic section is requested, all topics are requested by default unless
some are manually specified as arguments. Expanded per-partition information
can be printed with the -d flag, and internal topics can be printed with the -i
flag.

In the broker section, the controller node is suffixed with *.
`,
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			// We first evaluate whether any section was requested.
			// If none were, we default to all sections. Only after
			// do we evaluate whether detailed was requested, which
			// implies topics (and must come after defaulting all
			// sections).
			requestedSections := 0
			for _, v := range []*bool{&cluster, &brokers, &topics} {
				if *v {
					requestedSections++
				}
			}
			if len(args) > 0 || detailed || internal {
				topics = true
			}
			if requestedSections == 0 { // default to all sections
				cluster, brokers, topics, internal = true, true, true, true
				requestedSections = 4
			}

			// If the user requested more than one section, we
			// print a header for the section.
			header := func(name string, fn func()) {
				if requestedSections > 1 {
					fmt.Println(name)
					fmt.Println(strings.Repeat("=", len(name)))
					defer fmt.Println()
				}
				fn()
			}

			var m kadm.Metadata
			if topics || len(args) > 0 {
				m, err = adm.Metadata(context.Background(), args...)
			} else {
				m, err = adm.BrokerMetadata(context.Background())
			}
			out.MaybeDie(err, "unable to request metadata: %v", err)

			// We only print the cluster section if the response
			// has a cluster.
			if cluster && m.Cluster != "" {
				header("CLUSTER", func() {
					fmt.Printf("%s\n", m.Cluster)
				})
			}
			if brokers {
				header("BROKERS", func() {
					printBrokers(m.Controller, m.Brokers)
				})
			}
			if topics && len(m.Topics) > 0 {
				header("TOPICS", func() {
					PrintTopics(m.Topics, internal, detailed, "text")
				})
			}
		},
	}

	cmd.Flags().BoolVarP(&cluster, "print-cluster", "c", false, "Print cluster section")
	cmd.Flags().BoolVarP(&brokers, "print-brokers", "b", false, "Print brokers section")
	cmd.Flags().BoolVarP(&topics, "print-topics", "t", false, "Print topics section (implied if any topics are specified)")
	cmd.Flags().BoolVarP(&internal, "print-internal-topics", "i", false, "Print internal topics (if all topics requested, implies -t)")
	cmd.Flags().BoolVarP(&detailed, "print-detailed-topics", "d", false, "Print per-partition information for topics (implies -t)")
	return cmd
}

func printBrokers(controllerID int32, brokers kadm.BrokerDetails) {
	headers := []string{"ID", "HOST", "PORT"}
	args := func(b *kadm.BrokerDetail) []interface{} {
		ret := []interface{}{b.NodeID, b.Host, b.Port}
		if b.NodeID == controllerID {
			ret[0] = fmt.Sprintf("%d*", b.NodeID)
		}
		return ret
	}

	// Rack is included if any broker has a rack.
	for i := range brokers {
		if brokers[i].Rack != nil {
			headers = append(headers, "RACK")
			orig := args
			args = func(b *kadm.BrokerDetail) []interface{} {
				var rack string
				if b.Rack != nil {
					rack = *b.Rack
				}
				return append(orig(b), rack)
			}

			break
		}
	}

	tw := out.NewTable(headers...)
	defer tw.Flush()
	for _, broker := range brokers {
		tw.Print(args(&broker)...)
	}
}

// Used for structured output. Note: structured output is always detailed and doens't require the -d flag.
type TopicPartitionDetail struct {
	PartitionID        int32   `json:"partition_id" yaml:"partition_id"`                 // Partition is the partition number these details are for.
	LeaderID           int32   `json:"leader_id" yaml:"leader_id"`                       // Leader is the broker leader  if there is one  otherwise -1.
	LeaderEpoch        int32   `json:"leader_epoch" yaml:"leader_epoch"`                 // LeaderEpoch is the leader's current epoch.
	ReplicaIDs         []int32 `json:"replica_ids" yaml:"replica_ids"`                   // Replicas is the list of replicas.
	OfflineReplicasIDs []int32 `json:"offline_replicas_ids" yaml:"offline_replicas_ids"` // OfflineReplicas is the list of offline replicas.
	Err                error   `json:"error" yaml:"error"`                               // Err is non-nil if the partition currently has a load error.
}
type Topic struct {
	Name           string                 `json:"name" yaml:"name"`
	PartitionCount int                    `json:"partition_count" yaml:"partition_count"`
	Partitions     []TopicPartitionDetail `json:"partitions" yaml:"partitions"`
	ReplicaCount   int                    `json:"replica_count" yaml:"replica_count"`
	IsInternal     bool                   `json:"is_internal" yaml:"is_internal"`
}
type Topics struct {
	Topics []Topic `json:"topics" yaml:"topics"`
}

func (collection *Topics) sortTopics() {
	sort.Slice(collection.Topics, func(i, j int) bool {
		l, r := collection.Topics[i], collection.Topics[j]
		return l.Name < r.Name
	})
}

func (collection *Topics) AddTopic(newTopic kadm.TopicDetail) {
	partitionsDetail := []TopicPartitionDetail{}
	for _, partition := range newTopic.Partitions.Sorted() {
		// init offlineReplicasts to empty slice so json/yaml print will print [] instead of NULL
		// Other int32s here should at least have leader 0, replica 0, etc, even on a single node cluster.
		offlineReplicas := make([]int32, 0)
		if partition.OfflineReplicas != nil {
			offlineReplicas = partition.OfflineReplicas
		}
		partitionsDetail = append(partitionsDetail,
			TopicPartitionDetail{
				PartitionID:        partition.Partition,
				LeaderID:           partition.Leader,
				LeaderEpoch:        partition.LeaderEpoch,
				ReplicaIDs:         int32s(partition.Replicas).sort(),
				OfflineReplicasIDs: int32s(offlineReplicas).sort(),
				Err:                partition.Err,
			},
		)
	}
	collection.Topics = append(
		collection.Topics,
		Topic{
			Name:           newTopic.Topic,
			PartitionCount: len(newTopic.Partitions),
			Partitions:     partitionsDetail,
			ReplicaCount:   newTopic.Partitions.NumReplicas(),
			IsInternal:     newTopic.IsInternal,
		},
	)
}

func PrintTopics(topics kadm.TopicDetails, internal, detailed bool, format string) {
	topicsCollection := Topics{}
	for _, topic := range topics {
		topicsCollection.AddTopic(topic)
	}
	topicsCollection.sortTopics()

	if !detailed && format == "text" {
		tw := out.NewTable("NAME", "PARTITIONS", "REPLICAS")
		defer tw.Flush()
		for _, topic := range topicsCollection.Topics {
			if !internal && topic.IsInternal {
				continue
			}
			tw.Print(topic.Name, topic.PartitionCount, topic.ReplicaCount)
		}
		return
	}

	if format != "text" {
		out.StructredPrint[any](topicsCollection, format)
	} else {
		buf := new(bytes.Buffer)
		buf.Grow(512)
		defer func() { os.Stdout.Write(buf.Bytes()) }()

		for i, topic := range topicsCollection.Topics {
			if topic.IsInternal && !internal {
				continue
			}
			if i > 0 {
				fmt.Fprintln(buf)
			}

			// "foo, 20 partitions, 3 replicas"
			fmt.Fprintf(buf, "%s", topic.Name)
			if topic.IsInternal {
				fmt.Fprint(buf, " (internal)")
			}
			fmt.Fprintf(buf, ", %d partitions", topic.PartitionCount)
			if topic.PartitionCount > 0 {
				fmt.Fprintf(buf, ", %d replicas", topic.ReplicaCount)
			}
			buf.WriteString("\n")

			// We include certain columns if any partition has a
			// non-default value.
			var useEpoch, useOffline, useErr bool
			for _, p := range topic.Partitions {
				if p.LeaderEpoch != -1 {
					useEpoch = true
				}
				if len(p.OfflineReplicasIDs) > 0 {
					useOffline = true
				}
				if p.Err != nil {
					useErr = true
				}
			}

			// Since this is a nested table, we use one leading empty
			// header, which tabs the entire table in one. We also use an
			// empty leading column in our args below.
			headers := []string{"", "partition", "leader"}
			if useEpoch {
				headers = append(headers, "epoch")
			}
			headers = append(headers, "replicas") // TODO add isr see #1928
			if useOffline {
				headers = append(headers, "offline-replicas")
			}
			if useErr {
				headers = append(headers, "load-error")
			}

			args := func(p *TopicPartitionDetail) []interface{} {
				ret := []interface{}{"", p.PartitionID, p.LeaderID}
				if useEpoch {
					ret = append(ret, p.LeaderEpoch)
				}
				ret = append(ret, p.ReplicaIDs)
				if useOffline {
					ret = append(ret, p.OfflineReplicasIDs)
				}
				if useErr {
					if p.Err != nil {
						ret = append(ret, p.Err.Error())
					} else {
						ret = append(ret, "-")
					}
				}
				return ret
			}

			tw := out.NewTableTo(buf, headers...)
			for _, part := range topic.Partitions {
				tw.Print(args(&part)...)
			}
			tw.Flush()
		}
	}
}

type int32s []int32

func (is int32s) sort() []int32 {
	sort.Slice(is, func(i, j int) bool { return is[i] < is[j] })
	return is
}
