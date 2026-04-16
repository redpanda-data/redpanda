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
	"io"
	"os"
	"slices"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

func newMetadataCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		cluster  bool
		brokers  bool
		topics   bool
		internal bool
		detailed bool
	)
	cmd := &cobra.Command{
		Use:     "info",
		Aliases: []string{"status", "metadata"},
		Short:   "Request broker metadata",
		Long: `Request broker metadata information.

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

Using this command with --format json/yaml implies that all sections are 
included.
`,
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]metadataResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
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
			if requestedSections == 0 || !f.IsText() { // default to all sections
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

			if !f.IsText() {
				err = printRawMetadataToW(cmd.OutOrStdout(), f, m)
				out.MaybeDie(err, "unable to print metadata in %q format: %v", f.Kind, err)
				return
			}
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
					PrintTopics(m.Topics, internal, detailed)
				})
			}
		},
	}
	p.InstallKafkaFlags(cmd)
	p.InstallFormatFlag(cmd)
	cmd.Flags().BoolVarP(&cluster, "print-cluster", "c", false, "Print cluster section")
	cmd.Flags().BoolVarP(&brokers, "print-brokers", "b", false, "Print brokers section")
	cmd.Flags().BoolVarP(&topics, "print-topics", "t", false, "Print topics section (implied if any topics are specified)")
	cmd.Flags().BoolVarP(&internal, "print-internal-topics", "i", false, "Print internal topics (if all topics requested, implies -t)")
	cmd.Flags().BoolVarP(&detailed, "print-detailed-topics", "d", false, "Print per-partition information for topics (implies -t)")
	return cmd
}

func printBrokers(controllerID int32, brokers kadm.BrokerDetails) {
	headers := []string{"ID", "HOST", "PORT"}
	args := func(b *kadm.BrokerDetail) []any {
		ret := []any{b.NodeID, b.Host, b.Port}
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
			args = func(b *kadm.BrokerDetail) []any {
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

func PrintTopics(topics kadm.TopicDetails, internal, detailed bool) {
	if !detailed {
		tw := out.NewTable("NAME", "PARTITIONS", "REPLICAS")
		defer tw.Flush()

		for _, topic := range topics.Sorted() {
			if !internal && topic.IsInternal {
				continue
			}
			parts := len(topic.Partitions)
			replicas := topic.Partitions.NumReplicas()
			tw.Print(topic.Topic, parts, replicas)
		}
		return
	}

	buf := new(bytes.Buffer)
	buf.Grow(512)
	defer func() { os.Stdout.Write(buf.Bytes()) }()

	for i, topic := range topics.Sorted() {
		if topic.IsInternal && !internal {
			continue
		}
		if i > 0 {
			fmt.Fprintln(buf)
		}

		// "foo, 20 partitions, 3 replicas"
		fmt.Fprintf(buf, "%s", topic.Topic)
		if topic.IsInternal {
			fmt.Fprint(buf, " (internal)")
		}
		fmt.Fprintf(buf, ", %d partitions", len(topic.Partitions))
		if len(topic.Partitions) > 0 {
			fmt.Fprintf(buf, ", %d replicas", len(topic.Partitions[0].Replicas))
		}
		buf.WriteString("\n")

		// We include certain columns if any partition has a
		// non-default value.
		var useEpoch, useOffline, useErr bool
		for _, p := range topic.Partitions.Sorted() {
			if p.LeaderEpoch != -1 {
				useEpoch = true
			}
			if len(p.OfflineReplicas) > 0 {
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

		args := func(p *kadm.PartitionDetail) []any {
			ret := []any{"", p.Partition, p.Leader}
			if useEpoch {
				ret = append(ret, p.LeaderEpoch)
			}
			ret = append(ret, int32s(p.Replicas).sort())
			if useOffline {
				ret = append(ret, int32s(p.OfflineReplicas).sort())
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
		for _, part := range topic.Partitions.Sorted() {
			tw.Print(args(&part)...)
		}
		tw.Flush()
	}
}

type metadataResponse struct {
	ClusterName  string         `json:"cluster_name" yaml:"cluster_name"`
	ControllerID int            `json:"controller_id" yaml:"controller_id"`
	Brokers      []BrokerDetail `json:"brokers" yaml:"brokers"`
	Topics       []TopicDetail  `json:"topics" yaml:"topics"`
}

type BrokerDetail struct {
	ID   int32   `json:"id" yaml:"id"`
	Host string  `json:"host" yaml:"host"`
	Port int32   `json:"port" yaml:"port"`
	Rack *string `json:"rack,omitempty" yaml:"rack,omitempty"`
}

type TopicDetail struct {
	Name           string            `json:"name" yaml:"name"`
	IsInternal     bool              `json:"is_internal" yaml:"is_internal"`
	PartitionCount int               `json:"partition_count" yaml:"partition_count"`
	ReplicasCount  int               `json:"replicas_count" yaml:"replicas_count"`
	Partitions     []PartitionDetail `json:"partitions" yaml:"partitions"`
}

type PartitionDetail struct {
	Partition       int32   `json:"partition" yaml:"partition"`
	Leader          int32   `json:"leader" yaml:"leader"`
	LeaderEpoch     int32   `json:"leader_epoch" yaml:"leader_epoch"`
	Replicas        []int32 `json:"replicas" yaml:"replicas"`
	OfflineReplicas []int32 `json:"offline_replicas,omitempty" yaml:"offline_replicas,omitempty"`
	Error           string  `json:"error,omitempty" yaml:"error,omitempty"`
}

func printRawMetadataToW(w io.Writer, f config.OutFormatter, m kadm.Metadata) error {
	resp := metadataResponse{
		ClusterName:  m.Cluster,
		ControllerID: int(m.Controller),
	}
	var brokers []BrokerDetail
	for _, b := range m.Brokers {
		brokers = append(brokers, BrokerDetail{
			ID:   b.NodeID,
			Host: b.Host,
			Port: b.Port,
			Rack: b.Rack,
		})
	}
	resp.Brokers = brokers
	var topics []TopicDetail
	if len(m.Topics) > 0 {
		for _, topic := range m.Topics.Sorted() {
			td := TopicDetail{
				Name:           topic.Topic,
				IsInternal:     topic.IsInternal,
				PartitionCount: len(topic.Partitions),
			}
			if len(topic.Partitions) > 0 {
				td.ReplicasCount = len(topic.Partitions[0].Replicas)
			}
			var partitions []PartitionDetail
			for _, p := range topic.Partitions.Sorted() {
				pd := PartitionDetail{
					Partition:       p.Partition,
					Leader:          p.Leader,
					LeaderEpoch:     p.LeaderEpoch,
					Replicas:        p.Replicas,
					OfflineReplicas: p.OfflineReplicas,
				}
				if p.Err != nil {
					pd.Error = p.Err.Error()
				}
				partitions = append(partitions, pd)
			}
			td.Partitions = partitions
			topics = append(topics, td)
		}
	}
	resp.Topics = topics
	_, _, t, err := f.Format(&resp)
	if err != nil {
		return fmt.Errorf("unable to print in the requested format %q: %v", f.Kind, err)
	}
	_, err = fmt.Fprintln(w, t)
	if err != nil {
		return err
	}
	return nil
}

type int32s []int32

func (is int32s) sort() []int32 {
	slices.Sort(is)
	return is
}
