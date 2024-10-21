// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		detailed bool
		internal bool
		re       bool
	)
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List topics, optionally listing specific topics",
		Long: `List topics, optionally listing specific topics.

This command lists all topics that you have access to by default. If specifying
topics or regular expressions, this command can be used to know exactly what
topics you would delete if using the same input to the delete command.

Alternatively, you can request specific topics to list, which can be used to
check authentication errors (do you not have access to a topic you were
expecting to see?), or to list all topics that match regular expressions.

The --regex flag (-r) opts into parsing the input topics as regular expressions
and listing any non-internal topic that matches any of expressions. The input
expressions are wrapped with ^ and $ so that the expression must match the
whole topic name. Regular expressions cannot be used to match internal topics,
as such, specifying both -i and -r will exit with failure.

Lastly, --detailed flag (-d) opts in to printing extra per-partition
information.
`,
		Run: func(_ *cobra.Command, topics []string) {
			// The purpose of the regex flag really is for users to
			// know what topics they will delete when using regex.
			// We forbid deleting internal topics (redpanda
			// actually does not expose these currently), so we
			// make -r and -i incompatible.

			f := p.Formatter
			if detailed {
				if h, ok := f.Help([]detailedListTopic{}); ok {
					out.Exit(h)
				}
			} else {
				if h, ok := f.Help([]summarizedList{}); ok {
					out.Exit(h)
				}
			}
			if internal && re {
				out.Exit("cannot list with internal topics and list by regular expression")
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			if re {
				topics, err = regexTopics(adm, topics)
				out.MaybeDie(err, "unable to filter topics by regex: %v", err)
			}

			listed, err := adm.ListTopicsWithInternal(context.Background(), topics...)
			out.MaybeDie(err, "unable to request metadata: %v", err)

			if detailed {
				printDetailedListView(f, detailedListView(internal, listed), os.Stdout)
			} else {
				printSummarizedListView(f, summarizedListView(internal, listed), os.Stdout)
			}
			out.MaybeDie(err, "unable to summarize metadata: %v", err)
		},
	}

	p.InstallFormatFlag(cmd)
	cmd.Flags().BoolVarP(&detailed, "detailed", "d", false, "Print per-partition information for topics")
	cmd.Flags().BoolVarP(&internal, "internal", "i", false, "Print internal topics")
	cmd.Flags().BoolVarP(&re, "regex", "r", false, "Parse topics as regex; list any topic that matches any input topic expression")
	return cmd
}

type summarizedList struct {
	Name       string `json:"name" yaml:"name"`
	Partitions int    `json:"partitions" yaml:"partitions"`
	Replicas   int    `json:"replicas" yaml:"replicas"`
}

func summarizedListView(internal bool, topics kadm.TopicDetails) (resp []summarizedList) {
	resp = make([]summarizedList, 0, len(topics))
	for _, topic := range topics.Sorted() {
		if !internal && topic.IsInternal {
			continue
		}
		s := summarizedList{
			Name:       topic.Topic,
			Partitions: len(topic.Partitions),
			Replicas:   topic.Partitions.NumReplicas(),
		}
		resp = append(resp, s)
	}
	return
}

func printSummarizedListView(f config.OutFormatter, topics []summarizedList, w io.Writer) {
	if isText, _, t, err := f.Format(topics); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, t)
		return
	}

	tw := out.NewTableTo(w, "NAME", "PARTITIONS", "REPLICAS")
	defer tw.Flush()
	for _, topic := range topics {
		tw.Print(topic.Name, topic.Partitions, topic.Replicas)
	}
}

type detailedListTopic struct {
	Name          string                  `json:"name" yaml:"name"`
	Partitions    int                     `json:"partitions" yaml:"partitions"`
	Replicas      int                     `json:"replicas" yaml:"replicas"`
	PartitionList []detailedListPartition `json:"partition_list" yaml:"partition_list"`
	isOffline     bool
	isInternal    bool
	isEpoch       bool
	isLoadErr     bool
}

type detailedListPartition struct {
	Partition       int32   `json:"partition" yaml:"partition"`
	Leader          int32   `json:"leader" yaml:"leader"`
	Epoch           int32   `json:"epoch,omitempty" yaml:"epoch"`
	Replicas        []int32 `json:"replicas" yaml:"replicas"`
	OfflineReplicas []int32 `json:"offline_replicas" yaml:"offline_replicas"`
	LoadError       string  `json:"load_error" yaml:"load_error"`
}

func detailedListView(internal bool, topics kadm.TopicDetails) (resp []detailedListTopic) {
	resp = make([]detailedListTopic, 0, len(topics))
	for _, topic := range topics.Sorted() {
		if !internal && topic.IsInternal {
			continue
		}
		d := detailedListTopic{
			Name:       topic.Topic,
			Partitions: len(topic.Partitions),
			isInternal: topic.IsInternal,
		}
		if len(topic.Partitions) > 0 {
			d.Replicas = topic.Partitions.NumReplicas()
			d.PartitionList = make([]detailedListPartition, len(topic.Partitions))
		}

		for k, p := range topic.Partitions.Sorted() {
			d.PartitionList[k].Partition = p.Partition
			d.PartitionList[k].Replicas = int32s(p.Replicas).sort()
			d.PartitionList[k].Leader = p.Leader
			if p.LeaderEpoch != -1 {
				d.isEpoch = true
				d.PartitionList[k].Epoch = p.LeaderEpoch
			}
			if len(p.OfflineReplicas) > 0 {
				d.isOffline = true
				d.PartitionList[k].OfflineReplicas = int32s(p.OfflineReplicas).sort()
			}
			if p.Err != nil {
				d.isLoadErr = true
				d.PartitionList[k].LoadError = p.Err.Error()
			}
		}
		resp = append(resp, d)
	}
	return
}

func printDetailedListView(f config.OutFormatter, topics []detailedListTopic, w io.Writer) {
	if isText, _, t, err := f.Format(topics); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, t)
		return
	}

	for i, topic := range topics {
		var topicName string
		if topic.isInternal {
			topicName = fmt.Sprintf("%s (internal)", topic.Name)
		} else {
			topicName = topic.Name
		}
		fmt.Fprintf(w, "%s, %d partitions, %d replicas\n", topicName, topic.Partitions, topic.Replicas)
		headers := []string{"", "partition", "leader"}
		if topic.isEpoch {
			headers = append(headers, "epoch")
		}
		headers = append(headers, "replicas")
		if topic.isOffline {
			headers = append(headers, "offline_replicas")
		}
		if topic.isLoadErr {
			headers = append(headers, "load_error")
		}
		printPartitionTable(w, headers, topic, topic.PartitionList)
		if i != len(topics)-1 {
			fmt.Fprintf(w, "\n")
		}
	}
}

func printPartitionTable(w io.Writer, headers []string, topic detailedListTopic, partitions []detailedListPartition) {
	tw := out.NewTableTo(w, headers...)
	defer tw.Flush()
	for _, p := range partitions {
		part := []any{"", p.Partition, p.Leader}
		if topic.isEpoch {
			part = append(part, p.Epoch)
		}
		part = append(part, p.Replicas)
		if topic.isOffline {
			part = append(part, p.OfflineReplicas)
		}
		if topic.isLoadErr {
			part = append(part, p.LoadError)
		}
		tw.Print(part...)
	}
}
