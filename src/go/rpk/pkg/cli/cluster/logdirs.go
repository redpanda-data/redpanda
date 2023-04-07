// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
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
	"github.com/twmb/types"
)

func newLogdirsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logdirs",
		Short: "Describe log directories on Redpanda brokers",
	}
	p.InstallKafkaFlags(cmd)
	cmd.AddCommand(
		newLogdirsDescribeCommand(fs, p),
	)
	return cmd
}

func newLogdirsDescribeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		broker        int32
		sortBySize    bool
		topics        []string
		aggregateInto string
	)

	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Describe log directories on Redpanda brokers",
		Long: `Describe log directories on Redpanda brokers.

This command prints information about log directories on brokers, particularly,
the base directory that topics and partitions are located in, and the size of
data that has been written to the partitions. The size you see may not exactly
match the size on disk as reported by du: Redpanda allocates files in chunks.
The chunks will show up in du, while the actual bytes so far written to the
file will show up in this command.

The directory returned is the root directory for partitions. Within Redpanda,
the partition data lives underneath the the returned root directory in

    kafka/{topic}/{partition}_{revision}/

where revision is a Redpanda internal concept.
`,

		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			var s kadm.TopicsSet
			if len(topics) > 0 {
				listed, err := adm.ListTopics(context.Background(), topics...)
				out.MaybeDie(err, "unable to describe topics: %v", err)
				listed.EachError(func(d kadm.TopicDetail) {
					fmt.Fprintf(os.Stderr, "unable to discover the partitions on topic %q: %v", d.Topic, d.Err)
				})
				s = listed.TopicsSet()
			}

			type row struct {
				Broker    int32
				Dir       string
				Topic     string
				Partition int32
				Size      int64
				Err       string
			}
			var rows []row

			eachDir := func(d kadm.DescribedLogDir) {
				if d.Err != nil {
					rows = append(rows, row{
						Broker: d.Broker,
						Dir:    d.Dir,
						Err:    d.Err.Error(),
					})
					return
				}
				d.Topics.Each(func(p kadm.DescribedLogDirPartition) {
					rows = append(rows, row{
						Broker:    d.Broker,
						Dir:       d.Dir,
						Topic:     p.Topic,
						Partition: p.Partition,
						Size:      p.Size,
					})
				})
			}

			if broker >= 0 {
				desc, err := adm.DescribeBrokerLogDirs(context.Background(), broker, s)
				out.MaybeDie(err, "unable to describe broker log dirs: %v", err)
				desc.Each(eachDir)
			} else {
				desc, err := adm.DescribeAllLogDirs(context.Background(), s)
				out.HandleShardError("DescribeLogDirs", err)
				desc.Each(eachDir)
			}

			// First we deeply sort our rows, we will use this for
			// in-place aggregating.
			types.Sort(rows)

			// For aggregate into, we merge rows. If shouldChange
			// returns true, we know we need to move to a new row.
			collapse := func(shouldChange func(prior, current row) bool) {
				if len(rows) == 0 {
					return
				}
				prior := rows[0]
				keep := rows[:0]
				for _, current := range rows[1:] {
					if shouldChange(prior, current) {
						keep = append(keep, prior)
						prior = current
						continue
					}
					prior.Size += current.Size
				}
				rows = append(keep, prior)
			}

			var headers []string
			var rowfn func(*out.TabWriter, row)
			switch strings.ToLower(aggregateInto) {
			default:
				out.Die("unrecognized --aggregate-into %q", aggregateInto)

			case "broker":
				headers = []string{"broker", "size", "error"}
				collapse(func(prior, current row) bool { return prior.Broker != current.Broker })
				rowfn = func(tw *out.TabWriter, r row) { tw.Print(r.Broker, r.Size, r.Err) }

			case "dir":
				headers = []string{"broker", "dir", "size", "error"}
				collapse(func(prior, current row) bool { return prior.Broker != current.Broker || prior.Dir != current.Dir })
				rowfn = func(tw *out.TabWriter, r row) { tw.Print(r.Broker, r.Dir, r.Size, r.Err) }

			case "topic":
				headers = []string{"broker", "dir", "topic", "size", "error"}
				collapse(func(prior, current row) bool {
					return prior.Broker != current.Broker || prior.Dir != current.Dir || prior.Topic != current.Topic
				})
				rowfn = func(tw *out.TabWriter, r row) { tw.Print(r.Broker, r.Dir, r.Topic, r.Size, r.Err) }

			case "", "partition":
				headers = []string{"broker", "dir", "topic", "partition", "size", "error"}
				rowfn = func(tw *out.TabWriter, r row) { tw.PrintStructFields(r) }
			}

			// Finally, if we are sorting by size, we perform a
			// stable sort. We want stable to preserve ordering for
			// what we have already ordered and aggregated.
			if sortBySize {
				sort.SliceStable(rows, func(i, j int) bool {
					return rows[i].Size < rows[j].Size
				})
			}

			tw := out.NewTable(headers...)
			defer tw.Flush()
			for _, row := range rows {
				rowfn(tw, row)
			}
		},
	}

	cmd.Flags().Int32VarP(&broker, "broker", "b", -1, "If non-negative, the specific broker to describe")
	cmd.Flags().BoolVar(&sortBySize, "sort-by-size", false, "If true, sort by size")
	cmd.Flags().StringSliceVar(&topics, "topics", nil, "Specific topics to describe")
	cmd.Flags().StringVar(&aggregateInto, "aggregate-into", "", "If non-empty, what column to aggregate into starting from the partition column (broker, dir, topic)")
	return cmd
}
