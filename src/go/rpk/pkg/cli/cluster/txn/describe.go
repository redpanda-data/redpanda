// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package txn

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

type describeResponse struct {
	Coordinator    int32         `json:"coordinator_id" yaml:"coordinator_id"`
	TxnID          string        `json:"transaction_id" yaml:"transaction_id"`
	ProducerID     int64         `json:"producer_id" yaml:"producer_id"`
	ProducerEpoch  int16         `json:"producer_epoch" yaml:"producer_epoch"`
	State          string        `json:"state" yaml:"state"`
	StartTimestamp string        `json:"start_timestamp" yaml:"start_timestamp"`
	Timeout        time.Duration `json:"timeout" yaml:"timeout"`
	Topic          string        `json:"topic" yaml:"topic"`
	Partition      int32         `json:"partition" yaml:"partition"`
}

func newDescribeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var printPartitions bool
	cmd := &cobra.Command{
		Use:   "describe [TXN-IDS...]",
		Short: "Describe transactional IDs",
		Long: `Describe transactional IDs.

This command, in comparison to 'list', is a more detailed per-transaction view
of transactional IDs. In addition to the state and producer ID, this command
also outputs when a transaction started, the epoch of the producer ID, how long
until the transaction times out, and the partitions currently a part of the
transaction. For information on what the columns in the output mean, see
'rpk cluster txn --help'.

By default, all topics in a transaction are merged into one line. To print a
row per topic, use --format=long. To include partitions with topics, use
--print-partitions; --format=json/yaml will return the equivalent of the long
format with print partitions included.

If no transactional IDs are requested, all transactional IDs are printed.
`,
		Run: func(cmd *cobra.Command, txnIDs []string) {
			f := p.Formatter
			if h, ok := f.Help([]describeResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			described, err := adm.DescribeTransactions(cmd.Context(), txnIDs...)
			out.HandleShardError("DescribeTransactions", err)

			headers := []string{
				"coordinator",
				"transactional-id",
				"producer-id",
				"producer-epoch",
				"state",
				"start-timestamp",
				"timeout",
			}
			common := func(x kadm.DescribedTransaction) []interface{} {
				return out.StructFields(struct {
					Coordinator    int32
					TxnID          string
					ProducerID     int64
					ProducerEpoch  int16
					State          string
					StartTimestamp string
					Timeout        time.Duration
				}{
					x.Coordinator,
					x.TxnID,
					x.ProducerID,
					x.ProducerEpoch,
					x.State,
					time.UnixMilli(x.StartTimestamp).Format(rfc3339Milli),
					time.Duration(x.TimeoutMillis) * time.Millisecond,
				})
			}

			switch f.Kind {
			case "json", "yaml":
				result := []describeResponse{}
				for _, x := range described.Sorted() {
					for _, t := range x.Topics.Sorted() {
						for _, p := range t.Partitions {
							result = append(result, describeResponse{
								Coordinator:    x.Coordinator,
								TxnID:          x.TxnID,
								ProducerID:     x.ProducerID,
								ProducerEpoch:  x.ProducerEpoch,
								State:          x.State,
								StartTimestamp: strconv.Itoa(int(x.StartTimestamp)),
								Timeout:        time.Duration(x.TimeoutMillis),
								Topic:          t.Topic,
								Partition:      p,
							})
						}
					}
				}
				_, _, s, err := f.Format(result)
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			case "short", "text":
				tw := out.NewTable(append(headers, "topics")...)
				defer tw.Flush()
				for _, x := range described.Sorted() {
					// Without partitions, we format like "foo,bar,baz".
					// With partitions, we format "foo[0;1;2],bar[1;2;3]
					var topics string
					if printPartitions {
						var ts []string
						for _, t := range x.Topics.Sorted() {
							var ps []string
							for _, p := range t.Partitions {
								ps = append(ps, strconv.Itoa(int(p)))
							}
							ts = append(ts, fmt.Sprintf("%s[%s]", t.Topic, strings.Join(ps, ";")))
						}
						topics = strings.Join(ts, ",")
					} else {
						ts := x.Topics.Topics()
						sort.Strings(ts)
						topics = strings.Join(ts, ",")
					}
					tw.Print(append(common(x), topics)...)
				}

			case "long", "wide":
				if printPartitions {
					tw := out.NewTable(append(headers, "topic", "partition")...)
					defer tw.Flush()
					for _, x := range described.Sorted() {
						for _, t := range x.Topics.Sorted() {
							for _, p := range t.Partitions {
								tw.Print(append(common(x), t.Topic, p)...)
							}
						}
					}
				} else {
					tw := out.NewTable(append(headers, "topic")...)
					defer tw.Flush()
					for _, x := range described.Sorted() {
						for _, t := range x.Topics.Sorted() {
							tw.Print(append(common(x), t.Topic)...)
						}
					}
				}
			default:
				out.Die("unrecognized format %q", f.Kind)
			}
		},
	}

	cmd.Flags().BoolVarP(&printPartitions, "print-partitions", "p", false, "Include per-topic partitions that are in the transaction")
	return cmd
}
