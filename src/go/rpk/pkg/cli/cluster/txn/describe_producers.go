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
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

func newDescribeProducersCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		topics     []string
		partitions []int32
		all        bool
	)
	cmd := &cobra.Command{
		Use:   "describe-producers",
		Short: "Describe transactional producers to partitions",
		Long: `Describe transactional producers to partitions.

This command describes partitions that active transactional producers are
producing to. For more information on the producer ID and epoch columns, see
'rpk cluster txn --help'.

The last timestamp corresponds to the timestamp of the last record that was
written by the client. The transaction start offset corresponds to the offset
that the transaction is began at. All consumers configured to read only
committed records cannot read past the transaction start offset.

The output includes a few advanced fields that can be used for sanity checking:
the last sequence is the last sequence number that the producer has written,
and the coordinator epoch is the epoch of the broker that is being written to.
The last sequence should always go up and then wrap back to 0 at MaxInt32. The
coordinator epoch should remain fixed, or rarely, increase.

You can query all topics and partitions that have active producers with --all.
To filter for specific topics, use --topics. You can additionally filter by
partitions with --partitions.
`,

		Run: func(cmd *cobra.Command, txnIDs []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			if len(topics) == 0 && len(partitions) > 0 {
				out.Die("cannot specify partitions without any topics")
			}
			if !all && len(topics) == 0 {
				out.Die("must specify at least one of --all or --topics")
			}
			// If the TopicsSet is empty, the request returns all partitions.
			var s kadm.TopicsSet
			for _, topic := range topics {
				s.Add(topic, partitions...)
			}

			described, err := adm.DescribeProducers(cmd.Context(), s)
			out.HandleShardError("DescribeProducers", err)

			tw := out.NewTable(
				"leader",
				"topic",
				"partition",
				"producer-id",
				"producer-epoch",
				"last-timestamp",
				"last-sequence",
				"coordinator-epoch",
				"txn-start-offset",
				"error",
			)
			defer tw.Flush()

			type fields struct {
				Leader           int32
				Topic            string
				Partition        int32
				ProducerID       int64
				ProducerEpoch    int16
				LastSequence     int32
				LastTimestamp    string
				CoordinatorEpoch int32
				TxnStartOffset   int64
				Err              string
			}

			for _, d := range described.SortedPartitions() {
				if d.Err != nil {
					tw.PrintStructFields(fields{
						Leader:    d.Leader,
						Topic:     d.Topic,
						Partition: d.Partition,
						Err:       d.Err.Error(),
					})
				}
				if len(d.ActiveProducers) == 0 {
					continue
				}
				for _, p := range d.ActiveProducers.Sorted() {
					tw.PrintStructFields(fields{
						Leader:           p.Leader,
						Topic:            p.Topic,
						Partition:        p.Partition,
						ProducerID:       p.ProducerID,
						ProducerEpoch:    p.ProducerEpoch,
						LastSequence:     p.LastSequence,
						LastTimestamp:    time.UnixMilli(p.LastTimestamp).Format(rfc3339Milli),
						CoordinatorEpoch: p.CoordinatorEpoch,
						TxnStartOffset:   p.CurrentTxnStartOffset,
					})
				}
			}
		},
	}

	cmd.Flags().StringSliceVarP(&topics, "topics", "t", nil, "Topic to describe producers for (repeatable)")
	cmd.Flags().Int32SliceVarP(&partitions, "partitions", "p", nil, "Partitions to describe producers for (repeatable)")
	cmd.Flags().BoolVarP(&all, "all", "a", false, "Query all producer IDs on any topic")

	cmd.MarkFlagsMutuallyExclusive("topics", "all")
	cmd.MarkFlagsMutuallyExclusive("partitions", "all")
	return cmd
}
