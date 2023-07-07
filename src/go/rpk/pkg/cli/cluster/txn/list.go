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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List transactions and their current states",
		Long: `List transactions and their current states

This command lists all known transactions in the cluster, the producer ID for
the transactional ID, and the and the state of the transaction. For information
on what the columns in the output mean, see 'rpk cluster txn --help'.
`,

		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			listed, err := adm.ListTransactions(cmd.Context(), nil, nil)
			out.HandleShardError("ListTransactions", err)

			tw := out.NewTable("coordinator", "transactional-id", "producer-id", "state")
			defer tw.Flush()
			for _, x := range listed.Sorted() {
				tw.PrintStructFields(struct {
					Broker     int32
					TxnID      string
					ProducerID int64
					State      string
				}{x.Coordinator, x.TxnID, x.ProducerID, x.State})
			}
		},
	}
}
