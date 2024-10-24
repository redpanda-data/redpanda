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

type listResponse struct {
	Broker     int32  `json:"broker" yaml:"broker"`
	TxnID      string `json:"transaction_id" yaml:"transaction_id"`
	ProducerID int64  `json:"producer_id" yaml:"producer_id"`
	State      string `json:"state" yaml:"state"`
}

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
			f := p.Formatter
			if h, ok := f.Help([]listResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			listed, err := adm.ListTransactions(cmd.Context(), nil, nil)
			out.HandleShardError("ListTransactions", err)

			response := []listResponse{}
			for _, x := range listed.Sorted() {
				response = append(response, listResponse{
					Broker:     x.Coordinator,
					TxnID:      x.TxnID,
					ProducerID: x.ProducerID,
					State:      x.State,
				})
			}
			if isText, _, s, err := f.Format(response); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			tw := out.NewTable("coordinator", "transactional-id", "producer-id", "state")
			defer tw.Flush()
			for _, r := range response {
				tw.PrintStructFields(r)
			}
		},
	}
}
