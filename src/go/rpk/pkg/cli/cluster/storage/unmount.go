// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package storage

import (
	"fmt"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newUnmountCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unmount [TOPIC]",
		Short: "Unmount a topic from the Redpanda cluster",
		Long: `Unmount a topic from the Redpanda cluster and secure it in Tiered
Storage.

This command performs an operation that:
1. Rejects all writes to the topic
2. Flushes data to Tiered Storage
3. Removes the topic from the cluster

Key Points:
- During unmounting, any attempted writes or reads will receive an
  UNKNOWN_TOPIC_OR_PARTITION error.
- The unmount operation works independently of other topic configurations like
  remote.delete=false.
- After unmounting, the topic can be remounted to this cluster or a different
  cluster if the log segments are moved to that cluster's Tiered Storage.
`,
		Example: `
Unmount topic 'my-topic' from the cluster in the 'my-namespace'
  rpk cluster storage unmount my-namespace/my-topic
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, topics []string) {
			pf, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(pf)
			adm, err := adminapi.NewClient(cmd.Context(), fs, pf)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			n, t := nsTopic(topics[0])
			if t == "" {
				out.Die("topic is required")
			}

			mg, err := adm.UnmountTopics(cmd.Context(), rpadmin.UnmountConfiguration{Topics: []rpadmin.NamespacedTopic{{Namespace: string2pointer(n), Topic: t}}})
			out.MaybeDie(err, "unable to unmount topic: %v", err)
			fmt.Printf(`
Topic unmounting from your Redpanda Cluster topic %v
has started with Migration ID %v
To check the status run 'rpk cluster storage status-mount %d'\n`, t, mg.ID, mg.ID)
		},
	}
	return cmd
}
