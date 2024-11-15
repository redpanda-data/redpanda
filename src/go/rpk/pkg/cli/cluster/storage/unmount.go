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
	"strings"

	dataplanev1alpha2 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1alpha2"
	"connectrpc.com/connect"
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
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)

			ns, t := nsTopic(topics[0])
			if t == "" {
				out.Die("topic is required")
			}

			var id int
			if p.FromCloud {
				if ns != "" && strings.ToLower(ns) != "kafka" {
					out.Die("Namespace %q not allowed. Only kafka topics can be unmounted in Redpanda Cloud clusters", ns)
				}
				cl, err := createDataplaneClient(p)
				out.MaybeDieErr(err)

				resp, err := cl.CloudStorage.UnmountTopics(
					cmd.Context(),
					connect.NewRequest(
						&dataplanev1alpha2.UnmountTopicsRequest{
							Topics: []string{t},
						}),
				)
				out.MaybeDie(err, "unable to unmount topic: %v", err)
				if resp != nil {
					id = int(resp.Msg.MountTaskId)
				}
			} else {
				adm, err := adminapi.NewClient(cmd.Context(), fs, p)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				mg, err := adm.UnmountTopics(cmd.Context(), rpadmin.UnmountConfiguration{Topics: []rpadmin.NamespacedTopic{{Namespace: string2pointer(ns), Topic: t}}})
				out.MaybeDie(err, "unable to unmount topic: %v", err)
				id = mg.ID
			}

			fmt.Printf(`Topic unmounting from your Redpanda Cluster topic %q has started with 
Migration ID %v.

To check the status run 'rpk cluster storage status-mount %d.
`, t, id, id)
		},
	}
	return cmd
}
