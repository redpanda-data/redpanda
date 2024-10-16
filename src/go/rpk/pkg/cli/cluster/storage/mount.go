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

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newMountCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var to string

	cmd := &cobra.Command{
		Use:   "mount [TOPIC]",
		Short: "Mount a topic",
		Long: `Mount a topic to the Redpanda cluster from Tiered Storage.

This command mounts a topic in the Redpanda cluster using log segments stored
in Tiered Storage. The topic may be optionally renamed with the --to flag.

Requirements:
- Tiered Storage must be enabled.
- Log segments for the topic must be available in Tiered Storage.
- A topic with the same name must not already exist in the cluster.`,
		Example: `
Mounts topic my-typic from Tiered Storage to the cluster in the my-namespace
	rpk cluster storage mount my-topic

Mount topic my-topic from Tiered Storage to the cluster in the my-namespace 
with my-new-topic as the new topic name
	rpk cluster storage mount my-namespace/my-topic --to my-namespace/my-new-topic
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, from []string) {
			pf, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(pf)
			adm, err := adminapi.NewClient(cmd.Context(), fs, pf)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			n, t := nsTopic(from[0])
			if t == "" {
				out.Die("topic is required")
			}
			topic := rpadmin.InboundTopic{
				SourceTopic: rpadmin.NamespacedTopic{
					Namespace: string2pointer(n),
					Topic:     t,
				},
			}
			an, at := nsTopic(to)
			alias := t
			if at != "" {
				alias = at
				topic.Alias = &rpadmin.NamespacedTopic{
					Namespace: string2pointer(an),
					Topic:     alias,
				}
			}

			mg, err := adm.MountTopics(cmd.Context(), rpadmin.MountConfiguration{Topics: []rpadmin.InboundTopic{topic}})
			out.MaybeDie(err, "unable to mount topic: %v", err)

			fmt.Printf(`
Topic mount from Tiered Storage topic %v to your Redpanda Cluster topic %v
has started with Migration ID %v
To check the status run 'rpk cluster storage status-mount %d'\n`, t, alias, mg.ID, mg.ID)
		},
	}
	cmd.Flags().StringVar(&to, "to", "", "New namespace/topic name for the mounted topic (optional)")
	return cmd
}

// nsTopic splits a topic string consisting of <namespace>/<topicName> and
// returns each component, if the namespace is not specified, returns 'kafka'.
func nsTopic(nst string) (namespace string, topic string) {
	nsTopic := strings.SplitN(nst, "/", 2)
	if len(nsTopic) == 1 {
		namespace = "kafka"
		topic = nsTopic[0]
	} else {
		namespace = nsTopic[0]
		topic = nsTopic[1]
	}
	return namespace, topic
}

func string2pointer(s string) *string {
	return &s
}
