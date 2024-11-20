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
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)

			ns, t := nsTopic(from[0])
			if t == "" {
				out.Die("topic is required")
			}
			an, at := nsTopic(to)
			alias := t
			var id int
			if p.FromCloud {
				if ns != "" && strings.ToLower(ns) != "kafka" {
					out.Die("Namespace %q not allowed. Only kafka topics can be mounted in Redpanda Cloud clusters", ns)
				}
				if an != "" && strings.ToLower(an) != "kafka" {
					out.Die("Failed to parse '--to' flag: namespace %q not allowed. Only kafka topics can be mounted in Redpanda Cloud clusters", an)
				}
				cl, err := createDataplaneClient(p)
				out.MaybeDieErr(err)

				topicMount := &dataplanev1alpha2.MountTopicsRequest_TopicMount{
					SourceTopicReference: t,
				}
				if at != "" {
					alias = at
					topicMount.Alias = alias
				}
				resp, err := cl.CloudStorage.MountTopics(
					cmd.Context(),
					connect.NewRequest(
						&dataplanev1alpha2.MountTopicsRequest{
							Topics: []*dataplanev1alpha2.MountTopicsRequest_TopicMount{topicMount},
						}),
				)
				out.MaybeDie(err, "unable to mount topic: %v", err)
				if resp.Msg != nil {
					id = int(resp.Msg.MountTaskId)
				}
			} else {
				adm, err := adminapi.NewClient(cmd.Context(), fs, p)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)
				topic := rpadmin.InboundTopic{
					SourceTopicReference: rpadmin.NamespacedTopic{
						Namespace: string2pointer(ns),
						Topic:     t,
					},
				}
				if at != "" {
					alias = at
					topic.Alias = &rpadmin.NamespacedTopic{
						Namespace: string2pointer(an),
						Topic:     alias,
					}
				}
				mg, err := adm.MountTopics(cmd.Context(), rpadmin.MountConfiguration{Topics: []rpadmin.InboundTopic{topic}})
				out.MaybeDie(err, "unable to mount topic: %v", err)
				id = mg.ID
			}

			fmt.Printf(`Topic mount from Tiered Storage topic %q to your Redpanda Cluster topic %q
has started with Migration ID %v.

To check the status run 'rpk cluster storage status-mount %d.
`, t, alias, id, id)
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
