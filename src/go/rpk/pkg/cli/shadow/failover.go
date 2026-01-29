// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"fmt"
	"strings"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newFailoverCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		noConfirm bool
		all       bool
		topic     string
	)
	cmd := &cobra.Command{
		Use:   "failover [LINK_NAME]",
		Args:  cobra.ExactArgs(1),
		Short: "Failover a Redpanda Shadow Link",
		Long: `Failover a Redpanda Shadow Link.

This command performs a failover operation for a Shadow Link. Failover converts
shadow topics into regular topics on the shadow cluster, allowing producers
and consumers to interact with them directly. After failover, the Shadow Link
stops replicating data from the source cluster.

Use the --all flag to failover all shadow topics associated with the Shadow
Link, or use the --topic flag to failover a specific topic. You must specify
either --all or --topic.

The command prompts you to confirm the failover operation. Use the --no-confirm
flag to skip the confirmation prompt.

WARNING: Failover is a critical operation. After failover, shadow topics become
regular topics and replication stops. Ensure your applications are ready to
connect to the shadow cluster before performing a failover.
`,
		Example: `
Failover all topics for a Shadow Link:
  rpk shadow failover my-shadow-link --all

Failover a specific topic:
  rpk shadow failover my-shadow-link --topic my-topic

Failover without confirmation:
  rpk shadow failover my-shadow-link --all --no-confirm
`,
		Run: func(cmd *cobra.Command, args []string) {
			if !all && topic == "" {
				out.Die("either --all or --topic must be provided")
			}
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			prof := cfg.VirtualProfile()
			config.CheckExitServerlessAdmin(prof)

			linkName := args[0]

			if prof.CheckFromCloud() {
				dpClient, err := publicapi.DataplaneClientFromRpkProfile(prof)
				out.MaybeDie(err, "unable to initialize dataplane client: %v", err)

				if !noConfirm {
					sl, err := dpClient.ShadowLink.GetShadowLink(cmd.Context(), connect.NewRequest(&dataplanev1.GetShadowLinkRequest{
						Name: linkName,
					}))
					out.MaybeDie(err, "unable to get Shadow Link %q: %v", linkName, handleConnectError(err, "get", linkName))

					printDataplaneCloudOverview(sl.Msg.GetShadowLink())

					var confirmed bool
					if all {
						confirmed, err = out.Confirm("Are you sure you want to failover all topics for Shadow Link %q?", linkName)
					} else {
						confirmed, err = out.Confirm("Are you sure you want to failover the topic %q for Shadow Link %q?", topic, linkName)
					}
					out.MaybeDie(err, "unable to confirm Shadow Link failover: %v", err)
					if !confirmed {
						out.Exit("Command execution canceled.")
					}
				}

				_, err = dpClient.ShadowLink.FailOver(cmd.Context(), connect.NewRequest(&adminv2.FailOverRequest{
					Name:            linkName,
					ShadowTopicName: topic,
				}))
				out.MaybeDie(err, "unable to failover Shadow Link: %v", handleConnectError(err, "failover", linkName))

				fmt.Printf(`Successfully initiated the Fail Over for Shadow Link %q. To check the status, run:
  rpk shadow status %[1]s
`, linkName)
				return
			}

			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			if !noConfirm {
				sl, err := cl.ShadowLinkService().GetShadowLink(cmd.Context(), connect.NewRequest(&adminv2.GetShadowLinkRequest{
					Name: linkName,
				}))
				out.MaybeDie(err, "unable to get Redpanda Shadow Link %q: %v", linkName, handleConnectError(err, "get", linkName))
				printOverview(sl.Msg.GetShadowLink())
				var confirmed bool
				if all {
					confirmed, err = out.Confirm("Are you sure you want to failover all topics for Shadow Link %q?", linkName)
				} else {
					confirmed, err = out.Confirm("Are you sure you want to failover the topic %q for Shadow Link %q?", topic, linkName)
				}
				out.MaybeDie(err, "unable to confirm Shadow Link failover: %v", err)
				if !confirmed {
					out.Exit("Command execution canceled.")
				}
			}
			_, err = cl.ShadowLinkService().FailOver(cmd.Context(), connect.NewRequest(&adminv2.FailOverRequest{
				Name:            linkName,
				ShadowTopicName: topic,
			}))
			out.MaybeDie(err, "unable to failover Shadow Link: %v", handleConnectError(err, "failover", linkName))

			fmt.Printf(`Successfully initiated the Fail Over for Shadow Link %q. To check the status, run:
  rpk shadow status %[1]s
`, linkName)
		},
	}

	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	cmd.Flags().BoolVar(&all, "all", false, "Failover all shadow topics associated with the Shadow Link")
	cmd.Flags().StringVar(&topic, "topic", "", "Specific topic to failover. If --all is not set, at least a topic must be provided")

	cmd.MarkFlagsMutuallyExclusive("all", "topic")
	return cmd
}

func printDataplaneCloudOverview(link *dataplanev1.ShadowLink) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	tw.Print("NAME", link.GetName())
	tw.Print("UID", link.GetUid())
	tw.Print("STATE", strings.TrimPrefix(link.GetState().String(), "SHADOW_LINK_STATE_"))
}
