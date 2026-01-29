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
	"os"
	"strings"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		noConfirm   bool
		forceDelete bool
	)
	cmd := &cobra.Command{
		Use:   "delete [LINK_NAME]",
		Args:  cobra.ExactArgs(1),
		Short: "Delete a Redpanda Shadow Link",
		Long: `Delete a Redpanda Shadow Link.

This command deletes a Shadow Link by name. By default, you cannot delete a
Shadow Link that has active shadow topics. Use 'rpk shadow failover' first to
deactivate topics before deletion, or use the --force flag to delete the Shadow
Link and failover all its active shadow topics.

The command prompts you to confirm the deletion. Use the --no-confirm flag to
skip the confirmation prompt. The --force flag automatically disables the
confirmation prompt.

WARNING: Deleting a Shadow Link with --force permanently removes all shadow
topics and stops replication. This operation cannot be undone.
`,
		Example: `
Delete a Shadow Link:
  rpk shadow delete my-shadow-link

Delete a Shadow Link without confirmation:
  rpk shadow delete my-shadow-link --no-confirm

Force delete a Shadow Link with active shadow topics:
  rpk shadow delete my-shadow-link --force
`,
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			prof := cfg.VirtualProfile()
			config.CheckExitServerlessAdmin(prof)

			linkName := args[0]

			promptConfirm := func(isCloud bool) {
				if !noConfirm && !forceDelete {
					msg := "Are you sure you want to delete this Shadow Link?"
					if isCloud {
						msg = "Are you sure you want to delete this Shadow Link? This action is not recoverable and will fail over all the synced shadow topics."
					}
					ok, err := out.Confirm("%s", msg)
					out.MaybeDie(err, "unable to confirm Shadow Link deletion: %v", err)
					if !ok {
						out.Exit("Shadow Link deletion cancelled")
					}
				}
			}
			if prof.CheckFromCloud() {
				cloudClient, err := publicapi.NewValidatedCloudClientSet(
					cfg.DevOverrides().PublicAPIURL,
					prof.CurrentAuth().AuthToken,
					auth0.NewClient(cfg.DevOverrides()).Audience(),
					[]string{prof.CurrentAuth().ClientID},
				)
				out.MaybeDieErr(err)

				link, err := cloudClient.ShadowLinkByNameAndRPID(cmd.Context(), linkName, prof.CloudCluster.ClusterID)
				out.MaybeDie(err, "unable to find Shadow Link %q", linkName)

				printCloudShadowLinkInfo(link)
				promptConfirm(true)

				op, err := cloudClient.ShadowLink.DeleteShadowLink(cmd.Context(), connect.NewRequest(&controlplanev1.DeleteShadowLinkRequest{
					Id: link.GetId(),
				}))
				out.MaybeDie(err, "unable to delete Shadow Link: %v", err)

				spinner := out.NewSpinner(cmd.Context(), "Deleting Shadow Link...", out.WithElapsedTime())
				isComplete, err := waitForOperation(cmd.Context(), cloudClient, op.Msg.GetOperation().GetId())
				if err != nil {
					spinner.Fail(fmt.Sprintf("unable to confirm Shadow Link deletion: %v", err))
					os.Exit(1)
				}
				if !isComplete {
					spinner.Stop()
					out.Exit("Shadow link deletion is taking longer than expected. Please check the status of the shadow link using 'rpk shadow status %q'", linkName)
				}
				spinner.Success(fmt.Sprintf("Shadow Link %q deleted successfully", linkName))
				os.Exit(0)
			}
			// Self-hosted path
			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			link, err := cl.ShadowLinkService().GetShadowLink(cmd.Context(), connect.NewRequest(&adminv2.GetShadowLinkRequest{
				Name: linkName,
			}))
			out.MaybeDie(err, "unable to get Redpanda Shadow Link information: %v", handleConnectError(err, "get", linkName))
			printShadowLinkInfo(link.Msg.GetShadowLink())
			promptConfirm(false)

			_, err = cl.ShadowLinkService().DeleteShadowLink(cmd.Context(), connect.NewRequest(&adminv2.DeleteShadowLinkRequest{
				Name:  linkName,
				Force: forceDelete,
			}))
			out.MaybeDie(err, "unable to delete Redpanda Shadow Link %q: %v", linkName, handleConnectError(err, "delete", linkName))

			fmt.Printf("Shadow Link %q deleted successfully\n", linkName)
		},
	}
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	cmd.Flags().BoolVarP(&forceDelete, "force", "f", false, "If set, forces a delete while there are active shadow topics; disables confirmation prompt as well")
	return cmd
}

func printShadowLinkInfo(link *adminv2.ShadowLink) {
	// Not possible as we get here after error checking, but adding for safety.
	if link == nil {
		fmt.Println("No Shadow Link information available")
		return
	}
	tw := out.NewTable()
	defer tw.Flush()
	tw.Print("Link Name:", link.GetName())
	tw.Print("Bootstrap Servers:")
	for _, srv := range link.GetConfigurations().GetClientOptions().GetBootstrapServers() {
		tw.Print("", fmt.Sprintf("- %s", srv))
	}
}

func printCloudShadowLinkInfo(link *controlplanev1.ShadowLink) {
	if link == nil {
		fmt.Println("No Shadow Link information available")
		return
	}
	tw := out.NewTabWriter()
	defer tw.Flush()
	tw.Print("NAME", link.GetName())
	tw.Print("ID", link.GetId())
	tw.Print("STATE", strings.TrimPrefix(link.GetState().String(), "STATE_"))
	tw.Print("SHADOW REDPANDA ID", link.GetShadowRedpandaId())
	if bss := link.GetClientOptions().GetBootstrapServers(); len(bss) > 0 {
		tw.Print("BOOTSTRAP SERVERS", "")
		for _, bs := range bss {
			tw.Print("", fmt.Sprintf("- %s", bs))
		}
	}
}
