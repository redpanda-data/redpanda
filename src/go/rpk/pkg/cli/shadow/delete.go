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

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var noConfirm bool
	cmd := &cobra.Command{
		Use:   "delete [LINK_NAME]",
		Args:  cobra.ExactArgs(1),
		Short: "Delete a Redpanda Shadow Link",
		Long: `Delete a Redpanda Shadow Link

Delete a Shadow Link by name. You cannot delete Shadow Links that have active
Shadow topics. Run the failover command first to deactivate topics before 
deletion.

The command prompts for confirmation by default. Use the --no-confirm flag to
skip the confirmation prompt.
`,
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			config.CheckExitCloudAdmin(p) // TODO: remove, for now is there because we don't support it in cloud yet.

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			linkName := args[0]
			link, err := cl.ShadowLinkService().GetShadowLink(cmd.Context(), connect.NewRequest(&adminv2.GetShadowLinkRequest{
				Name: linkName,
			}))
			out.MaybeDie(err, "unable to get Redpanda Shadow Link information: %v", err)

			printShadowLinkInfo(link.Msg.GetShadowLink())
			if !noConfirm {
				ok, err := out.Confirm("Are you sure you want to delete this Shadow Link?")
				out.MaybeDie(err, "unable to confirm Shadow Link deletion: %v", err)
				if !ok {
					out.Exit("Shadow Link deletion cancelled")
				}
			}

			_, err = cl.ShadowLinkService().DeleteShadowLink(cmd.Context(), connect.NewRequest(&adminv2.DeleteShadowLinkRequest{
				Name: linkName,
			}))
			out.MaybeDie(err, "unable to delete Redpanda Shadow Link %q: %v", linkName, err)

			fmt.Printf("Shadow Link %q deleted successfully\n", linkName)
		},
	}
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}

func printShadowLinkInfo(link *adminv2.ShadowLink) {
	// Not possible as we get here after error checking, but adding for safety.
	if link == nil {
		fmt.Println("No Shadow Link information available")
		return
	}
	// TODO: once we support cloud, confirm if we should query the cloud API to get the client information.
	tw := out.NewTable()
	defer tw.Flush()
	tw.Print("Link Name:", link.GetName())
	tw.Print("Bootstrap Servers:")
	for _, srv := range link.GetConfigurations().GetClientOptions().GetBootstrapServers() {
		tw.Print("", fmt.Sprintf("- %s", srv))
	}
}
