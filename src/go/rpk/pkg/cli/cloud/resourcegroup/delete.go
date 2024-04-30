// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resourcegroup

import (
	"fmt"

	controlplanev1beta2 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1beta2"

	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type deleteResponse struct {
	Name string `json:"name" yaml:"name"`
	ID   string `json:"id" yaml:"id"`
}

func deleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var noConfirm bool
	cmd := &cobra.Command{
		Use:   "delete [NAME]",
		Args:  cobra.ExactArgs(1),
		Short: "Delete resource groups in Redpanda Cloud",
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(deleteResponse{}); ok {
				out.Exit(h)
			}
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			priorProfile := cfg.ActualProfile()
			_, authVir, clearedProfile, _, err := oauth.LoadFlow(cmd.Context(), fs, cfg, auth0.NewClient(cfg.DevOverrides()), false, false, cfg.DevOverrides().CloudAPIURL)
			out.MaybeDie(err, "unable to authenticate with Redpanda Cloud: %v", err)

			oauth.MaybePrintSwapMessage(clearedProfile, priorProfile, authVir)
			authToken := authVir.AuthToken

			cl, err := publicapi.NewControlPlaneClientSet(cfg.DevOverrides().PublicAPIURL, authToken)
			out.MaybeDie(err, "unable to create the public api client: %v", err)

			name := args[0]
			listed, err := cl.ResourceGroup.ListResourceGroups(cmd.Context(), connect.NewRequest(&controlplanev1beta2.ListResourceGroupsRequest{
				Filter: &controlplanev1beta2.ListResourceGroupsRequest_Filter{Name: name},
			}))
			out.MaybeDie(err, "unable to find resource group %q: %v", name, err)
			if len(listed.Msg.ResourceGroups) == 0 {
				out.Die("unable to find resource group %q", name)
			}
			if len(listed.Msg.ResourceGroups) > 1 {
				// This is currently not possible, the filter is an exact
				// filter. This is just being cautious.
				out.Die("multiple resources group were found for %q, please provide an exact match", name)
			}
			resourceGroup := listed.Msg.ResourceGroups[0]
			if !noConfirm {
				confirmed, err := out.Confirm("Confirm deletion of resource group %q with ID %q?", resourceGroup.Name, resourceGroup.Id)
				out.MaybeDie(err, "unable to confirm deletion: %v", err)
				if !confirmed {
					out.Exit("Deletion canceled.")
				}
			}

			_, err = cl.ResourceGroup.DeleteResourceGroup(cmd.Context(), connect.NewRequest(&controlplanev1beta2.DeleteResourceGroupRequest{Id: resourceGroup.Id}))
			out.MaybeDie(err, "unable to delete resource group %q: %v", name, err)
			res := deleteResponse{resourceGroup.Name, resourceGroup.Id}
			if isText, _, s, err := f.Format(res); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				fmt.Println(s)
				return
			}
			tw := out.NewTable("name", "id")
			defer tw.Flush()
			tw.PrintStructFields(res)
		},
	}
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}
