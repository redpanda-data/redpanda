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
	"sort"
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

type listShadowRowResponse struct {
	Name  string `json:"name" yaml:"name"`
	UID   string `json:"UID" yaml:"UID"`
	State string `json:"state" yaml:"state"`
}

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Args:  cobra.NoArgs,
		Short: "List Redpanda Shadow Links",
		Long: `List Redpanda Shadow Links.

This command lists all Shadow Links on the shadow cluster, showing their
names, unique identifiers, and current states. Use this command to get an
overview of all configured Shadow Links and their operational status.
`,
		Example: `
List all Shadow Links:
  rpk shadow list
`,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]listShadowRowResponse{}); ok {
				out.Exit(h)
			}
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			prof := cfg.VirtualProfile()
			config.CheckExitServerlessAdmin(prof)

			var resp []listShadowRowResponse
			if prof.CheckFromCloud() {
				cloudClient, err := publicapi.NewValidatedCloudClientSet(
					cfg.DevOverrides().PublicAPIURL,
					prof.CurrentAuth().AuthToken,
					auth0.NewClient(cfg.DevOverrides()).Audience(),
					[]string{prof.CurrentAuth().ClientID},
				)
				out.MaybeDieErr(err)

				link, err := cloudClient.ShadowLinkListItems(cmd.Context(), &controlplanev1.ListShadowLinksRequest_Filter{
					ShadowRedpandaId: prof.CloudCluster.ClusterID,
				})
				out.MaybeDie(err, "unable to list Shadow Links for cluster with ID %q: %v", prof.CloudCluster.ClusterID, err)

				for _, l := range link {
					resp = append(resp, listShadowRowResponse{
						Name:  l.GetName(),
						UID:   l.GetId(),
						State: strings.TrimPrefix(l.GetState().String(), "STATE_"),
					})
				}
			} else {
				cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				link, err := cl.ShadowLinkService().ListShadowLinks(cmd.Context(), connect.NewRequest(&adminv2.ListShadowLinksRequest{}))
				out.MaybeDie(err, "unable to list Redpanda Shadow Links: %v", handleConnectError(err, "list", ""))
				for _, l := range link.Msg.GetShadowLinks() {
					resp = append(resp, listShadowRowResponse{
						Name:  l.GetName(),
						UID:   l.GetUid(),
						State: strings.TrimPrefix(l.GetStatus().GetState().String(), "SHADOW_LINK_STATE_"),
					})
				}
			}

			// Sort by name, then by state.
			sort.Slice(resp, func(i, j int) bool {
				if resp[i].Name == resp[j].Name {
					return resp[i].State < resp[j].State
				}
				return resp[i].Name < resp[j].Name
			})

			if isText, _, s, err := f.Format(resp); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				fmt.Println(s)
				return
			}

			tw := out.NewTable("NAME", "UID", "STATE")
			defer tw.Flush()
			for _, r := range resp {
				tw.PrintStructFields(r)
			}
		},
	}

	p.InstallFormatFlag(cmd)
	return cmd
}
