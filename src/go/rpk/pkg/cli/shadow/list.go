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
	"strings"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Args:  cobra.NoArgs,
		Short: "List Redpanda Shadow Links",
		Long:  `List Redpanda Shadow Links`, // TODO: add more details.
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			config.CheckExitCloudAdmin(p) // TODO: remove, for now is there because we don't support it in cloud yet.

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			link, err := cl.ShadowLinkService().ListShadowLinks(cmd.Context(), connect.NewRequest(&adminv2.ListShadowLinksRequest{}))
			out.MaybeDie(err, "unable to list Redpanda Shadow Links: %v", err)

			tw := out.NewTable("NAME", "UID", "STATE")
			defer tw.Flush()
			for _, l := range link.Msg.GetShadowLinks() {
				tw.Print(l.GetName(), l.GetUid(), strings.TrimPrefix(l.GetStatus().GetState().String(), "SHADOW_LINK_STATE_"))
			}
		},
	}
	// TODO: support output formats other than text once we confirm the fields format reported in the state.
	p.InstallFormatFlag(cmd)
	return cmd
}
