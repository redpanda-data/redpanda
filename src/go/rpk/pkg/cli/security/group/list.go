// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package group

import (
	"maps"
	"slices"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type listGroupsResponse struct {
	Groups []string `json:"groups" yaml:"groups"`
}

func listCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List groups that have role mappings",
		Long: `List groups that have any role mapping.

This command shows all groups that are mapped to at
least one Redpanda role.`,
		Example: `
List all groups with role mappings:
  rpk security group list`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help(listGroupsResponse{}); ok {
				out.Exit(h)
			}
			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(prof)

			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			if !adminapi.HasMinimumVersion(cmd.Context(), cl, minVersion) {
				out.Die("rpk security group requires Redpanda version %s or later", minVersion.String())
			}

			res, err := cl.SecurityService().ListRoles(cmd.Context(), connect.NewRequest(&adminv2.ListRolesRequest{}))
			out.MaybeDie(err, "unable to list roles: %v", err)

			groups := collectGroupsFromAdminRoles(res.Msg.Roles)

			resp := listGroupsResponse{Groups: groups}
			if isText, _, s, err := f.Format(resp); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			tw := out.NewTable("NAME")
			defer tw.Flush()
			for _, g := range groups {
				tw.Print(g)
			}
		},
	}
	return cmd
}

// collectGroupsFromAdminRoles extracts unique sorted group names from adminv2 roles.
func collectGroupsFromAdminRoles(roles []*adminv2.Role) []string {
	groupSet := map[string]struct{}{}
	for _, role := range roles {
		for _, m := range role.GetMembers() {
			if g := m.GetGroup(); g != nil {
				groupSet[g.Name] = struct{}{}
			}
		}
	}
	return slices.Sorted(maps.Keys(groupSet))
}
