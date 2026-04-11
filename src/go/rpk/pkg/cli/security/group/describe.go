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
	"sort"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type describeGroupResponse struct {
	Roles []string `json:"roles" yaml:"roles"`
}

func describeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "describe [GROUP]",
		Aliases: []string{"info"},
		Short:   "Describe the roles assigned to a group",
		Long: `Describe the roles assigned to a group.

This command shows which Redpanda roles are mapped to the specified group.`,
		Example: `
Describe Redpanda roles assigned to the "engineering" group:
  rpk security group describe engineering`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(describeGroupResponse{}); ok {
				out.Exit(h)
			}
			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			config.CheckExitCloudAdmin(prof)

			groupName := args[0]

			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			if !adminapi.HasMinimumVersion(cmd.Context(), cl, minVersion) {
				out.Die("rpk security group requires Redpanda version %s or later", minVersion.String())
			}

			res, err := cl.SecurityService().ListRoles(cmd.Context(), connect.NewRequest(&adminv2.ListRolesRequest{}))
			out.MaybeDie(err, "unable to list roles: %v", err)

			roles := rolesForGroup(res.Msg.Roles, groupName)

			resp := describeGroupResponse{Roles: roles}
			if isText, _, s, err := f.Format(resp); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			tw := out.NewTable("ROLE")
			defer tw.Flush()
			for _, r := range roles {
				tw.Print(r)
			}
		},
	}
	return cmd
}

// rolesForGroup returns sorted role names from adminv2 roles that contain the given group as a member.
func rolesForGroup(roles []*adminv2.Role, groupName string) []string {
	var result []string
	for _, role := range roles {
		for _, m := range role.GetMembers() {
			if m.GetGroup().GetName() == groupName {
				result = append(result, role.Name)
				break
			}
		}
	}
	sort.Strings(result)
	return result
}
