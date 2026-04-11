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
	"fmt"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func unassignCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var roles []string
	cmd := &cobra.Command{
		Use:     "unassign [GROUP] --role [ROLES...]",
		Aliases: []string{"remove"},
		Short:   "Unassign a group from a Redpanda role",
		Long: `Unassign an IdP group from a Redpanda role.

Group assignments are only supported on local (non-cloud) clusters running
Redpanda ` + minVersion.String() + ` or later.
`,
		Example: `
Unassign group "engineering" from role "data-reader"
  rpk security group unassign engineering --role data-reader

Unassign group "engineering" from multiple roles
  rpk security group unassign engineering --role data-reader,data-writer
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(prof)

			if prof.CheckFromCloud() {
				out.Die("group unassign is not supported for cloud clusters")
			}

			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			if !adminapi.HasMinimumVersion(cmd.Context(), cl, minVersion) {
				out.Die("group unassign requires Redpanda version %s or later", minVersion.String())
			}

			groupName := args[0]
			for _, r := range roles {
				_, err = cl.SecurityService().RemoveRoleMembers(cmd.Context(), connect.NewRequest(&adminv2.RemoveRoleMembersRequest{
					RoleName: r,
					Members:  []*adminv2.RoleMember{{Member: &adminv2.RoleMember_Group{Group: &adminv2.RoleGroup{Name: groupName}}}},
				}))
				out.MaybeDie(err, "unable to unassign group %q from role %q: %v", groupName, r, err)
			}

			fmt.Printf("Successfully unassigned group %q from role(s) %v\n", groupName, roles)
		},
	}

	cmd.Flags().StringSliceVar(&roles, "role", nil, "Role to unassign the group from (repeatable)")
	cmd.MarkFlagRequired("role")
	return cmd
}
