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

func assignCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var roles []string
	cmd := &cobra.Command{
		Use:     "assign [GROUP] --role [ROLES...]",
		Aliases: []string{"add"},
		Short:   "Assign a group to a Redpanda role",
		Long: `Assign an IdP group to a Redpanda role.

Group assignments are only supported on local (non-cloud) clusters running
Redpanda ` + minVersion.String() + ` or later.
`,
		Example: `
Assign group "engineering" to role "data-reader"
  rpk security group assign engineering --role data-reader

Assign group "engineering" to multiple roles
  rpk security group assign engineering --role data-reader,data-writer
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(prof)

			if prof.CheckFromCloud() {
				out.Die("group assign is not supported for cloud clusters")
			}

			cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			if !adminapi.HasMinimumVersion(cmd.Context(), cl, minVersion) {
				out.Die("group assign requires Redpanda version %s or later", minVersion.String())
			}

			groupName := args[0]
			for _, r := range roles {
				_, err = cl.SecurityService().AddRoleMembers(cmd.Context(), connect.NewRequest(&adminv2.AddRoleMembersRequest{
					RoleName: r,
					Members:  []*adminv2.RoleMember{{Member: &adminv2.RoleMember_Group{Group: &adminv2.RoleGroup{Name: groupName}}}},
				}))
				out.MaybeDie(err, "unable to assign group %q to role %q: %v", groupName, r, err)
			}

			fmt.Printf("Successfully assigned group %q to role(s) %v\n", groupName, roles)
		},
	}

	cmd.Flags().StringSliceVar(&roles, "role", nil, "Role to assign the group to (repeatable)")
	cmd.MarkFlagRequired("role")
	return cmd
}
