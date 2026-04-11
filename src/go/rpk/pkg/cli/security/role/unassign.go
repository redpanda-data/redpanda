// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package role

import (
	"fmt"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func unassignCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var principals []string
	var groups []string
	cmd := &cobra.Command{
		Use:     "unassign [ROLE] --principal [PRINCIPALS...] --group [GROUPS...]",
		Aliases: []string{"remove"},
		Short:   "Unassign a Redpanda role from a principal or group",
		Long: `Unassign a Redpanda role from a principal or group.

The '--principal' flag accepts principals with the format
'<PrincipalPrefix>:<Principal>'. If 'PrincipalPrefix' is not provided, then
defaults to 'User:'.

The '--group' flag removes the role from an identity provider group.
Group assignments are only supported on local (non-cloud) clusters running
Redpanda ` + minGroupVersion.String() + ` or later.
`,
		Example: `
Unassign role "redpanda-admin" from user "red"
  rpk security role unassign redpanda-admin --principal red

Unassign role "redpanda-admin" from users "red" and "panda"
  rpk security role unassign redpanda-admin --principal red,panda

Unassign role "data-reader" from group "engineering"
  rpk security role unassign data-reader --group engineering

Unassign role "data-reader" from both a user and a group
  rpk security role unassign data-reader --principal alice --group engineering
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]string{}); ok {
				out.Exit(h)
			}
			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(prof)

			roleName := args[0]

			// Handle principals (cloud+local).
			var toRemove []rpadmin.RoleMember
			if len(principals) > 0 {
				toRemove = parseRoleMember(principals)
				if prof.CheckFromCloud() {
					cl, err := publicapi.DataplaneClientFromRpkProfile(prof)
					out.MaybeDie(err, "unable to initialize cloud API client: %v", err)

					_, err = cl.Security.UpdateRoleMembership(cmd.Context(), connect.NewRequest(&dataplanev1.UpdateRoleMembershipRequest{
						RoleName: roleName,
						Remove:   roleMemberToMembership(toRemove),
					}))
					out.MaybeDie(err, "unable to unassign role %q from principal(s) %v: %v", roleName, principals, err)
				} else {
					cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
					out.MaybeDie(err, "unable to initialize admin api client: %v", err)

					_, err = cl.UnassignRole(cmd.Context(), roleName, toRemove)
					out.MaybeDie(err, "unable to unassign role %q from principal(s) %v: %v", roleName, principals, err)
				}
			}

			// Handle groups (local-only).
			if len(groups) > 0 {
				if prof.CheckFromCloud() {
					out.Die("--group is not supported for cloud clusters")
				}
				cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
				out.MaybeDie(err, "unable to initialize admin api client: %v", err)

				if !adminapi.HasMinimumVersion(cmd.Context(), cl, minGroupVersion) {
					out.Die("--group requires Redpanda version %s or later", minGroupVersion.String())
				}
				members := make([]*adminv2.RoleMember, len(groups))
				for i, g := range groups {
					members[i] = &adminv2.RoleMember{Member: &adminv2.RoleMember_Group{Group: &adminv2.RoleGroup{Name: g}}}
				}
				_, err = cl.SecurityService().RemoveRoleMembers(cmd.Context(), connect.NewRequest(&adminv2.RemoveRoleMembersRequest{
					RoleName: roleName,
					Members:  members,
				}))
				out.MaybeDie(err, "unable to unassign group(s) from role %q: %v", roleName, err)
			}

			// Output principals.
			if len(toRemove) > 0 {
				if isText, _, s, err := f.Format(toRemove); !isText {
					out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
					out.Exit(s)
				}
				fmt.Printf("Successfully unassigned role %q from\n", roleName)
				tw := out.NewTable("NAME", "PRINCIPAL-TYPE")
				defer tw.Flush()
				for _, m := range toRemove {
					tw.PrintStructFields(m)
				}
			}

			// Output groups.
			if len(groups) > 0 {
				fmt.Printf("Successfully unassigned role %q from group(s) %v\n", roleName, groups)
			}
		},
	}

	cmd.Flags().StringSliceVar(&principals, "principal", nil, "Principal to unassign the role from (repeatable)")
	cmd.Flags().StringSliceVar(&groups, "group", nil, "Group to unassign the role from (repeatable)")
	cmd.MarkFlagsOneRequired("principal", "group")
	return cmd
}
