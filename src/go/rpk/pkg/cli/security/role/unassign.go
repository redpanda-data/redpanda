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
	cmd := &cobra.Command{
		Use:     "unassign [ROLE] --principal [PRINCIPALS...]",
		Aliases: []string{"remove"},
		Short:   "Unassign a Redpanda role from a principal",
		Long: `Unassign a Redpanda role from a principal.

The '--principal' flag accepts principals with the format
'<PrincipalPrefix>:<Principal>'. If 'PrincipalPrefix' is not provided, then
defaults to 'User:'.
`,
		Example: `
Unassign role "redpanda-admin" from user "red"
  rpk security role unassign redpanda-admin --principal red

Unassign role "redpanda-admin" from users "red" and "panda"
  rpk security role unassign redpanda-admin --principal red,panda

Unassign role "redpanda-admin" from group "pandas"
  rpk security role unassign redpanda-admin --principal Group:pandas
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
		},
	}

	cmd.Flags().StringSliceVar(&principals, "principal", nil, "Principal to unassign the role from (repeatable)")
	cmd.MarkFlagRequired("principal")
	return cmd
}
