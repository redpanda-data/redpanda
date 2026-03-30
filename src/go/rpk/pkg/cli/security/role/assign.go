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

func assignCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var principals []string
	cmd := &cobra.Command{
		Use:     "assign [ROLE] --principal [PRINCIPALS...]",
		Aliases: []string{"add"},
		Short:   "Assign a Redpanda role to a principal",
		Long: `Assign a Redpanda role to a principal.

The '--principal' flag accepts principals with the format
'<PrincipalPrefix>:<Principal>'. If 'PrincipalPrefix' is not provided, then
defaults to 'User:'.
`,
		Example: `
Assign role "redpanda-admin" to user "red"
  rpk security role assign redpanda-admin --principal red

Assign role "redpanda-admin" to users "red" and "panda"
  rpk security role assign redpanda-admin --principal red,panda

Assign role "redpanda-admin" to group "pandas"
  rpk security role assign redpanda-admin --principal Group:pandas
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
			var toAdd []rpadmin.RoleMember
			if len(principals) > 0 {
				toAdd = parseRoleMember(principals)
				if prof.CheckFromCloud() {
					cl, err := publicapi.DataplaneClientFromRpkProfile(prof)
					out.MaybeDie(err, "unable to initialize cloud API client: %v", err)

					_, err = cl.Security.UpdateRoleMembership(cmd.Context(), connect.NewRequest(&dataplanev1.UpdateRoleMembershipRequest{
						RoleName: roleName,
						Add:      roleMemberToMembership(toAdd),
					}))
					out.MaybeDie(err, "unable to assign role %q to principal(s) %v: %v", roleName, principals, err)
				} else {
					cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
					out.MaybeDie(err, "unable to initialize admin api client: %v", err)

					_, err = cl.AssignRole(cmd.Context(), roleName, toAdd)
					out.MaybeDie(err, "unable to assign role %q to principal(s) %v: %v", roleName, principals, err)
				}
			}

			// Output principals.
			if len(toAdd) > 0 {
				if isText, _, s, err := f.Format(toAdd); !isText {
					out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
					out.Exit(s)
				}
				fmt.Printf("Successfully assigned role %q to\n", roleName)
				tw := out.NewTable("NAME", "PRINCIPAL-TYPE")
				defer tw.Flush()
				for _, m := range toAdd {
					tw.PrintStructFields(m)
				}
			}
		},
	}

	cmd.Flags().StringSliceVar(&principals, "principal", nil, "Principal to assign the role to (repeatable)")
	cmd.MarkFlagRequired("principal")
	return cmd
}
