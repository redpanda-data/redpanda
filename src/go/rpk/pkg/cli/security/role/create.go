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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type createResponse struct {
	Roles []string `json:"roles" yaml:"roles"`
}

func createCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "create [ROLE]",
		Short: "Create a role in Redpanda",
		Long: `Create a role in Redpanda.

After creating a role you may bind ACLs to the role using the '--allow-role' 
flag in the 'rpk security acl create' command.`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(createResponse{}); ok {
				out.Exit(h)
			}
			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(prof)

			roleName := args[0]
			if prof.CheckFromCloud() {
				cl, err := publicapi.DataplaneClientFromRpkProfile(prof)
				out.MaybeDie(err, "unable to initialize cloud API client: %v", err)

				_, err = cl.Security.CreateRole(cmd.Context(), connect.NewRequest(&dataplanev1.CreateRoleRequest{
					Role: &dataplanev1.Role{Name: roleName},
				}))
				out.MaybeDie(err, "unable to create role %q: %v", roleName, err)
			} else {
				cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
				out.MaybeDie(err, "unable to initialize admin api client: %v", err)

				_, err = cl.CreateRole(cmd.Context(), roleName)
				out.MaybeDie(err, "unable to create role %q: %v", roleName, adminapi.TryDecodeMessageFromErr(err))
			}
			if isText, _, s, err := f.Format(createResponse{[]string{roleName}}); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			fmt.Printf(`Successfully created role %[1]q

ACLs can now be added to this role using
  rpk security acl create --allow-role "RedpandaRole:%[1]v" [acl-flags]

Check 'rpk security acl create --help' for more information about how to create 
an ACL. 
`, roleName)
		},
	}
}
