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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func deleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var noConfirm bool
	cmd := &cobra.Command{
		Use:   "delete [ROLE]",
		Short: "Delete a role in Redpanda",
		Long: `Delete a role in Redpanda.

This action will remove all associated ACLs from the role and unassign members.

The flag '--no-confirm' can be used to avoid the confirmation prompt.
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

			if prof.CheckFromCloud() {
				cl, err := publicapi.DataplaneClientFromRpkProfile(prof)
				out.MaybeDie(err, "unable to initialize cloud API client: %v", err)

				err = describeAndPrintRoleCloud(cmd.Context(), cl, f, roleName, true, true)
				out.MaybeDieErr(err)

				if !noConfirm {
					confirmed, err := out.Confirm("Confirm deletion of role %q?  This action will remove all associated ACLs and unassign role members", roleName)
					out.MaybeDie(err, "unable to confirm deletion: %v", err)
					if !confirmed {
						out.Exit("Deletion canceled.")
					}
				}

				_, err = cl.Security.DeleteRole(cmd.Context(), connect.NewRequest(&dataplanev1.DeleteRoleRequest{
					RoleName:   roleName,
					DeleteAcls: true,
				}))
				out.MaybeDie(err, "unable to delete role %q: %v", roleName, err)
			} else {
				cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
				out.MaybeDie(err, "unable to initialize admin api client: %v", err)

				adm, err := kafka.NewAdmin(fs, prof)
				out.MaybeDie(err, "unable to initialize kafka client: %v", err)
				defer adm.Close()

				err = describeAndPrintRole(cmd.Context(), cl, adm, f, roleName, true, true)
				out.MaybeDieErr(err)

				if !noConfirm {
					confirmed, err := out.Confirm("Confirm deletion of role %q?  This action will remove all associated ACLs and unassign role members", roleName)
					out.MaybeDie(err, "unable to confirm deletion: %v", err)
					if !confirmed {
						out.Exit("Deletion canceled.")
					}
				}

				err = cl.DeleteRole(cmd.Context(), roleName, true)
				out.MaybeDie(err, "unable to delete role %q: %v", roleName, err)
			}

			if f.Kind == "text" {
				fmt.Printf("Successfully deleted role %q\n", roleName)
			}
		},
	}
	p.InstallKafkaFlags(cmd)
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}
