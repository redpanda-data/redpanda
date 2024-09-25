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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
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
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			roleName := args[0]

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

			if f.Kind == "text" {
				fmt.Printf("Successfully deleted role %q\n", roleName)
			}
		},
	}
	p.InstallKafkaFlags(cmd)
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}
