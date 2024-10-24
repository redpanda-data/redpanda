// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package user

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeleteUserCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var oldUser string
	cmd := &cobra.Command{
		Use:   "delete [USER]",
		Short: "Delete a SASL user",
		Long: `Delete a SASL user.

This command deletes the specified SASL account from Redpanda. This does not
delete any ACLs that may exist for this user.
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(credentials{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitNotServerlessAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// Backwards compat: we favor the new format (an
			// argument), but if that is empty, we use the old
			// flag. If still empty, we error.
			var user string
			if len(args) > 0 {
				user = args[0]
			} else if len(oldUser) > 0 {
				user = oldUser
			} else {
				out.Die("missing required username argument")
			}

			err = cl.DeleteUser(cmd.Context(), user)
			out.MaybeDie(err, "unable to delete user %q: %s", user, err)
			if isText, _, s, err := f.Format(credentials{user, "", ""}); !isText {
				out.MaybeDie(err, "unable to print response in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			fmt.Printf("Deleted user %q.\n", user)
		},
	}

	cmd.Flags().StringVar(&oldUser, "delete-username", "", "The user to be deleted")
	cmd.Flags().MarkDeprecated("delete-username", "The username now does not require a flag")

	return cmd
}
