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
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newUpdateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var newPass, mechanism string
	cmd := &cobra.Command{
		Use:   "update [USER] --new-password [PW]",
		Short: "Update SASL user credentials",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitNotServerlessAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			user := args[0]
			err = cl.UpdateUser(cmd.Context(), user, newPass, strings.ToUpper(mechanism))
			out.MaybeDie(err, "unable to update the client credentials for user %q: %v", user, err)
			if isText, _, s, err := f.Format(credentials{user, "", mechanism}); !isText {
				out.MaybeDie(err, "unable to print credentials in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			out.Exit("Updated user %q successfully.", user)
		},
	}

	cmd.Flags().StringVar(&newPass, "new-password", "", "New user's password.")
	cmd.Flags().StringVar(&mechanism, "mechanism", adminapi.ScramSha256, "SASL mechanism to use for the user you are updating (scram-sha-256, scram-sha-512, case insensitive)")
	cmd.MarkFlagRequired("new-password")
	cmd.MarkFlagRequired("mechanism")

	return cmd
}
