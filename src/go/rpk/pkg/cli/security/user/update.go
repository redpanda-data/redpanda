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
	"strings"

	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newUpdateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var newPass, mechanism string
	cmd := &cobra.Command{
		Use:   "update [USER] --new-password [PW] --mechanism [MECHANISM]",
		Short: "Update SASL user credentials",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			user := args[0]
			if p.CheckFromCloud() && !p.CloudCluster.IsServerless() {
				cl, err := publicapi.DataplaneClientFromRpkProfile(p)
				out.MaybeDie(err, "unable to initialize cloud client: %v", err)

				req := connect.NewRequest(
					&dataplanev1.UpdateUserRequest{
						User: &dataplanev1.UpdateUserRequest_User{
							Name:      user,
							Password:  newPass,
							Mechanism: stringToDataplaneMechanism(mechanism),
						},
					},
				)
				_, err = cl.User.UpdateUser(cmd.Context(), req)
				out.MaybeDie(err, "unable to update the client credentials for user %q: %v", user, err)
			} else {
				cl, err := adminapi.NewClient(cmd.Context(), fs, p)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				err = cl.UpdateUser(cmd.Context(), user, newPass, strings.ToUpper(mechanism))
				out.MaybeDie(err, "unable to update the client credentials for user %q: %v", user, err)
			}
			if isText, _, s, err := f.Format(credentials{user, "", mechanism}); !isText {
				out.MaybeDie(err, "unable to print credentials in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			out.Exit("Updated user %q successfully.", user)
		},
	}

	cmd.Flags().StringVar(&newPass, "new-password", "", "New user's password.")
	cmd.Flags().StringVar(&mechanism, "mechanism", "", fmt.Sprintf("SASL mechanism to use for the user you are updating (%v, %v, case insensitive)", adminapi.ScramSha256, adminapi.ScramSha512))
	cmd.MarkFlagRequired("new-password")
	cmd.MarkFlagRequired("mechanism")
	cmd.RegisterFlagCompletionFunc("mechanism", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{adminapi.ScramSha256, adminapi.ScramSha512}, cobra.ShellCompDirectiveDefault
	})
	return cmd
}
