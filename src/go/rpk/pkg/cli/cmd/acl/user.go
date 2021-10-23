// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package acl

import (
	"fmt"
	"strings"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewUserCommand(fs afero.Fs) *cobra.Command {
	var apiUrls []string
	cmd := &cobra.Command{
		Use:   "user",
		Short: "Manage users",
	}
	cmd.PersistentFlags().StringSliceVar(
		&apiUrls,
		config.FlagAdminHosts2,
		[]string{},
		"The comma-separated list of Admin API addresses (<IP>:<port>)."+
			" You must specify one for each node.",
	)

	cmd.AddCommand(NewCreateUserCommand(fs))
	cmd.AddCommand(NewDeleteUserCommand(fs))
	cmd.AddCommand(NewListUsersCommand(fs))
	return cmd
}

// UserAPI encapsulates functions needed for a user API.
type UserAPI interface {
	CreateUser(username, password string) error
	DeleteUser(username string) error
	ListUsers() ([]string, error)
}

func NewCreateUserCommand(fs afero.Fs) *cobra.Command {
	var userOld, pass, passOld, mechanism string
	cmd := &cobra.Command{
		Use:   "create [USER} -p [PASS]",
		Short: "Create an ACL user.",
		Args:  cobra.MaximumNArgs(1), // when the deprecated user flag is removed, change this to cobra.ExactArgs(1)
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// Backwards compatibility: we favor the new user
			// format and the new password flag. If either are
			// empty, we check the old. If either of those are
			// empty, we error.
			var user string
			if len(args) > 0 {
				user = args[0]
			} else if userOld != "" { // backcompat
				user = userOld
			} else {
				out.Die("missing required username argument")
			}
			if pass == "" {
				if passOld == "" { // backcompat
					out.Die("missing required --password")
				}
				pass = passOld
			}

			switch strings.ToLower(mechanism) {
			case "scram-sha-256":
				mechanism = admin.ScramSha256
			case "scram-sha-512":
				mechanism = admin.ScramSha512
			default:
				out.Die("unsupported mechanism %q", mechanism)
			}

			err = cl.CreateUser(user, pass, mechanism)
			out.MaybeDie(err, "unable to create user %q: %v", user, err)
			fmt.Printf("Created user %q.\n", user)
		},
	}

	cmd.Flags().StringVar(&userOld, "new-username", "", "")
	cmd.Flags().MarkDeprecated("new-username", "the username now does not require a flag") // Oct 2021

	cmd.Flags().StringVarP(&pass, "password", "p", "", "new user's password")
	cmd.Flags().StringVar(&passOld, "new-password", "", "")
	cmd.Flags().MarkDeprecated("new-password", "renamed to --password") // Oct 2021

	cmd.Flags().StringVar(
		&mechanism,
		"mechanism",
		strings.ToLower(admin.ScramSha256),
		"SASL mechanism to use (scram-sha-256, scram-sha-512, case insensitive)",
	)

	return cmd
}

func NewDeleteUserCommand(fs afero.Fs) *cobra.Command {
	var oldUser string
	cmd := &cobra.Command{
		Use:   "delete [USER]",
		Short: "Delete an ACL user.",
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
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

			err = cl.DeleteUser(user)
			out.MaybeDie(err, "unable to delete user %q: %s", user, err)
			fmt.Printf("Deleted user %q.\n", user)
		},
	}

	cmd.Flags().StringVar(&oldUser, "delete-username", "", "The user to be deleted")
	cmd.Flags().MarkDeprecated("delete-username", "the username now does not require a flag")

	return cmd
}

func NewListUsersCommand(fs afero.Fs) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List users.",
		Run: func(cmd *cobra.Command, _ []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			users, err := cl.ListUsers()
			out.MaybeDie(err, "unable to list users: %v", err)

			tw := out.NewTable("Username")
			defer tw.Flush()
			for _, u := range users {
				tw.Print(u)
			}
		},
	}
}
