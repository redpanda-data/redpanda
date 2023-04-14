// Copyright 2021 Redpanda Data, Inc.
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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newUserCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "user",
		Short: "Manage SASL users",
		Long: `Manage SASL users.

If SASL is enabled, a SASL user is what you use to talk to Redpanda, and ACLs
control what your user has access to. See 'rpk acl --help' for more information
about ACLs, and 'rpk acl user create --help' for more information about
creating SASL users. Using SASL requires setting "enable_sasl: true" in the
redpanda section of your redpanda.yaml.
`,
	}
	p.InstallAdminFlags(cmd)
	p.InstallKafkaFlags(cmd) // old ACL user commands have this, and Kafka SASL creds are used for admin API basic auth
	cmd.AddCommand(
		newCreateUserCommand(fs, p),
		newDeleteUserCommand(fs, p),
		newListUsersCommand(fs, p),
	)
	return cmd
}

// UserAPI encapsulates functions needed for a user API.
type UserAPI interface {
	CreateUser(username, password string) error
	DeleteUser(username string) error
	ListUsers() ([]string, error)
}

func newCreateUserCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var userOld, pass, newPass, mechanism string
	cmd := &cobra.Command{
		Use:   "create [USER] -p [PASS]",
		Short: "Create a SASL user",
		Long: `Create a SASL user.

This command creates a single SASL user with the given password, optionally
with a custom "mechanism". SASL consists of three parts: a username, a
password, and a mechanism. The mechanism determines which authentication flow
the client will use for this user/pass.

Redpanda currently supports two mechanisms: SCRAM-SHA-256, the default, and
SCRAM-SHA-512, which is the same flow but uses sha512 rather than sha256.

Using SASL requires setting "enable_sasl: true" in the redpanda section of your
redpanda.yaml. Before a created SASL account can be used, you must also create
ACLs to grant the account access to certain resources in your cluster. See the
acl help text for more info.
`,

		Args: cobra.MaximumNArgs(1), // when the deprecated user flag is removed, change this to cobra.ExactArgs(1)
		Run: func(cmd *cobra.Command, args []string) {
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

			// Redpanda added support for using our Kafka SASL
			// credentials for basic auth. We use --password on
			// commands to set the Kafka SASL password, but we also
			// use --password here to specify the new user
			// password. Historically, this was fine.
			//
			// Now, we support --new-password AND --password
			// (--new-password was the original flag), and we add
			// the short form -p to --new-password. Previously, -p
			// was on --password, which meant -p and --password set
			// the same value and we could not tell.
			//
			// Now, if we detect --user, we require --new-password
			// (or -p). If we only see --password (i.e. only
			// "password" is set), we fail.
			//
			// See #6360.
			//
			// Better long term is for people to use -X.
			userFlag := cmd.Flag(config.FlagSASLUser).Value.String()
			if userFlag != "" && newPass == "" {
				out.Die("unable to create user with when using basic auth, use --new-password to specify the new user's password")
			}

			if newPass != "" {
				pass = newPass
			}

			switch strings.ToLower(mechanism) {
			case "scram-sha-256":
				mechanism = admin.ScramSha256
			case "scram-sha-512":
				mechanism = admin.ScramSha512
			default:
				out.Die("unsupported mechanism %q", mechanism)
			}

			err = cl.CreateUser(cmd.Context(), user, pass, mechanism)
			out.MaybeDie(err, "unable to create user %q: %v", user, err)
			fmt.Printf("Created user %q.\n", user)
		},
	}

	cmd.Flags().StringVar(&userOld, "new-username", "", "")
	cmd.Flags().MarkHidden("new-username")

	cmd.Flags().StringVar(&pass, "password", "", "New user's password (NOTE: if using --password for the admin API, use --new-password)")
	cmd.Flags().StringVarP(&newPass, "new-password", "p", "", "")
	cmd.Flags().MarkHidden("new-password")

	cmd.Flags().StringVar(&mechanism, "mechanism", strings.ToLower(admin.ScramSha256), "SASL mechanism to use for the user you are creating (scram-sha-256, scram-sha-512, case insensitive); not to be confused with the global flag --sasl-mechanism which is used for authenticating the rpk client")

	return cmd
}

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

			err = cl.DeleteUser(cmd.Context(), user)
			out.MaybeDie(err, "unable to delete user %q: %s", user, err)
			fmt.Printf("Deleted user %q.\n", user)
		},
	}

	cmd.Flags().StringVar(&oldUser, "delete-username", "", "The user to be deleted")
	cmd.Flags().MarkDeprecated("delete-username", "The username now does not require a flag")

	return cmd
}

func newListUsersCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List SASL users",
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			users, err := cl.ListUsers(cmd.Context())
			out.MaybeDie(err, "unable to list users: %v", err)

			tw := out.NewTable("Username")
			defer tw.Flush()
			for _, u := range users {
				tw.Print(u)
			}
		},
	}
}
