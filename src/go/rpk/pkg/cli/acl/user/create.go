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
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type credentials struct {
	User      string `json:"user" yaml:"user"`
	Password  string `json:"password,omitempty" yaml:"password,omitempty"`
	Mechanism string `json:"mechanism,omitempty" yaml:"mechanism,omitempty"`
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
			f := p.Formatter
			if h, ok := f.Help(credentials{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := adminapi.NewClient(fs, p)
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

			userFlag := cmd.Flag(config.FlagSASLUser).Value.String()
			pass = cmd.Flag("password").Value.String()
			var generated bool
			// We either run the command without password:
			//   rpk acl user create foo
			// Or we run the command with user/password for basic auth:
			//   rpk acl user create foo --user my_user --pass my_pass
			// In both cases, we auto-generate a random password:
			if (newPass == "" && pass == "") || (newPass == "" && userFlag != "" && pass != "") {
				gen, err := generatePassword(30)
				out.MaybeDie(err, "unable to generate a password: %v; you can specify your own password using the '--password' flag", err)
				pass = gen
				generated = true
			}

			if newPass != "" {
				pass = newPass
			}

			switch strings.ToLower(mechanism) {
			case "scram-sha-256":
				mechanism = adminapi.ScramSha256
			case "scram-sha-512":
				mechanism = adminapi.ScramSha512
			default:
				out.Die("unsupported mechanism %q", mechanism)
			}

			err = cl.CreateUser(cmd.Context(), user, pass, mechanism)
			out.MaybeDie(err, "unable to create user %q: %v", user, err)
			c := credentials{user, "", mechanism}
			if generated {
				c.Password = pass // We only want to display the password if it was generated.
			}
			if isText, _, s, err := f.Format(c); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			fmt.Printf("Created user %q.\n", user)
			if generated {
				fmt.Println("Automatically generated password:")
				// In a new line so it can be easily 'tail -1'-ed
				fmt.Println(pass)
			}
		},
	}

	cmd.Flags().StringVar(&userOld, "new-username", "", "")
	cmd.Flags().MarkHidden("new-username")

	// This is needed here in order to show the password flag with a different
	// usage text for this command only.
	p.InstallKafkaFlags(cmd)
	passwordFlag := cmd.PersistentFlags().Lookup("password")
	passwordFlag.Usage = "New user's password (NOTE: if using --password for the admin API, use --new-password)"
	passwordFlag.Hidden = false

	cmd.Flags().StringVarP(&newPass, "new-password", "p", "", "")
	cmd.Flags().MarkHidden("new-password")

	cmd.Flags().StringVar(&mechanism, "mechanism", strings.ToLower(adminapi.ScramSha256), "SASL mechanism to use for the user you are creating (scram-sha-256, scram-sha-512, case insensitive)")

	return cmd
}

func generatePassword(passLength int) (string, error) {
	lowercase := "abcdefghijklmnopqrstuvwxyz"
	uppercase := strings.ToUpper(lowercase)
	numbers := "0123456789"
	allCharSet := lowercase + uppercase + numbers

	randSize := big.NewInt(int64(len(allCharSet)))
	var pass string
	for i := 0; i < passLength; i++ {
		r, err := rand.Int(rand.Reader, randSize)
		if err != nil {
			return "", fmt.Errorf("unable to generate secure random number: %v", err)
		}
		pass += string(allCharSet[r.Int64()])
	}

	return pass, nil
}
