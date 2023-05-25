// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package auth

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newCreateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		set         []string
		description string
	)
	cmd := &cobra.Command{
		Use:   "create [NAME]",
		Short: "Create an rpk cloud auth",
		Long: `Create an rpk cloud auth.

This command creates a new rpk cloud auth. The default SSO login does not need
any additional auth values (the token is populated on login) so you can just
create an empty auth. You can also use --set to set key=value pairs for client
credentials.

The --set flag supports autocompletion, suggesting the -X key format. If you
begin writing a YAML path, the flag will suggest the rest of the path.

rpk always switches the current cloud auth to the newly created auth.
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			yAct, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "unable to load rpk.yaml: %v", err)

			name := args[0]
			if a := yAct.Auth(name); a != nil {
				out.Die("cloud auth %q already exists", name)
			}

			var a config.RpkCloudAuth
			for _, kv := range set {
				split := strings.SplitN(kv, "=", 2)
				if len(split) != 2 {
					out.Die("invalid key=value pair %q", kv)
				}
				err := config.Set(&a, split[0], split[1])
				out.MaybeDieErr(err)
			}
			a.Name = name
			a.Description = description

			if a.ClientID != "" || a.ClientSecret != "" {
				if a.ClientID == "" || a.ClientSecret == "" {
					out.Die("client-id and client-secret must both be set or both be empty")
				}
			}

			yAct.CurrentCloudAuth = yAct.PushAuth(a)
			err = yAct.Write(fs)
			out.MaybeDie(err, "unable to write rpk.yaml: %v", err)
			fmt.Printf("Created and switched to new cloud auth %q.\n", name)
		},
	}
	cmd.Flags().StringSliceVarP(&set, "set", "s", nil, "A key=value pair to set in the cloud auth")
	cmd.Flags().StringVar(&description, "description", "", "Optional description of the auth")
	cmd.RegisterFlagCompletionFunc("set", createSetCompletion)
	return cmd
}

func createSetCompletion(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	var possibilities []string
	t := reflect.TypeOf(config.RpkCloudAuth{})
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		tag := sf.Tag.Get("yaml")
		if comma := strings.IndexByte(tag, ','); comma != -1 {
			tag = tag[:comma]
		}
		if tag == "name" || tag == "description" {
			// name is an arg, description is a flag: do not autocomplete these
			continue
		}
		if strings.HasPrefix(tag, toComplete) {
			possibilities = append(possibilities, tag+"=")
		}
	}
	return possibilities, cobra.ShellCompDirectiveNoSpace
}
