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
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Manage rpk cloud authentications",
		Long: `Manage rpk cloud authentications.

An rpk cloud authentication allows you to talk to Redpanda Cloud. Most likely,
you will only ever need to use a single SSO based login and you will not need
this command space. Multiple authentications can be useful if you have multiple
Redpanda Cloud accounts for different organizations and want to swap between
them, or if you use SSO as well as client credentials. It is recommended to
only use a single SSO based login.
`,
	}

	cmd.AddCommand(
		newCreateCommand(fs, p),
		newDeleteCommand(fs, p),
		newEditCommand(fs, p),
		newListCommand(fs, p),
		newRenameToCommand(fs, p),
		newUseCommand(fs, p),
	)

	return cmd
}

func validAuths(fs afero.Fs, p *config.Params) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		cfg, err := p.Load(fs)
		if err != nil {
			return nil, cobra.ShellCompDirectiveError
		}
		y, ok := cfg.ActualRpkYaml()
		if !ok {
			return nil, cobra.ShellCompDirectiveDefault
		}
		var names []string
		for i := range y.CloudAuths {
			a := &y.CloudAuths[i]
			if strings.HasPrefix(a.Name, toComplete) {
				names = append(names, a.Name)
			}
		}
		return names, cobra.ShellCompDirectiveDefault
	}
}

func findName(y *config.RpkYaml, name string) map[int]struct{} {
	nameMatches := make(map[int]struct{})
	for i, a := range y.CloudAuths {
		if a.Name == name {
			nameMatches[i] = struct{}{}
		}
	}
	return nameMatches
}
