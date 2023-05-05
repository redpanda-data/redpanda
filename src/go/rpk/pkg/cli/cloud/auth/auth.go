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
	return func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
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
