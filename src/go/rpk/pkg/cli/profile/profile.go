// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package profile

import (
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage rpk profiles",
		Long: `Manage rpk profiles.

An rpk profile talks to a single Redpanda cluster. You can create multiple
profiles for multiple clusters and swap between them with 'rpk profile use'.
Multiple profiles may be useful if, for example, you use rpk to talk to
a localhost cluster, a dev cluster, and a prod cluster, and you want to keep
your configuration in one place.
`,
	}

	cmd.AddCommand(
		newCreateCommand(fs, p),
		newDeleteCommand(fs, p),
		newEditCommand(fs, p),
		newListCommand(fs, p),
		newPrintCommand(fs, p),
		newRenameToCommand(fs, p),
		newSetCommand(fs, p),
		newUseCommand(fs, p),
	)

	return cmd
}

func validProfiles(fs afero.Fs, p *config.Params) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
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
		for i := range y.Profiles {
			p := &y.Profiles[i]
			if strings.HasPrefix(p.Name, toComplete) {
				names = append(names, p.Name)
			}
		}
		return names, cobra.ShellCompDirectiveDefault
	}
}
