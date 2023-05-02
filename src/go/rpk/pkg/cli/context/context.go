// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package context

import (
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "context",
		Aliases: []string{"cx", "ctx"},
		Short:   "Manage rpk contexts",
	}

	cmd.AddCommand(
		newCreateCommand(fs, p),
		newDeleteCommand(fs, p),
		newDuplicateToCommand(fs, p),
		newEditCommand(fs, p),
		newListCommand(fs, p),
		newPrintCommand(fs, p),
		newRenameToCommand(fs, p),
		newSetCommand(fs, p),
		newUseCommand(fs, p),
	)

	return cmd
}

func validContexts(fs afero.Fs, p *config.Params) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
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
		for i := range y.Contexts {
			cx := &y.Contexts[i]
			if strings.HasPrefix(cx.Name, toComplete) {
				names = append(names, cx.Name)
			}
		}
		return names, cobra.ShellCompDirectiveDefault
	}
}
