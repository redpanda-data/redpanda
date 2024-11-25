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
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newCurrentCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var noNewline bool
	cmd := &cobra.Command{
		Use:   "current",
		Short: "Print the current profile name",
		Long: `Print the current profile name.

This is a tiny command that simply prints the current profile name, which may
be useful in scripts, or a PS1, or to confirm what you have selected.
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			if !noNewline {
				defer fmt.Println()
			}
			y, ok := cfg.ActualRpkYaml()
			if !ok {
				return
			}
			fmt.Print(y.CurrentProfile)
		},
	}
	cmd.Flags().BoolVarP(&noNewline, "no-newline", "n", false, "Do not print a newline after the profile name")
	return cmd
}
