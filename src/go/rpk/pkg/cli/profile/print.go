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
	"gopkg.in/yaml.v3"
)

func newPrintCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var raw bool
	cmd := &cobra.Command{
		Use:   "print [NAME]",
		Short: "Print rpk profile configuration",
		Long: `Print rpk profile configuration.

If no name is specified, this command prints the current profile as it is
loaded in rpk with internal defaults, user specified flags, and environment
variables applied. If you wish to print the current raw profile as it exists
in rpk.yaml, you can use the --raw flag.
`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: validProfiles(fs, p),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			y := cfg.VirtualRpkYaml()
			if raw {
				var ok bool
				y, ok = cfg.ActualRpkYaml()
				if !ok {
					out.Die("rpk.yaml file does not exist")
				}
			}

			if len(args) == 0 {
				args = append(args, y.CurrentProfile)
			}
			p := y.Profile(args[0])
			if p == nil {
				out.Die("profile %s does not exist", args[0])
			}

			m, err := yaml.Marshal(p)
			out.MaybeDie(err, "unable to encode profile: %v", err)
			fmt.Println(string(m))
		},
	}
	cmd.Flags().BoolVar(&raw, "raw", false, "Print raw configuration from rpk.yaml, without environment variables nor flags applied")
	return cmd
}
