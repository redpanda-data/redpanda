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
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newSetDefaultsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "set-defaults [KEY=VALUE]+",
		Short: "Set rpk default fields",
		Long: `Set rpk default fields.

This command takes a list of key=value pairs to write to the defaults section
of rpk.yaml. The defaults section contains a set of settings that apply to all
profiles and changes the way that rpk acts. For a list of default flags and
what they mean, check 'rpk -X help' and look for any key that begins with
"defaults".

This command supports autocompletion of valid keys. You can also use the
format 'set key value' if you intend to only set one key.
`,

		Args:              cobra.MinimumNArgs(1),
		ValidArgsFunction: validSetDefaultsArgs,
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			// Other set commands are `set key value`, if people
			// use that older form by force of habit, we support
			// it.
			if len(args) == 2 && !strings.Contains(args[0], "=") {
				args = []string{args[0] + "=" + args[1]}
			}

			y, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "unable to load rpk.yaml: %v", err)

			err = doSetDefaults(y, args)
			out.MaybeDieErr(err)
			err = y.Write(fs)
			out.MaybeDieErr(err)
			fmt.Println("rpk.yaml updated successfully.")
		},
	}
}

func doSetDefaults(y *config.RpkYaml, set []string) error {
	for _, kv := range set {
		split := strings.SplitN(kv, "=", 2)
		if len(split) != 2 {
			return fmt.Errorf("invalid key=value pair %q", kv)
		}
		k, v := split[0], split[1]
		if y, ok := config.XFlagYamlPath(k); ok {
			k = y
		}
		err := config.Set(&y, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func validSetDefaultsArgs(_ *cobra.Command, _ []string, toComplete string) (ps []string, d cobra.ShellCompDirective) {
	var possibilities []string
	_, ypaths := config.XRpkDefaultsFlags()
	for _, p := range ypaths {
		if strings.HasPrefix(p, toComplete) {
			possibilities = append(possibilities, p+"=")
		}
	}
	return possibilities, cobra.ShellCompDirectiveNoSpace
}
