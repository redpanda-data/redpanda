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

func newSetGlobalsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "set-globals [KEY=VALUE]+",
		Short: "Set rpk globals fields",
		Long: `Set rpk globals fields.

This command takes a list of key=value pairs to write to the global config 
section of rpk.yaml. The globals section contains a set of settings that apply
to all profiles and changes the way that rpk acts. For a list of global flags
and what they mean, check 'rpk -X help' and look for any key that begins with
"globals".

This command supports autocompletion of valid keys. You can also use the
format 'set key value' if you intend to only set one key.
`,

		Args:              cobra.MinimumNArgs(1),
		ValidArgsFunction: validSetGlobalArgs,
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			// Other set commands are `set key value`, if people
			// use that older form by force of habit, we support
			// it.
			if len(args) == 2 && !strings.Contains(args[0], "=") {
				args = []string{args[0] + "=" + args[1]}
			}

			y, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "unable to load rpk.yaml: %v", err)

			err = doSetGlobals(y, args)
			out.MaybeDieErr(err)
			err = y.Write(fs)
			out.MaybeDieErr(err)
			fmt.Println("rpk.yaml updated successfully.")
		},
	}
}

func doSetGlobals(y *config.RpkYaml, set []string) error {
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

func validSetGlobalArgs(_ *cobra.Command, _ []string, toComplete string) (ps []string, d cobra.ShellCompDirective) {
	var possibilities []string
	_, ypaths := config.XRpkGlobalFlags()
	for _, p := range ypaths {
		if strings.HasPrefix(p, toComplete) {
			possibilities = append(possibilities, p+"=")
		}
	}
	return possibilities, cobra.ShellCompDirectiveNoSpace
}
