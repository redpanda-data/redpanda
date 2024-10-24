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

func newSetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "set [KEY=VALUE]+",
		Short: "Set fields in the current rpk profile",
		Long: `Set fields in the current rpk profile.

As in the create command, this command takes a list of key=value pairs to write
to the current profile.

The key can either be the name of a -X flag or the path to the field in the
profile's yaml format. For example, using --set tls.enabled=true OR --set
kafka_api.tls.enabled=true is equivalent. The former corresponds to the -X flag
tls.enabled, while the latter corresponds to the path kafka_api.tls.enabled in
the profile's yaml.

This command supports autocompletion of valid keys, suggesting the -X key
format. If you begin writing a YAML path, this command will suggest the rest of
the path.

You can also use the format 'set key value' if you intend to only set one key.
`,

		Args:              cobra.MinimumNArgs(1),
		ValidArgsFunction: validSetArgs,
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

			p := y.Profile(y.CurrentProfile)
			if p == nil {
				out.Die("current profile %q does not exist", y.CurrentProfile)
			}
			err = doSet(p, args)
			out.MaybeDieErr(err)
			err = y.Write(fs)
			out.MaybeDieErr(err)
			fmt.Printf("Profile %q updated successfully.\n", y.CurrentProfile)
		},
	}
}

func doSet(p *config.RpkProfile, set []string) error {
	for _, kv := range set {
		split := strings.SplitN(kv, "=", 2)
		if len(split) != 2 {
			return fmt.Errorf("invalid key=value pair %q", kv)
		}
		k, v := split[0], split[1]
		if y, ok := config.XFlagYamlPath(k); ok {
			k = y
		}
		err := config.Set(&p, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// By default, we show the yaml paths for autocompletion. If a person uses a -X
// flag name, we then switch to displaying X flag completions.
func validSetArgs(_ *cobra.Command, _ []string, toComplete string) (ps []string, d cobra.ShellCompDirective) {
	defer func() {
		for i, p := range ps {
			ps[i] = p + "="
		}
	}()

	xf, ypaths := config.XProfileFlags()
	ypaths = append(ypaths, "description", "prompt") // we have no xflag for the description nor prompt field, the latter is a global that can also be edited per profile
	if len(toComplete) == 0 {
		return ypaths, cobra.ShellCompDirectiveNoSpace
	}
	var possibilities []string
	for _, p := range ypaths {
		if strings.HasPrefix(p, toComplete) {
			possibilities = append(possibilities, p)
		}
	}
	if len(possibilities) > 0 {
		return possibilities, cobra.ShellCompDirectiveNoSpace
	}
	for _, p := range xf {
		if strings.HasPrefix(p, toComplete) {
			possibilities = append(possibilities, p)
		}
	}
	return possibilities, cobra.ShellCompDirectiveNoSpace
}
