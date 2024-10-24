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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newUseCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "use [NAME]",
		Short: "Select the rpk cloud authentication to use",
		Long: `Select the rpk cloud authentication to use.

This swaps the current cloud authentication to the specified cloud
authentication. If your current profile is a cloud profile, this unsets the
current profile (since the authentication is now different). If your current
profile is for a self hosted cluster, the profile is kept.
`,

		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: validAuths(fs, p),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				out.Die("rpk.yaml file does not exist")
			}

			name := args[0]
			nameMatches := findName(y, name)
			if len(nameMatches) > 1 {
				out.Die("multiple cloud auths with name %q exist, something went wrong in the underlying rpk.yaml file; please delete this auth", name)
			} else if len(nameMatches) == 0 {
				out.Die("cloud auth %q does not exist", name)
			}

			var idx int
			for i := range nameMatches {
				idx = i
			}
			a := &y.CloudAuths[idx]

			var profileCleared bool
			var priorProfile string
			p := y.Profile(y.CurrentProfile)
			if p != nil {
				if !p.CloudCluster.HasAuth(*a) {
					priorProfile = y.CurrentProfile
					y.CurrentProfile = ""
					profileCleared = true
				}
			}

			y.MakeAuthCurrent(&a)
			err = y.Write(fs)
			out.MaybeDie(err, "unable to write rpk.yaml: %v", err)

			if profileCleared {
				fmt.Printf("Clearing the current profile %q which is using the auth that is being swapped from.\n", priorProfile)
			}
			fmt.Printf("Set current cloud auth to %q.\n", name)
		},
	}
	return cmd
}
