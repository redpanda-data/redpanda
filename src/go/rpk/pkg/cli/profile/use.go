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

func newUseCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:               "use [NAME]",
		Short:             "Select the rpk profile to use",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: ValidProfiles(fs, p),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				out.Die("rpk.yaml file does not exist")
			}

			name := args[0]
			p := y.Profile(name)
			if p == nil {
				out.Die("profile %q does not exist", name)
			}
			priorAuth, currentAuth := y.MoveProfileToFront(&p)

			err = y.Write(fs)
			out.MaybeDieErr(err)
			fmt.Printf("Set current profile to %q.\n", name)
			config.MaybePrintAuthSwitchMessage(priorAuth, currentAuth)
		},
	}

	return cmd
}
