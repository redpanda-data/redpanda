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

func newRenameToCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:     "rename-to [NAME]",
		Short:   "Rename the current rpk auth",
		Aliases: []string{"rename"},
		Args:    cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				out.Die("rpk.yaml file does not exist")
			}

			p := y.Auth(y.CurrentCloudAuth)
			if p == nil {
				out.Die("current auth %q does not exist", y.CurrentCloudAuth)
				return
			}
			to := args[0]
			if y.Auth(to) != nil {
				out.Die("destination cloud auth %q already exists", to)
			}
			p.Name = to
			y.CurrentCloudAuth = to
			y.MoveAuthToFront(p)
			err = y.Write(fs)
			out.MaybeDieErr(err)
			fmt.Printf("Renamed current cloud auth to %q.\n", to)
		},
	}
}
