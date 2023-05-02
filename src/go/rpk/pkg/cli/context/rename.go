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
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newRenameToCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:     "rename-to [NAME]",
		Short:   "Rename the current rpk context",
		Aliases: []string{"rename"},
		Args:    cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				out.Die("rpk.yaml file does not exist")
			}

			cx := y.Context(y.CurrentContext)
			if cx == nil {
				out.Die("current context %q does not exist", y.CurrentContext)
				return
			}
			to := args[0]
			if y.Context(to) != nil {
				out.Die("destination context %q already exists", to)
			}
			cx.Name = to
			y.CurrentContext = to
			y.MoveContextToFront(cx)
			err = y.Write(fs)
			out.MaybeDieErr(err)
			fmt.Printf("Renamed current context to %q.\n", to)
		},
	}
}
