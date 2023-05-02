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

func newUseCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:               "use [NAME]",
		Short:             "Select the rpk context to use",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: validContexts(fs, p),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				out.Die("rpk.yaml file does not exist")
			}

			name := args[0]
			cx := y.Context(name)
			if cx == nil {
				out.Die("context %q does not exist", name)
			}
			y.CurrentContext = name
			y.MoveContextToFront(cx)

			err = y.Write(fs)
			out.MaybeDieErr(err)
			fmt.Printf("Set current context to %q.\n", name)
		},
	}

	return cmd
}
