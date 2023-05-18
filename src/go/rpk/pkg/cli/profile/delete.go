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

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:               "delete [NAME]",
		Short:             "Delete an rpk context",
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
			cleared, err := deleteCtx(fs, y, name)
			out.MaybeDieErr(err)
			fmt.Printf("Deleted context %q.\n", name)
			if cleared {
				fmt.Println("This was the selected context; rpk will use defaults until a new context is selected.")
			}
		},
	}
}

func deleteCtx(
	fs afero.Fs,
	y *config.RpkYaml,
	name string,
) (cleared bool, err error) {
	idx := -1
	for i, cx := range y.Profiles {
		if cx.Name == name {
			idx = i
			break
		}
	}
	if idx == -1 {
		return false, fmt.Errorf("context %q does not exist", name)
	}
	y.Profiles = append(y.Profiles[:idx], y.Profiles[idx+1:]...)
	if y.CurrentProfile == name {
		y.CurrentProfile = ""
		cleared = true
	}
	if err := y.Write(fs); err != nil {
		return false, fmt.Errorf("unable to write rpk file: %v", err)
	}
	return cleared, nil
}
