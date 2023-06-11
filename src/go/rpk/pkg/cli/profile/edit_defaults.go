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
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newEditDefaultsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "edit-defaults",
		Short: "Edit rpk defaults",
		Long: `Edit rpk defaults.

This command opens your default editor to edit the specified profile, or
the current profile if no profile is specified.
`,
		Args: cobra.ExactArgs(0),
		Run: func(*cobra.Command, []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			y, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "unable to load config: %v", err)

			y.Defaults, err = rpkos.EditTmpYAMLFile(fs, y.Defaults)
			out.MaybeDieErr(err)

			err = y.Write(fs)
			out.MaybeDie(err, "unable to write rpk.yaml: %v", err)
			fmt.Println("Defaults updated successfully.")
		},
	}
	return cmd
}
