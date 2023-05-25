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
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newEditCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "edit [NAME]",
		Short: "Edit an rpk auth",
		Long: `Edit an rpk auth.

This command opens your default editor to edit the specified cloud auth, or the
current cloud auth if no cloud auth is specified.
`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: validAuths(fs, p),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			y, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDieErr(err)
			if len(args) == 0 {
				args = append(args, y.CurrentCloudAuth)
			}
			name := args[0]
			a := y.Auth(name)
			if a == nil {
				out.Die("auth %s does not exist", name)
				return
			}

			update, err := rpkos.EditTmpYAMLFile(fs, *a)
			out.MaybeDieErr(err)

			var renamed, updatedCurrent bool
			if update.Name != name {
				renamed = true
				if y.CurrentCloudAuth == name {
					updatedCurrent = true
					y.CurrentCloudAuth = update.Name
				}
			}
			*a = update

			err = y.Write(fs)
			out.MaybeDie(err, "unable to write rpk.yaml: %v", err)

			if renamed {
				fmt.Printf("Cloud auth %q updated successfully and renamed to %q.\n", name, a.Name)
				if updatedCurrent {
					fmt.Printf("Current cloud auth has been updated to %q.\n", a.Name)
				}
			} else {
				fmt.Printf("Cloud auth %q updated successfully.\n", name)
			}
		},
	}
}
