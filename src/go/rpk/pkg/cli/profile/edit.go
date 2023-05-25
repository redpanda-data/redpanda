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

func newEditCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var raw bool
	cmd := &cobra.Command{
		Use:   "edit [NAME]",
		Short: "Edit an rpk profile",
		Long: `Edit an rpk profile.

This command opens your default editor to edit the specified profile, or
the current profile if no profile is specified.

If you are editing the current profile, rpk also populates what you edit
with internal defaults, user specified flags, and environment variables.
If you want to edit the current raw profile as it exists in rpk.yaml, you
can use the --raw flag.
`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: validProfiles(fs, p),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			y := cfg.VirtualRpkYaml()
			if raw {
				var ok bool
				y, ok = cfg.ActualRpkYaml()
				if !ok {
					out.Die("rpk.yaml file does not exist")
				}
			}

			if len(args) == 0 {
				args = append(args, y.CurrentProfile)
			}
			name := args[0]
			p := y.Profile(name)
			if p == nil {
				out.Die("profile %s does not exist", name)
				return
			}

			update, err := rpkos.EditTmpYAMLFile(fs, *p)
			out.MaybeDieErr(err)

			var renamed, updatedCurrent bool
			if update.Name != name {
				renamed = true
				if y.CurrentProfile == name {
					updatedCurrent = true
					y.CurrentProfile = update.Name
				}
			}
			*p = update

			err = y.Write(fs)
			out.MaybeDie(err, "unable to write rpk.yaml: %v", err)

			if renamed {
				fmt.Printf("Profile %q updated successfully and renamed to %q.\n", name, p.Name)
				if updatedCurrent {
					fmt.Printf("Current profile has been updated to %q.\n", p.Name)
				}
			} else {
				fmt.Printf("Profile %q updated successfully.\n", name)
			}
		},
	}

	cmd.Flags().BoolVar(&raw, "raw", false, "Edit profile directly as it exists in rpk.yaml without any environment variables nor flags applied")
	return cmd
}
