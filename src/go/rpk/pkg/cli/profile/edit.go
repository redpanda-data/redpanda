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
	return &cobra.Command{
		Use:   "edit [NAME]",
		Short: "Edit an rpk profile",
		Long: `Edit an rpk profile.

This command opens your default editor to edit the specified profile, or
the current profile if no profile is specified. If the profile does not
exist, this command creates it and switches to it.
`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: ValidProfiles(fs, p),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			y, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			if len(args) == 0 {
				args = append(args, y.CurrentProfile)
			}
			name := args[0]
			p := y.Profile(name)
			if p == nil {
				priorAuth, currentAuth := y.PushProfile(config.RpkProfile{Name: name})
				// Defer, so that if we out.Die, we don't print the switch message
				// (and we want to print this last anyway).
				defer config.MaybePrintAuthSwitchMessage(priorAuth, currentAuth)
				p = y.Profile(name)
			}

			preFromCloud := p.FromCloud
			preCloudDetails := p.CloudCluster
			update, err := rpkos.EditTmpYAMLFile(fs, *p)
			out.MaybeDieErr(err)

			if preFromCloud {
				if !update.FromCloud || preCloudDetails != update.CloudCluster {
					out.Die("cannot change a cloud profile to a non-cloud profile, and cannot change cloud cluster details; please create and edit a new profile")
				}
			}

			// If a user clears the name by accident, we keep the old name.
			if update.Name == "" {
				update.Name = name
			}

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
}
