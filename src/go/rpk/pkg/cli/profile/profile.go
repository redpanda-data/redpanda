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
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage rpk profiles",
		Long: `Manage rpk profiles.

An rpk profile talks to a single Redpanda cluster. You can create multiple
profiles for multiple clusters and swap between them with 'rpk profile use'.
Multiple profiles may be useful if, for example, you use rpk to talk to
a localhost cluster, a dev cluster, and a prod cluster, and you want to keep
your configuration in one place.
`,
	}

	cmd.AddCommand(
		newCreateCommand(fs, p),
		newClearCommand(fs, p),
		newCurrentCommand(fs, p),
		newDeleteCommand(fs, p),
		newEditCommand(fs, p),
		newEditGlobalsCommand(fs, p),
		newListCommand(fs, p),
		newPrintCommand(fs, p),
		newPrintGlobalsCommand(fs, p),
		newPromptCommand(fs, p),
		newRenameToCommand(fs, p),
		newSetCommand(fs, p),
		newSetGlobalsCommand(fs, p),
		newUseCommand(fs, p),
	)

	return cmd
}

// ValidProfiles is a cobra.ValidArgsFunction that returns the names of
// existing profiles.
func ValidProfiles(fs afero.Fs, p *config.Params) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		cfg, err := p.Load(fs)
		if err != nil {
			return nil, cobra.ShellCompDirectiveError
		}
		y, ok := cfg.ActualRpkYaml()
		if !ok {
			return nil, cobra.ShellCompDirectiveDefault
		}
		var names []string
		for i := range y.Profiles {
			p := &y.Profiles[i]
			if strings.HasPrefix(p.Name, toComplete) {
				names = append(names, p.Name)
			}
		}
		return names, cobra.ShellCompDirectiveDefault
	}
}

/////////////////////
// Cloud profile helpers
/////////////////////

// ErrNoCloudClusters is returned from from CreateFlow or
// PromptCloudClusterProfile if there are no cloud clusters available.
var ErrNoCloudClusters = errors.New("no cloud clusters available")

// RpkCloudProfileName is the default profile name used when a user creates a
// cloud cluster profile with no name.
const RpkCloudProfileName = "rpk-cloud"

// ProfileExistsError is returned from CreateFlow if trying to create a profile
// that already exists.
type ProfileExistsError struct {
	Name string
}

func (e *ProfileExistsError) Error() string {
	return fmt.Sprintf("profile %q already exists", e.Name)
}

// MaybeDieExistingName exits if err is non-nil, but if err is
// ProfileExistsError, this exits with a more detailed message.
func MaybeDieExistingName(err error) {
	if ee := (*ProfileExistsError)(nil); errors.As(err, &ee) {
		fmt.Printf(`Unable to automatically create profile %[1]q due to a name conflict with
an existing profile, please rename the existing profile.
`, ee.Name)
		os.Exit(1)
	}
	out.MaybeDieErr(err)
}
