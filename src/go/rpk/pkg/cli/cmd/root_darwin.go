package cmd

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// On MacOS this is a no-op.
func addPlatformDependentCmds(fs afero.Fs, cmd *cobra.Command) {}
