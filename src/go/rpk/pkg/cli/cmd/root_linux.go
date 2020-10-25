package cmd

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func addPlatformDependentCmds(fs afero.Fs, cmd *cobra.Command) {
	cmd.AddCommand(NewTuneCommand(fs))
	cmd.AddCommand(NewCheckCommand(fs))
	cmd.AddCommand(NewIoTuneCmd(fs))
	cmd.AddCommand(NewStartCommand(fs))
	cmd.AddCommand(NewStopCommand(fs))
}
