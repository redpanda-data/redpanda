package cmd

import (
	"vectorized/pkg/cli/cmd/container"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewContainerCommand(fs afero.Fs) *cobra.Command {
	command := &cobra.Command{
		Use:   "container",
		Short: "Manage a local container cluster",
	}

	command.AddCommand(container.Start(fs))
	command.AddCommand(container.Stop(fs))
	command.AddCommand(container.Purge(fs))

	return command
}
