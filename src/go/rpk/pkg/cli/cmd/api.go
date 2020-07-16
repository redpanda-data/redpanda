package cmd

import (
	"vectorized/pkg/cli/cmd/api"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewApiCommand(fs afero.Fs) *cobra.Command {
	command := &cobra.Command{
		Use:   "api",
		Short: "Interact with the Redpanda API",
	}
	command.AddCommand(api.NewTopicCommand(fs))
	return command
}
