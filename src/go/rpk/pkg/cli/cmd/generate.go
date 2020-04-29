package cmd

import (
	"vectorized/pkg/cli/cmd/generate"

	"github.com/spf13/cobra"
)

func NewGenerateCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "generate [template]",
		Short: "Generate a configuration template for related services.",
	}
	command.AddCommand(generate.NewGrafanaDashboardCmd())
	return command
}
