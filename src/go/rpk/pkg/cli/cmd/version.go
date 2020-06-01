package cmd

import (
	"vectorized/pkg/cli"
	"vectorized/pkg/cli/cmd/version"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func NewVersionCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "version",
		Short:        "Check the current version",
		Long:         "",
		SilenceUsage: true,
		Run: func(_ *cobra.Command, _ []string) {
			log.SetFormatter(cli.NewNoopFormatter())
			log.Info(version.Pretty())
		},
	}
	return command
}
