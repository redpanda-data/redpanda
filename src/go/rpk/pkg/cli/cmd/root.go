package cmd

import (
	"os"
	"vectorized/pkg/cli"

	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"
)

func Execute() {
	verbose := false
	fs := afero.NewOsFs()

	if !terminal.IsTerminal(int(os.Stdout.Fd())) {
		color.NoColor = true
	}
	log.SetFormatter(cli.NewRpkLogFormatter())
	cobra.OnInitialize(func() {
		// This is only executed when a subcommand (e.g. rpk check) is
		// specified.
		if verbose {
			log.SetLevel(log.DebugLevel)
		} else {
			log.SetLevel(log.InfoLevel)
		}
	})

	rootCmd := &cobra.Command{
		Use:   "rpk",
		Short: "rpk is the Redpanda CLI & toolbox",
		Long:  "",
	}

	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose",
		"v", false, "enable verbose logging (default false)")
	rootCmd.AddCommand(NewTuneCommand(fs))
	rootCmd.AddCommand(NewSandboxCommand(fs))
	rootCmd.AddCommand(NewCheckCommand(fs))
	rootCmd.AddCommand(NewIoTuneCmd(fs))
	rootCmd.AddCommand(NewStartCommand(fs))
	rootCmd.AddCommand(NewModeCommand(fs))
	rootCmd.AddCommand(NewConfigCommand(fs))
	rootCmd.AddCommand(NewStatusCommand(fs))

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
