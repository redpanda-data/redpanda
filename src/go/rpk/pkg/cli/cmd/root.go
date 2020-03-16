package cmd

import (
	"os"
	"vectorized/pkg/cli"

	"github.com/fatih/color"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/crypto/ssh/terminal"
)

func Execute() {
	cfgFile := ""
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
		initConfig(fs, cfgFile)
	})

	rootCmd := &cobra.Command{
		Use:   "rpk",
		Short: "rpk is the Redpanda CLI & toolbox",
		Long:  "",
	}

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config",
		"", "config file (default is $HOME/.rpk.yaml)")
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

// initConfig reads in config file and ENV variables if set.
func initConfig(fs afero.Fs, configFile string) {
	if configFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(configFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".rpk" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".rpk")
	}

	viper.AutomaticEnv() // read in environment variables that match
	viper.SetEnvPrefix("RP")
	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", viper.ConfigFileUsed())
	}
}
