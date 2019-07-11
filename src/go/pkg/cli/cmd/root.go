package cmd

import (
	"os"

	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type CobraRoot struct {
	*cobra.Command
	cfgFile string
	verbose bool
}

// cobraRoot represents the base command when called without any subcommands
var cobraRoot = &CobraRoot{
	Command: &cobra.Command{
		Use:   "rpk",
		Short: "This is a Red Panda queue Swiss Army Knife CLI",
		Long:  "",
	},
}

func Execute() {
	if err := cobraRoot.Execute(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(cobraRoot.initConfig)

	cobraRoot.PersistentFlags().StringVar(&cobraRoot.cfgFile, "config",
		"", "config file (default is $HOME/.rpk.yaml)")
	cobraRoot.PersistentFlags().BoolVarP(&cobraRoot.verbose, "verbose",
		"v", false, "enable verbose logging (default false)")

	cobraRoot.AddCommand(NewTuneCommand())
}

// initConfig reads in config file and ENV variables if set.
func (root *CobraRoot) initConfig() {
	if root.cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(root.cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			log.Println(err)
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
		log.Println("Using config file:", viper.ConfigFileUsed())
	}

	if root.verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
}
