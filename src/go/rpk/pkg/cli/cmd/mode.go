package cmd

import (
	"fmt"
	"strings"
	"vectorized/pkg/config"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

var availableModes []string = []string{
	"dev",
	"prod",
	"development",
	"production",
}

func NewModeCommand(fs afero.Fs) *cobra.Command {
	var configFile string
	command := &cobra.Command{
		Use:   "mode <mode>",
		Short: "Enable a default configuration mode",
		Long:  "",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("requires a mode [%s]", strings.Join(availableModes, ", "))
			}
			mode := strings.ToLower(args[0])
			if !checkSupported(mode) {
				return fmt.Errorf(
					"%v is not a supported mode. Available modes: %s",
					mode,
					strings.Join(availableModes, ", "),
				)
			}
			return nil
		},
		RunE: func(_ *cobra.Command, args []string) error {
			// Safe to access args[0] because it was validated in Args
			return executeMode(fs, configFile, args[0])
		},
	}
	command.Flags().StringVar(
		&configFile,
		"config",
		config.DefaultConfig().ConfigFile,
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations",
	)
	return command
}

func executeMode(fs afero.Fs, configFile string, mode string) error {
	conf, err := config.ReadOrGenerate(fs, configFile)
	if err != nil {
		return err
	}
	switch mode {
	case "development", "dev":
		conf = setDevelopment(conf)
	case "production", "prod":
		conf = setProduction(conf)
	}
	log.Infof("Writing '%s' mode defaults to '%s'", mode, configFile)
	return config.WriteConfig(fs, conf, configFile)
}

func setDevelopment(conf *config.Config) *config.Config {
	// Defaults to setting all tuners to false
	conf.Rpk = &config.RpkConfig{CoredumpDir: conf.Rpk.CoredumpDir}
	return conf
}

func setProduction(conf *config.Config) *config.Config {
	rpk := conf.Rpk
	rpk.EnableUsageStats = true
	rpk.TuneNetwork = true
	rpk.TuneDiskScheduler = true
	rpk.TuneNomerges = true
	rpk.TuneDiskIrq = true
	rpk.TuneCpu = true
	rpk.TuneAioEvents = true
	rpk.TuneClocksource = true
	rpk.EnableMemoryLocking = true
	return conf
}

func checkSupported(mode string) bool {
	for _, m := range availableModes {
		if mode == m {
			return true
		}
	}
	return false
}
