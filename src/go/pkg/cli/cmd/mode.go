package cmd

import (
	"fmt"
	"strings"
	"vectorized/pkg/cli"
	"vectorized/pkg/redpanda"

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
	var redpandaConfigFile string
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
			return executeMode(fs, redpandaConfigFile, args[0])
		},
	}
	command.Flags().StringVar(
		&redpandaConfigFile,
		"redpanda-cfg",
		"",
		"Redpanda config file, if not set the file will be searched for in default locations",
	)
	return command
}

func executeMode(fs afero.Fs, redpandaConfigFile string, mode string) error {
	configFile, err := cli.GetOrFindConfig(fs, redpandaConfigFile)
	if err != nil {
		return err
	}
	config, err := redpanda.ReadConfigFromPath(fs, configFile)
	if err != nil {
		return err
	}
	switch mode {
	case "development", "dev":
		config = setDevelopment(config)
	case "production", "prod":
		config = setProduction(config)
	}
	log.Infof("Writing '%s' mode defaults to '%s'", mode, configFile)
	return redpanda.WriteConfig(fs, config, configFile)
}

func setDevelopment(config *redpanda.Config) *redpanda.Config {
	// Defaults to setting all tuners to false
	config.Rpk = &redpanda.RpkConfig{CoredumpDir: config.Rpk.CoredumpDir}
	return config
}

func setProduction(config *redpanda.Config) *redpanda.Config {
	rpk := config.Rpk
	rpk.TuneNetwork = true
	rpk.TuneDiskScheduler = true
	rpk.TuneNomerges = true
	rpk.TuneDiskIrq = true
	rpk.TuneCpu = true
	rpk.TuneAioEvents = true
	rpk.TuneClocksource = true
	rpk.EnableMemoryLocking = true
	rpk.TuneCoredump = true
	return config
}

func checkSupported(mode string) bool {
	for _, m := range availableModes {
		if mode == m {
			return true
		}
	}
	return false
}
