package cmd

import (
	"fmt"
	"strings"
	"vectorized/pkg/config"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewModeCommand(fs afero.Fs) *cobra.Command {
	var configFile string
	command := &cobra.Command{
		Use:   "mode <mode>",
		Short: "Enable a default configuration mode",
		Long:  "",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("requires a mode [%s]", strings.Join(config.AvailableModes(), ", "))
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
	config.CheckAndPrintNotice(conf.LicenseKey)
	conf, err = config.SetMode(mode, conf)
	if err != nil {
		return err
	}
	log.Infof("Writing '%s' mode defaults to '%s'", mode, configFile)
	return config.WriteConfig(fs, conf, configFile)
}
