package cmd

import (
	"path/filepath"
	"vectorized/cli"
	"vectorized/redpanda"
	"vectorized/tuners/iotune"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewIoTuneCmd(fs afero.Fs) *cobra.Command {
	var configFileFlag string
	var duration int
	var directories []string
	command := &cobra.Command{
		Use:   "iotune",
		Short: "Measure filesystem performance and create IO configuration file",
		RunE: func(ccmd *cobra.Command, args []string) error {
			configFile, err := cli.GetOrFindConfig(fs, configFileFlag)
			if err != nil {
				return err
			}
			ioConfigFile := redpanda.GetIOConfigPath(filepath.Dir(configFile))
			config, err := redpanda.ReadConfigFromPath(fs, configFile)
			if err != nil {
				return err
			}
			var evalDirectories []string
			if evalDirectories != nil {
				log.Infof("Overriding evaluation directories with '%v'",
					directories)
				evalDirectories = directories
			} else {
				evalDirectories = []string{config.Directory}
			}

			return execIoTune(fs, evalDirectories, ioConfigFile, duration)
		},
	}

	command.Flags().StringVar(&configFileFlag,
		"redpanda-cfg", "", "Redpanda config file, if not set the file "+
			"will be searched for in default locations")
	command.Flags().StringSliceVar(&directories,
		"directories", nil, "List of directories to evaluate")
	command.Flags().IntVar(&duration,
		"duration", 30, "Duration of tests in seconds")
	return command
}

func execIoTune(
	fs afero.Fs, directories []string, ioConfigFile string, duration int,
) error {
	tuner := iotune.NewIoTuneTuner(fs, directories, ioConfigFile, duration)
	log.Info("Starting iotune...")
	result := tuner.Tune()
	if err := result.GetError(); err != nil {
		return err
	}
	log.Infof("IO configuration file stored '%s'", ioConfigFile)
	return nil
}
