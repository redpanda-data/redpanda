package cmd

import (
	"path/filepath"
	"time"
	"vectorized/pkg/cli"
	"vectorized/pkg/redpanda"
	"vectorized/pkg/tuners/iotune"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewIoTuneCmd(fs afero.Fs) *cobra.Command {
	var (
		configFileFlag string
		duration       int
		directories    []string
		timeout        time.Duration
	)
	command := &cobra.Command{
		Use:   "iotune",
		Short: "Measure filesystem performance and create IO configuration file",
		RunE: func(ccmd *cobra.Command, args []string) error {
			timeout += time.Duration(duration) * time.Second
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
				evalDirectories = []string{config.Redpanda.Directory}
			}

			return execIoTune(fs, evalDirectories, ioConfigFile, duration, timeout)
		},
	}
	command.Flags().StringVar(&configFileFlag,
		"redpanda-cfg", "", "Redpanda config file, if not set the file "+
			"will be searched for in default locations")
	command.Flags().StringSliceVar(&directories,
		"directories", nil, "List of directories to evaluate")
	command.Flags().IntVar(&duration,
		"duration", 30, "Duration of tests in seconds")
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		1*time.Hour,
		"The maximum time after --duration to wait for iotune to complete. "+
			"The value passed is a sequence of decimal numbers, each with optional "+
			"fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. "+
			"Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'",
	)
	return command
}

func execIoTune(
	fs afero.Fs,
	directories []string,
	ioConfigFile string,
	duration int,
	timeout time.Duration,
) error {
	tuner := iotune.NewIoTuneTuner(fs, directories, ioConfigFile, duration, timeout)
	log.Info("Starting iotune...")
	result := tuner.Tune()
	if err := result.GetError(); err != nil {
		return err
	}
	log.Infof("IO configuration file stored as '%s'", ioConfigFile)
	return nil
}
