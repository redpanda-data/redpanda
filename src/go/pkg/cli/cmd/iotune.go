package cmd

import (
	"math"
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
		timeoutMs      int
	)
	command := &cobra.Command{
		Use:   "iotune",
		Short: "Measure filesystem performance and create IO configuration file",
		RunE: func(ccmd *cobra.Command, args []string) error {
			totalTimeout := (time.Duration(duration) * time.Second) + (time.Duration(timeoutMs) * time.Millisecond)
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

			return execIoTune(fs, evalDirectories, ioConfigFile, duration, totalTimeout)
		},
	}
	command.Flags().StringVar(&configFileFlag,
		"redpanda-cfg", "", "Redpanda config file, if not set the file "+
			"will be searched for in default locations")
	command.Flags().StringSliceVar(&directories,
		"directories", nil, "List of directories to evaluate")
	command.Flags().IntVar(&duration,
		"duration", 30, "Duration of tests in seconds")
	command.Flags().IntVar(
		&timeoutMs,
		"timeout",
		math.MaxInt64,
		"The maximum amount of time (in ms) after --duration to wait for iotune to complete",
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
