// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build !windows

package cmd

import (
	"path/filepath"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewIoTuneCmd(fs afero.Fs) *cobra.Command {
	var (
		configFile  string
		outputFile  string
		duration    time.Duration
		directories []string
		timeout     time.Duration
	)
	command := &cobra.Command{
		Use:   "iotune",
		Short: "Measure filesystem performance and create IO configuration file.",
		Run: func(cmd *cobra.Command, args []string) {
			timeout += duration
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			var evalDirectories []string
			if len(directories) != 0 {
				log.Infof("Overriding evaluation directories with '%v'",
					directories)
				evalDirectories = directories
			} else {
				evalDirectories = []string{cfg.Redpanda.Directory}
			}

			err = execIoTune(fs, evalDirectories, outputFile, duration, timeout)
			out.MaybeDie(err, "error during IoTune execution: %v", err)
		},
	}
	command.Flags().StringVar(
		&configFile,
		"config",
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations.",
	)
	command.Flags().StringVar(
		&outputFile,
		"out",
		filepath.Join(filepath.Dir(config.Default().ConfigFile), "io-config.yaml"),
		"The file path where the IO config will be written",
	)
	command.Flags().StringSliceVar(&directories,
		"directories", []string{}, "List of directories to evaluate")
	command.Flags().DurationVar(
		&duration,
		"duration",
		10*time.Minute,
		"Duration of tests."+
			"The value passed is a sequence of decimal numbers, each with optional "+
			"fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. "+
			"Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'",
	)
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
	duration, timeout time.Duration,
) error {
	tuner := tuners.NewIoTuneTuner(
		fs,
		directories,
		ioConfigFile,
		duration,
		timeout,
	)
	log.Info("Starting iotune...")
	result := tuner.Tune()
	if err := result.Error(); err != nil {
		return err
	}
	log.Infof("IO configuration file stored as '%s'", ioConfigFile)
	return nil
}
