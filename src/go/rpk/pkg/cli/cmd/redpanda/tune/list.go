// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package tune

import (
	"sort"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/factory"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type tunerInfo struct {
	Name      string
	Enabled   bool
	Supported bool
	Reason    string
}

func newListCommand(fs afero.Fs) *cobra.Command {
	tunerParams := factory.TunerParams{}
	var cfgFile string

	command := &cobra.Command{
		Use:   "list",
		Short: "List available tuners",
		Long: `List available redpanda tuners and check if they are enabled and 
supported by your system

To enable a tuner it must be set in the redpanda.yaml configuration file
under rpk section, e.g:

  rpk:
      tune_cpu: true
      tune_swappiness: true

You may use 'rpk redpanda config set' to enable or disable a tuner.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			// Using cpu mask and timeout defaults since we are not executing
			// any tuner.
			tunerParams.CPUMask = "all"
			tunerFactory := factory.NewDirectExecutorTunersFactory(fs, *cfg, 10000*time.Millisecond)

			params, err := factory.MergeTunerParamsConfig(&tunerParams, cfg)
			out.MaybeDieErr(err)

			var list []tunerInfo

			for _, name := range factory.AvailableTuners() {
				tuner := tunerFactory.CreateTuner(name, params)
				enabled := factory.IsTunerEnabled(name, cfg.Rpk)
				supported, reason := tuner.CheckIfSupported()
				list = append(list, tunerInfo{name, enabled, supported, reason})
			}
			printTunerList(list)
		},
	}
	addTunerParamsFlags(command, &tunerParams)
	command.Flags().StringVar(
		&cfgFile,
		config.FlagConfig,
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in $PWD or /etc/redpanda/redpanda.yaml.",
	)
	return command
}

func printTunerList(list []tunerInfo) {
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})
	headers := []string{
		"Tuner",
		"Enabled",
		"Supported",
		"Unsupported-Reason",
	}
	table := out.NewTable(headers...)
	defer table.Flush()
	for _, tuner := range list {
		table.PrintStructFields(tuner)
	}
}
