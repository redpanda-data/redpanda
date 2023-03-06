// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package redpanda

import (
	"fmt"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/ui"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCheckCommand(fs afero.Fs) *cobra.Command {
	var (
		configFile string
		timeout    time.Duration
	)
	command := &cobra.Command{
		Use:   "check",
		Short: "Check if system meets redpanda requirements",
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			err = executeCheck(fs, cfg, timeout)
			out.MaybeDie(err, "unable to check: %v", err)
		},
	}
	command.Flags().StringVar(
		&configFile,
		"config",
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in $PWD or /etc/redpanda/redpanda.yaml.",
	)
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		2000*time.Millisecond,
		"The maximum amount of time to wait for the checks and tune processes to complete. "+
			"The value passed is a sequence of decimal numbers, each with optional "+
			"fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. "+
			"Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'",
	)
	return command
}

func appendToTable(t *tablewriter.Table, r tuners.CheckResult) {
	t.Append([]string{
		r.Desc,
		r.Required,
		r.Current,
		fmt.Sprint(r.Severity),
		fmt.Sprint(printResult(r.Severity, r.IsOk)),
	})
}

func executeCheck(
	fs afero.Fs, cfg *config.Config, timeout time.Duration,
) error {
	results, err := tuners.Check(fs, cfg, timeout)
	if err != nil {
		return err
	}
	table := ui.NewRpkTable(os.Stdout)
	table.SetHeader([]string{
		"Condition",
		"Required",
		"Current",
		"Severity",
		"Passed",
	})

	for _, res := range results {
		appendToTable(table, res)
	}
	fmt.Printf("\nSystem check results\n")
	table.Render()
	return nil
}

func printResult(sev tuners.Severity, isOk bool) string {
	if isOk {
		return color.GreenString("%v", isOk)
	}
	switch sev {
	case tuners.Fatal:
		return color.RedString("%v", isOk)
	case tuners.Warning:
		return color.YellowString("%v", isOk)
	}

	return fmt.Sprint(isOk)
}
