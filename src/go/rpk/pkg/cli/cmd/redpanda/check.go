// Copyright 2021 Vectorized, Inc.
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
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/ui"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/tuners"
)

func NewCheckCommand(fs afero.Fs, mgr config.Manager) *cobra.Command {
	var (
		configFile string
		timeout    time.Duration
	)
	command := &cobra.Command{
		Use:          "check",
		Short:        "Check if system meets redpanda requirements.",
		SilenceUsage: true,
		RunE: func(ccmd *cobra.Command, args []string) error {
			return executeCheck(fs, mgr, configFile, timeout)
		},
	}
	command.Flags().StringVar(
		&configFile,
		"config",
		"",
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations.",
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
	fs afero.Fs, mgr config.Manager, configFile string, timeout time.Duration,
) error {
	conf, err := mgr.FindOrGenerate(configFile)
	if err != nil {
		return err
	}
	results, err := tuners.Check(fs, conf, timeout)
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
