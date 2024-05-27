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
	"time"

	"github.com/fatih/color"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCheckCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "check",
		Short: "Check if system meets redpanda requirements",
		Args:  cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			y, err := p.LoadVirtualRedpandaYaml(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			err = executeCheck(fs, y, timeout)
			out.MaybeDie(err, "unable to check: %v", err)
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Second, "The maximum amount of time to wait for the checks and tune process to complete (e.g. 300ms, 1.5s, 2h45m)")
	return cmd
}

func executeCheck(
	fs afero.Fs, y *config.RedpandaYaml, timeout time.Duration,
) error {
	results, err := tuners.Check(fs, y, timeout)
	if err != nil {
		return err
	}
	tw := out.NewTable(
		"Condition",
		"Required",
		"Current",
		"Severity",
		"Passed",
	)
	defer tw.Flush()

	for _, r := range results {
		tw.PrintStrings(
			r.Desc,
			r.Required,
			r.Current,
			fmt.Sprint(r.Severity),
			fmt.Sprint(printResult(r.Severity, r.IsOk)),
		)
	}
	fmt.Printf("\nSystem check results\n")
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
