// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/spf13/afero"
)

const preferredClkSource = "tsc"

func NewClockSourceChecker(fs afero.Fs) Checker {
	return NewEqualityChecker(
		ClockSource,
		"Clock Source",
		Warning,
		preferredClkSource,
		func() (interface{}, error) {
			content, err := afero.ReadFile(fs,
				"/sys/devices/system/clocksource/clocksource0/current_clocksource")
			if err != nil {
				return "", err
			}
			return strings.TrimSpace(string(content)), nil
		},
	)
}

func NewClockSourceTuner(fs afero.Fs, executor executors.Executor) Tunable {
	return NewCheckedTunable(
		NewClockSourceChecker(fs),
		func() TuneResult {
			err := executor.Execute(commands.NewWriteFileCmd(fs,
				"/sys/devices/system/clocksource/clocksource0/current_clocksource",
				preferredClkSource))
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			// tsc clocksource is only available in x86 architectures.
			if runtime.GOARCH != "amd64" && runtime.GOARCH != "386" {
				return false, "Clocksource setting not available for this architecture"
			}
			content, err := afero.ReadFile(fs,
				"/sys/devices/system/clocksource/clocksource0/available_clocksource")
			if err != nil {
				return false, err.Error()
			}
			availableSrcs := strings.Fields(string(content))

			for _, src := range availableSrcs {
				if src == preferredClkSource {
					return true, ""
				}
			}
			return false, fmt.Sprintf(
				"Preferred clocksource '%s' not available", preferredClkSource)
		},
		executor.IsLazy(),
	)
}
