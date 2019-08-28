package sys

import (
	"fmt"
	"strings"
	"vectorized/pkg/checkers"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"

	"github.com/spf13/afero"
)

const prefferedClkSource = "tsc"

func NewClockSourceChecker(fs afero.Fs) checkers.Checker {
	return checkers.NewEqualityChecker(
		"Clock Source",
		checkers.Warning,
		prefferedClkSource,
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

func NewClockSourceTuner(
	fs afero.Fs, executor executors.Executor,
) tuners.Tunable {
	return tuners.NewCheckedTunable(
		NewClockSourceChecker(fs),
		func() tuners.TuneResult {
			err := executor.Execute(commands.NewWriteFileCmd(fs,
				"/sys/devices/system/clocksource/clocksource0/current_clocksource",
				prefferedClkSource))
			if err != nil {
				return tuners.NewTuneError(err)
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			content, err := afero.ReadFile(fs,
				"/sys/devices/system/clocksource/clocksource0/available_clocksource")
			if err != nil {
				return false, err.Error()
			}
			availableSrcs := strings.Fields(string(content))

			for _, src := range availableSrcs {
				if src == prefferedClkSource {
					return true, ""
				}
			}
			return false, fmt.Sprintf(
				"Preffered clocksource '%s' not avaialable", prefferedClkSource)
		},
		executor.IsLazy(),
	)
}
