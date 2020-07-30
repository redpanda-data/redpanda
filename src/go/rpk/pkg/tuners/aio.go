package tuners

import (
	"fmt"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const maxAIOEvents = 1048576
const maxAIOEventsFile = "/proc/sys/fs/aio-max-nr"

func NewMaxAIOEventsChecker(fs afero.Fs) Checker {
	return NewIntChecker(
		MaxAIOEvents,
		"Max AIO Events",
		Warning,
		func(current int) bool {
			return current >= maxAIOEvents
		},
		func() string {
			return fmt.Sprintf(">= %d", maxAIOEvents)
		},
		func() (int, error) {
			return utils.ReadIntFromFile(fs, maxAIOEventsFile)
		},
	)
}

func NewMaxAIOEventsTuner(fs afero.Fs, executor executors.Executor) Tunable {
	return NewCheckedTunable(
		NewMaxAIOEventsChecker(fs),
		func() TuneResult {
			log.Debugf("Setting max AIO events to %d", maxAIOEvents)
			err := executor.Execute(
				commands.NewWriteFileCmd(
					fs,
					maxAIOEventsFile,
					fmt.Sprint(maxAIOEvents),
				),
			)
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		executor.IsLazy(),
	)
}
