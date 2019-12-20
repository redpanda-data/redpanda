package sys

import (
	"fmt"
	"strconv"
	"strings"
	"vectorized/pkg/checkers"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const maxAIOEvents = 1048576

func NewMaxAIOEventsChecker(fs afero.Fs) checkers.Checker {
	return checkers.NewEqualityChecker(
		"Max AIO Events",
		checkers.Warning,
		1024*1024,
		func() (interface{}, error) {
			content, err := afero.ReadFile(fs, "/proc/sys/fs/aio-max-nr")
			if err != nil {
				return 0, err
			}
			return strconv.Atoi(strings.TrimSpace(string(content)))
		},
	)
}

func NewMaxAIOEventsTuner(
	fs afero.Fs, executor executors.Executor,
) tuners.Tunable {
	return tuners.NewCheckedTunable(
		NewMaxAIOEventsChecker(fs),
		func() tuners.TuneResult {

			log.Debugf("Setting max AIO events to %d", maxAIOEvents)
			err := executor.Execute(
				commands.NewWriteFileCmd(
					fs, "/proc/sys/fs/aio-max-nr", fmt.Sprint(maxAIOEvents)))
			if err != nil {
				return tuners.NewTuneError(err)
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		executor.IsLazy(),
	)
}
