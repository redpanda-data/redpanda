package tuners

import (
	"fmt"
	"strconv"
	"strings"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const maxAIOEvents = 1048576

func NewMaxAIOEventsChecker(fs afero.Fs) Checker {
	return NewEqualityChecker(
		MaxAIOEvents,
		"Max AIO Events",
		Warning,
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

func NewMaxAIOEventsTuner(fs afero.Fs, executor executors.Executor) Tunable {
	return NewCheckedTunable(
		NewMaxAIOEventsChecker(fs),
		func() TuneResult {

			log.Debugf("Setting max AIO events to %d", maxAIOEvents)
			err := executor.Execute(
				commands.NewWriteFileCmd(
					fs, "/proc/sys/fs/aio-max-nr", fmt.Sprint(maxAIOEvents)))
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
