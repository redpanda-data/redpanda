package memory

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

const (
	File       string = "/proc/sys/vm/swappiness"
	Swappiness int    = 1
)

func NewSwappinessChecker(fs afero.Fs) checkers.Checker {
	return checkers.NewEqualityChecker(
		"Swappiness",
		checkers.Warning,
		Swappiness,
		func() (interface{}, error) {
			content, err := afero.ReadFile(fs, File)
			if err != nil {
				return -1, err
			}
			return strconv.Atoi(strings.TrimSpace(string(content)))
		},
	)
}

func NewSwappinessTuner(
	fs afero.Fs, executor executors.Executor,
) tuners.Tunable {
	return tuners.NewCheckedTunable(
		NewSwappinessChecker(fs),
		func() tuners.TuneResult {

			log.Debugf("Setting swappiness to %d", Swappiness)
			err := executor.Execute(
				commands.NewWriteFileCmd(
					fs, File, fmt.Sprint(Swappiness)))
			if err != nil {
				log.Errorf("got an error while writing %d to %s: %v", Swappiness, File, err)
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
