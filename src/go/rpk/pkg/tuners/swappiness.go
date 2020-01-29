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

const (
	File               string = "/proc/sys/vm/swappiness"
	ExpectedSwappiness int    = 1
)

func NewSwappinessChecker(fs afero.Fs) Checker {
	return NewEqualityChecker(
		Swappiness,
		"Swappiness",
		Warning,
		ExpectedSwappiness,
		func() (interface{}, error) {
			content, err := afero.ReadFile(fs, File)
			if err != nil {
				return -1, err
			}
			return strconv.Atoi(strings.TrimSpace(string(content)))
		},
	)
}

func NewSwappinessTuner(fs afero.Fs, executor executors.Executor) Tunable {
	return NewCheckedTunable(
		NewSwappinessChecker(fs),
		func() TuneResult {
			log.Debugf("Setting swappiness to %d", ExpectedSwappiness)
			err := executor.Execute(
				commands.NewWriteFileCmd(
					fs, File, fmt.Sprint(ExpectedSwappiness)))
			if err != nil {
				log.Errorf("got an error while writing %d to %s: %v", ExpectedSwappiness, File, err)
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
