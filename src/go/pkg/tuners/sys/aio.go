package sys

import (
	"fmt"
	"strconv"
	"strings"
	"vectorized/pkg/checkers"
	"vectorized/pkg/tuners"

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

func NewMaxAIOEventsTuner(fs afero.Fs) tuners.Tunable {
	return tuners.NewCheckedTunable(
		NewMaxAIOEventsChecker(fs),
		func() tuners.TuneResult {

			log.Debugf("Setting max AIO events to %d", maxAIOEvents)
			afero.WriteFile(fs, "/proc/sys/fs/aio-max-nr", []byte(fmt.Sprint(maxAIOEvents)), 0644)
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}
