package memory

import (
	"strconv"
	"strings"
	"vectorized/pkg/checkers"

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
