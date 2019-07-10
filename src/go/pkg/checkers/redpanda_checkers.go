package checkers

import (
	"vectorized/redpanda"
	"vectorized/system"

	"github.com/spf13/afero"
)

func NewConfigChecker(config *redpanda.Config) Checker {
	return NewEqualityChecker(
		"Config file valid",
		true,
		true,
		func() (interface{}, error) {
			return redpanda.CheckConfig(config), nil
		})
}

func NewDataDirWritableChecker(fs afero.Fs, path string) Checker {
	return NewEqualityChecker(
		"Data directory is writable",
		true,
		true,
		func() (interface{}, error) {
			return system.DirectoryIsWriteable(fs, path)
		})
}
