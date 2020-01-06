package checkers

import (
	"github.com/spf13/afero"
)

func NewFileExistanceChecker(
	fs afero.Fs, desc string, severity Severity, filePath string,
) Checker {
	return NewEqualityChecker(
		desc,
		severity,
		true,
		func() (interface{}, error) {
			return afero.Exists(fs, filePath)
		})
}
