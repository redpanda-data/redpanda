package checkers

import (
	"vectorized/pkg/utils"

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
			return utils.FileExists(fs, filePath), nil
		})
}
