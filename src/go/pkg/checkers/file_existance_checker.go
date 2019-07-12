package checkers

import (
	"vectorized/utils"

	"github.com/spf13/afero"
)

func NewFileExistanceChecker(
	fs afero.Fs, desc string, isCritical bool, filePath string,
) Checker {
	return NewEqualityChecker(
		desc,
		isCritical,
		true,
		func() (interface{}, error) {
			return utils.FileExists(fs, filePath), nil
		})
}
