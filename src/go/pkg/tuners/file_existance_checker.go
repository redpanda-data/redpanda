package tuners

import (
	"github.com/spf13/afero"
)

func NewFileExistanceChecker(
	fs afero.Fs, id CheckerID, desc string, severity Severity, filePath string,
) Checker {
	return NewEqualityChecker(
		id,
		desc,
		severity,
		true,
		func() (interface{}, error) {
			return afero.Exists(fs, filePath)
		})
}
