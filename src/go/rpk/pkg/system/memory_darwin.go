package system

import (
	"errors"

	"github.com/spf13/afero"
)

func getMemInfo(_ afero.Fs) (*MemInfo, error) {
	return nil, errors.New("Memory info collection not available in MacOS")
}
