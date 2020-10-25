package disk

import (
	"errors"

	"github.com/spf13/afero"
)

func NewDevice(_ uint64, _ afero.Fs) (BlockDevice, error) {
	return nil, errors.New("NewDevice not available in MacOS")
}
