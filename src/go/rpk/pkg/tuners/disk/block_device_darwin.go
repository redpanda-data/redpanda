package disk

import (
	"errors"

	"github.com/spf13/afero"
)

func NewDevice(_, _ int, _ afero.Fs) (BlockDevice, error) {
	return nil, errors.New("NewDevice not available in MacOS")
}
