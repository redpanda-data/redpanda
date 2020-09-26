package cli

import (
	"fmt"
	"vectorized/pkg/redpanda"

	"github.com/spf13/afero"
)

func GetOrFindInstallDir(fs afero.Fs, installDir string) (string, error) {
	if installDir != "" {
		return installDir, nil
	}
	foundConfig, err := redpanda.FindInstallDir(fs)
	if err != nil {
		return "", fmt.Errorf("Unable to find redpanda installation. " +
			"Please provide the install directory with flag --install-dir")
	}
	return foundConfig, nil
}
