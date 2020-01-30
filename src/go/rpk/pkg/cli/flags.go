package cli

import (
	"fmt"
	"vectorized/pkg/redpanda"

	"github.com/spf13/afero"
)

func GetOrFindConfig(fs afero.Fs, configFile string) (string, error) {
	if configFile != "" {
		return configFile, nil
	}
	foundConfig, err := redpanda.FindConfig(fs)
	if err != nil {
		return "", fmt.Errorf("Unable to find redpanda config file" +
			" in default locations. Please provide file location " +
			" manually with --redpanda-cfg")
	}
	return foundConfig, nil
}

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
