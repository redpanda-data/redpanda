package redpanda

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

var redpandaInstallDirContent = []string{
	"bin/rpk",
	"bin/redpanda",
	"lib",
	"libexec/rpk",
	"libexec/rpk.bin",
	"libexec/redpanda",
	"libexec/redpanda.bin",
}

type pathProvider func() ([]string, error)

var configPathProviders = []pathProvider{
	currentDirectory,
	sysConfDirectory,
	currentDirectoryParents,
}

func GetIOConfigPath(configFileDirectory string) string {
	return filepath.Join(configFileDirectory, "io-config.yaml")
}

func FindConfig(fs afero.Fs) (string, error) {
	log.Debugf("Looking for redpanda config file")

	for _, provider := range configPathProviders {
		paths, err := provider()
		if err != nil {
			return "", err
		}
		for _, path := range paths {
			candidate := filepath.Join(path, "redpanda.yaml")
			log.Debugf("Looking for redpanda config file in '%s'", path)
			if utils.FileExists(fs, candidate) {
				return candidate, nil
			}
		}
	}
	return "", errors.New("Redpanda config not found")
}

func FindInstallDir(fs afero.Fs) (string, error) {
	log.Debugf("Looking for redpanda install directory")
	execPath, err := os.Executable()
	if err != nil {
		return "", err
	}
	installDirCandidate := filepath.Dir(filepath.Dir(execPath))
	for _, path := range redpandaInstallDirContent {
		installDirPath := filepath.Join(installDirCandidate, path)
		log.Debugf("Checking if path '%s' exists", installDirPath)
		if !utils.FileExists(fs, installDirPath) {
			return "", fmt.Errorf("Directory '%s' does not contain '%s'",
				installDirCandidate, path)
		}
	}
	log.Debugf("Redpanda is installed in '%s'", installDirCandidate)
	return installDirCandidate, nil
}

func sysConfDirectory() ([]string, error) {
	return []string{"/etc/redpanda"}, nil
}

func getCurrentDirectory() (string, error) {
	path, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return path, nil
}

func currentDirectory() ([]string, error) {
	currentDir, err := getCurrentDirectory()
	if err != nil {
		return nil, err
	}
	return []string{currentDir}, nil
}

func currentDirectoryParents() ([]string, error) {
	currentDir, err := getCurrentDirectory()
	if err != nil {
		return nil, err
	}
	var result []string
	for dir := filepath.Dir(currentDir); dir != string(filepath.Separator); dir = filepath.Dir(dir) {
		result = append(result, dir)
	}
	return result, nil
}
