package config

import (
	"errors"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func FindConfigFile(fs afero.Fs) (string, error) {
	log.Debugf("Looking for the redpanda config file")
	var configPathProviders = []func() ([]string, error){
		currentDirectory,
		sysConfDirectory,
		currentDirectoryParents,
	}

	for _, provider := range configPathProviders {
		paths, err := provider()
		if err != nil {
			return "", err
		}
		for _, path := range paths {
			candidate := filepath.Join(path, "redpanda.yaml")
			log.Debugf("Looking for redpanda config file in '%s'", path)
			if exists, _ := afero.Exists(fs, candidate); exists {
				return candidate, nil
			}
		}
	}
	return "", errors.New("Redpanda config not found")
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
