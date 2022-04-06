// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func FindConfigFile(fs afero.Fs) (string, error) {
	log.Debugf("Looking for the redpanda config file")
	var configPathProviders = []func() ([]string, error){
		currentDirectory,
		sysConfDirectory,
		homeDirectory,
	}

	lookedUpPaths := []string{}
	for _, provider := range configPathProviders {
		paths, err := provider()
		if err != nil {
			return "", err
		}
		for _, path := range paths {
			candidate := filepath.Join(path, "redpanda.yaml")
			lookedUpPaths = append(lookedUpPaths, candidate)
			log.Debugf("Looking for redpanda config file in '%s'", path)
			if exists, _ := afero.Exists(fs, candidate); exists {
				return candidate, nil
			}
		}
	}
	// os.PathError can be checked with os.IsNotExist.
	return "", &os.PathError{
		Op:   "Open",
		Path: strings.Join(lookedUpPaths, ", "),
		Err:  os.ErrNotExist,
	}
}

func sysConfDirectory() ([]string, error) {
	return []string{"/etc/redpanda"}, nil
}

func currentDirectory() ([]string, error) {
	currentDir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	return []string{currentDir}, nil
}

func homeDirectory() ([]string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	return []string{homeDir}, nil
}
