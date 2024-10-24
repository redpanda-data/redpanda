// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package redpanda

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
	"go.uber.org/zap"
)

var redpandaInstallDirContent = []string{
	"bin/rpk",
	"bin/redpanda",
}

func GetIOConfigPath(configFileDirectory string) string {
	return filepath.Join(configFileDirectory, "io-config.yaml")
}

func FindInstallDir(fs afero.Fs) (string, error) {
	zap.L().Sugar().Debugf("Looking for redpanda install directory")
	execPath, err := os.Executable()
	if err != nil {
		return "", err
	}
	installDirCandidate := filepath.Dir(filepath.Dir(execPath))
	for _, path := range redpandaInstallDirContent {
		installDirPath := filepath.Join(installDirCandidate, path)
		zap.L().Sugar().Debugf("Checking if path '%s' exists", installDirPath)
		if exists, _ := afero.Exists(fs, installDirPath); !exists {
			return "", fmt.Errorf("Directory '%s' does not contain '%s'",
				installDirCandidate, path)
		}
	}
	zap.L().Sugar().Debugf("Redpanda is installed in '%s'", installDirCandidate)
	return installDirCandidate, nil
}
