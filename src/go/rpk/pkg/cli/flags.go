// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cli

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
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
