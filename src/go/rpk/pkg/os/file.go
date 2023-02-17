// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package os

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/afero"
)

// ReplaceFile either writes a new file with newPerms, or replaces an existing
// file and preserves the permissions of the original file.
func ReplaceFile(fs afero.Fs, filename string, contents []byte, newPerms os.FileMode) (rerr error) {
	exists, err := afero.Exists(fs, filename)
	if err != nil {
		return fmt.Errorf("unable to determine if file %q exists: %v", filename, err)
	}

	// Create a temp file first.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	bFilename := fmt.Sprintf("redpanda-%v", r.Int())
	temp := filepath.Join(filepath.Dir(filename), bFilename)

	err = afero.WriteFile(fs, temp, contents, newPerms)
	if err != nil {
		return fmt.Errorf("error writing to temporary file: %v", err)
	}
	defer func() {
		if rerr != nil {
			if removeErr := fs.Remove(temp); removeErr != nil {
				rerr = fmt.Errorf("%s, unable to remove temp file: %v", rerr, removeErr)
			} else {
				rerr = fmt.Errorf("%s, temp file removed from disk", rerr)
			}
		}
	}()

	// If we are replacing an existing file, we try to preserve the original
	// file ownership.
	if exists {
		stat, err := fs.Stat(filename)
		if err != nil {
			return fmt.Errorf("unable to stat existing file: %v", err)
		}

		err = fs.Chmod(temp, stat.Mode())
		if err != nil {
			return fmt.Errorf("unable to chmod temp config file: %v", err)
		}

		err = PreserveUnixOwnership(fs, stat, temp)
		if err != nil {
			return err
		}
	}

	err = fs.Rename(temp, filename)
	return err
}
