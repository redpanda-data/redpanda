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
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
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

	// If the directory does not exist, create it. We do not preserve perms
	// if not-exist because there are no perms to preserve.
	if err := fs.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		return err
	}

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

// EditTmpFile writes f to a temporary file, launches the user's editor, and
// returns the contents of the file (and deletes the file) after the editor is
// closed.
func EditTmpFile(fs afero.Fs, f []byte) ([]byte, error) {
	file, err := afero.TempFile(fs, "/tmp", "rpk_*.yaml")
	filename := file.Name()
	defer func() {
		if err := fs.Remove(filename); err != nil {
			fmt.Fprintf(os.Stderr, "unable to remove temporary file %q\n", filename)
		}
	}()
	if err != nil {
		return nil, fmt.Errorf("unable to create temporary file %q: %w", filename, err)
	}
	if _, err = file.Write(f); err != nil {
		return nil, fmt.Errorf("failed to write out temporary file %q: %w", filename, err)
	}
	if err = file.Close(); err != nil {
		return nil, fmt.Errorf("error closing temporary file %q: %w", filename, err)
	}

	// Launch editor
	editor := os.Getenv("EDITOR")
	if editor == "" {
		if runtime.GOARCH == "windows" {
			editor = "notepad.exe"
		} else {
			const fallbackEditor = "/usr/bin/nano"
			if _, err := os.Stat(fallbackEditor); err != nil {
				return nil, errors.New("please set $EDITOR to use this command")
			} else {
				editor = fallbackEditor
			}
		}
	}

	child := exec.Command(editor, filename)
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	child.Stdin = os.Stdin
	err = child.Run()
	if err != nil {
		return nil, fmt.Errorf("error running editor: %w", err)
	}

	read, err := afero.ReadFile(fs, filename)
	if err != nil {
		return nil, fmt.Errorf("error reading temporary file %q: %w", filename, err)
	}
	return read, err
}

// EditTmpYAMLFile is the same as EditTmpFile, but yaml.Marshal's v
// before writing and decodes into and returns a new T.
func EditTmpYAMLFile[T any](fs afero.Fs, v T) (T, error) {
	var update T
	f, err := yaml.Marshal(v)
	if err != nil {
		return update, fmt.Errorf("unable to encode: %w", err)
	}
	read, err := EditTmpFile(fs, f)
	if err != nil {
		return update, fmt.Errorf("unable to edit: %w", err)
	}
	if len(read) == 0 || string(read) == "\n" {
		return update, fmt.Errorf("no changes made")
	}
	if err := yaml.Unmarshal(read, &update); err != nil {
		return update, fmt.Errorf("unable to parse edited file: %w", err)
	}
	return update, nil
}
