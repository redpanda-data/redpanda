// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build !windows

package redpanda

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/sys/unix"
)

type Launcher interface {
	Start(installDir string, args *RedpandaArgs) error
}

type launcher struct{}

type RedpandaArgs struct {
	ConfigFilePath string
	SeastarFlags   map[string]string
	ExtraArgs      []string
}

func NewLauncher() Launcher {
	return &launcher{}
}

func (*launcher) Start(installDir string, args *RedpandaArgs) error {
	binary, err := getBinary(installDir)
	if err != nil {
		return err
	}

	if args.ConfigFilePath == "" {
		return errors.New("Redpanda config file is required")
	}
	redpandaArgs := collectRedpandaArgs(args)

	var rpEnv []string
	ldLibraryPathPattern := regexp.MustCompile("^LD_LIBRARY_PATH=.*$")
	for _, ev := range os.Environ() {
		if !ldLibraryPathPattern.MatchString(ev) {
			rpEnv = append(rpEnv, ev)
		}
	}
	fmt.Printf("Running:\n%s %s\n", binary, strings.Join(redpandaArgs, " "))
	return unix.Exec(binary, redpandaArgs, rpEnv)
}

func getBinary(installDir string) (string, error) {
	path, err := exec.LookPath(filepath.Join(installDir, "bin", "redpanda"))
	if err != nil {
		return "", err
	}
	return path, nil
}

func collectRedpandaArgs(args *RedpandaArgs) []string {
	redpandaArgs := []string{
		"redpanda",
		"--redpanda-cfg",
		args.ConfigFilePath,
	}

	singleFlags := []string{"overprovisioned"}

	isSingle := func(f string) bool {
		for _, flag := range singleFlags {
			if flag == f {
				return true
			}
		}
		return false
	}

	for flag, value := range args.SeastarFlags {
		single := isSingle(flag)
		if single && value != "true" {
			// If it's a 'single'-type flag and it's set to false,
			// then there's no need to include it.
			continue
		}
		if single || value == "" {
			redpandaArgs = append(redpandaArgs, "--"+flag)
			continue
		}
		redpandaArgs = append(
			redpandaArgs,
			fmt.Sprintf("--%s=%s", flag, value),
		)
	}
	return append(redpandaArgs, args.ExtraArgs...)
}
