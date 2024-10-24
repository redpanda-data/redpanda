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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type Proc interface {
	RunWithSystemLdPath(timeout time.Duration, command string, args ...string) ([]string, error)
	IsRunning(timeout time.Duration, processName string) bool
}

func NewProc() Proc {
	return &proc{}
}

type proc struct{}

func (*proc) RunWithSystemLdPath(
	timeout time.Duration, command string, args ...string,
) ([]string, error) {
	return run(timeout, command, SystemLdPathEnv(), args...)
}

func (proc *proc) IsRunning(timeout time.Duration, processName string) bool {
	lines, err := proc.RunWithSystemLdPath(timeout, "ps", "--no-headers", "-C", processName)
	if err != nil {
		return false
	}
	return len(lines) > 0
}

func IsRunningPID(fs afero.Fs, pid int) (bool, error) {
	// See http://man7.org/linux/man-pages/man5/proc.5.html
	// section "/proc/[pid]/stat" for info on the info layout and possible
	// process states.
	deadStates := []string{"Z", "X", "x"}
	l, err := utils.ReadEnsureSingleLine(
		fs,
		fmt.Sprintf("/proc/%d/stat", pid),
	)
	if err != nil {
		// If the process info isn't there, it's because it doesn't exist
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	parts := strings.Split(l, " ")
	if len(parts) < 3 {
		return false, fmt.Errorf("corrupt info for process %d", pid)
	}
	state := parts[2]
	for _, s := range deadStates {
		if state == s {
			return false, nil
		}
	}
	return true, nil
}

func SystemLdPathEnv() []string {
	var env []string
	ldLibraryPathPattern := regexp.MustCompile("^LD_LIBRARY_PATH=.*$")
	for _, v := range os.Environ() {
		if !ldLibraryPathPattern.MatchString(v) {
			env = append(env, v)
		}
	}

	return env
}

func run(
	timeout time.Duration, command string, env []string, args ...string,
) ([]string, error) {
	zap.L().Sugar().Debugf("Running command '%s' with arguments '%s'", command, args)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, command, args...)
	var out bytes.Buffer
	var errout bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errout
	cmd.Env = env
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("err=%s, stderr=%s", err, errout.String())
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return strings.Split(out.String(), "\n"), nil
}

// IsRunningSudo checks if you are running the program with sudo by checking the
// caller UID and the env variable SUDO_UID.
func IsRunningSudo() bool {
	sudoID, set := os.LookupEnv("SUDO_UID")
	return os.Getuid() == 0 && set && sudoID != "0"
}
