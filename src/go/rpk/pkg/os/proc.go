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

	log "github.com/sirupsen/logrus"
)

type Proc interface {
	RunWithSystemLdPath(timeout time.Duration, command string, args ...string) ([]string, error)
	IsRunning(timeout time.Duration, processName string) bool
}

func NewProc() Proc {
	return &proc{}
}

type proc struct{}

func (proc *proc) RunWithSystemLdPath(
	timeout time.Duration, command string, args ...string,
) ([]string, error) {
	var env []string
	ldLibraryPathPattern := regexp.MustCompile("^LD_LIBRARY_PATH=.*$")
	for _, v := range os.Environ() {
		if !ldLibraryPathPattern.MatchString(v) {
			env = append(env, v)
		}
	}
	return run(timeout, command, env, args...)
}

func (proc *proc) IsRunning(timeout time.Duration, processName string) bool {
	lines, err := proc.RunWithSystemLdPath(timeout, "ps", "--no-headers", "-C", processName)
	if err != nil {
		return false
	}
	return len(lines) > 0
}

func run(
	timeout time.Duration, command string, env []string, args ...string,
) ([]string, error) {
	log.Debugf("Running command '%s' with arguments '%s'", command, args)
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
