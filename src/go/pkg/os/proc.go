package os

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Proc interface {
	RunWithSystemLdPath(command string, args ...string) ([]string, error)
	IsRunning(processName string) bool
}

func NewProc() Proc {
	return &proc{}
}

type proc struct {
	Proc
}

func (*proc) RunWithSystemLdPath(
	command string, args ...string,
) ([]string, error) {

	var env []string
	ldLibraryPathPattern := regexp.MustCompile("^LD_LIBRARY_PATH=.*$")
	for _, v := range os.Environ() {
		if !ldLibraryPathPattern.MatchString(v) {
			env = append(env, v)
		}
	}
	return run(command, env, args...)
}

func (proc *proc) IsRunning(processName string) bool {
	lines, err := proc.RunWithSystemLdPath("ps", "--no-headers", "-C", processName)
	if err != nil {
		return false
	}
	return len(lines) > 0
}

func run(command string, env []string, args ...string) ([]string, error) {
	log.Debugf("Running command '%s' with arguments '%s'", command, args)
	cmd := exec.Command(command, args...)
	var out bytes.Buffer
	var errout bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errout
	cmd.Env = env
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("err=%s, stderr=%s", err, errout.String())
	}
	return strings.Split(out.String(), "\n"), nil
}
