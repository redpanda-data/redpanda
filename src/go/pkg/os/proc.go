package os

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Proc interface {
	Run(command string, args ...string) ([]string, error)
	IsRunning(processName string) bool
}

func NewProc() Proc {
	return &proc{}
}

type proc struct {
	Proc
}

func (*proc) Run(command string, args ...string) ([]string, error) {
	log.Debugf("Running command '%s' with arguments '%s'", command, args)
	cmd := exec.Command(command, args...)
	var out bytes.Buffer
	var errout bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errout
	err := cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("err=%s, stderr=%s", err, errout.String())
	}
	return strings.Split(out.String(), "\n"), nil
}

func (proc *proc) IsRunning(processName string) bool {
	lines, err := proc.Run("ps", "--no-headers", "-C", processName)
	if err != nil {
		return false
	}
	return len(lines) > 0
}
