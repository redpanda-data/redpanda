package commands

import (
	"bufio"
	"fmt"
	"time"
	"vectorized/pkg/os"
)

type executeCommand struct {
	Command
	cmd     string
	args    []string
	proc    os.Proc
	timeout time.Duration
}

func NewLaunchCmd(proc os.Proc, timeout time.Duration, cmd string, args ...string) Command {
	return &executeCommand{
		cmd:     cmd,
		args:    args,
		proc:    proc,
		timeout: timeout,
	}
}

func (c *executeCommand) Execute() error {
	_, err := c.proc.RunWithSystemLdPath(c.timeout, c.cmd, c.args...)
	return err
}

func (c *executeCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "%s \\\n", c.cmd)
	for _, arg := range c.args {
		fmt.Fprintf(w, " %s \\\n", arg)
	}
	return w.Flush()
}
