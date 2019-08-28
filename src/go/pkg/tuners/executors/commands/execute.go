package commands

import (
	"bufio"
	"fmt"
	"vectorized/pkg/os"
)

type executeCommand struct {
	Command
	cmd  string
	args []string
	proc os.Proc
}

func NewLaunchCmd(proc os.Proc, cmd string, args ...string) Command {
	return &executeCommand{
		cmd:  cmd,
		args: args,
		proc: proc,
	}
}

func (c *executeCommand) Execute() error {
	_, err := c.proc.RunWithSystemLdPath(c.cmd, c.args...)
	return err
}

func (c *executeCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "%s \\\n", c.cmd)
	for _, arg := range c.args {
		fmt.Fprintf(w, " %s \\\n", arg)
	}
	return w.Flush()
}
