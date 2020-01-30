package commands

import (
	"bufio"
	"fmt"

	"github.com/lorenzosaino/go-sysctl"
	log "github.com/sirupsen/logrus"
)

type sysctlSetCommand struct {
	Command
	key   string
	value string
}

func NewSysctlSetCmd(key string, value string) Command {
	return &sysctlSetCommand{
		key:   key,
		value: value,
	}
}

func (c *sysctlSetCommand) Execute() error {
	log.Debugf("Setting key '%s' to '%s' with systemctl", c.key, c.value)
	return sysctl.Set(c.key, c.value)
}

func (c *sysctlSetCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "sysctl -w %s=%s\n", c.key, c.value)
	return w.Flush()
}
