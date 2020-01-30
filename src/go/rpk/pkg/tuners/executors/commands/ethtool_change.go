package commands

import (
	"bufio"
	"fmt"
	"vectorized/pkg/tuners/ethtool"

	log "github.com/sirupsen/logrus"
)

type ethtoolChangeCommand struct {
	Command
	intf    string
	config  map[string]bool
	ethtool ethtool.EthtoolWrapper
}

func NewEthtoolChangeCmd(
	ethtool ethtool.EthtoolWrapper, intf string, config map[string]bool,
) Command {
	return &ethtoolChangeCommand{
		intf:    intf,
		config:  config,
		ethtool: ethtool,
	}
}

func (c *ethtoolChangeCommand) Execute() error {
	log.Debugf("Chaninging interface '%s', features '%v'", c.intf, c.config)
	return c.ethtool.Change(c.intf, c.config)
}

func (c *ethtoolChangeCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "ethtool -K %s ", c.intf)
	for feature, state := range c.config {
		stateString := "on"
		if state == false {
			stateString = "off"
		}
		fmt.Fprintf(w, "%s %s", feature, stateString)
	}
	fmt.Fprintln(w)
	return w.Flush()
}
