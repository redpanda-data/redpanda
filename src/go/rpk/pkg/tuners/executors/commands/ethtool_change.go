// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build !windows

package commands

import (
	"bufio"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"go.uber.org/zap"
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
	zap.L().Sugar().Debugf("Chaninging interface '%s', features '%v'", c.intf, c.config)
	return c.ethtool.Change(c.intf, c.config)
}

func (c *ethtoolChangeCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "ethtool -K %s ", c.intf)
	for feature, state := range c.config {
		stateString := "on"
		if !state {
			stateString = "off"
		}
		fmt.Fprintf(w, "%s %s", feature, stateString)
	}
	fmt.Fprintln(w)
	return w.Flush()
}
