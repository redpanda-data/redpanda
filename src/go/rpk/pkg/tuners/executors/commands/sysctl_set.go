// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package commands

import (
	"bufio"
	"fmt"

	"github.com/lorenzosaino/go-sysctl"
	"go.uber.org/zap"
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
	zap.L().Sugar().Debugf("Setting key '%s' to '%s' with systemctl", c.key, c.value)
	return sysctl.Set(c.key, c.value)
}

func (c *sysctlSetCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "sysctl -w %s=%s\n", c.key, c.value)
	return w.Flush()
}
