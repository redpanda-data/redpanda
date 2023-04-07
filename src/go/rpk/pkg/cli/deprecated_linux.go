// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package cli

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda/tune"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rp "github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func Deprecated(newCmd *cobra.Command, newUse string) *cobra.Command {
	newCmd.Deprecated = fmt.Sprintf("use %q instead", newUse)
	newCmd.Hidden = true

	if children := newCmd.Commands(); len(children) > 0 {
		for _, child := range children {
			Deprecated(child, newUse+" "+child.Name())
		}
	}
	return newCmd
}

func NewCheckCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return Deprecated(
		redpanda.NewCheckCommand(fs, p),
		"rpk redpanda check",
	)
}

func NewConfigCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return Deprecated(
		redpanda.NewConfigCommand(fs, p),
		"rpk redpanda config",
	)
}

func NewModeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return Deprecated(
		redpanda.NewModeCommand(fs, p),
		"rpk redpanda mode",
	)
}

func NewStartCommand(fs afero.Fs, p *config.Params, launcher rp.Launcher) *cobra.Command {
	return Deprecated(
		redpanda.NewStartCommand(fs, p, launcher),
		"rpk redpanda start",
	)
}

func NewStatusCommand() *cobra.Command {
	return Deprecated(
		debug.NewInfoCommand(),
		"rpk debug info",
	)
}

func NewStopCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return Deprecated(
		redpanda.NewStopCommand(fs, p),
		"rpk redpanda stop",
	)
}

func NewTuneCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return Deprecated(
		tune.NewCommand(fs, p),
		"rpk redpanda tune",
	)
}
