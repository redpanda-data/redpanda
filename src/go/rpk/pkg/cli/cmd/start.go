// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// +build linux

package cmd

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/redpanda"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	rp "github.com/vectorizedio/redpanda/src/go/rpk/pkg/redpanda"
)

func NewStartCommand(
	fs afero.Fs, mgr config.Manager, launcher rp.Launcher,
) *cobra.Command {
	return common.Deprecated(
		redpanda.NewStartCommand(fs, mgr, launcher),
		"rpk redpanda start",
	)
}
