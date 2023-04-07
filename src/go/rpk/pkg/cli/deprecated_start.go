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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rp "github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewStartCommand(fs afero.Fs, p *config.Params, launcher rp.Launcher) *cobra.Command {
	return common.Deprecated(
		redpanda.NewStartCommand(fs, p, launcher),
		"rpk redpanda start",
	)
}
