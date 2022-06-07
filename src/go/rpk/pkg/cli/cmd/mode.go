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

package cmd

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewModeCommand(fs afero.Fs) *cobra.Command {
	return common.Deprecated(
		redpanda.NewModeCommand(fs),
		"rpk redpanda mode",
	)
}
